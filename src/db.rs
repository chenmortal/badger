use std::{
    collections::{HashSet, VecDeque},
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU32},
        Arc,
    },
};

use crate::{
    closer::Closer,
    default::{KV_WRITES_ENTRIES_CHANNEL_CAPACITY, LOCK_FILE},
    errors::DBError,
    key_registry::KeyRegistry,
    kv::{KeyTs, ValueStruct},
    lock::DirLockGuard,
    lsm::{
        levels::LevelsController,
        memtable::{new_mem_table, MemTable},
    },
    manifest::open_create_manifestfile,
    metrics::{calculate_size, set_metrics_enabled},
    options::Options,
    publisher::Publisher,
    rayon::set_global_rayon_pool,
    table::block::{self, Block},
    txn::oracle::Oracle,
    vlog::{threshold::VlogThreshold, ValueLog},
    write::WriteReq,
};
use anyhow::anyhow;
use bytes::Buf;
use stretto::AsyncCache;
use tokio::sync::{mpsc::Sender, RwLock};
use tokio::sync::{
    mpsc::{self, Receiver},
    Mutex,
};

pub(crate) type BlockCache = AsyncCache<Vec<u8>, Block>;
pub(crate) type IndexCache = AsyncCache<u64, Vec<u8>>;
#[derive(Debug)]
pub(crate) struct NextId(AtomicU32);
impl NextId {
    #[inline]
    pub(crate) fn new() -> Self {
        Self(AtomicU32::new(0))
    }
    #[inline]
    pub(crate) fn get_next_id(&self) -> u32 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
    #[inline]
    pub(crate) fn add_next_id(&self) -> u32 {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    #[inline]
    pub(crate) fn store(&self, val: u32) {
        self.0.store(val, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Debug, Clone)]
pub struct DB(Arc<DBInner>);
impl Deref for DB {
    type Target = DBInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
pub struct DBInner {
    pub(crate) next_mem_fid: NextId,
    pub(crate) key_registry: KeyRegistry,
    pub(crate) memtable: Option<Arc<RwLock<MemTable>>>,
    pub(crate) immut_memtable: RwLock<VecDeque<Arc<MemTable>>>,
    pub(crate) block_cache: Option<BlockCache>,
    pub(crate) index_cache: Option<IndexCache>,
    pub(crate) level_controller: LevelsController,
    pub(crate) oracle: Arc<Oracle>,
    pub(crate) send_write_req: Sender<WriteReq>,
    pub(crate) flush_memtable: Sender<Arc<MemTable>>,
    pub(crate) recv_memtable: Mutex<Receiver<Arc<MemTable>>>,
    pub(crate) vlog: ValueLog,
    banned_namespaces: RwLock<HashSet<u64>>,
    pub(crate) publisher: Publisher,
    is_closed: AtomicBool,
    pub(crate) block_writes: AtomicBool,
}
impl DBInner {
    pub async fn open(opt: Options) -> anyhow::Result<DB> {
        Options::init(opt)?;
        let mut dir_lock_guard = None;
        let mut value_dir_lock_guard = None;

        if !Options::bypass_lock_guard() {
            dir_lock_guard =
                DirLockGuard::acquire_lock(Options::dir(), LOCK_FILE, Options::read_only())?.into();
            if Options::value_dir().canonicalize()? != Options::dir().canonicalize()? {
                value_dir_lock_guard = DirLockGuard::acquire_lock(
                    &Options::value_dir(),
                    LOCK_FILE,
                    Options::read_only(),
                )?
                .into();
            };
        }
        // }
        set_global_rayon_pool()?;
        let manifest_file = open_create_manifestfile()?;

        let mut block_cache = None;
        if Options::block_cache_size() > 0 {
            let mut num_in_cache = Options::block_cache_size() / Options::block_size();
            if num_in_cache == 0 {
                num_in_cache = 1;
            }
            block_cache = stretto::AsyncCacheBuilder::<Vec<u8>, block::Block>::new(
                num_in_cache * 8,
                Options::block_cache_size() as i64,
            )
            .set_buffer_items(64)
            .set_metrics(true)
            .finalize(tokio::spawn)?
            .into();
        }
        let mut index_cache = None;
        if Options::index_cache_size() > 0 {
            let index_sz = (Options::memtable_size() as f64 * 0.05) as usize;
            let mut num_in_cache = Options::index_cache_size() as usize / index_sz;
            if num_in_cache == 0 {
                num_in_cache = 1;
            }
            index_cache = stretto::AsyncCacheBuilder::<u64, Vec<u8>>::new(
                num_in_cache * 8,
                Options::index_cache_size(),
            )
            .set_buffer_items(64)
            .set_metrics(true)
            .finalize(tokio::spawn)?
            .into();
        }

        let key_registry = KeyRegistry::open().await?;

        set_metrics_enabled(Options::metrics_enabled());

        calculate_size().await;
        // let mut update_size_closer = Closer::new();
        // let update_size_handle = tokio::spawn(update_size(update_size_closer.sem_clone()));

        let next_mem_fid = NextId::new();
        let mut memtable = None;
        if !Options::read_only() {
            memtable = Arc::new(RwLock::new(
                new_mem_table(&key_registry, &next_mem_fid)
                    .await
                    .map_err(|e| anyhow!("Cannot create memtable {}", e))?,
            ))
            .into();
        }

        let level_controller = LevelsController::new(
            &manifest_file.manifest,
            key_registry.clone(),
            &block_cache,
            &index_cache,
        )
        .await?;
        let threshold = VlogThreshold::new();

        let vlog = ValueLog::new(threshold, key_registry.clone())?;
        let closer = Closer::new(1);
        let publisher = Publisher::new(closer.clone());
        let (send_write_req, receiver) = mpsc::channel(KV_WRITES_ENTRIES_CHANNEL_CAPACITY);
        let (flush_memtable, recv_memtable) = mpsc::channel(Options::num_memtables());
        // recv_memtable.recv();
        let db: DB = DB(Arc::new(Self {
            next_mem_fid,
            key_registry,
            memtable,
            immut_memtable: VecDeque::with_capacity(Options::num_memtables()).into(),
            block_cache,
            index_cache,
            level_controller,
            oracle: Default::default(),
            send_write_req,
            flush_memtable,
            vlog,
            banned_namespaces: Default::default(),
            publisher,
            is_closed: AtomicBool::new(false),
            block_writes: AtomicBool::new(false),
            recv_memtable: recv_memtable.into(),
        }));
        let flush_memtable = Closer::new(1);
        let _p = tokio::spawn(db.clone().flush_memtable(flush_memtable.clone()));
        drop(value_dir_lock_guard);
        drop(dir_lock_guard);
        
        Ok(db)
    }

    pub(crate) fn update_size() {}
    pub(crate) fn is_closed(&self) -> bool {
        self.is_closed.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub(crate) async fn is_banned(&self, key: &[u8]) -> Result<(), DBError> {
        match Options::name_space_offset() {
            Some(offset) => {
                if key.len() <= offset + 8 {
                    return Ok(());
                }
                let mut p = &key[offset..offset + 8];
                let name_space = p.get_u64();
                let banned_r = self.banned_namespaces.read().await;
                let r = banned_r.contains(&name_space);
                drop(banned_r);
                if r {
                    Err(DBError::BannedKey)
                } else {
                    Ok(())
                }
            }
            None => Ok(()),
        }
    }
    pub(crate) async fn get_value(&self, key_ts: &KeyTs) -> anyhow::Result<ValueStruct> {
        // todo!();
        let v = ValueStruct::default();
        Ok(v)
    }
}
