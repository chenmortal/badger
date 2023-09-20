use std::{
    collections::HashSet,
    fs::{create_dir_all, metadata, read_dir, set_permissions, Permissions},
    ops::Deref,
    os::unix::prelude::PermissionsExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU32},
        Arc,
    },
};

use crate::{
    default::{LOCK_FILE, MAX_VALUE_THRESHOLD},
    errors::DBError,
    fb::fb::TableIndex,
    key_registry::KeyRegistry,
    kv::{KeyTs, ValueStruct},
    lock::DirLockGuard,
    lsm::{
        levels::LevelsController,
        memtable::{self, new_mem_table, MemTable},
    },
    manifest::open_create_manifestfile,
    metrics::{calculate_size, set_lsm_size, set_metrics_enabled, set_vlog_size, update_size},
    options::Options,
    skl::skip_list::SKL_MAX_NODE_SIZE,
    table::block::{self, Block},
    txn::{entry::DecEntry, oracle::Oracle},
    util::Closer,
    value::threshold::VlogThreshold,
};
use anyhow::anyhow;
use anyhow::bail;
use bytes::Buf;
use log::debug;
use stretto::AsyncCache;
use tokio::sync::RwLock;
use tokio::{sync::mpsc, task::JoinHandle};
struct JoinHandles {
    update_size: JoinHandle<()>,
}
pub(crate) type BlockCache = AsyncCache<Vec<u8>, Block>;
pub(crate) type IndexCache = AsyncCache<u64, Vec<u8>>;
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
    lock: RwLock<()>,
    pub(crate) opt: Arc<Options>,
    pub(crate) next_mem_fid: AtomicU32,
    pub(crate) key_registry: KeyRegistry,
    memtable: Option<MemTable>,
    pub(crate) block_cache: Option<BlockCache>,
    pub(crate) index_cache: Option<IndexCache>,
    pub(crate) level_controller: Option<LevelsController>,
    pub(crate) oracle: Arc<Oracle>,
    banned_namespaces: RwLock<HashSet<u64>>,
    is_closed: AtomicBool,
}
impl DBInner {
    pub async fn open(opt: &mut Options) -> anyhow::Result<()> {
        opt.check_set_options()?;
        let mut dir_lock_guard = None;
        let mut value_dir_lock_guard = None;
        // if !opt.in_memory {
        opt.create_dirs()?;
        if !opt.bypass_lock_guard {
            dir_lock_guard = DirLockGuard::acquire_lock(&opt.dir, LOCK_FILE, opt.read_only)?.into();
            if opt.value_dir.canonicalize()? != opt.dir.canonicalize()? {
                value_dir_lock_guard =
                    DirLockGuard::acquire_lock(&opt.value_dir, LOCK_FILE, opt.read_only)?.into();
            };
        }
        // }
        let manifest_file = open_create_manifestfile(&opt)?;
        let imm = Vec::<MemTable>::with_capacity(opt.num_memtables);
        let (sender, receiver) = mpsc::channel::<MemTable>(opt.num_memtables);
        let threshold = VlogThreshold::new(&opt);

        // let mut db = DB::default();
        let mut block_cache = None;
        if opt.block_cache_size > 0 {
            let mut num_in_cache = opt.block_cache_size / opt.block_size;
            if num_in_cache == 0 {
                num_in_cache = 1;
            }
            block_cache = stretto::AsyncCacheBuilder::<Vec<u8>, block::Block>::new(
                num_in_cache * 8,
                opt.block_cache_size as i64,
            )
            .set_buffer_items(64)
            .set_metrics(true)
            .finalize(tokio::spawn)?
            .into();
        }
        let mut index_cache = None;
        if opt.index_cache_size > 0 {
            let index_sz = (opt.memtable_size as f64 * 0.05) as usize;
            let mut num_in_cache = opt.index_cache_size as usize / index_sz;
            if num_in_cache == 0 {
                num_in_cache = 1;
            }
            index_cache = stretto::AsyncCacheBuilder::<u64, Vec<u8>>::new(
                num_in_cache * 8,
                opt.index_cache_size,
            )
            .set_buffer_items(64)
            .set_metrics(true)
            .finalize(tokio::spawn)?
            .into();
            // db.index_cache = index_cache.into();
        }

        let key_registry = KeyRegistry::open(opt).await?;

        let db_opt = Arc::new(opt.clone());
        set_metrics_enabled(db_opt.metrics_enabled);

        calculate_size(&db_opt).await;
        let mut update_size_closer = Closer::new();
        let update_size_handle =
            tokio::spawn(update_size(db_opt.clone(), update_size_closer.sem_clone()));

        let next_mem_fid = NextId::new();
        let mut memtable = None;
        if !db_opt.read_only {
            memtable = new_mem_table(&db_opt, &key_registry, &next_mem_fid)
                .await
                .map_err(|e| anyhow!("Cannot create memtable {}", e))?
                .into();
        }

        let levels_controller = LevelsController::new(
            db_opt,
            &manifest_file.manifest,
            key_registry.clone(),
            &block_cache,
            &index_cache,
        )
        .await?;

        drop(value_dir_lock_guard);
        drop(dir_lock_guard);
        Ok(())
    }

    pub(crate) fn update_size() {}
    pub(crate) fn is_closed(&self) -> bool {
        self.is_closed.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub(crate) async fn is_banned(&self, key: &[u8]) -> Result<(), DBError> {
        match self.opt.name_space_offset {
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
    pub(crate) async fn send_to_write_channel(&self, entries: Vec<DecEntry>) {
        
    }
}
impl Options {
    pub(crate) fn check_set_options(&mut self) -> anyhow::Result<()> {
        if self.num_compactors == 1 {
            bail!("Cannot have 1 compactor. Need at least 2");
        }
        // if self.in_memory && (self.dir != PathBuf::from("") || self.value_dir != PathBuf::from(""))
        // {
        //     bail!("Cannot use badger in Disk-less mode with Dir or ValueDir set");
        // }

        log::set_max_level(self.log_level);
        self.max_batch_size = (15 * self.memtable_size) / 100;
        self.max_batch_count = self.max_batch_size / (SKL_MAX_NODE_SIZE);
        self.max_value_threshold = MAX_VALUE_THRESHOLD.min(self.max_batch_size) as f64;
        if self.vlog_percentile < 0.0 || self.vlog_percentile > 1.0 {
            bail!("vlog_percentile must be within range of 0.0-1.0")
        }
        if self.value_threshold > MAX_VALUE_THRESHOLD {
            bail!(
                "Invalid ValueThreshold, must be less or equal to {}",
                MAX_VALUE_THRESHOLD
            );
        }
        if self.value_threshold > self.max_batch_size {
            bail!("Valuethreshold {} greater than max batch size of {}. Either reduce Valuethreshold or increase max_table_size",self.value_threshold,self.max_batch_size);
        }
        if !(self.valuelog_file_size >= 1 << 20 && self.valuelog_file_size < 2 << 30) {
            bail!(DBError::ValuelogSize);
        }
        if self.read_only {
            self.compactl0_on_close = false;
        }
        match self.compression {
            _ => {}
        }
        let need_cache = match self.compression {
            crate::options::CompressionType::None => true,
            _ => false,
        };
        if need_cache && self.block_cache_size == 0 {
            panic!("Block_Cache_Size should be set since compression are enabled")
        }
        Ok(())
    }
    fn create_dirs(&self) -> anyhow::Result<()> {
        for path in [&self.dir, &self.value_dir] {
            if !path
                .try_exists()
                .map_err(|e| anyhow!("Invalid Dir : {}", e))?
            {
                if self.read_only {
                    bail!("Cannot find directory {:?} for read-only open", path)
                }
                create_dir_all(path)
                    .map_err(|e| anyhow!("Error Creating Dir: {:?} : {}", path, e))?;
                set_permissions(path, Permissions::from_mode(0o700))
                    .map_err(|e| anyhow!("Error Set Permissions 0o700: {:?} : {}", path, e))?;
            };
        }
        Ok(())
    }
}
