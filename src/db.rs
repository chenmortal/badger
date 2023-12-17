use std::{
    collections::{HashSet, VecDeque},
    ops::Deref,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    config::Config,
    default::KV_WRITES_ENTRIES_CHANNEL_CAPACITY,
    errors::DBError,
    key_registry::KeyRegistry,
    level::levels::LevelsController,
    memtable::MemTable,
    txn::oracle::{max_txn_ts, Oracle},
    util::closer::Closer,
    util::metrics::calculate_size,
    util::{
        cache::{BlockCache, IndexCache},
        lock::DBLockGuard,
        publisher::Publisher,
        rayon::init_global_rayon_pool,
    },
    vlog::{threshold::VlogThreshold, ValueLog},
    write::WriteReq,
};
use bytes::Buf;
use tokio::sync::{mpsc::Sender, RwLock};
use tokio::sync::{
    mpsc::{self, Receiver},
    Mutex,
};

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
    pub(crate) key_registry: KeyRegistry,
    pub(crate) memtable: Option<Arc<RwLock<MemTable>>>,
    pub(crate) immut_memtable: RwLock<VecDeque<Arc<MemTable>>>,
    pub(crate) block_cache: Option<BlockCache>,
    pub(crate) index_cache: IndexCache,
    pub(crate) level_controller: LevelsController,
    pub(crate) oracle: Oracle,
    pub(crate) send_write_req: Sender<WriteReq>,
    pub(crate) flush_memtable: Sender<Arc<MemTable>>,
    pub(crate) recv_memtable: Mutex<Receiver<Arc<MemTable>>>,
    pub(crate) vlog: ValueLog,
    banned_namespaces: RwLock<HashSet<u64>>,
    pub(crate) publisher: Publisher,
    is_closed: AtomicBool,
    pub(crate) block_writes: AtomicBool,
    pub(crate) opt: Config,
    pub(crate) lock_guard: Option<DBLockGuard>,
}
impl DB {
    pub async fn open(mut opt: Config) -> anyhow::Result<DB> {
        opt.init()?;

        let lock_guard = opt.lock_guard.try_build()?;

        init_global_rayon_pool()?;
        let manifest = opt.manifest.open()?;
        let block_cache = opt.block_cache.try_build()?;
        let index_cache = opt.index_cache.build()?;

        let key_registry = opt.key_registry.open().await?;

        calculate_size(opt.level_controller.dir(), &opt.vlog.value_dir()).await;
        // let mut update_size_closer = Closer::new();
        // let update_size_handle = tokio::spawn(update_size(update_size_closer.sem_clone()));

        let immut_memtable = opt.memtable.open_many(&key_registry).await?.into();
        let mut memtable = None;
        if !opt.read_only() {
            memtable = Arc::new(RwLock::new(opt.memtable.new(&key_registry).await?)).into();
        }

        let level_controller = opt
            .level_controller
            .build(
                opt.table.clone(),
                manifest.clone(),
                key_registry.clone(),
                &block_cache,
                &index_cache,
            )
            .await?;

        let max_version = max_txn_ts(&immut_memtable, &level_controller).await?;
        let oracle = Oracle::new(opt.txn, max_version);

        let threshold = VlogThreshold::new(opt.vlog_threshold);
        let mut closer = Closer::new(1);
        level_controller
            .clone()
            .spawn_compact(
                &mut closer,
                opt.table.clone(),
                key_registry.clone(),
                index_cache.clone(),
                block_cache.clone(),
                oracle.clone(),
            )
            .await;
        let mut vlog = ValueLog::new(threshold, key_registry.clone(), opt.vlog.clone())?;
        vlog.open().await?;
        let closer = Closer::new(1);
        let publisher = Publisher::new(closer.clone());
        let (send_write_req, recv_write_req) = mpsc::channel(KV_WRITES_ENTRIES_CHANNEL_CAPACITY);
        let (flush_memtable, recv_memtable) = mpsc::channel(opt.num_memtables());
        let db: DB = DB(Arc::new(DBInner {
            key_registry,
            memtable,
            immut_memtable,
            block_cache,
            index_cache,
            level_controller,
            oracle,
            send_write_req,
            flush_memtable,
            vlog,
            banned_namespaces: Default::default(),
            publisher,
            is_closed: AtomicBool::new(false),
            block_writes: AtomicBool::new(false),
            recv_memtable: recv_memtable.into(),
            opt,
            lock_guard,
        }));
        let closer = Closer::new(1);
        tokio::spawn(db.clone().do_writes(recv_write_req, closer));
        let flush_memtable = Closer::new(1);
        let _p = tokio::spawn(db.clone().flush_memtable(flush_memtable.clone()));

        // drop(value_dir_lock_guard);
        // drop(dir_lock_guard);

        Ok(db)
    }
}
impl DBInner {
    pub(crate) fn update_size() {}
    pub(crate) fn is_closed(&self) -> bool {
        self.is_closed.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub(crate) async fn is_banned(&self, key: &[u8]) -> Result<(), DBError> {
        match self.opt.name_space_offset() {
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
}
#[cfg(test)]
mod tests {
    use crate::{
        config::Config,
        db::DB,
        txn::{Txn, TxnUpdate},
    };

    struct TxnTestUp;
    impl TxnUpdate for TxnTestUp {
        async fn update(self, txn: &mut Txn) -> anyhow::Result<()> {
            txn.set("a", "1").await?;
            println!("a");
            Ok(())
        }
    }
    #[tokio::test]
    async fn test_write() -> anyhow::Result<()> {
        let config = Config::default();
        let db = DB::open(config).await?;
        db.update(TxnTestUp).await?;
        Ok(())
    }
}
