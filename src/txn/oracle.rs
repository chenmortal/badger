use std::{
    collections::{HashSet, VecDeque},
    ops::Deref,
    sync::Arc,
};

use anyhow::{bail, Ok};
use parking_lot::{Mutex, MutexGuard};
use tokio::sync::RwLock;

use crate::{
    errors::DBError, kv::TxnTs, level::levels::LevelsControllerInner, memtable::MemTable,
    util::closer::Closer,
};

use super::{water_mark::WaterMark, Txn, TxnConfig};
#[derive(Debug, Clone)]
pub(crate) struct Oracle(Arc<OracleInner>);
impl Deref for Oracle {
    type Target = OracleInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
pub(crate) struct OracleInner {
    inner: Mutex<OracleCore>,
    closer: Closer,
    read_mark: WaterMark,
    txn_mark: WaterMark,
    config: TxnConfig,
    pub(super) send_write_req: Mutex<()>,
}
impl Deref for OracleInner {
    type Target = Mutex<OracleCore>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl Default for OracleInner {
    fn default() -> Self {
        OracleInner::new(TxnConfig::default(), 0.into())
    }
}
#[derive(Debug, Default)]
pub(crate) struct OracleCore {
    next_txn_ts: TxnTs, //when open db, next_txn_ts will be set to max_version;
    discard_ts: TxnTs,
    last_cleanup_ts: TxnTs,
    committed_txns: Vec<CommittedTxn>,
}
#[derive(Debug, Clone)]
struct CommittedTxn {
    ts: TxnTs,
    conflict_keys: HashSet<u64>,
}
impl Oracle {
    pub(crate) fn new(config: TxnConfig, max_version: TxnTs) -> Self {
        Self(OracleInner::new(config, max_version).into())
    }
}
impl OracleInner {
    fn new(config: TxnConfig, max_version: TxnTs) -> Self {
        let closer = Closer::new(2);
        let mut inner = OracleCore::default();
        inner.next_txn_ts = max_version + 1;
        Self {
            inner: Mutex::new(inner),
            read_mark: WaterMark::new("badger.PendingReads", closer.clone(), max_version),
            txn_mark: WaterMark::new("badger.TxnTimestamp", closer.clone(), max_version),
            send_write_req: Mutex::new(()),
            closer,
            config,
        }
    }

    #[inline]
    pub(crate)  fn discard_at_or_below(&self) -> TxnTs {
        if self.config.managed_txns {
            let lock = self.inner.lock();
            let ts = lock.discard_ts;
            drop(lock);
            return ts.into();
        }
        return self.read_mark.done_until();
    }

    #[inline]
    pub(crate) async fn get_latest_read_ts(&self) -> anyhow::Result<TxnTs> {
        if self.config.managed_txns {
            panic!("ReadTimestamp should not be retrieved for managed DB");
        }
        let inner_lock = self.inner.lock();
        let read_ts = inner_lock.next_txn_ts - 1;
        self.read_mark.begin(read_ts).await?;
        drop(inner_lock);
        self.txn_mark.wait_for_mark(read_ts).await?;

        Ok(read_ts)
    }
    #[inline]
    pub(crate) async fn get_latest_commit_ts(&self, txn: &Txn) -> anyhow::Result<TxnTs> {
        let mut inner_lock = self.inner.lock();

        //check read-write conflict
        let read_key_hash_r = txn.read_key_hash().lock();
        if read_key_hash_r.len() != 0 {
            for commit_txn in inner_lock.committed_txns.iter() {
                if commit_txn.ts > txn.read_ts {
                    for hash in read_key_hash_r.iter() {
                        if commit_txn.conflict_keys.contains(hash) {
                            drop(read_key_hash_r);
                            drop(inner_lock);
                            bail!(DBError::Conflict)
                        };
                    }
                }
            }
        }
        drop(read_key_hash_r);

        let commit_ts = if !self.config.managed_txns {
            self.done_read(txn).await?;
            self.cleanup_committed_txns(&mut inner_lock);

            let txn_ts = inner_lock.next_txn_ts;
            inner_lock.next_txn_ts += 1;
            self.txn_mark.begin(txn_ts).await?;
            txn_ts
        } else {
            txn.commit_ts
        };

        debug_assert!(commit_ts >= inner_lock.last_cleanup_ts);

        if self.config.managed_txns {
            inner_lock.committed_txns.push(CommittedTxn {
                ts: commit_ts,
                conflict_keys: txn.conflict_keys().unwrap().clone(),
            });
        }
        drop(inner_lock);
        Ok(commit_ts)
    }

    fn cleanup_committed_txns(&self, guard: &mut MutexGuard<OracleCore>) {
        if !self.config.detect_conflicts {
            return;
        }
        let max_read_tx = if self.config.managed_txns {
            guard.discard_ts
        } else {
            self.read_mark.done_until()
        };
        debug_assert!(max_read_tx >= guard.last_cleanup_ts);
        if max_read_tx == guard.last_cleanup_ts {
            return;
        }

        guard.last_cleanup_ts = max_read_tx;
        guard.committed_txns = guard
            .committed_txns
            .iter()
            .filter(|txn| txn.ts > max_read_tx)
            .cloned()
            .collect();
    }

    pub(crate) fn config(&self) -> TxnConfig {
        self.config
    }

    pub(crate) fn txn_mark(&self) -> &WaterMark {
        &self.txn_mark
    }

    pub(crate) fn read_mark(&self) -> &WaterMark {
        &self.read_mark
    }
}
pub(crate) async fn max_txn_ts(
    immut_memtable: &RwLock<VecDeque<Arc<MemTable>>>,
    controller: &LevelsControllerInner,
) -> anyhow::Result<TxnTs> {
    let mut max_version = TxnTs::default();
    let mem_r = immut_memtable.read().await;
    for mem in mem_r.iter() {
        max_version = max_version.max(mem.max_version);
    }
    drop(mem_r);
    for info in controller.get_table_info().await? {
        max_version = max_version.max(info.max_version());
    }
    Ok(max_version)
}
