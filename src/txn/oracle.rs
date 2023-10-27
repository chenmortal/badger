use std::{collections::HashSet, ops::Deref};

use anyhow::{bail, Ok};
use parking_lot::{Mutex, MutexGuard};

use crate::{errors::DBError, options::Options, util::closer::Closer, kv::TxnTs};

use super::{
    txn::Txn,
    water_mark::{Mark, WaterMark},
};
#[derive(Debug)]
pub(crate) struct Oracle {
    inner: Mutex<OracleInner>,
    closer: Closer,
    read_mark: WaterMark,
    txn_mark: WaterMark,
    pub(super) send_write_req: Mutex<()>,
}
impl Deref for Oracle {
    type Target = Mutex<OracleInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl Default for Oracle {
    fn default() -> Self {
        Oracle::new()
    }
}
#[derive(Debug, Default)]
pub(crate) struct OracleInner {
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
    pub(crate) fn new() -> Self {
        let closer = Closer::new(2);
        Self {
            inner: Mutex::new(OracleInner::default()),
            read_mark: WaterMark::new("badger.PendingReads", closer.clone()),
            txn_mark: WaterMark::new("badger.TxnTimestamp", closer.clone()),
            send_write_req: Mutex::new(()),
            closer,
        }
    }

    #[inline]
    pub(crate) async fn discard_at_or_below(&self) -> TxnTs {
        if Options::managed_txns() {
            let lock = self.inner.lock();
            let ts = lock.discard_ts;
            drop(lock);
            return ts.into();
        }
        return self.read_mark.done_until();
    }
    #[inline]
    pub(crate) async fn done_commit(&self, commit_ts: TxnTs) -> anyhow::Result<()> {
        if !Options::managed_txns() {
            self.txn_mark
                .sender
                .send(Mark::new(commit_ts, true))
                .await?;
        }
        Ok(())
    }
    #[inline]
    pub(crate) async fn get_latest_read_ts(&self) -> anyhow::Result<TxnTs> {
        if Options::managed_txns() {
            panic!("ReadTimestamp should not be retrieved for managed DB");
        }
        let inner_lock = self.inner.lock();
        let read_ts = inner_lock.next_txn_ts.sub_one();
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

        let commit_ts = if !Options::managed_txns() {
            self.done_read(txn).await?;
            self.cleanup_committed_txns(&mut inner_lock);

            let txn_ts = inner_lock.next_txn_ts;
            inner_lock.next_txn_ts.add_one_mut();
            self.txn_mark.begin(txn_ts).await?;
            txn_ts
        } else {
            txn.commit_ts
        };

        debug_assert!(commit_ts >= inner_lock.last_cleanup_ts);

        if Options::detect_conflicts() {
            inner_lock.committed_txns.push(CommittedTxn {
                ts: commit_ts,
                conflict_keys: txn.conflict_keys().unwrap().clone(),
            });
        }
        drop(inner_lock);
        Ok(commit_ts)
    }
    pub(super) async fn done_read(&self, txn: &Txn) -> anyhow::Result<()> {
        if !txn
            .done_read()
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.read_mark
                .sender
                .send(Mark::new(txn.read_ts, true))
                .await?;
        };
        Ok(())
    }
    fn cleanup_committed_txns(&self, guard: &mut MutexGuard<OracleInner>) {
        if !Options::detect_conflicts() {
            return;
        }
        let max_read_tx = if Options::managed_txns() {
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
}
