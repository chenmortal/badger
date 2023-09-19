use std::{collections::HashSet, ops::Deref, sync::Arc};

use tokio::{sync::Mutex, task::JoinHandle};

use crate::options::Options;

use super::{water_mark::WaterMark, TxnTs};
#[derive(Debug)]
pub(crate) struct Oracle {
    inner: Mutex<OracleInner>,
    is_managed: bool,
    read_mark: WaterMark,
    txn_mark: WaterMark,
}
impl Deref for Oracle {
    type Target = Mutex<OracleInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
#[derive(Debug, Default)]
pub(crate) struct OracleInner {
    detect_conflicts: bool,
    next_txn_ts: TxnTs, //when open db, next_txn_ts will be set to max_version;
    discard_ts: u64,
    last_cleanup_ts: u64,
    committed_txns: Vec<CommittedTxn>,
    task_handler: Vec<JoinHandle<()>>,
}
#[derive(Debug)]
struct CommittedTxn {
    ts: u64,
    conflict_keys: HashSet<u64>,
}
impl Oracle {
    pub(crate) fn new(opt: &Arc<Options>) -> Self {
        Self {
            inner: Mutex::new(OracleInner::new(opt)),
            is_managed: opt.managed_txns,
            read_mark: WaterMark::new("badger.PendingReads"),
            txn_mark: WaterMark::new("badger.TxnTimestamp"),
        }
    }

    #[inline]
    pub(crate) async fn discard_at_or_below(&self) -> TxnTs {
        if self.is_managed {
            let lock = self.inner.lock().await;
            let ts = lock.discard_ts;
            drop(lock);
            return ts.into();
        }
        return self.read_mark.done_until();
    }
    #[inline]
    pub(crate) async fn get_latest_read_ts(&self) -> anyhow::Result<TxnTs> {
        if self.is_managed {
            panic!("ReadTimestamp should not be retrieved for managed DB");
        }

        let inner_m = self.inner.lock().await;
        let read_ts = inner_m.next_txn_ts.sub_one();
        self.read_mark.begin(read_ts).await?;
        drop(inner_m);
        self.txn_mark.wait_for_mark(read_ts).await?;

        Ok(read_ts)
    }
}
impl OracleInner {
    fn new(opt: &Arc<Options>) -> Self {
        Self {
            // is_managed: opt.managed_txns,
            detect_conflicts: opt.detect_conflicts,
            next_txn_ts: TxnTs(0),
            discard_ts: 0,
            last_cleanup_ts: 0,
            committed_txns: Vec::new(),
            task_handler: Vec::new(),
        }
    }
}
