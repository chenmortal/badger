use std::{collections::HashSet, sync::Arc};

use tokio::{sync::Mutex, task::JoinHandle};

use crate::options::Options;

use super::water_mark::WaterMark;
pub(crate) struct Oracle {
    inner: Arc<Mutex<OracleInner>>,
    is_managed: bool,
    read_mark: WaterMark,
    txn_mark: WaterMark,
}
struct OracleInner {
    detect_conflicts: bool,
    next_txn_ts: u64,
    discard_ts: u64,
    last_cleanup_ts: u64,
    committed_txns: Vec<CommittedTxn>,
    task_handler: Vec<JoinHandle<()>>,
}

struct CommittedTxn {
    ts: u64,
    conflict_keys: HashSet<u64>,
}
impl Oracle {
    pub(crate) fn new(opt: &Arc<Options>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(OracleInner::new(opt))),
            is_managed: opt.managed_txns,
            read_mark: WaterMark::new("badger.PendingReads"),
            txn_mark: WaterMark::new("badger.TxnTimestamp"),
        }
    }

    #[inline]
    pub(crate) async fn discard_at_or_below(&self) -> u64 {
        if self.is_managed {
            let lock = self.inner.lock().await;
            let ts = lock.discard_ts;
            drop(lock);
            return ts;
        }
        return self.read_mark.get_done_until();
    }
}
impl OracleInner {
    fn new(opt: &Arc<Options>) -> Self {
        Self {
            // is_managed: opt.managed_txns,
            detect_conflicts: opt.detect_conflicts,
            next_txn_ts: 0,
            discard_ts: 0,
            last_cleanup_ts: 0,
            committed_txns: Vec::new(),
            task_handler: Vec::new(),
        }
    }
}
