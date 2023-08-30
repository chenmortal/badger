use std::collections::HashSet;

use tokio::task::JoinHandle;

use crate::options::Options;

use super::water_mark::WaterMark;

pub(crate) struct Oracle {
    is_managed: bool,
    detect_conflicts: bool,
    next_txn_ts: u64,
    discard_ts: u64,
    last_cleanup_ts: u64,
    committed_txns: Vec<CommittedTxn>,
    task_handler: Vec<JoinHandle<()>>,
    read_mark: WaterMark,
    txn_mark: WaterMark,
}

struct CommittedTxn {
    ts: u64,
    conflict_keys: HashSet<u64>,
}
impl Oracle {
    pub(crate) fn new(opt: Options) -> Self {
        Self {
            is_managed: opt.managed_txns,
            detect_conflicts: opt.detect_conflicts,
            next_txn_ts: 0,
            discard_ts: 0,
            last_cleanup_ts: 0,
            committed_txns: Vec::new(),
            task_handler: Vec::new(),
            read_mark: WaterMark::new("badger.PendingReads"),
            txn_mark: WaterMark::new("badger.TxnTimestamp"),
        }
    }
}
