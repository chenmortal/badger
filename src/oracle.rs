use std::collections::HashSet;

struct Oracle {
    is_managed:bool,
    detect_conflicts:bool,
    next_txn_ts:u64,
    discard_ts:u64,
    last_cleanup_ts:u64,
    committed_txns:Vec<CommittedTxn>,
}
struct CommittedTxn{
    ts:u64,
    conflict_keys:HashSet<u64>,
}