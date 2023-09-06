use std::{collections::HashSet, sync::Arc};

use tokio::sync::RwLock;

pub(crate) struct CompactStatus{
    lock:RwLock<()>,
    pub(crate) levels:Vec<Arc<LevelCompactStatus>>,
    tables:HashSet<u64>,
}
impl CompactStatus {
    pub(crate) fn new(max_levels:usize)->Self{
        Self{
            lock: Default::default(),
            levels: Vec::with_capacity(max_levels),
            tables: Default::default(),
        }
    }
}
#[derive(Debug,Default)]
pub(crate) struct LevelCompactStatus{
    ranges:Vec<KeyRange>,
    del_size:i64,
}
#[derive(Debug,Default)]
struct KeyRange{
    left:Vec<u8>,
    right:Vec<u8>,
    inf:bool,
    size:i64,
}