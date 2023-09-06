use std::sync::Arc;

use tokio::sync::RwLock;

use crate::{db::DB, table::Table};

pub(crate) struct LevelHandler{
    lock:RwLock<()>,
    tables:Vec<Arc<Table>>,
    total_size:i64,
    total_stale_size:i64,
    level:usize,
    str_level:String,
    db:Arc<DB>,
}
impl LevelHandler {
    pub(crate) fn new(db:Arc<DB>,level:usize)->Self{
        let str_level = format!("l{}",level);
        Self{
            lock: Default::default(),
            tables: Default::default(),
            total_size: Default::default(),
            total_stale_size: Default::default(),
            level,
            str_level,
            db,
        }
    }
}