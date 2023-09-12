use std::sync::{Arc, RwLockWriteGuard};

use anyhow::bail;
use tokio::sync::RwLock;

use crate::{db::DB, table::Table, util::compare_key};
pub(crate) struct LevelHandler(Arc<RwLock<LevelHandlerInner>>);
struct LevelHandlerInner {
    tables: Vec<Table>,
    total_size: usize,
    total_stale_size: u32,
    level: usize,
    str_level: String,
    db: Arc<DB>,
}
impl LevelHandler {
    pub(crate) fn new(db: Arc<DB>, level: usize) -> Self {
        let str_level = format!("l{}", level);
        let inner = LevelHandlerInner {
            tables: Default::default(),
            total_size: Default::default(),
            total_stale_size: Default::default(),
            level,
            str_level,
            db,
        };
        Self(Arc::new(RwLock::new(inner)))
    }
    pub(crate) async fn init_tables(&self, tables: &Vec<Table>) {
        let mut inner_w = self.0.write().await;
        inner_w.tables = tables.clone();
        inner_w.total_size = 0;
        inner_w.total_stale_size = 0;

        tables.iter().for_each(|t| {
            inner_w.total_size += t.size();
            inner_w.total_stale_size = t.stale_data_size();
        });

        if inner_w.level == 0 {
            inner_w.tables.sort_by(|a, b| a.id().cmp(&b.id()));
        } else {
            inner_w
                .tables
                .sort_by(|a, b| compare_key(a.smallest(), b.smallest()));
        }
        drop(inner_w);
    }
    pub(crate) async fn validate(&self)->anyhow::Result<()>{
        let inner_r = self.0.read().await;
        if inner_r.level==0{
            return Ok(());
        }
        let num_tables = inner_r.tables.len();
        for j in 1..num_tables{
            let pre = &inner_r.tables[j-1];
            let now = &inner_r.tables[j];
            let pre_r = pre.0.biggest.read().await;

            if compare_key(&pre_r, inner_r.tables[j].smallest()).is_ge() {
                drop(pre_r);
                bail!("Inter: Biggest(j-1)[{}] \n{}\n vs Smallest(j)[{}]: \n{}\n: level={} j={} num_tables={}",pre.id(),);
            };
            let now_r = inner_r.tables[j].0.biggest.read().await;

        }
        Ok(())
    }
}
