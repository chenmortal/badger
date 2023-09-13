use std::io;
use std::sync::{Arc, RwLockWriteGuard};

use anyhow::anyhow;
use anyhow::bail;
use tokio::sync::RwLock;

use crate::{db::DB, table::Table, util::compare_key};
#[derive(Debug)]
pub(crate) struct LevelHandler(Arc<RwLock<LevelHandlerInner>>);
#[derive(Debug)]
struct LevelHandlerInner {
    tables: Vec<Table>,
    total_size: usize,
    total_stale_size: u32,
    level: usize,
    str_level: String,
}
impl LevelHandler {
    pub(crate) fn new(level: usize) -> Self {
        let str_level = format!("l{}", level);
        let inner = LevelHandlerInner {
            tables: Default::default(),
            total_size: Default::default(),
            total_stale_size: Default::default(),
            level,
            str_level,
            // db,
        };
        Self(Arc::new(RwLock::new(inner)))
    }
    #[inline]
    pub(crate) async fn get_level(&self) -> usize {
        let inner_r = self.0.read().await;
        let level = inner_r.level;
        drop(inner_r);
        level
    }
    #[inline]
    pub(crate) async fn get_total_size(&self) -> usize {
        let inner_r = self.0.read().await;
        let total_size = inner_r.total_size;
        drop(inner_r);
        total_size
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
    pub(crate) async fn validate(&self) -> anyhow::Result<()> {
        let inner_r = self.0.read().await;
        if inner_r.level == 0 {
            return Ok(());
        }
        let num_tables = inner_r.tables.len();
        for j in 1..num_tables {
            let pre = &inner_r.tables[j - 1];
            let now = &inner_r.tables[j];
            let pre_biggest_r = pre.0.biggest.read().await;

            if compare_key(&pre_biggest_r, now.smallest()).is_ge() {
                let e = anyhow!(
                    "Inter: Biggest(j-1)[{}] 
{:?}
vs Smallest(j)[{}]: 
{:?}
: level={} j={} num_tables={}",
                    pre.id(),
                    pre_biggest_r.as_slice(),
                    now.id(),
                    now.smallest(),
                    inner_r.level,
                    j,
                    num_tables
                );
                drop(pre_biggest_r);
                return Err(e);
            };
            drop(pre_biggest_r);

            let now_biggest_r = now.0.biggest.read().await;
            if compare_key(now.smallest(), &now_biggest_r).is_gt() {
                let e = anyhow!(
                    "Intra:
{:?}
vs
{:?}
: level={} j={} num_tables={}",
                    now.smallest(),
                    now_biggest_r.as_slice(),
                    inner_r.level,
                    j,
                    num_tables
                );
                drop(now_biggest_r);
                return Err(e);
            };
            drop(now_biggest_r);
        }
        Ok(())
    }
    pub(crate) async fn sync_mmap(&self) -> Result<(), io::Error> {
        let mut err = None;
        let tables_r = self.0.read().await;
        for table in tables_r.tables.iter() {
            match table.sync_mmap() {
                Ok(_) => {}
                Err(e) => {
                    if err.is_none() {
                        err = e.into();
                    }
                }
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
