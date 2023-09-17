use std::{cmp::Ordering, mem::size_of};
use std::io;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use tokio::sync::RwLock;

use crate::{table::Table, util::compare_key};

use super::compaction::KeyRange;
#[derive(Debug, Clone)]
pub(crate) struct LevelHandler(pub(crate) Arc<LevelHandlerInner>);
impl Deref for LevelHandler {
    type Target = Arc<LevelHandlerInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
pub(crate) struct LevelHandlerInner {
    pub(crate) handler_tables: RwLock<LevelHandlerTables>,
    level: usize,
}
impl Deref for LevelHandlerInner {
    type Target=RwLock<LevelHandlerTables>;

    fn deref(&self) -> &Self::Target {
        &self.handler_tables
    }
}
#[derive(Debug, Default)]
pub(crate) struct LevelHandlerTables {
    pub(crate) tables: Vec<Table>,
    total_size: usize,
    total_stale_size: u32,
}
impl LevelHandler {
    pub(crate) fn new(level: usize) -> Self {
        let inner = LevelHandlerInner {
            handler_tables: RwLock::new(LevelHandlerTables::default()),
            level,
        };
        Self(Arc::new(inner))
    }
    #[inline]
    pub(crate) fn get_level(&self) -> usize {
        self.0.level
    }
    #[inline]
    pub(crate) fn get_str_level(&self) -> String {
        format!("l{}", self.level)
    }
    #[inline]
    pub(crate) async fn get_total_size(&self) -> usize {
        let inner_r = self.0.handler_tables.read().await;
        let total_size = inner_r.total_size;
        drop(inner_r);
        total_size
    }
    pub(crate) async fn init_tables(&self, tables: &Vec<Table>) {
        let mut inner_w = self.0.handler_tables.write().await;
        inner_w.tables = tables.clone();
        inner_w.total_size = 0;
        inner_w.total_stale_size = 0;

        tables.iter().for_each(|t| {
            inner_w.total_size += t.size();
            inner_w.total_stale_size = t.stale_data_size();
        });

        if self.0.level == 0 {
            inner_w.tables.sort_by(|a, b| a.id().cmp(&b.id()));
        } else {
            inner_w
                .tables
                .sort_by(|a, b| compare_key(a.smallest(), b.smallest()));
        }
        drop(inner_w);
    }
    pub(crate) async fn validate(&self) -> anyhow::Result<()> {
        let inner_r = self.0.handler_tables.read().await;
        if self.0.level == 0 {
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
                    self.0.level,
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
                    self.0.level,
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
        let tables_r = self.0.handler_tables.read().await;
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
    pub(crate) async fn overlapping_tables(&self, key_range: &KeyRange) -> (usize, usize) {
        let left = key_range.get_left();
        let right = key_range.get_right();
        if left.len() == 0 || right.len() == 0 {
            return (0, 0);
        };

        let self_r = self.0.handler_tables.read().await;
        let tables = &self_r.tables;
        let left_index = match binary_search_biggest(tables, left).await {
            Ok(index) => index,  // val[index].biggest == value
            Err(index) => index, // val[index].biggest > value
        };
        let right_index =
            match tables.binary_search_by(|table| compare_key(table.smallest(), right)) {
                Ok(index) => index + 1, //for i in left..right ; not include right so this need add 1;
                Err(index) => index,
            };
        drop(self_r);
        (left_index, right_index)
    }
}

//fix from std binary_search_by
#[inline]
async fn binary_search_biggest(tables: &Vec<Table>, value: &[u8]) -> Result<usize, usize> {
    // INVARIANTS:
    // - 0 <= left <= left + size = right <= self.len()
    // - f returns Less for everything in self[..left]
    // - f returns Greater for everything in self[right..]

    // 0 1 2 3 4 5 t=2.5
    // l=0 r=6 s=6 mid=3 g
    // l=0 r=3 s=3 mid=1 l
    // l=2 r=3 s=1 mid=2 l
    // l=3 r=1
    // return 3
    let mut size = tables.len();
    let mut left = 0;
    let mut right = size;
    while left < right {
        let mid = left + size / 2;

        // SAFETY: the while condition means `size` is strictly positive, so
        // `size/2 < size`. Thus `left + size/2 < left + size`, which
        // coupled with the `left + size <= self.len()` invariant means
        // we have `left + size/2 < self.len()`, and this is in-bounds.
        let mid_r = unsafe { tables.get_unchecked(mid) }.0.biggest.read().await;
        let mid_slice: &[u8] = mid_r.as_ref();
        let cmp = compare_key(mid_slice, value);
        drop(mid_r);

        // The reason why we use if/else control flow rather than match
        // is because match reorders comparison operations, which is perf sensitive.
        // This is x86 asm for u8: https://rust.godbolt.org/z/8Y8Pra.
        if cmp == Ordering::Less {
            left = mid + 1;
        } else if cmp == Ordering::Greater {
            right = mid;
        } else {
            return Ok(mid);
        }

        size = right - left;
    }
    Err(left)
}
#[test]
fn test_a(){
    dbg!(size_of::<LevelHandler>());
}