use std::{collections::HashSet, sync::Arc};

use tokio::sync::RwLock;
use zstd::zstd_safe::WriteBuf;

use crate::{
    table::Table,
    util::{compare_key, key_with_ts, parse_key},
};
#[derive(Debug)]
pub(crate) struct CompactStatus(pub(crate) RwLock<CompactStatusInner>);
#[derive(Debug)]
pub(crate) struct CompactStatusInner {
    pub(crate) levels: Vec<LevelCompactStatus>,
    tables: HashSet<u64>,
}
impl CompactStatus {
    pub(crate) fn new(max_levels: usize) -> Self {
        let inner = CompactStatusInner {
            levels: Vec::with_capacity(max_levels),
            tables: Default::default(),
        };
        Self(RwLock::new(inner))
    }
}
#[derive(Debug,Default)]
pub(crate) struct  LevelCompactStatus(Arc<LevelCompactStatusInner>);
#[derive(Debug, Default)]
pub(crate) struct LevelCompactStatusInner {
    ranges: Vec<KeyRange>,
    del_size: i64,
}
#[derive(Debug, Default, Clone)]
pub(crate) struct KeyRange {
    left: Vec<u8>,
    right: Vec<u8>,
    inf: bool,
    size: i64,
}
impl KeyRange {
    pub(crate) async fn from_tables(tables: &[Table]) -> Option<KeyRange> {
        if tables.len() == 0 {
            return None;
        }
        let mut smallest = tables[0].smallest();
        let mut biggest = tables[0].0.biggest.read().await.to_vec();
        for i in 1..tables.len() {
            if compare_key(tables[i].smallest(), smallest).is_lt() {
                smallest = tables[i].smallest();
            };
            let biggest_i_r = tables[i].0.biggest.read().await;
            if compare_key(biggest_i_r.as_slice(), biggest.as_slice()).is_gt() {
                biggest = biggest_i_r.to_vec();
            }
            drop(biggest_i_r)
        }
        Self {
            left: key_with_ts(parse_key(smallest).unwrap(), u64::MAX),
            right: key_with_ts(parse_key(&biggest).unwrap(), 0),
            inf: false,
            size: 0,
        }
        .into()
    }
    pub(crate) async fn from_table(table: &Table) -> Self {
        let smallest = table.smallest();
        let left = key_with_ts(parse_key(smallest).unwrap(), u64::MAX);
        let biggest = table.0.biggest.read().await;
        let right = key_with_ts(parse_key(&biggest).unwrap(), 0);
        drop(biggest);
        Self {
            left,
            right,
            inf: false,
            size: 0,
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.left.len() == 0 && self.right.len() == 0 && !self.inf
    }
    #[inline]
    fn to_string(&self) -> String {
        format!(
            "[left={:?}, right={:?}, inf={}]",
            self.left, self.right, self.inf
        )
    }
    #[inline]
    pub(crate) fn extend_borrow(&mut self, key_range: &KeyRange) {
        if key_range.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = key_range.clone();
        }
        if self.left.len() == 0 || compare_key(&key_range.left, &self.left).is_lt() {
            self.left = key_range.left.clone();
        }
        if self.right.len() == 0 || compare_key(&key_range.right, &self.right).is_gt() {
            self.right = key_range.right.clone();
        }
        if key_range.inf {
            self.inf = true
        }
    }
    #[inline]
    pub(crate) fn extend(&mut self, key_range: KeyRange) {
        if key_range.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = key_range;
            return;
        }
        if self.left.len() == 0 || compare_key(&key_range.left, &self.left).is_lt() {
            self.left = key_range.left;
        }
        if self.right.len() == 0 || compare_key(&key_range.right, &self.right).is_gt() {
            self.right = key_range.right;
        }
        if key_range.inf {
            self.inf = true
        }
    }
    #[inline]
    pub(crate) fn is_overlaps_with(&self, other: &KeyRange) -> bool {
        if self.is_empty() || self.is_empty() {
            return true;
        }

        // if other.is_empty() {
        //     return false;
        // }

        if self.inf || other.inf {
            return true;
        }

        if compare_key(&self.left, &other.right).is_gt()
            || compare_key(&self.right, &other.left).is_lt()
        {
            return false;
        }

        return true;
    }

    #[inline]
    pub(crate) fn get_left(&self) -> &[u8] {
        self.left.as_ref()
    }

    #[inline]
    pub(crate) fn get_right(&self) -> &[u8] {
        self.right.as_ref()
    }
}

impl PartialEq for KeyRange {
    fn eq(&self, other: &Self) -> bool {
        self.left == other.left && self.right == other.right && self.inf == other.inf
    }
}
