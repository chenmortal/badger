use std::{collections::HashSet, ops::Deref};

use tokio::sync::RwLock;

use crate::{
    table::Table,
    util::{compare_key, key_with_ts, parse_key},
};

use super::levels::CompactDef;
#[derive(Debug)]
pub(crate) struct CompactStatus(pub(crate) RwLock<CompactStatusInner>);
impl Deref for CompactStatus {
    type Target = RwLock<CompactStatusInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
pub(crate) struct CompactStatusInner {
    pub(crate) levels: Vec<LevelCompactStatus>,
    pub(super) tables: HashSet<u64>,
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
#[derive(Debug, Default)]
pub(crate) struct LevelCompactStatus(pub(super) LevelCompactStatusInner);
#[derive(Debug, Default)]
pub(crate) struct LevelCompactStatusInner {
    pub(super) ranges: Vec<KeyRange>,
    del_size: i64,
}
#[derive(Debug, Default, Clone)]
pub(crate) struct KeyRange {
    pub(super) left: Vec<u8>,
    pub(super) right: Vec<u8>,
    inf: bool,
    size: i64,
}

impl CompactStatus {
    pub(super) async fn compare_and_add(&self, compact_def: &CompactDef) -> bool {
        let mut status_w = self.0.write().await;

        let this_level = compact_def.this_level.get_level();
        let next_level = compact_def.next_level.get_level();

        debug_assert!(this_level < status_w.levels.len());

        if status_w.levels[this_level].is_overlaps_with(&compact_def.this_range) {
            return false;
        };

        if status_w.levels[next_level].is_overlaps_with(&compact_def.next_range) {
            return false;
        };

        status_w.levels[this_level]
            .0
            .ranges
            .push(compact_def.this_range.clone());

        status_w.levels[next_level]
            .0
            .ranges
            .push(compact_def.next_range.clone());

        for table in compact_def.top.iter() {
            status_w.tables.insert(table.id());
        }
        for table in compact_def.bottom.iter() {
            status_w.tables.insert(table.id());
        }
        drop(status_w);
        true
    }
    pub(super) async fn is_overlaps_with(&self, level: usize, target: &KeyRange) -> bool {
        let inner_r = self.0.read().await;
        let r = inner_r.levels[level].is_overlaps_with(target);
        drop(inner_r);
        r
    }
}
impl LevelCompactStatus {
    fn is_overlaps_with(&self, target: &KeyRange) -> bool {
        for key_range in self.0.ranges.iter() {
            if key_range.is_overlaps_with(target) {
                return true;
            };
        }
        return false;
    }
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
            left: key_with_ts(parse_key(smallest), u64::MAX),
            right: key_with_ts(parse_key(&biggest), 0),
            inf: false,
            size: 0,
        }
        .into()
    }
    pub(crate) async fn from_table(table: &Table) -> Self {
        let smallest = table.smallest();
        let left = key_with_ts(parse_key(smallest), u64::MAX);
        let biggest = table.0.biggest.read().await;
        let right = key_with_ts(parse_key(&biggest), 0);
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

    //is overlapped by target
    #[inline]
    pub(crate) fn is_overlaps_with(&self, target: &KeyRange) -> bool {
        //empty is always overlapped by target
        if self.is_empty() {
            return true;
        }

        //self is not empty, so is not overlapped by empty
        if target.is_empty() {
            return false;
        }

        if self.inf || target.inf {
            return true;
        }

        if compare_key(&self.left, &target.right).is_gt()  //  [..target.right] [self.left..]
            || compare_key(&self.right, &target.left).is_lt()
        // [..self.right] [target.left..]
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
    #[inline]
    pub(crate) fn default_with_inf() -> Self {
        let mut k = Self::default();
        k.inf = true;
        k
    }
}

impl PartialEq for KeyRange {
    fn eq(&self, other: &Self) -> bool {
        self.left == other.left && self.right == other.right && self.inf == other.inf
    }
}
