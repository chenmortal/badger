use std::{
    collections::HashSet,
    ops::{Deref, DerefMut, Range},
    sync::Arc,
    time::Duration,
};

use crate::{
    kv::KeyTs,
    level::{levels::LEVEL0, plan::CompactPlan},
    table::Table,
    txn::oracle::Oracle,
    util::{closer::Closer, SSTableId},
};
use bytes::Bytes;
use parking_lot::RwLock;
use rand::Rng;
use tokio::select;

use super::{
    level_handler::LevelHandlerTables,
    levels::{Level, LevelsController, LevelsControllerInner},
};
#[derive(Debug, Clone)]
pub(crate) struct CompactPriority {
    level: Level,
    score: f64,
    adjusted: f64,
    drop_prefixes: Vec<Bytes>,
    targets: CompactTargets,
}

impl CompactPriority {
    pub(crate) fn level(&self) -> Level {
        self.level
    }

    pub(crate) fn targets(&self) -> &CompactTargets {
        &self.targets
    }

    pub(crate) fn adjusted(&self) -> f64 {
        self.adjusted
    }

    pub(crate) fn drop_prefixes(&self) -> &[Bytes] {
        self.drop_prefixes.as_ref()
    }

    pub(crate) fn targets_mut(&mut self) -> &mut CompactTargets {
        &mut self.targets
    }
}
#[derive(Debug, Clone)]
pub(crate) struct CompactTargets {
    base_level: Level,
    target_size: Vec<usize>,
    file_size: Vec<usize>,
}

impl CompactTargets {
    pub(crate) fn file_size(&self) -> &[usize] {
        self.file_size.as_ref()
    }

    pub(crate) fn file_size_mut(&mut self) -> &mut Vec<usize> {
        &mut self.file_size
    }
}

impl LevelsController {
    pub(crate) async fn spawn_compact(self, closer: &mut Closer, oracle: &Oracle) {
        for task_id in 0..self.level_config().num_compactors() {
            tokio::spawn(
                self.clone()
                    .pre_compact(task_id, closer.clone(), oracle.clone()),
            );
        }
    }
    pub(crate) async fn pre_compact(
        self,
        compact_task_id: usize,
        closer: Closer,
        oracle: Oracle,
    ) -> anyhow::Result<()> {
        let sleep =
            tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(0..1000)));
        select! {
            _=sleep=>{},
            _=closer.captured()=>{return Ok(());}
        }

        let mut count = 0;
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        // self.last_level().level();
        self.last_level_handler().level();
        let priority = CompactPriority {
            level: self.last_level_handler().level(),
            score: 0.,
            adjusted: 0.,
            drop_prefixes: vec![],
            targets: self.level_targets().await,
        };
        self.compact(compact_task_id, priority, oracle).await?;
        loop {
            select! {
                _=ticker.tick()=>{
                    count+=1;
                    if self.level_config().lmax_compaction() && compact_task_id==2 && count >= 200{
                        count=0;
                    };
                }
                _=closer.captured()=>{return Ok(());}
            }
        }
    }
    async fn compact(
        &self,
        compact_task_id: usize,
        mut priority: CompactPriority,
        oracle: Oracle,
    ) -> anyhow::Result<()> {
        let priority_level = priority.level;
        debug_assert!(priority.level < self.max_level());
        if priority.targets.base_level == LEVEL0 {
            priority.targets = self.level_targets().await;
        }

        let this_level_handler = self.level_handler(priority.level).clone();
        let next_level_handler = if priority.level == LEVEL0 {
            self.level_handler(priority.targets.base_level).clone()
        } else {
            this_level_handler.clone()
        };

        let mut plan = CompactPlan::new(
            compact_task_id,
            priority,
            this_level_handler,
            next_level_handler,
        );
        plan.fix(self, &oracle).await?;
        self.run_compact(priority_level, &mut plan, &oracle).await?;
        Ok(())
    }
}

impl LevelsControllerInner {
    async fn level_targets(&self) -> CompactTargets {
        let levels_len = self.levels().len();
        assert!(levels_len < u8::MAX as usize);
        let levels_bound: Level = (levels_len as u8).into();
        let mut targets = CompactTargets {
            base_level: 0.into(),
            target_size: vec![0; levels_len],
            file_size: vec![0; levels_len],
        };
        let mut level_size = self.last_level_handler().get_total_size().await;
        let base_level_size = self.level_config().base_level_size();
        for i in (1..levels_len).rev() {
            targets.target_size[i] = level_size.max(base_level_size);
            if targets.base_level == 0.into() && level_size <= base_level_size {
                targets.base_level = (i as u8).into();
            }
            level_size /= self.level_config().level_size_multiplier();
        }

        let mut table_size = self.table_config().table_size();
        for i in 0..levels_len {
            targets.file_size[i] = if i == 0 {
                self.level_config().memtable_size()
            } else if i <= targets.base_level.into() {
                table_size
            } else {
                table_size *= self.level_config().table_size_multiplier();
                table_size
            }
        }

        for level in targets.base_level..levels_bound {
            if self.level_handler(level).get_total_size().await > 0 {
                break;
            }
            targets.base_level = level;
        }

        let base_level: usize = targets.base_level.into();
        let levels = &self.levels();

        if base_level < levels.len() - 1
            && levels[base_level].get_total_size().await == 0
            && levels[base_level + 1].get_total_size().await < targets.target_size[base_level + 1]
        {
            targets.base_level += 1;
        }
        targets
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct KeyTsRange {
    left: KeyTs,
    right: KeyTs,
    inf: bool,
}
impl From<&[Table]> for KeyTsRange {
    fn from(value: &[Table]) -> Self {
        if value.len() == 0 {
            return KeyTsRange::default();
        }

        let mut smallest = value[0].smallest();
        let mut biggest = value[0].biggest();
        for i in 1..value.len() {
            smallest = smallest.min(value[i].smallest());
            biggest = biggest.max(value[i].biggest());
        }
        Self {
            left: KeyTs::new(smallest.key().clone(), u64::MAX.into()),
            right: KeyTs::new(biggest.key().clone(), 0.into()),
            inf: false,
        }
    }
}
impl From<&Table> for KeyTsRange {
    fn from(value: &Table) -> Self {
        Self {
            left: KeyTs::new(value.smallest().key().clone(), u64::MAX.into()),
            right: KeyTs::new(value.biggest().key().clone(), 0.into()),
            inf: false,
        }
    }
}
impl KeyTsRange {
    pub(crate) fn inf_default() -> Self {
        Self {
            left: KeyTs::default(),
            right: KeyTs::default(),
            inf: true,
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty() && !self.inf
    }

    #[inline]
    pub(crate) fn is_overlaps_with(&self, target: &Self) -> bool {
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

        //  [..target.right] [self.left..] [..self.right] [target.left..]
        if target.right < self.left || self.right < target.left {
            return false;
        }

        return true;
    }
    #[inline]
    pub(crate) fn extend(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other;
            return;
        }
        if self.left.is_empty() || other.left < self.left {
            self.left = other.left;
        }
        if self.right.is_empty() || self.right < other.right {
            self.right = other.right;
        }

        if other.inf {
            self.inf = true
        }
    }
    #[inline]
    pub(crate) fn extend_borrow(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other.clone();
            return;
        }
        if self.left.is_empty() || other.left < self.left {
            self.left = other.left.clone();
        }
        if self.right.is_empty() || self.right < other.right {
            self.right = other.right.clone();
        }

        if other.inf {
            self.inf = true
        }
    }

    pub(crate) fn set_right(&mut self, right: KeyTs) {
        self.right = right;
    }

    pub(crate) fn set_left(&mut self, left: KeyTs) {
        self.left = left;
    }

    pub(crate) fn right(&self) -> &KeyTs {
        &self.right
    }
}

impl LevelHandlerTables {
    pub(crate) fn overlap_tables(&self, range: &KeyTsRange) -> Range<usize> {
        if range.left.is_empty() || range.right.is_empty() {
            return 0..0;
        }
        let left_index = match self
            .tables
            .binary_search_by(|t| t.biggest().cmp(&range.left))
        {
            Ok(i) => i,
            Err(i) => i,
        };
        let right_index = match self
            .tables
            .binary_search_by(|t| t.smallest().cmp(&range.right))
        {
            Ok(i) => i + 1, // because t.smallest==range.right, so need this table,but return [left,right),so need +1 for [left,right)
            Err(i) => i,
        };
        left_index..right_index
    }
}
#[derive(Debug, Default)]
pub(crate) struct CompactStatus(Arc<RwLock<CompactStatusInner>>);
impl Deref for CompactStatus {
    type Target = RwLock<CompactStatusInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Default)]
pub(crate) struct CompactStatusInner {
    levels: Vec<LevelCompactStatus>,
    tables: HashSet<SSTableId>,
}

impl CompactStatusInner {
    pub(crate) fn levels_mut(&mut self) -> &mut Vec<LevelCompactStatus> {
        &mut self.levels
    }

    pub(crate) fn tables_mut(&mut self) -> &mut HashSet<SSTableId> {
        &mut self.tables
    }

    pub(crate) fn tables(&self) -> &HashSet<SSTableId> {
        &self.tables
    }

    pub(crate) fn levels(&self) -> &[LevelCompactStatus] {
        self.levels.as_ref()
    }
    pub(crate) fn push(&mut self, level: Level, range: KeyTsRange) {
        self.levels[level.to_usize()].ranges.push(range);
    }
}
impl CompactStatus {
    pub(crate) fn new(max_levels: usize) -> Self {
        let inner = CompactStatusInner {
            levels: Vec::with_capacity(max_levels),
            tables: Default::default(),
        };
        Self(RwLock::new(inner).into())
    }
    pub(crate) fn is_overlaps_with(&self, level: Level, target: &KeyTsRange) -> bool {
        let inner = self.read();
        let result = inner.levels[level.to_usize()].is_overlaps_with(target);
        drop(inner);
        result
    }
}
#[derive(Debug, Default)]
pub(crate) struct LevelCompactStatus(LevelCompactStatusInner);
impl Deref for LevelCompactStatus {
    type Target = LevelCompactStatusInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for LevelCompactStatus {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[derive(Debug, Default)]
pub(crate) struct LevelCompactStatusInner {
    ranges: Vec<KeyTsRange>,
    del_size: i64,
}

impl LevelCompactStatusInner {
    pub(crate) fn ranges_mut(&mut self) -> &mut Vec<KeyTsRange> {
        &mut self.ranges
    }
}
impl LevelCompactStatus {
    pub(crate) fn is_overlaps_with(&self, target: &KeyTsRange) -> bool {
        for range in self.ranges.iter() {
            if range.is_overlaps_with(target) {
                return true;
            }
        }
        return false;
    }
}

// #[derive(Debug, Default, Clone)]
// pub(crate) struct KeyRange {
//     pub(super) left: Vec<u8>,
//     pub(super) right: Vec<u8>,
//     inf: bool,
//     size: i64,
// }

// impl CompactStatus {
// pub(super) fn compare_and_add(&self, compact_def: &CompactDef) -> bool {
//     let mut status_w = self.0.write();

//     let this_level: usize = compact_def.this_level.level().into();
//     let next_level: usize = compact_def.next_level.level().into();

//     debug_assert!(this_level < status_w.levels.len());

//     if status_w.levels[this_level].is_overlaps_with(&compact_def.this_range) {
//         return false;
//     };

//     if status_w.levels[next_level].is_overlaps_with(&compact_def.next_range) {
//         return false;
//     };

//     status_w.levels[this_level]
//         .0
//         .ranges
//         .push(compact_def.this_range.clone());

//     status_w.levels[next_level]
//         .0
//         .ranges
//         .push(compact_def.next_range.clone());

//     for table in compact_def.top.iter() {
//         status_w.tables.insert(table.table_id());
//     }
//     for table in compact_def.bottom.iter() {
//         status_w.tables.insert(table.table_id());
//     }
//     drop(status_w);
//     true
// }
// pub(super) fn is_overlaps_with(&self, level: Level, target: &KeyRange) -> bool {
//     let inner_r = self.0.read();
//     let l: usize = level.into();
//     let r = inner_r.levels[l].is_overlaps_with(target);
//     drop(inner_r);
//     r
// }
// }
// impl LevelCompactStatus {
//     fn is_overlaps_with(&self, target: &KeyRange) -> bool {
//         for key_range in self.0.ranges.iter() {
//             if key_range.is_overlaps_with(target) {
//                 return true;
//             };
//         }
//         return false;
//     }
// }
// impl KeyRange {
//     pub(crate) fn from_tables(tables: &[Table]) -> Option<KeyRange> {
//         if tables.len() == 0 {
//             return None;
//         }
//         let mut smallest = tables[0].smallest();
//         let mut biggest = tables[0].biggest();
//         for i in 1..tables.len() {
//             if tables[i].smallest().cmp(smallest).is_lt() {
//                 smallest = tables[i].smallest();
//             };
//             let biggest_i_r = tables[i].biggest();
//             if biggest_i_r.cmp(biggest).is_gt() {
//                 biggest = biggest_i_r;
//             }
//         }
//         Self {
//             left: key_with_ts(parse_key(smallest), u64::MAX),
//             right: key_with_ts(parse_key(&biggest), 0),
//             inf: false,
//             size: 0,
//         }
//         .into()
//     }
//     pub(crate) fn from_table(table: &Table) -> Self {
//         let smallest = table.smallest();
//         let left = key_with_ts(parse_key(smallest), u64::MAX);
//         let biggest = table.biggest();
//         let right = key_with_ts(parse_key(&biggest), 0);
//         Self {
//             left,
//             right,
//             inf: false,
//             size: 0,
//         }
//     }

//     #[inline]
//     pub(crate) fn is_empty(&self) -> bool {
//         self.left.len() == 0 && self.right.len() == 0 && !self.inf
//     }
//     #[inline]
//     fn to_string(&self) -> String {
//         format!(
//             "[left={:?}, right={:?}, inf={}]",
//             self.left, self.right, self.inf
//         )
//     }
//     #[inline]
//     pub(crate) fn extend_borrow(&mut self, key_range: &KeyRange) {
//         if key_range.is_empty() {
//             return;
//         }
//         if self.is_empty() {
//             *self = key_range.clone();
//         }
//         if self.left.len() == 0 || compare_key(&key_range.left, &self.left).is_lt() {
//             self.left = key_range.left.clone();
//         }
//         if self.right.len() == 0 || compare_key(&key_range.right, &self.right).is_gt() {
//             self.right = key_range.right.clone();
//         }
//         if key_range.inf {
//             self.inf = true
//         }
//     }
//     #[inline]
//     pub(crate) fn extend(&mut self, key_range: KeyRange) {
//         if key_range.is_empty() {
//             return;
//         }
//         if self.is_empty() {
//             *self = key_range;
//             return;
//         }
//         if self.left.len() == 0 || compare_key(&key_range.left, &self.left).is_lt() {
//             self.left = key_range.left;
//         }
//         if self.right.len() == 0 || compare_key(&key_range.right, &self.right).is_gt() {
//             self.right = key_range.right;
//         }
//         if key_range.inf {
//             self.inf = true
//         }
//     }

//     //is overlapped by target
//     #[inline]
//     pub(crate) fn is_overlaps_with(&self, target: &KeyRange) -> bool {
//         //empty is always overlapped by target
//         if self.is_empty() {
//             return true;
//         }

//         //self is not empty, so is not overlapped by empty
//         if target.is_empty() {
//             return false;
//         }

//         if self.inf || target.inf {
//             return true;
//         }

//         if compare_key(&self.left, &target.right).is_gt()  //  [..target.right] [self.left..]
//             || compare_key(&self.right, &target.left).is_lt()
//         // [..self.right] [target.left..]
//         {
//             return false;
//         }

//         return true;
//     }

//     #[inline]
//     pub(crate) fn get_left(&self) -> &[u8] {
//         self.left.as_ref()
//     }

//     #[inline]
//     pub(crate) fn get_right(&self) -> &[u8] {
//         self.right.as_ref()
//     }
//     #[inline]
//     pub(crate) fn default_with_inf() -> Self {
//         let mut k = Self::default();
//         k.inf = true;
//         k
//     }
// }

// impl PartialEq for KeyRange {
//     fn eq(&self, other: &Self) -> bool {
//         self.left == other.left && self.right == other.right && self.inf == other.inf
//     }
// }
