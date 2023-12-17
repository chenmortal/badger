use std::{
    collections::HashSet,
    ops::{Deref, DerefMut, Range},
    sync::Arc,
    time::Duration,
};

use crate::{
    key_registry::KeyRegistry,
    kv::KeyTs,
    level::{levels::LEVEL0, plan::CompactPlan},
    table::{Table, TableConfig},
    txn::oracle::Oracle,
    util::{
        cache::{BlockCache, IndexCache},
        closer::Closer,
        SSTableId,
    },
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
    pub(crate) async fn spawn_compact(
        self,
        closer: &mut Closer,
        config: TableConfig,
        key_registry: KeyRegistry,
        index_cache: IndexCache,
        block_cache: Option<BlockCache>,
        oracle: Oracle,
    ) {
        for task_id in 0..self.level_config().num_compactors() {
            tokio::spawn(self.clone().pre_compact(
                task_id,
                closer.clone(),
                config.clone(),
                key_registry.clone(),
                index_cache.clone(),
                block_cache.clone(),
                oracle.clone(),
            ));
        }
    }
    pub(crate) async fn pre_compact(
        self,
        compact_task_id: usize,
        closer: Closer,
        config: TableConfig,
        key_registry: KeyRegistry,
        index_cache: IndexCache,
        block_cache: Option<BlockCache>,
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
        self.compact(
            compact_task_id,
            priority,
            config,
            key_registry,
            index_cache,
            block_cache,
            oracle,
        )
        .await?;
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
        config: TableConfig,
        key_registry: KeyRegistry,
        index_cache: IndexCache,
        block_cache: Option<BlockCache>,
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
        self.run_compact(
            priority_level,
            &mut plan,
            config,
            &key_registry,
            index_cache,
            block_cache,
            &oracle,
        )
        .await?;
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
    pub(crate) fn left(&self) -> &KeyTs {
        &self.left
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
