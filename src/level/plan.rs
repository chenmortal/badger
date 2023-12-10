use std::time::{Duration, SystemTime};

use tokio::sync::RwLockReadGuard;

use crate::{kv::KeyTs, table::Table, txn::oracle::Oracle};

use super::{
    compaction::{CompactPriority, CompactStatus, CompactStatusInner, KeyTsRange},
    level_handler::{LevelHandler, LevelHandlerTables},
    levels::{LevelsControllerInner, LEVEL0},
};
use anyhow::bail;
struct CompactPlanLockLevel<'a> {
    this_level: RwLockReadGuard<'a, LevelHandlerTables>,
    next_level: RwLockReadGuard<'a, LevelHandlerTables>,
}
#[derive(Clone)]
pub(super) struct CompactPlan {
    compact_task_id: usize,
    priority: CompactPriority,
    this_level_handler: LevelHandler,
    next_level_handler: LevelHandler,
    top: Vec<Table>,
    this_range: KeyTsRange,
    bottom: Vec<Table>,
    next_range: KeyTsRange,
    this_size: usize,
    splits: Vec<KeyTsRange>,
}
impl CompactPlan {
    pub(super) fn new(
        compact_task_id: usize,
        priority: CompactPriority,
        this_level_handler: LevelHandler,
        next_level_handler: LevelHandler,
    ) -> Self {
        Self {
            compact_task_id,
            priority,
            this_level_handler,
            next_level_handler,
            top: vec![],
            bottom: vec![],
            this_size: 0,
            this_range: KeyTsRange::default(),
            next_range: KeyTsRange::default(),
            splits: vec![],
        }
    }
    pub(super) async fn fix(
        &mut self,
        controller: &LevelsControllerInner,
        oracle: &Oracle,
    ) -> anyhow::Result<()> {
        if self.priority.level() == LEVEL0 {
            if !self.fill_tables_level0(controller).await {
                bail!("Unable to fill tables")
            };
        } else {
            if self.priority.level() != controller.max_level() - 1 {
                self.next_level_handler =
                    controller.level_handler(self.priority.level() + 1).clone();
            }
            if !self.fill_tables(controller, oracle).await {
                bail!("Unable to fill tables")
            };
        }
        Ok(())
    }

    async fn fill_tables(&mut self, controller: &LevelsControllerInner, oracle: &Oracle) -> bool {
        if self.this_level_handler.tables_len().await == 0 {
            return false;
        }

        if self.this_level_handler.level() == (controller.max_level() - 1) {
            return self.fill_max_level_tables(controller, oracle).await;
        }

        let lock = CompactPlanLockLevel {
            this_level: self.this_level_handler.read().await,
            next_level: self.next_level_handler.read().await,
        };
        let mut this_tables = lock.this_level.tables.clone();
        this_tables.sort_unstable_by(|a, b| a.max_version().cmp(&b.max_version()));

        for table in this_tables {
            self.this_size = table.size();
            self.this_range = table.as_ref().into();

            if controller
                .compact_status()
                .is_overlaps_with(self.this_level_handler.level(), &self.this_range)
            {
                continue;
            }
            self.top = vec![table.clone()];
            let index_range = lock.next_level.overlap_tables(&self.this_range);
            self.bottom = lock.next_level.tables[index_range].to_vec();

            if self.bottom.len() == 0 {
                self.next_range = self.this_range.clone();
                if !controller.compact_status().try_update(&lock, &self) {
                    continue;
                }
                return true;
            }
            self.next_range = self.bottom.as_slice().into();

            if controller
                .compact_status()
                .is_overlaps_with(self.next_level_handler.level(), &self.next_range)
            {
                continue;
            }

            if !controller.compact_status().try_update(&lock, &self) {
                continue;
            }
            return true;
        }
        false
    }
    async fn fill_max_level_tables(
        &mut self,
        controller: &LevelsControllerInner,
        oracle: &Oracle,
    ) -> bool {
        let lock = CompactPlanLockLevel {
            this_level: self.this_level_handler.read().await,
            next_level: self.next_level_handler.read().await,
        };
        let tables = &lock.this_level.tables;
        let mut sorted_tables = tables.to_vec();
        sorted_tables.sort_unstable_by(|a, b| b.stale_data_size().cmp(&a.stale_data_size()));

        if sorted_tables.len() > 0 && sorted_tables[0].stale_data_size() == 0 {
            return false;
        }
        self.bottom.clear();
        let now = SystemTime::now();

        for table in sorted_tables
            .drain(..)
            .filter(|t| t.max_version() <= oracle.discard_at_or_below())
            .filter(|t| now.duration_since(t.created_at()).unwrap() >= Duration::from_secs(60 * 60))
            .filter(|t| t.stale_data_size() >= 10 << 20)
        {
            self.this_size = table.size();
            self.this_range = table.as_ref().into();
            self.next_range = self.this_range.clone();
            if controller
                .compact_status()
                .is_overlaps_with(self.this_level_handler.level(), &self.this_range)
            {
                continue;
            }
            self.top = vec![table.clone()];

            let need_file_size =
                self.priority.targets().file_size()[self.this_level_handler.level().to_usize()];
            if table.size() >= need_file_size {
                break;
            }

            //collect_bottom_tables
            let mut total_size = table.size();
            let mut j = match tables.binary_search_by(|a| a.smallest().cmp(&table.smallest())) {
                Ok(i) => i,
                Err(i) => i,
            };
            assert_eq!(tables[j].table_id(), table.table_id());
            j += 1;
            while j < tables.len() {
                let new_table = &tables[j];
                total_size += new_table.size();
                if total_size >= need_file_size {
                    break;
                }
                self.bottom.push(new_table.clone());
                self.next_range.extend(new_table.into());
                j += 1;
            }
            if !controller.compact_status().try_update(&lock, &self) {
                self.bottom.clear();
                self.next_range = KeyTsRange::default();
                continue;
            };
            return true;
        }
        if self.top.len() == 0 {
            return false;
        }
        controller.compact_status().try_update(&lock, &self)
    }
    async fn fill_tables_level0(&mut self, controller: &LevelsControllerInner) -> bool {
        self.fill_tables_level0_to_levelbase(controller).await
            || self.fill_tables_level0_to_level0(controller).await
    }

    async fn fill_tables_level0_to_levelbase(
        &mut self,
        controller: &LevelsControllerInner,
    ) -> bool {
        if self.next_level_handler.level() == LEVEL0 {
            panic!("Base level can't be zero");
        }
        if 0.0 < self.priority.adjusted() && self.priority.adjusted() < 1.0 {
            return false;
        }

        let lock = CompactPlanLockLevel {
            this_level: self.this_level_handler.read().await,
            next_level: self.next_level_handler.read().await,
        };
        // let lock = self.lock_levels().await;
        if lock.this_level.tables.len() == 0 {
            return false;
        }

        if self.priority.drop_prefixes().len() > 0 {
            self.this_range = lock.this_level.tables.as_slice().into();
            self.top = lock.this_level.tables.clone();
        } else {
            self.top.clear();
            let mut result = KeyTsRange::default();
            for table in lock.this_level.tables.iter() {
                let target = table.into();
                if result.is_overlaps_with(&target) {
                    self.top.push(table.clone());
                    result.extend(target);
                } else {
                    break;
                }
            }
            self.this_range = result;
        }
        let index_range = lock.next_level.overlap_tables(&self.this_range);
        self.bottom = lock.next_level.tables[index_range].to_vec();

        self.next_range = if self.bottom.len() == 0 {
            self.this_range.clone()
        } else {
            self.bottom.as_slice().into()
        };
        controller.compact_status().try_update(&lock, &self)
    }
    async fn fill_tables_level0_to_level0(&mut self, controller: &LevelsControllerInner) -> bool {
        if self.compact_task_id != 0 {
            return false;
        }

        self.next_level_handler = controller.levels()[0].clone();
        self.next_range = KeyTsRange::default();
        self.bottom.clear();

        debug_assert!(self.this_level_handler.level() == LEVEL0);
        debug_assert!(self.next_level_handler.level() == LEVEL0);

        let targets = &mut self.priority.targets();
        let this_level_r = self.this_level_handler.read().await;

        let mut status_w = controller.compact_status().write();

        let now = SystemTime::now();

        let out = this_level_r
            .tables
            .iter()
            .filter(|t| t.size() < 2 * targets.file_size()[0])
            .filter(|t| now.duration_since(t.created_at()).unwrap() > Duration::from_secs(10))
            .filter(|t| !status_w.tables().contains(&t.table_id()))
            .map(|t| t.clone())
            .collect::<Vec<_>>();
        drop(this_level_r);

        if out.len() < 4 {
            return false;
        }

        self.this_range = KeyTsRange::inf_default();
        self.top = out;

        status_w.push(self.this_level_handler.level(), KeyTsRange::inf_default());

        for table in self.top.iter() {
            status_w.tables_mut().insert(table.table_id());
        }
        drop(status_w);
        self.priority.targets_mut().file_size_mut()[0] = usize::MAX;

        true
    }
    pub(super) fn add_splits(&mut self, _controller: &LevelsControllerInner) {
        self.splits.clear();

        let mut width = (self.bottom.len() as f64 / 5.0).ceil() as usize;
        width = width.max(3);

        let mut skr = self.this_range.clone();
        skr.extend_borrow(&self.next_range);

        for (i, t) in self.bottom.iter().enumerate() {
            if i == self.bottom.len() - 1 {
                skr.set_right(KeyTs::default());
                self.splits.push(skr.clone());
                return;
            }
            if i % width == width - 1 {
                let right = KeyTs::new(t.biggest().key().clone(), 0.into());
                skr.set_right(right);

                self.splits.push(skr.clone());
                skr.set_left(skr.right().clone());
            }
        }
    }
    pub(super) fn priority(&self) -> &CompactPriority {
        &self.priority
    }

    pub(super) fn this_level_handler(&self) -> &LevelHandler {
        &self.this_level_handler
    }

    pub(super) fn next_level_handler(&self) -> &LevelHandler {
        &self.next_level_handler
    }

    pub(super) fn splits(&self) -> &[KeyTsRange] {
        self.splits.as_ref()
    }

    pub(super) fn splits_mut(&mut self) -> &mut Vec<KeyTsRange> {
        &mut self.splits
    }

    pub(super) fn top(&self) -> &[Table] {
        self.top.as_ref()
    }

    pub(super) fn bottom(&self) -> &[Table] {
        self.bottom.as_ref()
    }
    pub(super) fn top_bottom(&self) -> Vec<Table> {
        let mut ret = Vec::with_capacity(self.top.len() + self.bottom.len());
        ret.extend_from_slice(self.top());
        ret.extend_from_slice(self.bottom());
        ret
    }
}
impl CompactStatus {
    fn try_update(&self, _lock: &CompactPlanLockLevel, plan: &CompactPlan) -> bool {
        let mut inner_w = self.write();
        let r = inner_w.try_update(_lock, plan);
        drop(inner_w);
        r
    }
}
impl CompactStatusInner {
    // pre condition: hold this level_handler_table and next level_handler_table read lock
    fn try_update(&mut self, _lock: &CompactPlanLockLevel, plan: &CompactPlan) -> bool {
        let this_level: usize = plan.this_level_handler.level().into();
        let next_level: usize = plan.next_level_handler.level().into();

        debug_assert!(this_level < self.levels().len());

        if self.levels()[this_level].is_overlaps_with(&plan.this_range)
            || self.levels()[next_level].is_overlaps_with(&plan.next_range)
        {
            return false;
        }

        self.levels_mut()[this_level]
            .ranges_mut()
            .push(plan.this_range.clone());
        self.levels_mut()[next_level]
            .ranges_mut()
            .push(plan.next_range.clone());

        for table in plan.top.iter() {
            self.tables_mut().insert(table.table_id());
        }
        for table in plan.bottom.iter() {
            self.tables_mut().insert(table.table_id());
        }

        true
    }
}
