use std::{collections::HashMap, sync::Arc, time::SystemTime};

use super::{
    compaction::CompactContext,
    levels::{Level, LevelsController, LEVEL0},
    plan::CompactPlan,
};
#[cfg(feature = "metrics")]
use crate::util::metrics::{add_num_compaction_tables, sub_num_compaction_tables};
use crate::{
    iter::{KvSeekIter, KvSinkIter, SinkIterator},
    kv::{KeyTs, KeyTsBorrow, Meta, TxnTs, ValueMeta, ValuePointer},
    level::compaction::KeyTsRange,
    pb::badgerpb4::ManifestChange,
    table::{
        iter::{SinkMergeIter, SinkMergeNodeIter, SinkTableConcatIter},
        write::TableBuilder,
        Table, TableConfig,
    },
    util::{metrics::add_num_bytes_compaction_written, sys::sync_dir, DBFileId},
};
use anyhow::bail;
use log::{debug, info};
use scopeguard::defer;
use tokio::task::JoinHandle;

impl LevelsController {
    pub(super) async fn run_compact(
        &self,
        compact_task_id: usize,
        level: Level,
        plan: &mut CompactPlan,
        config: TableConfig,
        context: CompactContext,
    ) -> anyhow::Result<()> {
        if plan.priority().targets().file_size().len() == 0 {
            bail!("Filesizes cannot be zero. Targets are not set");
        };

        let time_start = SystemTime::now();

        let this_level = plan.this_level_handler();
        let next_level = plan.next_level_handler();

        debug_assert!(plan.splits().len() == 0);

        if this_level.level() != next_level.level() {
            plan.add_splits(self);
        }

        if plan.splits().len() == 0 {
            plan.splits_mut().push(KeyTsRange::default());
        }

        let new_tables = self
            .compact_build_tables(level, plan, config, context.clone())
            .await?;

        let mut changes =
            Vec::with_capacity(new_tables.len() + plan.top().len() + plan.bottom().len());
        for table in &new_tables {
            changes.push(ManifestChange::new_create(
                table.table_id(),
                plan.next_level_handler().level(),
                table
                    .cipher()
                    .and_then(|x| x.cipher_key_id().into())
                    .unwrap_or_default(),
                table.config().compression(),
            ));
        }
        for table in plan.top() {
            changes.push(ManifestChange::new_delete(table.table_id()))
        }
        for table in plan.bottom() {
            changes.push(ManifestChange::new_delete(table.table_id()));
        }
        context.manifest.push_changes(changes)?;

        let new_tables_size = new_tables.iter().fold(0, |acc, x| acc + x.size());
        let old_tables_size = plan.top().iter().fold(0, |acc, x| acc + x.size())
            + plan.bottom().iter().fold(0, |acc, x| acc + x.size());

        #[cfg(feature = "metrics")]
        add_num_bytes_compaction_written(plan.next_level_handler().level(), new_tables_size);

        plan.next_level_handler()
            .replace(plan.bottom(), &new_tables)
            .await;
        plan.this_level_handler().delete(plan.top()).await;

        let table_to_string = |tables: &[Table]| {
            let mut v = Vec::with_capacity(tables.len());
            tables
                .iter()
                .for_each(|t| v.push(format!("{:>5}", Into::<u32>::into(t.table_id()))));
            v.join(".")
        };

        let dur = SystemTime::now().duration_since(time_start).unwrap();
        if dur.as_secs() > 2 {
            info!(
                "[{compact_task_id}] LOG Compact {}->{} ({},{} -> {} tables with {} splits). 
    [{} {}] -> [{}], took {}, deleted {} bytes",
                plan.this_level_handler().level(),
                plan.next_level_handler().level(),
                plan.top().len(),
                plan.bottom().len(),
                new_tables.len(),
                plan.splits().len(),
                table_to_string(plan.top()),
                table_to_string(plan.bottom()),
                table_to_string(&new_tables),
                dur.as_millis(),
                old_tables_size - new_tables_size
            );
        }

        if plan.this_level_handler().level() != LEVEL0
            && new_tables.len() > self.level_config().level_size_multiplier()
        {
            info!(
                "This Range (num_tables: {})
    Left:{:?},
    Right:{:?},            
            ",
                plan.top().len(),
                plan.this_range().left(),
                plan.this_range().right()
            );
            info!(
                "Next Range (num_tables: {})
    Left:{:?},
    Right:{:?},            
            ",
                plan.bottom().len(),
                plan.next_range().left(),
                plan.next_range().right()
            )
        }

        Ok(())
    }
    pub(super) async fn compact_build_tables(
        &self,
        level: Level,
        plan: &mut CompactPlan,
        config: TableConfig,
        context: CompactContext,
    ) -> anyhow::Result<Vec<Table>> {
        #[cfg(feature = "metrics")]
        {
            let num_tables = plan.top().len() + plan.bottom().len();
            add_num_compaction_tables(num_tables);
            defer!(sub_num_compaction_tables(num_tables));
        }
        let mut valid = Vec::new();
        't: for table in plan.bottom().iter() {
            for prefix in plan.priority().drop_prefixes().iter() {
                if table.smallest().key().starts_with(&prefix)
                    && table.biggest().key().starts_with(&prefix)
                {
                    continue 't;
                };
            }
            valid.push(table.clone());
        }

        let new_iter = || {
            let mut out: Vec<SinkMergeNodeIter> = Vec::new();
            if level == LEVEL0 {
                out = plan
                    .top()
                    .iter()
                    .rev()
                    .map(|t| t.iter(false).into())
                    .collect::<Vec<_>>();
            } else if plan.top().len() > 0 {
                assert_eq!(plan.top().len(), 1);
                out = vec![plan.top()[0].iter(false).into()];
            };
            out.push(SinkTableConcatIter::new(valid.clone(), false).into());
            out
        };
        let plan_clone = Arc::new(plan.clone());

        let mut compact_tasks = Vec::new();
        for kr in plan.splits() {
            let iters = new_iter();
            if let Some(merge_iter) = SinkMergeIter::new(iters) {
                compact_tasks.push(tokio::spawn(self.clone().sub_compact(
                    merge_iter,
                    kr.clone(),
                    plan_clone.clone(),
                    config.clone(),
                    context.clone(),
                )));
            };
        }

        let mut tables = Vec::new();
        for compact in compact_tasks {
            for table_task in compact.await?? {
                tables.push(table_task.await??);
            }
        }
        sync_dir(self.level_config().dir())?;

        tables.sort_unstable_by(|a, b| a.biggest().cmp(b.biggest()));
        Ok(tables)
    }
    async fn sub_compact(
        self,
        mut merge_iter: SinkMergeIter,
        key_range: KeyTsRange,
        plan: Arc<CompactPlan>,
        config: TableConfig,
        compact_context: CompactContext,
    ) -> anyhow::Result<Vec<JoinHandle<anyhow::Result<Table>>>> {
        let all_tables = plan.top_bottom();

        let has_overlap = self
            .check_overlap(&all_tables, plan.next_level_handler().level() + 1)
            .await;

        let discard_ts = compact_context.oracle.discard_at_or_below();

        let mut add_context = AddKeyContext::default();
        let left_bytes = key_range.left().serialize();
        let left_key_borrow: KeyTsBorrow = left_bytes.as_slice().into();

        if !key_range.left().is_empty() {
            merge_iter.seek(left_key_borrow)?;
        } else {
            merge_iter.next()?;
        }

        let mut table_task = Vec::new();
        while merge_iter.valid() {
            if !key_range.right().is_empty() && merge_iter.key().unwrap().ge(key_range.right()) {
                break;
            }
            let mut table_config = config.clone();
            table_config.set_table_size(
                plan.priority().targets().file_size()[plan.next_level_handler().level().to_usize()],
            );
            let mut builder = TableBuilder::new(
                table_config,
                compact_context.key_registry.latest_cipher().await?,
            );
            builder
                .add_keys(
                    &mut merge_iter,
                    &self,
                    &plan,
                    discard_ts,
                    &mut add_context,
                    &key_range,
                    has_overlap,
                )
                .await?;

            if builder.is_empty() {
                let _ = builder.finish().await?;
                continue;
            }

            let path = self
                .get_reserve_file_id()
                .join_dir(self.level_config().dir());
            let index_cache_clone = compact_context.index_cache.clone();
            let block_cache_clone = compact_context.block_cache.clone();
            table_task.push(tokio::spawn(async move {
                builder
                    .build(path, index_cache_clone, block_cache_clone)
                    .await
            }));
        }
        for (fid, discard) in &add_context.discard_stats {
            compact_context
                .discard_stats
                .update(*fid as u64, *discard as i64)
                .await?;
        }
        debug!("Discard stats: {:?}", add_context.discard_stats);
        Ok(table_task)
    }
    async fn check_overlap(&self, tables: &[Table], target_level: Level) -> bool {
        let key_range: KeyTsRange = tables.into();
        for handler in self.levels()[target_level.into()..].iter() {
            let handler_r = handler.read().await;
            let range = handler_r.overlap_tables(&key_range);
            if range.count() > 0 {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Default)]
struct AddKeyContext {
    last_key_ts: KeyTs,
    skip_key: KeyTs,
    num_versions: usize,
    discard_stats: HashMap<u32, u64>,
    first_key_has_discard_set: bool,
}
impl AddKeyContext {
    fn update_discard(&mut self, value: &ValueMeta) {
        if value.meta().contains(Meta::VALUE_POINTER) {
            let vp = ValuePointer::deserialize(value.value());
            match self.discard_stats.get_mut(&vp.fid()) {
                Some(v) => {
                    *v += vp.len() as u64;
                }
                None => {
                    self.discard_stats.insert(vp.fid(), vp.len() as u64);
                }
            };
        }
    }
}
impl TableBuilder {
    async fn add_keys(
        &mut self,
        iter: &mut SinkMergeIter,
        level_controller: &LevelsController,
        plan: &CompactPlan,
        discard_ts: TxnTs,
        context: &mut AddKeyContext,
        key_range: &KeyTsRange,
        has_overlap: bool,
    ) -> anyhow::Result<()> {
        let start = SystemTime::now();
        let mut num_keys = 0;
        let mut num_skips = 0;
        let mut range_check = 0;
        let mut table_key_range = KeyTsRange::default();
        while iter.valid() {
            let key_ts = iter.key().unwrap_or_default();
            let value = iter.value().unwrap_or_default();
            if plan.drop_prefixes().iter().any(|p| key_ts.starts_with(p)) {
                num_skips += 1;
                context.update_discard(&value);
                iter.next()?;
                continue;
            };

            if !context.skip_key.is_empty() {
                if key_ts.key() == context.skip_key.key() {
                    num_skips += 1;
                    context.update_discard(&value);
                    iter.next()?;
                    continue;
                }
                context.skip_key = KeyTs::default();
            }

            if key_ts.key() != context.last_key_ts.key() {
                context.first_key_has_discard_set = false;
                if !key_range.right().is_empty() && iter.key().unwrap().ge(key_range.right()) {
                    break;
                }
                if self.reacded_capacity() {
                    break;
                }
                context.last_key_ts = key_ts.into();
                context.num_versions = 0;
                context.first_key_has_discard_set =
                    value.meta().contains(Meta::DISCARD_EARLIER_VERSIONS);
                if table_key_range.left().is_empty() {
                    table_key_range.set_left(key_ts.into());
                }
                table_key_range.set_right(context.last_key_ts.clone());
                range_check += 1;

                if range_check % 5000 == 0 {
                    async fn exceeds_allowed_overlap(
                        key_range: &KeyTsRange,
                        level_controller: &LevelsController,
                        plan: &CompactPlan,
                    ) -> bool {
                        let n2n = plan.next_level_handler().level().to_usize() + 1;
                        if n2n <= 1 || n2n >= level_controller.levels().len() {
                            return false;
                        }
                        let handler = &level_controller.levels()[n2n];
                        let handler_r = handler.read().await;
                        let range = handler_r.overlap_tables(key_range);
                        range.count() >= 10
                    }
                    if exceeds_allowed_overlap(key_range, level_controller, plan).await {
                        break;
                    }
                }
            }
            let is_expired = value.is_deleted_or_expired();
            if key_ts.txn_ts() <= discard_ts && !value.meta().contains(Meta::MERGE_ENTRY) {
                context.num_versions += 1;
                let last_valid_version = value.meta().contains(Meta::DISCARD_EARLIER_VERSIONS)
                    || context.num_versions
                        == level_controller.level_config().num_versions_to_keep();
                if is_expired || last_valid_version {
                    context.skip_key = key_ts.into();

                    if (is_expired || !last_valid_version) && !has_overlap {
                        num_skips += 1;
                        context.update_discard(&value);
                        iter.next()?;
                        continue;
                    }
                }
            }
            num_keys += 1;

            let mut vptr_len = None;
            if value.meta().contains(Meta::VALUE_POINTER) {
                vptr_len = Some(ValuePointer::deserialize(&value.value()).len());
            }
            if context.first_key_has_discard_set {
                self.push_stale(&key_ts, &value, vptr_len);
            } else if is_expired {
                self.push_stale(&key_ts, &value, vptr_len);
            } else {
                self.push(&key_ts, &value, vptr_len);
            }
        }
        debug!(
            "[{}] LOG Compact. Added {num_keys} keys. Skipped {num_skips} keys. Iteration took: {}",
            plan.compact_task_id(),
            SystemTime::now().duration_since(start).unwrap().as_millis()
        );
        Ok(())
    }
}
