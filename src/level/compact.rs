use std::{collections::HashMap, path::PathBuf, sync::Arc, time::SystemTime};

use super::{
    levels::{Level, LevelsController, LEVEL0},
    plan::CompactPlan,
};
#[cfg(feature = "metrics")]
use crate::util::metrics::{add_num_compaction_tables, sub_num_compaction_tables};
use crate::{
    iter::{KvSeekIter, KvSinkIter, SinkIterator},
    key_registry::KeyRegistry,
    kv::{KeyTs, KeyTsBorrow, Meta, TxnTs, ValueMeta, ValuePointer},
    level::compaction::KeyTsRange,
    table::{
        iter::{SinkMergeIter, SinkMergeNodeIter, SinkTableConcatIter},
        write::TableBuilder,
        Table, TableConfig,
    },
    txn::oracle::Oracle,
    util::{
        cache::{BlockCache, IndexCache},
        DBFileId, Throttle,
    },
};
use anyhow::bail;

use log::{debug, error};
impl LevelsController {
    pub(super) async fn run_compact(
        &self,
        level: Level,
        plan: &mut CompactPlan,
        config: TableConfig,
        key_registry: &KeyRegistry,
        index_cache: IndexCache,
        block_cache: Option<BlockCache>,
        oracle: &Oracle,
    ) -> anyhow::Result<()> {
        if plan.priority().targets().file_size().len() == 0 {
            bail!("Filesizes cannot be zero. Targets are not set");
        };

        let _time_start = SystemTime::now();

        let this_level = plan.this_level_handler();
        let next_level = plan.next_level_handler();

        debug_assert!(plan.splits().len() == 0);

        if this_level.level() != next_level.level() {
            plan.add_splits(self);
        }

        if plan.splits().len() == 0 {
            plan.splits_mut().push(KeyTsRange::default());
        }

        let num_tables = plan.top().len() + plan.bottom().len();
        #[cfg(feature = "metrics")]
        add_num_compaction_tables(num_tables);
        let result = self
            .compact_build_tables(
                level,
                plan,
                config,
                key_registry,
                index_cache,
                block_cache,
                oracle,
            )
            .await;
        #[cfg(feature = "metrics")]
        sub_num_compaction_tables(num_tables);
        result?;
        Ok(())
    }
    pub(super) async fn compact_build_tables(
        &self,
        level: Level,
        plan: &mut CompactPlan,
        config: TableConfig,
        key_registry: &KeyRegistry,
        index_cache: IndexCache,
        block_cache: Option<BlockCache>,
        oracle: &Oracle,
    ) -> anyhow::Result<()> {
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
        let mut throttle = Throttle::new(3);
        for kr in plan.splits() {
            match throttle.acquire().await {
                Ok(permit) => {
                    let iters = new_iter();
                    if let Some(merge_iter) = SinkMergeIter::new(iters) {
                        tokio::spawn(permit.done_with_future(self.clone().sub_compact(
                            merge_iter,
                            kr.clone(),
                            plan_clone.clone(),
                            config.clone(),
                            key_registry.clone(),
                            index_cache.clone(),
                            block_cache.clone(),
                            oracle.clone(),
                        )));
                    };
                }
                Err(e) => {
                    error!("cannot start subcompaction: {}", e);
                    bail!(e)
                }
            }
        }

        Ok(())
    }
    async fn sub_compact(
        self,
        mut merge_iter: SinkMergeIter,
        key_range: KeyTsRange,
        plan: Arc<CompactPlan>,
        config: TableConfig,
        key_registry: KeyRegistry,
        index_cache: IndexCache,
        block_cache: Option<BlockCache>,
        oracle: Oracle,
    ) -> anyhow::Result<()> {
        let all_tables = plan.top_bottom();

        let has_overlap = self
            .check_overlap(&all_tables, plan.next_level_handler().level() + 1)
            .await;

        let discard_ts = oracle.discard_at_or_below();

        let mut context = AddKeyContext::default();
        let left_bytes = key_range.left().serialize();
        let left_key_borrow: KeyTsBorrow = left_bytes.as_slice().into();

        if !key_range.left().is_empty() {
            merge_iter.seek(left_key_borrow)?;
        } else {
            merge_iter.next()?;
        }

        let mut num_builds = 0;
        while merge_iter.valid() {
            if !key_range.right().is_empty() && merge_iter.key().unwrap().ge(key_range.right()) {
                break;
            }
            let mut table_config = config.clone();
            table_config.set_table_size(
                plan.priority().targets().file_size()[plan.next_level_handler().level().to_usize()],
            );
            let mut builder = TableBuilder::new(table_config, key_registry.latest_cipher().await?);
            builder
                .add_keys(
                    &mut merge_iter,
                    &self,
                    &plan,
                    discard_ts,
                    &mut context,
                    &key_range,
                    has_overlap,
                )
                .await?;

            if builder.is_empty() {
                let _ = builder.finish().await?;
                continue;
            }
            num_builds += 1;
            async fn build(
                mut builder: TableBuilder,
                path: PathBuf,
                index_cache: IndexCache,
                block_cache: Option<BlockCache>,
            ) -> anyhow::Result<()> {
                builder.build(path, index_cache, block_cache).await?;
                Ok(())
            }

            tokio::spawn(build(
                builder,
                self.get_reserve_file_id()
                    .join_dir(self.level_config().dir()),
                index_cache.clone(),
                block_cache.clone(),
            ));
        }

        Ok(())
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
