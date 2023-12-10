use std::{collections::HashMap, sync::Arc, time::SystemTime};

use super::{
    levels::{Level, LevelsController, LEVEL0},
    plan::CompactPlan,
};
#[cfg(feature = "metrics")]
use crate::util::metrics::{add_num_compaction_tables, sub_num_compaction_tables};
use crate::{
    level::compaction::KeyTsRange,
    table::{
        iter::{SinkMergeIter, SinkMergeNodeIter, SinkTableConcatIter},
        Table,
    },
    txn::oracle::Oracle,
    util::Throttle,
};
use anyhow::bail;
use log::error;
impl LevelsController {
    pub(super) async fn run_compact(
        &self,
        level: Level,
        plan: &mut CompactPlan,
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
        let result = self.compact_build_tables(level, plan, oracle).await;
        #[cfg(feature = "metrics")]
        sub_num_compaction_tables(num_tables);
        result?;
        Ok(())
    }
    pub(super) async fn compact_build_tables(
        &self,
        level: Level,
        plan: &mut CompactPlan,
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
        merge_iter: SinkMergeIter,
        key_range: KeyTsRange,
        plan: Arc<CompactPlan>,
        oracle: Oracle,
    ) -> anyhow::Result<()> {
        let all_tables = plan.top_bottom();

        let has_overlap = self
            .check_overlap(&all_tables, plan.next_level_handler().level() + 1)
            .await;

        let discard_ts = oracle.discard_at_or_below();

        let discard_status: HashMap<u32, i64> = HashMap::new();

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
