use super::{level_handler::LevelHandler, levels::LevelsController};
#[cfg(feature = "metrics")]
use crate::util::metrics::{add_num_bloom_not_exist_level, add_num_lsm_gets};
use crate::{
    kv::{KeyTs, TxnTs, ValueMeta},
    table::Table,
};

impl LevelsController {
    pub(crate) fn get(
        &self,
        key: &KeyTs,
        max_txn_ts: TxnTs,
        max_value_meta: &ValueMeta,
        start_level: usize,
    ) {
        let version = key.txn_ts();
        for level_handler in &self.levels()[start_level..] {}
    }
}
impl LevelHandler {
    pub(crate) async fn get(&self, key: &KeyTs) -> anyhow::Result<()> {
        if let Some(tables) = self.get_table_for_key(key).await {
            for table in tables {
                #[cfg(feature = "async_cache")]
                if !table.may_contain_key(key).await? {
                    #[cfg(feature = "metrics")]
                    add_num_bloom_not_exist_level(self.level(), 1);
                    continue;
                };
                #[cfg(not(feature = "async_cache"))]
                if !table.may_contain_key(key)? {
                    #[cfg(feature = "metrics")]
                    add_num_bloom_not_exist_level(self.level(), 1);
                    continue;
                };
                #[cfg(feature = "metrics")]
                add_num_lsm_gets(self.level(), 1);
            }
        };
        Ok(())
    }
    pub(crate) async fn get_table_for_key(&self, key: &KeyTs) -> Option<Vec<Table>> {
        let table_handlers = self.read().await;
        if self.level() == 0 {
            table_handlers
                .tables
                .iter()
                .rev()
                .map(|x| x.clone())
                .collect::<Vec<_>>()
                .into()
        } else {
            let table_index = table_handlers
                .tables
                .binary_search_by(|t| t.biggest().cmp(&key.serialize()))
                .ok()
                .unwrap();
            if table_index >= table_handlers.tables.len() {
                return None;
            }
            vec![table_handlers.tables[table_index].clone()].into()
        }
    }
}
