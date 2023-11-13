use bytes::Bytes;

use super::{level_handler::LevelHandler, levels::LevelsController};
#[cfg(feature = "metrics")]
use crate::util::metrics::{add_num_bloom_not_exist_level, add_num_lsm_gets};
use crate::{
    iter::{KvSeekIter, KvSinkIter},
    kv::{KeyTs, KeyTsBorrow, TxnTs, ValueMeta},
    table::Table,
};

impl LevelsController {
    pub(crate) async fn get(
        &self,
        key: &KeyTs,
        start_level: usize,
    ) -> anyhow::Result<Option<(TxnTs, ValueMeta)>> {
        let key_ts_bytes: Bytes = key.serialize().into();
        let mut max_txn = TxnTs::default();
        let mut max_value = None;
        for level_handler in &self.levels()[start_level..] {
            if let Some((txn, value)) = level_handler.get(&key_ts_bytes).await? {
                if txn == key.txn_ts() {
                    return Ok(Some((txn, value)));
                }
                if txn > max_txn {
                    max_txn = txn;
                    max_value = value.into();
                }
            };
        }
        if max_txn != TxnTs::default() && max_value.is_some() {
            return Ok(Some((max_txn, max_value.unwrap())));
        }
        Ok(None)
    }
}
impl LevelHandler {
    pub(crate) async fn get(
        &self,
        key_ts_bytes: &Bytes,
    ) -> anyhow::Result<Option<(TxnTs, ValueMeta)>> {
        if let Some(tables) = self.get_table_for_key(key_ts_bytes).await {
            let mut handles = Vec::with_capacity(tables.len());
            for table in tables {
                #[cfg(feature = "async_cache")]
                if !table.may_contain_key(key_ts_bytes.as_ref().into()).await? {
                    #[cfg(feature = "metrics")]
                    add_num_bloom_not_exist_level(self.level(), 1);
                    continue;
                };
                #[cfg(not(feature = "async_cache"))]
                if !table.may_contain_key(key_ts_bytes.as_ref().into())? {
                    #[cfg(feature = "metrics")]
                    add_num_bloom_not_exist_level(self.level(), 1);
                    continue;
                };
                async fn table_seek(
                    level: usize,
                    table: Table,
                    k: Bytes,
                ) -> anyhow::Result<Option<(TxnTs, ValueMeta)>> {
                    #[cfg(feature = "metrics")]
                    add_num_lsm_gets(level, 1);
                    let mut iter = table.iter(true);
                    if iter.seek(k.as_ref().into())? {
                        if let Some(key) = iter.key() {
                            if KeyTsBorrow::equal_key(k.as_ref(), &key) {
                                if let Some(v) = iter.value() {
                                    let txn = key.txn_ts();
                                    return Ok(Some((txn, v)));
                                };
                            };
                        };
                    };
                    Ok(None)
                }
                handles.push(table_seek(self.level(), table, key_ts_bytes.clone()));
            }
            let mut max_txn = TxnTs::default();
            let mut max_value = None;
            for handle in handles {
                if let Some((txn, value)) = handle.await? {
                    if txn > max_txn {
                        max_txn = txn;
                        max_value = value.into();
                    }
                };
            }
            if max_txn != TxnTs::default() && max_value.is_some() {
                let value = max_value.unwrap();
                if !value.meta().is_empty() {
                    return Ok(Some((max_txn, value)));
                }
            }
        };
        Ok(None)
    }
    pub(crate) async fn get_table_for_key(&self, key_ts_bytes: &Bytes) -> Option<Vec<Table>> {
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
                .binary_search_by(|t| t.biggest().cmp(&key_ts_bytes))
                .ok()
                .unwrap();
            if table_index >= table_handlers.tables.len() {
                return None;
            }
            vec![table_handlers.tables[table_index].clone()].into()
        }
    }
}
