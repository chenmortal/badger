use crate::kv::{KeyTs, KeyTsBorrow, TxnTs, ValueMeta};

use super::MemTable;

impl MemTable {
    #[inline]
    pub(crate) fn get(&self, key_ts: &KeyTs, allow_near: bool) -> Option<(TxnTs, ValueMeta)> {
        self.skip_list
            .get_key_value(&key_ts.serialize(), allow_near)
            .and_then(|(k, v)| {
                let key: KeyTsBorrow = k.into();
                if key.key() == key_ts.key() {
                    if let Some(value_meta) = ValueMeta::deserialize(v) {
                        return Some((key.txn_ts(), value_meta));
                    }
                }
                None
            })
    }
}
