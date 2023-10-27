use crate::kv::{KeyTs, ValueMeta};

use super::MemTable;

impl MemTable {
    #[inline]
    pub(crate) fn get(&self, key_ts: &KeyTs) -> Option<ValueMeta> {
        self.skip_list
            .get(&key_ts.serialize())
            .and_then(|x| ValueMeta::deserialize(x).into())
    }
}
