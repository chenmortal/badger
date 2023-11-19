use std::{ops::Deref, sync::Arc};

use crate::kv::{KeyTs, ValueMeta};
#[derive(Debug)]
pub(crate) enum PrefetchStatus {
    Prefetched,
    NoPrefetched,
}
impl Default for PrefetchStatus {
    fn default() -> Self {
        Self::NoPrefetched
    }
}
#[derive(Debug)]
pub struct Item(Arc<ItemInner>);
impl From<ItemInner> for Item {
    fn from(value: ItemInner) -> Self {
        Self(Arc::new(value))
    }
}
impl Deref for Item {
    type Target = ItemInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Default)]
pub struct ItemInner {
    key_ts: KeyTs,
    value_meta: ValueMeta,
    status: PrefetchStatus,
}

impl ItemInner {
    pub(crate) fn set_key_ts(&mut self, key_ts: KeyTs) {
        self.key_ts = key_ts;
    }

    pub(crate) fn set_value_meta(&mut self, value_meta: ValueMeta) {
        self.value_meta = value_meta;
    }

    pub(crate) fn set_status(&mut self, status: PrefetchStatus) {
        self.status = status;
    }
}
