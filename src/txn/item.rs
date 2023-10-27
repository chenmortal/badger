use std::{ops::Deref, sync::Arc};

use crate::kv::{Meta, PhyTs, TxnTs};

pub(crate) const PRE_FETCH_STATUS: u8 = 1;
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
    pub(crate) key: Vec<u8>,
    pub(crate) vptr: Vec<u8>,
    pub(crate) val: Vec<u8>,
    pub(crate) version: TxnTs,
    pub(crate) expires_at: PhyTs,
    pub(crate) meta: Meta,
    pub(crate) user_meta: u8,
    pub(crate) status: u8,
    // pub(crate) db: Option<DB>,
    // pub(crate) next: Option<Item>,
}

impl ItemInner {
    pub fn version(&self) -> TxnTs {
        self.version
    }

    pub fn meta(&self) -> Meta {
        self.meta
    }
}
