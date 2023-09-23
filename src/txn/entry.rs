use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc, atomic::AtomicUsize,
    },
};

use crate::kv::KeyTs;

use super::TxnTs;
#[derive(Debug, Default,Clone)]
pub struct Entry {
    key_ts: KeyTs,
    value: Vec<u8>,
    expires_at: u64,
    offset: u32,
    user_meta: u8,
    meta: u8,
}

impl Entry {
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        let key_ts = KeyTs::new(key, TxnTs::default());
        Self {
            key_ts,
            value: value.to_vec(),
            expires_at: 0,
            offset: 0,
            user_meta: 0,
            meta: 0,
        }
    }
    pub fn set_key(&mut self, key: Vec<u8>) {
        self.key_ts.set_key(key);
    }

    pub fn set_value(&mut self, value: Vec<u8>) {
        self.value = value;
    }

    pub fn key(&self) -> &[u8] {
        self.key_ts.key()
    }

    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    pub fn version(&self) -> TxnTs {
        *self.key_ts.txn_ts()
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn user_meta(&self) -> u8 {
        self.user_meta
    }

    pub fn meta(&self) -> u8 {
        self.meta
    }

    pub(crate) fn set_meta(&mut self, meta: u8) {
        self.meta = meta;
    }

    pub(crate) fn set_version(&mut self, version: TxnTs) {
        self.key_ts.set_txn_ts(version)
    }

    pub fn meta_mut(&mut self) -> &mut u8 {
        &mut self.meta
    }
}

///decorated entry with val_threshold
#[derive(Debug,Clone)]
pub(crate) struct DecEntry {
    entry: Entry,
    // header_len: Arc<AtomicUsize>,
    value_threshold: Arc<AtomicUsize>,
}
impl From<Entry> for DecEntry {
    fn from(value: Entry) -> Self {
        
        DecEntry {
            entry: value,
            // header_len: Default::default(),
            value_threshold: Default::default(),
        }
    }
}
impl Deref for DecEntry {
    type Target = Entry;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}
impl DerefMut for DecEntry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entry
    }
}
impl DecEntry {
    pub(crate) fn set_value_threshold(&self, threshold: usize) {
        self.value_threshold
            .store(threshold, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn estimate_size(&self) -> usize {
        if self.value().len() < self.value_threshold() {
            self.key().len() + self.value().len() + 2
        } else {
            self.key().len() + 12 + 2
        }
    }

    pub(crate) fn value_threshold(&self) -> usize {
        self.value_threshold
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
