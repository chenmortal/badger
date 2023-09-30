use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    kv::KeyTs,
    vlog::{header::EntryHeader, BIT_TXN, BIT_FIN_TXN},
};

use super::TxnTs;
#[derive(Debug, Default, Clone)]
pub struct Entry {
    key_ts: KeyTs,
    value: Vec<u8>,
    expires_at: u64,
    offset: usize,
    user_meta: u8,
    meta: u8,
    header_len: usize,
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
            header_len: 0,
        }
    }

    #[inline]
    pub(crate) fn new_ts(
        key_ts: &[u8],
        value: &[u8],
        header: &EntryHeader,
        offset: usize,
        header_len: usize,
    ) -> Self {
        let k: KeyTs = key_ts.into();
        Self {
            key_ts: k,
            value: value.to_vec(),
            expires_at: header.expires_at(),
            offset,
            user_meta: header.user_meta(),
            meta: header.meta(),
            header_len,
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
        self.key_ts.txn_ts()
    }
    pub(crate) fn key_ts(&self) ->&KeyTs{
        &self.key_ts
    }
    pub fn offset(&self) -> usize {
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

    pub(crate) fn set_user_meta(&mut self, user_meta: u8) {
        self.user_meta = user_meta;
    }

    pub(crate) fn set_expires_at(&mut self, expires_at: u64) {
        self.expires_at = expires_at;
    }

    pub(crate) fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    pub(crate) fn header_len(&self) -> usize {
        self.header_len
    }

    pub(crate) fn set_header_len(&mut self, header_len: usize) {
        self.header_len = header_len;
    }
    pub(crate) fn estimate_size(&self, threshold: usize) -> usize {
        if self.value().len() < threshold {
            self.key().len() + self.value().len() + 2
        } else {
            self.key().len() + 12 + 2
        }
    }
    pub(crate) fn clean_meta_bit(&mut self, clean_meta: u8) {
        self.meta = self.meta & (!clean_meta);
    }
}

// decorated entry with val_threshold
#[derive(Debug, Clone)]
pub(crate) struct DecEntry {
    pub(crate) entry: Entry,
    pub(crate) value_threshold: usize,
}
impl From<Entry> for DecEntry {
    fn from(value: Entry) -> Self {
        DecEntry {
            entry: value,
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
    pub(crate) fn try_set_value_threshold(&mut self, threshold: usize) {
        if self.value_threshold == 0 {
            self.value_threshold = threshold;
        }
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
    }
}