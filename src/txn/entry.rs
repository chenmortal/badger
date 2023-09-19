use std::{ops::Deref, sync::Arc};
#[derive(Debug,Default)]
pub struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
    expires_at: u64,
    version: u64,
    offset: u32,
    user_meta: u8,
    meta: u8,
    pubheader_len: usize,
    val_threshold: u64,
}

impl Entry {
    pub fn new(key:&[u8],value:&[u8])->Self{
        let mut entry = Entry::default();
        entry.key=key.to_vec();
        entry.value=value.to_vec();
        entry
    }
    pub fn set_key(&mut self, key: Vec<u8>) {
        self.key = key;
    }

    pub fn set_value(&mut self, value: Vec<u8>) {
        self.value = value;
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    pub fn version(&self) -> u64 {
        self.version
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
}
