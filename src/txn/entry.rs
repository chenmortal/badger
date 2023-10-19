use std::ops::{Deref, DerefMut};

use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};

use super::TxnTs;
use crate::{kv::KeyTs, vlog::header::EntryHeader};
use std::fmt::Debug;
#[derive(Debug, Default, Clone)]
pub struct Entry {
    key_ts: KeyTs,
    value_meta: ValueMeta,
    offset: usize,
    header_len: usize,
}
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub(crate) struct ValueMeta {
    value: Vec<u8>,
    expires_at: u64,
    user_meta: u8,
    meta: EntryMeta,
}
lazy_static! {
    static ref BINCODE_OPT: DefaultOptions = DefaultOptions::new();
}
impl ValueMeta {
    pub(crate) fn encode_size(&self) -> Result<u64, Box<bincode::ErrorKind>> {
        BINCODE_OPT.serialized_size(self)
    }
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Box<bincode::ErrorKind>> {
        BINCODE_OPT.serialize(&self)
    }
    pub(crate) fn decode(data: &[u8]) -> Result<ValueMeta, Box<bincode::ErrorKind>> {
        // BINCODE_OPT.serialized_size(t)
        BINCODE_OPT.deserialize::<Self>(data)
    }

    pub(crate) fn meta(&self) -> EntryMeta {
        self.meta
    }

    pub(crate) fn value(&self) -> &[u8] {
        self.value.as_ref()
    }
}
// impl From<&[u8]> for ValueMeta {
//     fn from(value: &[u8]) -> Self {
//         let p = Self::decode(value);
//     }
// }
#[test]
fn test_encode() {
    let mut v = ValueMeta::default();
    v.value = String::from("abc").as_bytes().to_vec();
    v.expires_at = 1;
    dbg!(v.encode_size().unwrap());
    let ve = v.encode().unwrap();
    dbg!(ve.len());
    let vd = ValueMeta::decode(&ve).unwrap();
    assert_eq!(vd, v);
}
#[derive(Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryMeta(u8);
bitflags::bitflags! {
    impl EntryMeta: u8 {
        const DELETE = 1<<0;
        const VALUE_POINTER = 1 << 1;
        const DISCARD_EARLIER_VERSIONS = 1 << 2;
        const MERGE_ENTRY=1<<3;
        const TXN=1<<6;
        const FIN_TXN=1<<7;
    }
}
impl std::fmt::Debug for EntryMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}
impl std::fmt::Display for EntryMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}

impl Entry {
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        let key_ts = KeyTs::new(key, TxnTs::default());
        let value_meta = ValueMeta {
            value: value.to_vec(),
            expires_at: 0,
            user_meta: 0,
            meta: EntryMeta::default(),
        };
        Self {
            key_ts,
            // value: value.to_vec(),
            // expires_at: 0,
            offset: 0,
            // user_meta: 0,
            // meta: EntryMeta::default(),
            header_len: 0,
            value_meta,
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
        let value_meta = ValueMeta {
            value: value.to_vec(),
            expires_at: 0,
            user_meta: 0,
            meta: EntryMeta::default(),
        };
        Self {
            key_ts: k,
            // value: value.to_vec(),
            // expires_at: header.expires_at(),
            offset,
            // user_meta: header.user_meta(),
            // meta: header.meta(),
            header_len,
            value_meta,
        }
    }
    pub fn set_key(&mut self, key: Vec<u8>) {
        self.key_ts.set_key(key);
    }

    pub fn set_value(&mut self, value: Vec<u8>) {
        self.value_meta.value = value;
    }

    pub fn key(&self) -> &[u8] {
        self.key_ts.key()
    }

    pub fn value(&self) -> &[u8] {
        self.value_meta.value.as_ref()
    }

    pub fn expires_at(&self) -> u64 {
        self.value_meta.expires_at
    }

    pub fn version(&self) -> TxnTs {
        self.key_ts.txn_ts()
    }
    pub(crate) fn key_ts(&self) -> &KeyTs {
        &self.key_ts
    }
    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn user_meta(&self) -> u8 {
        self.value_meta.user_meta
    }

    pub fn meta(&self) -> EntryMeta {
        self.value_meta.meta
    }

    pub(crate) fn set_meta(&mut self, meta: EntryMeta) {
        self.value_meta.meta = meta;
    }

    pub(crate) fn set_version(&mut self, version: TxnTs) {
        self.key_ts.set_txn_ts(version)
    }

    pub fn meta_mut(&mut self) -> &mut EntryMeta {
        &mut self.value_meta.meta
    }

    pub(crate) fn set_user_meta(&mut self, user_meta: u8) {
        self.value_meta.user_meta = user_meta;
    }

    pub(crate) fn set_expires_at(&mut self, expires_at: u64) {
        self.value_meta.expires_at = expires_at;
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

    // pub(crate) fn clean_meta_bit(&mut self, clean_meta: u8) {
    //     self.meta = self.meta & (!clean_meta);
    // }
    // pub(crate) fn add_meta_bit(&mut self, add_meta: u8) {
    //     self.meta |= add_meta;
    // }

    pub fn value_meta(&self) -> &ValueMeta {
        &self.value_meta
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
