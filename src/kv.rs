use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::VarInt;
// use serde::{Deserialize, Serialize};
use std::{mem, ops::Deref};

use crate::{
    util::{now_since_unix, DBFileId, VlogId},
    vlog::header::VlogEntryHeader,
};

#[derive(Debug, Default, Clone)]
pub struct Entry {
    key_ts: KeyTs,
    value_meta: ValueMeta,
    offset: usize,
    header_len: usize,
    value_threshold: usize,
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        let key_ts = KeyTs::new(key, TxnTs::default());
        let value_meta = ValueMeta {
            value,
            expires_at: PhyTs::default(),
            user_meta: 0,
            meta: Meta::default(),
        };
        Self {
            key_ts,
            offset: 0,
            header_len: 0,
            value_meta,
            value_threshold: 0,
        }
    }

    pub fn key(&self) -> &Bytes {
        &self.key_ts.key()
    }

    pub fn set_key<B: Into<Bytes>>(&mut self, key: B) {
        self.key_ts.set_key(key.into());
    }

    pub fn set_value<B: Into<Bytes>>(&mut self, value: B) {
        self.value_meta.value = value.into();
    }

    pub fn value(&self) -> &Bytes {
        &self.value_meta.value
    }

    pub fn set_expires_at(&mut self, expires_at: u64) {
        self.value_meta.expires_at = expires_at.into();
    }

    pub fn expires_at(&self) -> PhyTs {
        self.value_meta.expires_at
    }

    pub fn version(&self) -> TxnTs {
        self.key_ts.txn_ts()
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn set_user_meta(&mut self, user_meta: u8) {
        self.value_meta.user_meta = user_meta;
    }

    pub fn user_meta(&self) -> u8 {
        self.value_meta.user_meta
    }

    pub fn meta(&self) -> Meta {
        self.value_meta.meta
    }
}
impl Entry {
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
    pub(crate) fn key_ts(&self) -> &KeyTs {
        &self.key_ts
    }
    pub(crate) fn set_meta(&mut self, meta: Meta) {
        self.value_meta.meta = meta;
    }
    pub(crate) fn value_meta(&self) -> &ValueMeta {
        &self.value_meta
    }
    pub(crate) fn meta_mut(&mut self) -> &mut Meta {
        &mut self.value_meta.meta
    }

    pub(crate) fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }
    pub(crate) fn set_version(&mut self, version: TxnTs) {
        self.key_ts.set_txn_ts(version)
    }
    #[inline]
    pub(crate) fn new_ts(
        key_ts: &[u8],
        value: &[u8],
        header: &VlogEntryHeader,
        offset: usize,
        header_len: usize,
    ) -> Self {
        let k: KeyTs = key_ts.into();
        let value_meta = ValueMeta {
            value: value.to_vec().into(),
            expires_at: PhyTs::default(),
            user_meta: 0,
            meta: Meta::default(),
        };
        Self {
            key_ts: k,
            offset,
            header_len,
            value_meta,
            value_threshold: 0,
        }
    }
    #[inline]
    pub(crate) fn is_deleted(&self) -> bool {
        self.meta().contains(Meta::DELETE)
    }

    pub(crate) fn is_expired(&self) -> bool {
        if self.expires_at() == PhyTs::default() {
            return false;
        }
        self.expires_at() <= now_since_unix().as_secs().into()
    }
    pub(crate) fn try_set_value_threshold(&mut self, threshold: usize) {
        if self.value_threshold == 0 {
            self.value_threshold = threshold;
        }
    }

    pub(crate) fn value_threshold(&self) -> usize {
        self.value_threshold
    }
}
///this means TransactionTimestamp
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnTs(u64);
impl TxnTs {
    #[inline(always)]
    pub(crate) fn sub_one(&self) -> Self {
        Self(self.0 - 1)
    }
    #[inline(always)]
    pub(crate) fn add_one_mut(&mut self) {
        self.0 += 1;
    }
    #[inline(always)]
    pub(crate) fn to_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for TxnTs {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct PhyTs(u64);
impl From<u64> for PhyTs {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl PhyTs {
    #[inline(always)]
    pub(crate) fn to_u64(&self) -> u64 {
        self.0
    }
}
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub(crate) struct KeyTs {
    key: Bytes,
    txn_ts: TxnTs,
}
impl PartialOrd for KeyTs {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.key.partial_cmp(&other.key) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        other.txn_ts.partial_cmp(&self.txn_ts)
    }
}
impl Ord for KeyTs {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp(&other.key) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        other.txn_ts.cmp(&self.txn_ts)
    }
}
impl From<&[u8]> for KeyTs {
    fn from(value: &[u8]) -> Self {
        let len = value.len();
        if len <= 8 {
            Self {
                key: value.to_vec().into(),
                txn_ts: 0.into(),
            }
        } else {
            let mut p = &value[len - 8..];
            Self {
                key: value[..len - 8].to_vec().into(),
                txn_ts: p.get_u64().into(),
            }
        }
    }
}
impl KeyTs {
    pub(crate) fn new(key: Bytes, txn_ts: TxnTs) -> Self {
        Self { key, txn_ts }
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.key.len() + 8);
        v.put_slice(&self.key);
        v.put_u64(self.txn_ts.to_u64());
        v
    }

    pub(crate) fn key(&self) -> &Bytes {
        &self.key
    }

    pub(crate) fn txn_ts(&self) -> TxnTs {
        self.txn_ts
    }

    pub(crate) fn set_key(&mut self, key: Bytes) {
        self.key = key;
    }

    pub(crate) fn set_txn_ts(&mut self, txn_ts: TxnTs) {
        self.txn_ts = txn_ts;
    }

    pub(crate) fn len(&self) -> usize {
        self.key.len() + std::mem::size_of::<u64>()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct KeyTsBorrow<'a>(&'a [u8]);
impl<'a> KeyTsBorrow<'a> {
    pub(crate) fn key(&self) -> &[u8] {
        if self.len() > 8 {
            &self[..self.len() - 8]
        } else {
            &self[..]
        }
    }
    pub(crate) fn txn_ts(&self) -> TxnTs {
        if self.len() > 8 {
            let mut p = &self[self.len() - 8..];
            p.get_u64().into()
        } else {
            TxnTs::default()
        }
    }
}
impl Deref for KeyTsBorrow<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl PartialOrd for KeyTsBorrow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let self_split = self.len() - 8;
        let other_split = other.len() - 8;
        match self[..self_split].partial_cmp(&other[..other_split]) {
            Some(std::cmp::Ordering::Equal) => {}
            ord => {
                return ord;
            }
        }
        other[other_split..].partial_cmp(&self[self_split..])
    }
}
impl Ord for KeyTsBorrow<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        KeyTsBorrow::cmp(&self, &other)
    }
}
impl KeyTsBorrow<'_> {
    pub(crate) fn cmp(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        if left.len() > 8 && right.len() > 8 {
            let left_split = left.len() - 8;
            let right_split = right.len() - 8;
            match left[..left_split].cmp(&right[..right_split]) {
                std::cmp::Ordering::Equal => {}
                ord => {
                    return ord;
                }
            }
            right[right_split..].cmp(&left[left_split..])
        } else {
            left.cmp(right)
        }
    }
}
impl<'a> From<&'a [u8]> for KeyTsBorrow<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(value)
    }
}
impl<'a> AsRef<[u8]> for KeyTsBorrow<'a> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl<'a> Into<&'a [u8]> for KeyTsBorrow<'a> {
    fn into(self) -> &'a [u8] {
        &self.0
    }
}
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Meta(u8);
bitflags::bitflags! {
    impl Meta: u8 {
        const DELETE = 1<<0;
        const VALUE_POINTER = 1 << 1;
        const DISCARD_EARLIER_VERSIONS = 1 << 2;
        const MERGE_ENTRY=1<<3;
        const TXN=1<<6;
        const FIN_TXN=1<<7;
    }
}
impl std::fmt::Debug for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}
impl std::fmt::Display for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub(crate) struct ValueMeta {
    value: Bytes,
    expires_at: PhyTs,
    user_meta: u8,
    meta: Meta,
}

impl ValueMeta {
    pub(crate) fn serialized_size(&self) -> usize {
        2 + self.expires_at.0.required_space() + self.value.len()
    }
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut v = vec![0u8; self.serialized_size()];
        v[0] = self.user_meta;
        v[1] = self.meta().0;
        let p = self.expires_at.0.encode_var(&mut v[2..]);
        v[2 + p..].copy_from_slice(self.value());
        v
    }
    pub(crate) fn deserialize(data: &[u8]) -> Self {
        let (expires_at, size) = unsafe { u64::decode_var(&data[2..]).unwrap_unchecked() };
        Self {
            value: data[2 + size..].to_vec().into(),
            expires_at: expires_at.into(),
            user_meta: data[0],
            meta: Meta(data[1]),
        }
    }

    pub(crate) fn meta(&self) -> Meta {
        self.meta
    }

    pub(crate) fn value(&self) -> &Bytes {
        &self.value
    }
}

#[derive(Debug, Default)]
pub(crate) struct ValueInner {
    meta: Meta,
    user_meta: u8,
    expires_at: u64,
    value: Vec<u8>,
}
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ValuePointer {
    fid: u32,
    len: u32,
    offset: u32,
}
impl ValuePointer {
    const SIZE: usize = mem::size_of::<ValuePointer>();
    pub(crate) fn new(fid: u32, len: usize, offset: usize) -> Self {
        Self {
            fid: fid,
            len: len as u32,
            offset: offset as u32,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        *self == ValuePointer::default()
    }

    pub(crate) fn serialize(&self) -> Bytes {
        let mut res = BytesMut::with_capacity(Self::SIZE);
        res.put_u32(self.fid);
        res.put_u32(self.len);
        res.put_u32(self.offset);
        res.freeze()
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> Self {
        let mut p: &[u8] = bytes.as_ref();

        Self {
            fid: p.get_u32(),
            len: p.get_u32(),
            offset: p.get_u32(),
        }
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
    }
}
#[derive(Debug, Default)]
pub(crate) struct ValueStruct {
    inner: ValueInner,
    version: TxnTs,
}
impl ValueStruct {
    // fn encode(&self) -> Result<Vec<u8>, Box<bincode::ErrorKind>> {
    //     DefaultOptions::new()
    //         .with_varint_encoding()
    //         .serialize(&self.inner)
    // }
    pub(crate) fn value(&self) -> &Vec<u8> {
        &self.inner.value
    }
    pub(crate) fn meta(&self) -> Meta {
        self.inner.meta
    }
    pub(crate) fn user_meta(&self) -> u8 {
        self.inner.user_meta
    }
    pub(crate) fn expires_at(&self) -> u64 {
        self.inner.expires_at
    }
    pub(crate) fn version(&self) -> TxnTs {
        self.version
    }
}
#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::kv::{KeyTsBorrow, Meta, ValueMeta};

    use super::KeyTs;

    #[test]
    fn test_bytes_from() {
        use crate::kv::KeyTs;
        let key_ts = KeyTs::new("a".into(), 1.into());
        let bytes = key_ts.serialize();
        assert_eq!(KeyTs::from(bytes.as_ref()), key_ts);
    }
    #[test]
    fn test_ord() {
        let a = KeyTs::new("a".into(), 1.into());
        let b = KeyTs::new("b".into(), 0.into());
        let c = KeyTs::new("a".into(), 2.into());
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(a.cmp(&c), Ordering::Greater);
        let a = &a.serialize();
        let b = &b.serialize();
        let c = &c.serialize();
        let a = KeyTsBorrow(a);
        let b = KeyTsBorrow(b);
        let c = KeyTsBorrow(c);
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(a.cmp(&c), Ordering::Greater);
    }
    #[test]
    fn test_serialize() {
        let mut v = ValueMeta::default();
        v.value = String::from("abc").as_bytes().to_vec().into();
        v.expires_at = 123456789.into();
        v.meta = Meta(1);
        assert_eq!(v.serialized_size(), 9);
        assert_eq!(v, ValueMeta::deserialize(&v.serialize()));
    }
    #[test]
    fn test_meta() {
        assert!(Meta(0).is_empty());
        assert!(!Meta(1).is_empty())
    }
}
