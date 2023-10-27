use std::io::Read;

use bytes::{Buf, BufMut};
use integer_encoding::{VarInt, VarIntReader};

use crate::kv::{Entry, Meta, PhyTs};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct VlogEntryHeader {
    key_len: u32,
    value_len: u32,
    expires_at: PhyTs,
    meta: Meta,
    user_meta: u8,
}
pub(crate) const MAX_HEADER_SIZE: usize = 22;
impl VlogEntryHeader {
    pub(crate) fn new(e: &Entry) -> Self {
        Self {
            key_len: e.key_ts().len() as u32,
            value_len: e.value().len() as u32,
            expires_at: e.expires_at(),
            meta: e.meta(),
            user_meta: e.user_meta(),
        }
    }
    // +------+----------+------------+--------------+-----------+
    // | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
    // +------+----------+------------+--------------+-----------+
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(22);
        out.put_u8(self.meta.bits());
        out.put_u8(self.user_meta);
        out.put_slice(self.key_len.encode_var_vec().as_ref());
        out.put_slice(self.value_len.encode_var_vec().as_ref());
        out.put_slice(self.expires_at.to_u64().encode_var_vec().as_ref());
        out
    }
    pub(crate) fn decode(mut buf: &[u8]) -> (VlogEntryHeader, usize) {
        let meta = Meta::from_bits_retain(buf.get_u8());
        let user_meta = buf.get_u8();
        let mut index = 2;

        let (key_len, count) = u32::decode_var(buf).unwrap();
        index += count;
        buf.advance(count);

        let (value_len, count) = u32::decode_var(buf).unwrap();
        index += count;
        buf.advance(count);

        let (expires_at, count) = u64::decode_var(buf).unwrap();
        index += count;
        let e = Self {
            key_len,
            value_len,
            expires_at: expires_at.into(),
            meta,
            user_meta,
        };
        (e, index)
    }
    pub(super) fn decode_from<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let meta: u8 = 0;
        reader.read_exact(&mut [meta])?;
        let meta = Meta::from_bits_retain(meta);
        let user_meta: u8 = 0;
        reader.read_exact(&mut [user_meta])?;

        let key_len = reader.read_varint::<u32>()?;
        let value_len = reader.read_varint::<u32>()?;
        let expires_at = reader.read_varint::<u64>()?;

        Ok(Self {
            key_len,
            value_len,
            expires_at: expires_at.into(),
            meta,
            user_meta,
        })
    }

    pub(crate) fn key_len(&self) -> u32 {
        self.key_len
    }

    pub(crate) fn value_len(&self) -> u32 {
        self.value_len
    }

    pub(crate) fn meta(&self) -> Meta {
        self.meta
    }

    pub(crate) fn user_meta(&self) -> u8 {
        self.user_meta
    }

    pub(crate) fn expires_at(&self) -> PhyTs {
        self.expires_at
    }
}
