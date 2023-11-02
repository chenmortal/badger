use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::bail;
use bytes::Buf;
use prost::Message;

use crate::iter::DoubleEndedSinkIter;
use crate::iter::DoubleEndedSinkIterator;
use crate::iter::KvDoubleEndedSinkIter;
use crate::iter::KvSinkIter;
use crate::iter::SinkIter;
use crate::iter::SinkIterator;
use crate::kv::KeyTsBorrow;
use crate::kv::ValueMeta;
use crate::pb::badgerpb4::Checksum;
use crate::util::SSTableId;

use super::bytes_to_vec_u32;
use super::write::EntryHeader;
use super::write::HEADER_SIZE;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BlockId(u32);
impl Deref for BlockId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<u32> for BlockId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Into<u32> for BlockId {
    fn into(self) -> u32 {
        self.0
    }
}
impl From<usize> for BlockId {
    fn from(value: usize) -> Self {
        Self(value as u32)
    }
}
impl Into<usize> for BlockId {
    fn into(self) -> usize {
        self.0 as usize
    }
}
#[derive(Debug, Clone)]
pub(crate) struct Block(Arc<BlockInner>);
impl Deref for Block {
    type Target = BlockInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
pub(crate) struct BlockInner {
    table_id: SSTableId,
    block_id: BlockId,
    block_offset: u32,
    data: Vec<u8>, //actual data + entry_offsets + num_entries
    entries_index_start: usize,
    entry_offsets: Vec<u32>,
    checksum: Vec<u8>,
    checksum_len: usize,
}
impl Block {
    #[inline]
    pub(crate) fn deserialize(
        table_id: SSTableId,
        block_id: BlockId,
        block_offset: u32,
        data: Vec<u8>,
    ) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(BlockInner::deserialize(
            table_id,
            block_id,
            block_offset,
            data,
        )?)))
    }

    #[inline]
    pub(crate) fn get_entry_offsets(&self) -> &Vec<u32> {
        &self.0.entry_offsets
    }
    #[inline]
    pub(crate) fn get_actual_data(&self) -> &[u8] {
        &self.0.data[..self.0.entries_index_start]
    }
    #[inline]
    pub(crate) fn get_offset(&self) -> u32 {
        self.0.block_offset
    }
}

impl BlockInner {
    #[inline(always)]
    pub(crate) fn deserialize(
        table_id: SSTableId,
        block_id: BlockId,
        block_offset: u32,
        mut data: Vec<u8>,
    ) -> anyhow::Result<Self> {
        //read checksum len
        let mut read_pos = data.len() - 4;
        let mut checksum_len = &data[read_pos..read_pos + 4];
        let checksum_len = checksum_len.get_u32() as usize;
        if checksum_len > data.len() {
            bail!(
                "Invalid checksum len. Either the data is corrupt 
                        or the table Config are incorrectly set"
            );
        }

        //read checksum
        read_pos -= checksum_len;
        let checksum = (&data[read_pos..read_pos + checksum_len]).to_vec();

        data.truncate(read_pos);

        //read num_entries
        read_pos -= 4;
        let mut num_entries = &data[read_pos..read_pos + 4];
        let num_entries = num_entries.get_u32() as usize;

        //read entries_index_start
        let entries_index_start = read_pos - (num_entries * 4);

        let entry_offsets = bytes_to_vec_u32(&data[entries_index_start..read_pos]);

        Ok(Self {
            data,
            entries_index_start,
            entry_offsets,
            checksum,
            checksum_len,
            table_id,
            block_id,
            block_offset,
        })
    }
    fn data(&self) -> &[u8] {
        &self.data[..self.entries_index_start]
    }
    pub(super) fn iter(&self) -> SinkBlockIter {
        self.into()
    }
    pub(crate) fn verify(&self) -> anyhow::Result<()> {
        let checksum = Checksum::decode(self.checksum.as_ref())
            .map_err(|e| anyhow!("Failed decode pb::checksum for block for {}", e))?;
        checksum.verify(&self.data)?;
        Ok(())
    }
}

pub(crate) struct SinkBlockIter<'a> {
    inner: &'a BlockInner,
    base_key: &'a [u8],
    key: Vec<u8>,
    header: EntryHeader,
    entry_id: Option<usize>,

    back_key: Vec<u8>,
    back_header: EntryHeader,
    back_entry_id: Option<usize>,
}
impl<'a> From<&'a BlockInner> for SinkBlockIter<'a> {
    fn from(value: &'a BlockInner) -> Self {
        Self {
            inner: value,
            base_key: Default::default(),
            key: Default::default(),
            header: Default::default(),
            entry_id: None,
            back_key: Default::default(),
            back_header: Default::default(),
            back_entry_id: None,
        }
    }
}

impl<'a> SinkIter for SinkBlockIter<'a> {
    type Item = usize;

    fn item(&self) -> Option<&Self::Item> {
        self.entry_id.as_ref()
    }
}

impl<'a> DoubleEndedSinkIter for SinkBlockIter<'a> {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.back_entry_id.as_ref()
    }
}
//base key 123 1  iter.key=null
//123 100
//123 121  pre_overlap=6 overlap:4 -> iter.key=123 1;  diffkey=21  -> iter.key=123 121 (just create iter, and may not seek to  start , so also pre_overlap==0)
//123 122  pre_overlap=4 overlap:5 -> iter.key=123 12; diffkey=2   -> iter.key=123 122
//123 211  pre_overlap=5 overlap:3 -> iter.key=123  ;  diffkey=211 -> iter.key=123 211
impl<'a> SinkIterator for SinkBlockIter<'a> {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        match self.entry_id {
            Some(id) => {
                match self.back_entry_id {
                    Some(back_id) => {
                        if id + 1 == back_id {
                            return Ok(false);
                        }
                    }
                    None => {
                        if id == self.inner.entry_offsets.len() - 1 {
                            return Ok(false);
                        }
                    }
                }
                self.entry_id = Some(id + 1);
                let next_entry_offset = self.inner.entry_offsets[id + 1] as usize;
                let data = &self.inner.data()[next_entry_offset..];
                let next_header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                let prev_overlap = self.header.get_overlap();
                let next_overlap = next_header.get_overlap();
                if next_overlap > prev_overlap {
                    self.key.truncate(prev_overlap);
                    self.key
                        .extend_from_slice(&self.base_key[prev_overlap..next_overlap]);
                } else {
                    self.key.truncate(next_overlap);
                }
                self.key
                    .extend_from_slice(&data[HEADER_SIZE..HEADER_SIZE + next_header.get_diff()]);
                self.header = next_header;
                return Ok(true);
            }
            None => {
                if self.inner.entry_offsets.len() == 0 {
                    return Ok(false);
                }

                if self.base_key.len() == 0 {
                    let data = self.inner.data();
                    let header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                    self.base_key = &data[HEADER_SIZE..HEADER_SIZE + header.get_diff()];
                    self.header = header;
                }
                self.key = self.base_key.to_vec();
                self.entry_id = 0.into();
                return Ok(true);
            }
        }
    }
}
impl<'a> DoubleEndedSinkIterator for SinkBlockIter<'a> {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        match self.back_entry_id {
            Some(back_id) => {
                match self.entry_id {
                    Some(id) => {
                        if back_id - 1 == id {
                            return Ok(false);
                        }
                    }
                    None => {
                        if back_id == 0 {
                            return Ok(false);
                        }
                    }
                }

                self.back_entry_id = Some(back_id - 1);
                let next_back_entry_offset = self.inner.entry_offsets[back_id - 1] as usize;
                let data = &self.inner.data()[next_back_entry_offset..];
                let next_back_header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                let prev_back_overlap = self.back_header.get_overlap();
                let next_back_overlap = next_back_header.get_overlap();

                if next_back_overlap > prev_back_overlap {
                    self.back_key.truncate(prev_back_overlap);
                    self.back_key
                        .extend_from_slice(&self.base_key[prev_back_overlap..next_back_overlap]);
                } else {
                    self.back_key.truncate(next_back_overlap);
                }
                self.back_key.extend_from_slice(
                    &data[HEADER_SIZE..HEADER_SIZE + next_back_header.get_diff()],
                );

                self.back_header = next_back_header;
                return Ok(true);
            }
            None => {
                if self.inner.entry_offsets.len() == 0 {
                    return Ok(false);
                }

                if self.base_key.len() == 0 {
                    let data = self.inner.data();
                    let header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                    self.base_key = &data[HEADER_SIZE..HEADER_SIZE + header.get_diff()];
                    self.header = header;
                }

                let last_offset = *self.inner.entry_offsets.last().unwrap() as usize;
                let data = &self.inner.data()[last_offset..];
                self.back_header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                self.back_key =
                    data[HEADER_SIZE..HEADER_SIZE + self.back_header.get_diff()].to_vec();
                self.back_entry_id = Some(self.inner.entry_offsets.len() - 1);
                return Ok(true);
            }
        }
    }
}

impl<'a> KvSinkIter<ValueMeta> for SinkBlockIter<'a> {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        if self.key.len() == 0 {
            return None;
        }
        return Some(self.key.as_slice().into());
    }

    fn value(&self) -> Option<ValueMeta> {
        if let Some(entry_id) = self.entry_id {
            let next_entry_id = entry_id + 1;
            let end_offset = if next_entry_id == self.inner.entry_offsets.len() {
                self.inner.entries_index_start
            } else {
                self.inner.entry_offsets[next_entry_id] as usize
            };
            let start_offset =
                self.inner.entry_offsets[entry_id] as usize + HEADER_SIZE + self.header.get_diff();
            let value = &self.inner.data()[start_offset..end_offset];
            return ValueMeta::deserialize(value);
        }
        None
    }
}
impl<'a> KvDoubleEndedSinkIter<ValueMeta> for SinkBlockIter<'a> {
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        if self.back_key.len() == 0 {
            return None;
        }
        return Some(self.back_key.as_slice().into());
    }

    fn value_back(&self) -> Option<ValueMeta> {
        if let Some(back_entry_id) = self.back_entry_id {
            let last_entry_id = back_entry_id + 1;
            let end_offset = if last_entry_id == self.inner.entry_offsets.len() {
                self.inner.entries_index_start
            } else {
                self.inner.entry_offsets[last_entry_id] as usize
            };
            let start_offset = self.inner.entry_offsets[back_entry_id] as usize
                + HEADER_SIZE
                + self.back_header.get_diff();
            let value = &self.inner.data()[start_offset..end_offset];
            return ValueMeta::deserialize(value);
        }
        None
    }
}
#[cfg(test)]
mod test_iter {
    use crate::{
        iter::{DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIter, SinkIterator},
        kv::Entry,
        table::write::BlockBuilder,
    };

    use super::BlockInner;

    fn generate_instance(len: u32) -> anyhow::Result<BlockInner> {
        let mut block_builder = BlockBuilder::new(4096);
        for i in 0..len {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            block_builder.push_entry(
                &entry.key_ts().serialize().as_slice().into(),
                entry.value_meta(),
            );
        }
        block_builder.finish_block(crate::pb::badgerpb4::checksum::Algorithm::Crc32c);
        let data = block_builder.data().to_vec();
        let block_inner = BlockInner::deserialize(0.into(), (0 as usize).into(), 0, data)?;
        block_inner.verify()?;
        Ok(block_inner)
    }
    #[test]
    fn test_next() {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut block_iter = block.iter();
        for i in 0..len {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next().unwrap());
            assert_eq!(
                block_iter.key().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value().unwrap(), *entry.value_meta())
        }
    }
    #[test]
    fn test_next_back() {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut block_iter = block.iter();
        for i in (0..len).rev() {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next_back().unwrap());
            assert_eq!(
                block_iter.key_back().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value_back().unwrap(), *entry.value_meta())
        }
    }
    #[test]
    fn test_double_ended() {
        let len = 1000;
        let split = 500;
        let block = generate_instance(len).unwrap();
        let mut block_iter = block.iter();
        for i in 0..split {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next().unwrap());
            assert_eq!(
                block_iter.key().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value().unwrap(), *entry.value_meta())
        }
        for i in (split..len).rev() {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next_back().unwrap());
            assert_eq!(
                block_iter.key_back().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value_back().unwrap(), *entry.value_meta())
        }
        assert_eq!(block_iter.next().unwrap(), false);
        assert_eq!(block_iter.next_back().unwrap(), false);
    }
    #[test]
    fn test_rev_next() {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut block_iter = block.iter().rev();
        for i in (0..len).rev() {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next().unwrap());
            assert_eq!(
                block_iter.key().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value().unwrap(), *entry.value_meta())
        }
    }
    #[test]
    fn test_rev_next_back() {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut block_iter = block.iter().rev();
        for i in 0..len {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next_back().unwrap());
            assert_eq!(
                block_iter.key_back().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value_back().unwrap(), *entry.value_meta())
        }
    }
    #[test]
    fn test_rev_double_ended() {
        let len = 1000;
        let split = 500;
        let block = generate_instance(len).unwrap();
        let mut block_iter = block.iter().rev();
        for i in 0..split {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next_back().unwrap());
            assert_eq!(
                block_iter.key_back().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value_back().unwrap(), *entry.value_meta())
        }
        for i in (split..len).rev() {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next().unwrap());
            assert_eq!(
                block_iter.key().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value().unwrap(), *entry.value_meta())
        }
        assert_eq!(block_iter.next().unwrap(), false);
        assert_eq!(block_iter.next_back().unwrap(), false);
    }

    #[test]
    fn test_rev_rev() {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut block_iter = block.iter().rev().rev();
        for i in 0..len {
            let entry = Entry::new(i.to_string().into(), i.to_string().into());
            assert!(block_iter.next().unwrap());
            assert_eq!(
                block_iter.key().unwrap(),
                entry.key_ts().serialize().as_slice().into()
            );
            assert_eq!(block_iter.value().unwrap(), *entry.value_meta())
        }
    }
}
