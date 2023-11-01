use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::bail;
use bytes::Buf;
use prost::Message;

use crate::iter::DoubleEndedSinkIter;
use crate::iter::DoubleEndedSinkIterator;
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
#[derive(Debug, Clone, Copy)]
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

    pub(crate) fn verify(&self) -> anyhow::Result<()> {
        let checksum = Checksum::decode(self.0.checksum.as_ref())
            .map_err(|e| anyhow!("Failed decode pb::checksum for block for {}", e))?;
        checksum.verify(&self.0.data)?;
        Ok(())
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
                        or the table options are incorrectly set"
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
}
// struct BlockIter{

// }
// impl Iterator for BlockIter {
//     type Item;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

pub(crate) struct SinkBlockIter {
    inner: BlockInner,
    // base_key: &'a [u8],
    key: Vec<u8>,
    // key_ref: &'a [u8],
    value: Option<ValueMeta>,
    header: EntryHeader,
    entry_id: Option<usize>,

    back_key: Vec<u8>,
    // back_key_ref: &'a [u8],
    back_value: Option<ValueMeta>,
    back_header: EntryHeader,
    back_entry_id: Option<usize>,
}
// impl From<BlockInner> for SinkBlockIter<'a> {
//     fn from(value: &'a BlockInner) -> Self {
//         Self {
//             inner: value,
//             // base_key: Default::default(),
//             entry_id: None,
//             back_entry_id: None,
//             key: Default::default(),
//             // key_ref: Default::default(),
//             back_key: Default::default(),
//             // back_key_ref: Default::default(),
//             value: Default::default(),
//             back_value: Default::default(),
//             header: Default::default(),
//             back_header: Default::default(),
//         }
//     }
// }

// impl SinkIter for SinkBlockIter {
//     type Item = usize;

//     fn item(&self) -> Option<&Self::Item> {
//         self.entry_id.as_ref()
//     }
// }

// impl DoubleEndedSinkIter for SinkBlockIter {
//     fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
//         self.back_entry_id.as_ref()
//     }
// }
//base key 123 1  iter.key=null
//123 100
//123 121  pre_overlap=6 overlap:4 -> iter.key=123 1;  diffkey=21  -> iter.key=123 121 (just create iter, and may not seek to  start , so also pre_overlap==0)
//123 122  pre_overlap=4 overlap:5 -> iter.key=123 12; diffkey=2   -> iter.key=123 122
//123 211  pre_overlap=5 overlap:3 -> iter.key=123  ;  diffkey=211 -> iter.key=123 211
// impl SinkIterator for SinkBlockIter {
//     fn next(&mut self) -> Result<bool, anyhow::Error> {
//         match self.entry_id {
//             Some(id) => {
//                 match self.back_entry_id {
//                     Some(back_id) => {
//                         if id + 1 == back_id {
//                             return Ok(false);
//                         }
//                     }
//                     None => {
//                         if id == self.inner.entry_offsets.len() - 1 {
//                             return Ok(false);
//                         }
//                     }
//                 }
//                 self.entry_id = Some(id + 1);
//                 let entry_offset = self.inner.entry_offsets[id + 1] as usize;
//                 let next_header = EntryHeader::deserialize(
//                     &self.inner.data()[entry_offset..entry_offset + HEADER_SIZE],
//                 );

//                 // self.inner.data()[]
//                 return Ok(true);
//             }
//             None => {
//                 if self.inner.entry_offsets.len() != 0 {
//                     return Ok(false);
//                 }

//                 // if self.base_key.len() == 0 {
//                 //     let header = EntryHeader::deserialize(&self.inner.data()[..HEADER_SIZE]);
//                 //     self.base_key =
//                 //         &self.inner.data()[HEADER_SIZE..HEADER_SIZE + header.get_diff()];
//                 //     self.header = header;
//                 //     self.key = self.base_key.to_vec();
//                 //     self.key_ref=&self.key;
//                 // }
//                 self.entry_id = 0.into();
//                 return Ok(true);
//             }
//         };
//     }
// }
// impl DoubleEndedSinkIterator for SinkBlockIter {
//     fn next_back(&mut self) -> Result<bool, anyhow::Error> {
//         match self.back_entry_id {
//             Some(back_id) => {
//                 match self.entry_id {
//                     Some(id) => {
//                         if back_id - 1 == id {
//                             return Ok(false);
//                         }
//                     }
//                     None => {
//                         if back_id == 0 {
//                             return Ok(false);
//                         }
//                     }
//                 }
//                 self.back_entry_id = Some(back_id - 1);
//                 return Ok(true);
//             }
//             None => {
//                 if self.inner.entry_offsets.len() == 0 {
//                     return Ok(false);
//                 }
//                 // if self.base_key.len() == 0 {
//                 //     let header = EntryHeader::deserialize(&self.inner.data()[..HEADER_SIZE]);
//                 //     self.base_key =
//                 //         &self.inner.data()[HEADER_SIZE..HEADER_SIZE + header.get_diff()];
//                 // }
//                 self.back_entry_id = Some(self.inner.entry_offsets.len() - 1);
//                 return Ok(true);
//             }
//         };
//     }
// }
// impl<'a> KvSinkIter<'a, KeyTsBorrow<'a>, ValueMeta> for SinkBlockIter {
//     fn key(&self) -> Option<KeyTsBorrow<'a>> {
//         todo!()
//         // return Some(self.key.as_slice().into());
//         // return Some(self.key.as_slice().into());
//         // if let Some(id) = self.item() {
//         //     if *id == 0 {

//         //         // return Some(self.base_key.into());
//         //     }
//         //     return Some((&self.inner.data()[4..8]).into());
//         // }
//         // None
//     }

//     fn value_ref(&mut self) -> Option<&ValueMeta> {
//         todo!()
//     }

//     fn take_value(&mut self) -> Option<ValueMeta> {
//         todo!()
//     }
// }
#[test]
fn test_size() {
    dbg!(size_of::<SinkBlockIter>());
}
