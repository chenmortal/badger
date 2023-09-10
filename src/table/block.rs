use std::mem;
use std::{ops::Deref, sync::Arc};

use anyhow::anyhow;
use anyhow::bail;
use bytes::Buf;
use prost::Message;

use crate::pb::{self, badgerpb4::Checksum};

use super::bytes_to_vec_u32;
#[derive(Debug, Clone)]
pub(crate) struct Block(Arc<BlockInner>);
#[derive(Debug)]
struct BlockInner {
    offset: u32,
    data: Vec<u8>, //actual data + entry_offsets + num_entries
    entries_index_start: usize,
    entry_offsets: Vec<u32>,
    checksum: Vec<u8>,
    checksum_len: usize,
}
impl Block {
    pub(crate) fn new(offset: u32, mut data: Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(BlockInner::new(offset, data)?)))
    }

    pub(crate) fn verify(&self) -> anyhow::Result<()> {
        let checksum = Checksum::decode(self.0.checksum.as_ref())
                    .map_err(|e| anyhow!("Failed decode pb::checksum for block for {}", e))?;;
        checksum.verify(&self.0.data)?;        
        Ok(())
    }
}

impl BlockInner {
    #[inline(always)]
    pub(crate) fn new(offset: u32, mut data: Vec<u8>) -> anyhow::Result<Self> {
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
            offset,
            data,
            entries_index_start,
            entry_offsets,
            checksum,
            checksum_len,
        })
    }
}
#[test]
fn test_a(){
    dbg!(mem::size_of::<Block>());
    dbg!(mem::size_of::<BlockInner>());
}