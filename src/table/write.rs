use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU32, Arc},
};

use async_channel::{Receiver, Sender};
use bytes::{Buf, BufMut};
use scopeguard::defer;

use crate::{
    closer::WaitGroup,
    iter::{KvSinkIterator, SinkIterator},
    kv::{KeyTs, KeyTsBorrow, ValuePointer},
    options::CompressionType,
    txn::entry::{EntryMeta, ValueMeta}, key_registry::NONCE_SIZE,
};

use super::TableOption;
#[derive(Debug)]
pub(crate) struct Header {
    overlap: u16,
    diff: u16,
}
// Header + base_key (diff bytes)
pub(crate) const HEADER_SIZE: usize = 4;
impl Header {
    #[inline]
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(HEADER_SIZE);
        v.put_u16(self.overlap);
        v.put_u16(self.diff);
        v
    }
    #[inline]
    pub(crate) fn deserialize(mut data: &[u8]) -> Self {
        Header {
            overlap: data.get_u16(),
            diff: data.get_u16(),
        }
    }
    #[inline]
    pub(crate) fn get_diff(&self) -> usize {
        self.diff as usize
    }
    #[inline]
    pub(crate) fn get_overlap(&self) -> usize {
        self.overlap as usize
    }
}
#[derive(Debug, Default)]
struct BackendBlock {
    data: Vec<u8>,
    basekey: Vec<u8>,
    entry_offsets: Vec<u32>,
    end: usize,
}
impl BackendBlock {
    fn new(block_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(block_size + BLOCK_PADDING),
            basekey: Default::default(),
            entry_offsets: Default::default(),
            end: Default::default(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct TableBuilder(Arc<TableBuilderInner>);
impl Deref for TableBuilder {
    type Target = TableBuilderInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default)]
pub(crate) struct TableBuilderInner {
    alloc: Vec<u8>,
    cur_block: BackendBlock,
    compressed_size: AtomicU32,
    uncompressed_size: AtomicU32,
    len_offsets: u32,
    key_hashes: Vec<u32>,
    max_version: u64,
    on_disk_size: u32,
    stale_data_size: usize,
    opt: TableOption,

    wait_group: Option<WaitGroup>,
    send_block: Option<Sender<BackendBlock>>,
}
const MAX_BUFFER_BLOCK_SIZE: usize = 256 << 20; //256MB
/// When a block is encrypted, it's length increases. We add 256 bytes of padding to
/// handle cases when block size increases. This is an approximate number.
const BLOCK_PADDING: usize = 256;
impl TableBuilder {
    fn new(opt: TableOption) -> Self {
        let pre_alloc_size = MAX_BUFFER_BLOCK_SIZE.min(opt.table_size());
        let cur_block = BackendBlock::new(opt.block_size());
        let mut table_builder = TableBuilderInner::default();
        table_builder.cur_block = cur_block;
        table_builder.alloc = Vec::with_capacity(pre_alloc_size);
        table_builder.opt = opt;
        if table_builder.opt.compression == CompressionType::None
            && table_builder.opt.datakey.is_none()
        {
            return Self(table_builder.into());
        }
        let count = 2 * num_cpus::get();
        let (send, recv) = async_channel::bounded::<BackendBlock>(2 * count);
        table_builder.wait_group = WaitGroup::new(count as u32).into();
        table_builder.send_block = send.into();
        for _ in 0..count {
            let recv_clone = recv.clone();
            // tokio::spawn();
        }
        return Self(table_builder.into());
    }
    async fn run_backend_task(self, receiver: Receiver<BackendBlock>) {
        defer!(self.wait_group.as_ref().unwrap().done());
        let need_compress = self.opt.compression != CompressionType::None;
        while let Ok(block) = receiver.recv().await {
            // let tail=block.data[block.end..].to_vec();
            // if need_compress{
            //     self.compress_data();
            // }
        }
    }
    fn compress_data(&self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        match self.opt.compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Snappy => Ok(snap::raw::Encoder::new().compress_vec(data)?),
            CompressionType::ZSTD => Ok(zstd::encode_all(data, self.opt.zstd_compression_level())?),
        }
    }
    pub(crate) fn build_l0_table<'a, I, K, V>(
        mut iter: I,
        drop_prefixed: Vec<Vec<u8>>,
        opt: TableOption,
    ) -> anyhow::Result<()>
    where
        I: KvSinkIterator<'a, K, V> + SinkIterator,
        K: Into<KeyTsBorrow<'a>>,
        V: Into<ValueMeta>,
    {
        let table_builder = Self::new(opt);
        while iter.next()? {
            let key: KeyTsBorrow = iter.key().unwrap().into();
            let key_bytes = key.as_ref();
            if drop_prefixed.len() > 0 && drop_prefixed.iter().any(|x| key_bytes.starts_with(x)) {
                continue;
            }
            let value: ValueMeta = iter.take_value().unwrap().into();
            if value.meta().contains(EntryMeta::VALUE_POINTER) {
                let vp = ValuePointer::decode(&value.value());
            }
            // let p: &[u8] = key.as_ref();

            // let p = key.0;
            // if drop_prefixed.len() > 0 && d
            // let key = iter.key().unwrap();
            // let key_ref = key.as_ref();
            // if drop_prefixed.len() > 0 && drop_prefixed.iter().any(|x| key_ref.starts_with(x)) {
            //     continue;
            // }
            // iter.value()
        }
        Ok(())
        // while let Ok(s) = iter.next(){
        // }
    }
    fn add_internal(&mut self, key: KeyTsBorrow, value: ValueMeta, is_stale: bool) {
        if self.should_finish_block(&key, &value) {
            if is_stale{
                // self.stale_data_size+=key.as_ref().len()+4;
            }
        };
    }
    fn should_finish_block(&self, key: &KeyTsBorrow, value: &ValueMeta) -> bool {
        let cur_block = &self.cur_block;
        if self.cur_block.entry_offsets.len() == 0 {
            return false;
        }
        debug_assert!((cur_block.entry_offsets.len() as u32 + 1) * 4 + 4 + 8 + 4 < u32::MAX);
        let entries_offsets_size = (cur_block.entry_offsets.len() + 1) * 4 
        + 4 //size of list
        + 8 //sum64 in checksum proto
        + 4; //checksum length
        let mut estimate_size=cur_block.end+6+key.as_ref().len()+ value.encode_size().unwrap() as usize+ entries_offsets_size;
        if self.opt.datakey.is_some() {
            estimate_size+=NONCE_SIZE;
        }
        assert!(cur_block.end+estimate_size < u32::MAX as usize);

        return estimate_size > self.opt.block_size();
    }
}
