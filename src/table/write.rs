use std::{
    ops::Deref,
    sync::{atomic::AtomicU32, Arc},
};

use bytes::{Buf, BufMut};

use crate::{
    iter::{KvSinkIterator, SinkIterator},
    kv::{ KeyTsBorrow, ValuePointer},
    options::CompressionType,
    txn::{entry::{EntryMeta, ValueMeta}, TxnTs}, key_registry::NONCE_SIZE, bloom::Bloom,
};

use super::TableOption;
#[derive(Debug)]
pub(crate) struct EntryHeader {
    overlap: u16,
    diff: u16,
}
// Header + base_key (diff bytes)
pub(crate) const HEADER_SIZE: usize = 4;
impl EntryHeader {
    pub(crate) fn new(overlap:u16,diff:u16)->Self{
        Self{
            overlap,
            diff,
        }
    }
    #[inline]
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(HEADER_SIZE);
        v.put_u16(self.overlap);
        v.put_u16(self.diff);
        v
    }
    #[inline]
    pub(crate) fn deserialize(mut data: &[u8]) -> Self {
        
        Self {
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
}
impl BackendBlock {
    fn new(block_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(block_size + BLOCK_PADDING),
            basekey: Default::default(),
            entry_offsets: Default::default(),
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
    max_version: TxnTs,
    on_disk_size: u32,
    stale_data_size: usize,
    opt: TableOption,

    // wait_group: Option<WaitGroup>,
    // send_block: Option<Sender<BackendBlock>>,
}
const MAX_BUFFER_BLOCK_SIZE: usize = 256 << 20; //256MB
/// When a block is encrypted, it's length increases. We add 256 bytes of padding to
/// handle cases when block size increases. This is an approximate number.
const BLOCK_PADDING: usize = 256;
impl BackendBlock {
    fn len(&self)->usize{
        self.data.len()
    }
    fn diff_base_key(&self,new_key:&[u8])->usize{
        let mut i=0;
        let base_key:&[u8]=self.basekey.as_ref();
        while i < base_key.len().min(new_key.len()) {
            if base_key[i]!=new_key[i]{
                break;
            }
            i+=1;
        }
        i
    }
    fn should_finish_block(&self, key: &KeyTsBorrow, value: &ValueMeta,block_size:usize,is_encrypt:bool) -> bool {
        if self.entry_offsets.len() == 0 {
            return false;
        }
        debug_assert!((self.entry_offsets.len() as u32 + 1) * 4 + 4 + 8 + 4 < u32::MAX);
        let entries_offsets_size = (self.entry_offsets.len() + 1) * 4 
        + 4 //size of list
        + 8 //sum64 in checksum proto
        + 4; //checksum length
        let mut estimate_size=self.data.len()+6+key.as_ref().len()+ value.encode_size().unwrap() as usize+ entries_offsets_size;
        if is_encrypt{
            estimate_size+=NONCE_SIZE;
        }
        assert!(self.data.len()+estimate_size < u32::MAX as usize);
        estimate_size > block_size
    }

    fn push_entry(&mut self,key_ts: &KeyTsBorrow,value: ValueMeta){
        let diff_key=if self.basekey.len()==0 {
            self.basekey=key_ts.to_vec();
            key_ts
        }else{
            &key_ts[self.diff_base_key(&key_ts)..]
        };
        assert!(key_ts.len()-diff_key.len() <= u16::MAX as usize);
        assert!(diff_key.len() <= u16::MAX as usize);
        let entry_header=EntryHeader::new((key_ts.len()-diff_key.len()) as u16, diff_key.len() as u16);
        self.entry_offsets.push(self.data.len() as u32);
        self.data.extend_from_slice(&entry_header.serialize());
        self.data.extend_from_slice(diff_key);
        self.data.extend_from_slice(value.serialize().unwrap().as_ref());
        
    }
}
impl TableBuilderInner {
    fn new(opt: TableOption) -> Self {
        let pre_alloc_size = MAX_BUFFER_BLOCK_SIZE.min(opt.table_size());
        let cur_block = BackendBlock::new(opt.block_size());
        let mut table_builder = TableBuilderInner::default();
        table_builder.cur_block = cur_block;
        table_builder.alloc = Vec::with_capacity(pre_alloc_size);
        table_builder.opt = opt;
        table_builder
    }
    fn push_internal(&mut self, key_ts: &KeyTsBorrow, value: ValueMeta,vptr_len:Option<u32>, is_stale: bool) {
        if self.cur_block.should_finish_block(&key_ts, &value,self.opt.block_size(),self.opt.datakey.is_some()) {
            if is_stale{
                self.stale_data_size+=key_ts.len()+4;
            }
            todo!()
        };
        self.key_hashes.push(Bloom::hash(key_ts.key()));
        self.max_version=self.max_version.max(key_ts.txn_ts());
        self.cur_block.push_entry(key_ts, value);
        self.on_disk_size+=vptr_len.unwrap_or(0);
    }
    fn finish_block(&mut self){
        let cur_block=&mut self.cur_block;
        for offset in cur_block.entry_offsets.iter() {
            cur_block.data.put_u32(*offset);
        }

    }
    fn push(&mut self,key_ts: &KeyTsBorrow,value: ValueMeta,vptr_len:Option<u32>){
        self.push_internal(key_ts, value, vptr_len, false);
    }

}
impl TableBuilder {
    fn new(opt: TableOption) -> Self {
        let pre_alloc_size = MAX_BUFFER_BLOCK_SIZE.min(opt.table_size());
        let cur_block = BackendBlock::new(opt.block_size());
        let mut table_builder = TableBuilderInner::default();
        table_builder.cur_block = cur_block;
        table_builder.alloc = Vec::with_capacity(pre_alloc_size);
        table_builder.opt = opt;
        return Self(table_builder.into());
        // if table_builder.opt.compression == CompressionType::None
        //     && table_builder.opt.datakey.is_none()
        // {
        //     return Self(table_builder.into());
        // }
        // let count = 2 * num_cpus::get();
        // let (send, recv) = async_channel::bounded::<BackendBlock>(2 * count);
        // // table_builder.wait_group = WaitGroup::new(count as u32).into();
        // // table_builder.send_block = send.into();
        // for _ in 0..count {
        //     let recv_clone = recv.clone();
        //     // tokio::spawn();
        // }
        // return Self(table_builder.into());
    }
    async fn run_backend_task(self,) {
        // defer!(self.wait_group.as_ref().unwrap().done());
        // let need_compress = self.opt.compression != CompressionType::None;
        // while let Ok(block) = receiver.recv().await {
        //     // let tail=block.data[block.end..].to_vec();
        //     // if need_compress{
        //     //     self.compress_data();
        //     // }
        // }
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
        // let table_builder = Self::new(opt);
        let mut table_builder = TableBuilderInner::new(opt);
        while iter.next()? {
            let key_ts: KeyTsBorrow = iter.key().unwrap().into();
            let key_bytes = key_ts.as_ref();
            if drop_prefixed.len() > 0 && drop_prefixed.iter().any(|x| key_bytes.starts_with(x)) {
                continue;
            }
            let value: ValueMeta = iter.take_value().unwrap().into();
            let vptr_len=if value.meta().contains(EntryMeta::VALUE_POINTER) {
                let vp = ValuePointer::decode(&value.value());
                vp.len().into()
            }else {
                None
            };
            table_builder.push(&key_ts, value, vptr_len);
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

}
#[cfg(test)]
mod tests{
    use bytes::{BufMut, Buf};

    #[test]
    fn test_base_diff(){
        let a:&[u8]="abck".as_ref();
        let b:&[u8]="abcde".as_ref();
        let mut i=0;
        while i < a.len().min(b.len()) {
            if a[i]!=b[i]{
                break;
            }
            i+=1;
        }
        dbg!(i);
        // println!(i);
        // for i in 0..a.len().min(b.len()) {
            // if a[i]!=b[i]{
                // break;
            // }
        // }
    }

}