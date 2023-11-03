use std::{
    sync::{atomic::{AtomicU32, Ordering}, Arc}, mem::replace,  ptr, path::PathBuf, fs::OpenOptions,
};

use bytes::{Buf, BufMut};
use prost::Message;

use crate::{
    iter::{KvSinkIter, SinkIterator},
    kv::{ KeyTsBorrow, ValuePointer, TxnTs, ValueMeta, Meta},
    config::CompressionType,
 key_registry::{NONCE_SIZE, AesCipher, self, KeyRegistry}, pb::badgerpb4::{Checksum, checksum::Algorithm}, util::{rayon::{spawn_fifo, AsyncRayonHandle}, cache::{IndexCache, BlockCache}}, fb::fb, util::{bloom::Bloom, mmap::MmapFile},
};

use super::{TableConfig, vec_u32_to_bytes, try_encrypt, Table};
#[derive(Debug,Default)]
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
        debug_assert!(data.len() >=4);
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
pub(super) struct BlockBuilder {
    data: Vec<u8>,
    basekey: Vec<u8>,
    entry_offsets: Vec<u32>,
}
impl BlockBuilder {
    pub(super) fn new(block_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(block_size + BLOCK_PADDING),
            basekey: Default::default(),
            entry_offsets: Default::default(),
        }
    }

    pub(super) fn data(&self) -> &[u8] {
        self.data.as_ref()
    }
}


#[derive(Debug, Default)]
pub(crate) struct TableBuilder {
    // alloc: Vec<u8>,
    cur_block: BlockBuilder,
    compressed_size: Arc<AtomicU32>,
    cipher:Option<AesCipher>,
    uncompressed_size: u32,
    len_offsets: u32,
    key_hashes: Vec<u32>,
    max_version: TxnTs,
    on_disk_size: u32,
    stale_data_size: u32,
    config: TableConfig,
    compress_task:Vec<AsyncRayonHandle<anyhow::Result<BlockBuilder>>>,
}
// const MAX_BUFFER_BLOCK_SIZE: usize = 256 << 20; //256MB
/// When a block is encrypted, it's length increases. We add 256 bytes of padding to
/// handle cases when block size increases. This is an approximate number.
const BLOCK_PADDING: usize = 256;
impl BlockBuilder {
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

    pub(super) fn should_finish_block(&self, key: &KeyTsBorrow, value: &ValueMeta,block_size:usize,is_encrypt:bool) -> bool {
        if self.entry_offsets.len() == 0 {
            return false;
        }
        debug_assert!((self.entry_offsets.len() as u32 + 1) * 4 + 4 + 8 + 4 < u32::MAX);
        let entries_offsets_size = (self.entry_offsets.len() + 1) * 4 
        + 4 //size of list
        + 8 //sum64 in checksum proto
        + 4; //checksum length
        let mut estimate_size=self.data.len()+6+key.as_ref().len()+ value.serialized_size() + entries_offsets_size;
        if is_encrypt{
            estimate_size+=NONCE_SIZE;
        }
        assert!(self.data.len()+estimate_size < u32::MAX as usize);
        estimate_size > block_size
    }

    pub(super) fn push_entry(&mut self,key_ts: &KeyTsBorrow,value: &ValueMeta){
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
        self.data.extend_from_slice(value.serialize().as_ref());
        
    }

    pub(super) fn finish_block(&mut self,algo:Algorithm){
        self.data.extend_from_slice(&vec_u32_to_bytes(&self.entry_offsets));
        self.data.put_u32(self.entry_offsets.len() as u32);

        let checksum = Checksum::new(algo, &self.data);
        self.data.extend_from_slice(&checksum.encode_to_vec());
        self.data.put_u32(checksum.encoded_len() as u32);
    }
}
impl TableBuilder {
    pub(crate) fn new(table_config: TableConfig,cipher:Option<AesCipher>) -> Self {
        // let pre_alloc_size = MAX_BUFFER_BLOCK_SIZE.min(table_opt.table_size());
        let cur_block = BlockBuilder::new(table_config.block_size);
        let mut table_builder = Self::default();
        table_builder.cur_block = cur_block;
        // table_builder.alloc = Vec::with_capacity(pre_alloc_size);
        table_builder.config = table_config;
        table_builder.cipher=cipher;
        table_builder
    }

    fn push_internal(&mut self, key_ts: &KeyTsBorrow, value: &ValueMeta,vptr_len:Option<u32>, is_stale: bool) {
        if self.cur_block.should_finish_block(&key_ts, &value,self.config.block_size,self.cipher.is_some()) {
            if is_stale{
                self.stale_data_size+=key_ts.len() as u32+4;
            }
            self.finish_cur_block();
        };
        self.key_hashes.push(Bloom::hash(key_ts.key()));
        self.max_version=self.max_version.max(key_ts.txn_ts());
        self.cur_block.push_entry(key_ts, &value);
        self.on_disk_size+=vptr_len.unwrap_or(0);
    }

    fn finish_cur_block(&mut self){
        if self.cur_block.entry_offsets.len()==0 {
            return;
        }
        self.cur_block.finish_block(self.config.checksum_algo);
        self.uncompressed_size+=self.cur_block.data.len() as u32;

        self.len_offsets+=(self.cur_block.basekey.len() as f32/ 4.0).ceil() as u32 * 4 + 40;
        let mut finished_block = replace(&mut self.cur_block, BlockBuilder::new(self.config.block_size));
        let cipher = self.cipher.clone();
        let compression = self.config.compression;
        let compressed_size = self.compressed_size.clone();
        self.compress_task.push(spawn_fifo(move ||{
                    if compression!=CompressionType::None{
                        match compression.compress(&finished_block.data) {
                            Ok(compressed) => {
                                finished_block.data=compressed;
                            },
                            Err(e) => {
                                return Err(e);
                            },
                        }
                    }
                    if let Some(cipher) = cipher.as_ref() {
                        match try_encrypt(cipher.into(), &finished_block.data) {
                            Ok(ciphertext) => {
                                finished_block.data=ciphertext;
                            },
                            Err(e) => {return Err(e)},
                        }
                    }
                    compressed_size.fetch_add(finished_block.data.len() as u32, Ordering::AcqRel);
                    Ok(finished_block)
                }));
    }

    fn push(&mut self,key_ts: &KeyTsBorrow,value: &ValueMeta,vptr_len:Option<u32>){
        self.push_internal(key_ts, value, vptr_len, false);
    }

    pub(crate) fn is_empty(&self)->bool{
        self.key_hashes.len()==0
    }

    async fn done(&mut self)->anyhow::Result<TableBuildData>{
        self.finish_cur_block();
        let mut block_list=Vec::with_capacity(self.compress_task.len());
        for task in self.compress_task.drain(..) {
            block_list.push(task.await?);
        }
        let mut bloom=None; 
        if self.config.bloom_false_positive > 0.0{
            bloom = Bloom::new(&self.key_hashes, self.config.bloom_false_positive).into();
        }
        let (index,data_size) = self.build_index(&block_list,bloom.as_ref())?;
        let checksum = Checksum::new(self.config.checksum_algo, &index).encode_to_vec();
        let size=data_size as usize + index.len() + 4 + checksum.len() + 4;
        let build_data = TableBuildData{
                    block_list,
                    index,
                    checksum,
                    size,
                };
        Ok(build_data)
    }

    fn build_index(&mut self,block_list:&Vec<BlockBuilder>,bloom:Option<&Bloom>)-> anyhow::Result<(Vec<u8>,u32)>{
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(3<<20);
        let mut data_size=0;
        let mut block_offset=Vec::new();
        for block in block_list {
            let args = fb::BlockOffsetArgs{
                            key: builder.create_vector(block.basekey.as_ref()).into(),
                            offset: data_size,
                            len: block.data.len() as u32,
                        };

            data_size+=block.data.len() as u32;
            block_offset.push(fb::BlockOffset::create(&mut builder, &args));
        } 
        self.on_disk_size+=data_size;
        let table_index_args = fb::TableIndexArgs{
                    offsets: builder.create_vector(&block_offset).into(),
                    bloom_filter: bloom.and_then(|x|builder.create_vector(x).into()),
                    max_version: self.max_version.to_u64(),
                    key_count: self.key_hashes.len() as u32,
                    uncompressed_size: self.uncompressed_size,
                    on_disk_size:self.on_disk_size,
                    stale_data_size: self.stale_data_size,
                };
        let table_index = fb::TableIndex::create(&mut builder, &table_index_args);
        builder.finish(table_index, None);
        Ok((try_encrypt(self.cipher.as_ref(), builder.finished_data())?,data_size))
    }

    pub(crate) async fn build(&mut self,path:PathBuf,key_registry:&KeyRegistry,index_cache:Option<IndexCache>,block_cache:Option<BlockCache>)->anyhow::Result<Table>{
        let  build_data = self.done().await?;
        fn write_data(path:PathBuf,mut build_data: TableBuildData)->anyhow::Result<MmapFile>{
            let mut fp_open_opt = OpenOptions::new();
            fp_open_opt.read(true).write(true).create_new(true);
            let (mut mmap_f, is_new) = MmapFile::open(&path, fp_open_opt, build_data.size())?;
            debug_assert!(is_new);
            let written = build_data.read_all(&mut mmap_f.as_mut()[..])?;
            assert_eq!(written,mmap_f.len());
            mmap_f.raw_sync()?;
            Ok(mmap_f)
        }
        let mmap_f = tokio::task::spawn_blocking(move ||{write_data(path, build_data)}).await??;
        self.config.clone().open(mmap_f, key_registry, index_cache, block_cache).await   
    }

    pub(crate) async fn finish(&mut self)->anyhow::Result<Vec<u8>>{
        let mut data = self.done().await?;
        let mut buf=vec![0u8;data.size()];
        let written = data.read_all(&mut buf)?;
        assert_eq!(written,buf.len());
        Ok(buf)
    }


    pub(crate) fn build_l0_table<I,  V>(
        mut iter: I,
        drop_prefixed: Vec<Vec<u8>>,
        config: TableConfig,
        cipher:Option<AesCipher>,
    ) -> anyhow::Result<Self>
    where
        I: KvSinkIter<V> + SinkIterator,
        V: Into<ValueMeta>,
    {
        let mut table_builder = Self::new(config,cipher);
        while iter.next()? {
            let key_ts: KeyTsBorrow = iter.key().unwrap().into();
            let key_bytes = key_ts.as_ref();
            if drop_prefixed.len() > 0 && drop_prefixed.iter().any(|x| key_bytes.starts_with(x)) {
                continue;
            }
            let value: ValueMeta = iter.value().unwrap().into();
            let vptr_len=if value.meta().contains(Meta::VALUE_POINTER) {
                let vp = ValuePointer::deserialize(&value.value());
                vp.len().into()
            }else {
                None
            };
            table_builder.push(&key_ts, &value, vptr_len);
        }
        Ok(table_builder)
    }
}

pub(crate) struct TableBuildData{
    block_list:Vec<BlockBuilder>,
    index:Vec<u8>,
    checksum:Vec<u8>,
    size:usize,
}

impl TableBuildData {
    pub(crate) fn size(&self) -> usize {
        self.size
    }
}


impl TableBuildData {
    fn read_all(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        assert_eq!(buf.len(),self.size);
        let mut written=0;
        for block in self.block_list.iter() {
            unsafe {
                ptr::copy_nonoverlapping(block.data.as_slice().as_ptr(), buf.as_mut_ptr().add(written), block.data.len());
            };
            written+=block.data.len();
        }

        buf[written..written+self.index.len()].copy_from_slice(&self.index);
        written+=self.index.len();

        buf[written..written+4].copy_from_slice(&(self.index.len() as u32).to_be_bytes());
        written+=4;

        buf[written..written+self.checksum.len()].copy_from_slice(&self.checksum);
        written+=self.checksum.len();

        buf[written..written+4].copy_from_slice(&(self.checksum.len() as u32).to_be_bytes());
        written+=4;
        Ok(written)
    }
}
