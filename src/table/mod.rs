pub(crate) mod block;
pub(crate) mod builder;
pub(crate) mod index;
pub(crate) mod iter;
pub(crate) mod merge;
use std::mem;
use std::path::PathBuf;
use std::{sync::Arc, time::SystemTime};

use anyhow::anyhow;
use anyhow::bail;
use bytes::{Buf, BufMut};
use flatbuffers::InvalidFlatbuffer;
use prost::Message;
use snap::raw::Decoder;
use tokio::sync::{Mutex, RwLock};

use self::block::Block;
use self::iter::TableIter;
use crate::db::{BlockCache, IndexCache};
use crate::fb::fb::TableIndex;
use crate::iter::Iter;
use crate::key_registry::NONCE_SIZE;
use crate::key_registry::{AesCipher, KeyRegistry};
use crate::options::Options;
use crate::pb::badgerpb4::Checksum;
use crate::{
    default::SSTABLE_FILE_EXT, lsm::mmap::MmapFile, options::CompressionType,
    pb::badgerpb4::DataKey, util::parse_file_id,
};
#[derive(Debug)]
pub(crate) struct TableInner {
    lock: Mutex<()>,
    mmap_f: MmapFile,
    table_size: usize,
    smallest: Vec<u8>,
    pub(crate) biggest: RwLock<Vec<u8>>,
    index_buf: TableIndexBuf,
    cheap_index: CheapIndex,
    id: u64,
    checksum: Vec<u8>,
    created_at: SystemTime,
    index_start: usize,
    index_len: usize,
    has_bloom_filter: bool,
    cipher: Option<AesCipher>,
    opt: TableOption,
}
#[derive(Debug, Clone)]
pub(crate) struct Table(pub(crate) Arc<TableInner>);
#[derive(Debug)]
pub(crate) struct CheapIndex {
    max_version: u64,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    bloom_filter_len: usize,
    offsets_len: u32,
}
impl CheapIndex {
    fn new(table_index_buf: &TableIndexBuf) -> Self {
        let index = table_index_buf.to_table_index();
        let bloom_filter_len = match index.bloom_filter() {
            Some(s) => s.len(),
            None => 0,
        };
        let offsets_len = match index.offsets() {
            Some(s) => s.len() as u32,
            None => 0,
        };
        Self {
            max_version: index.max_version(),
            key_count: index.key_count(),
            uncompressed_size: index.uncompressed_size(),
            on_disk_size: index.on_disk_size(),
            bloom_filter_len,
            offsets_len,
        }
        // let p = index.max_version();
    }
}
// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
#[derive(Debug, Clone, Copy)]
pub enum ChecksumVerificationMode {
    // NoVerification indicates DB should not verify checksum for SSTable blocks.
    NoVerification,

    // OnTableRead indicates checksum should be verified while opening SSTtable.
    OnTableRead,

    // OnBlockRead indicates checksum should be verified on every SSTable block read.
    OnBlockRead,

    // OnTableAndBlockRead indicates checksum should be verified
    // on SSTable opening and on every block read.
    OnTableAndBlockRead,
}
impl Default for ChecksumVerificationMode {
    fn default() -> Self {
        Self::NoVerification
    }
}
#[derive(Debug)]
pub(crate) struct TableOption {
    // Open tables in read only mode.
    read_only: bool,
    metrics_enabled: bool,

    // Maximum size of the table.
    table_size: u64,
    table_capacity: u64, // 0.9x TableSize.

    // ChkMode is the checksum verification mode for Table.
    chk_mode: ChecksumVerificationMode,

    // BloomFalsePositive is the false positive probabiltiy of bloom filter.
    bloom_false_positive: f64,

    // BlockSize is the size of each block inside SSTable in bytes.
    block_size: usize,

    // DataKey is the key used to decrypt the encrypted text.
    pub(crate) datakey: Option<DataKey>,

    // Compression indicates the compression algorithm used for block compression.
    pub(crate) compression: CompressionType,

    zstd_compression_level: isize,

    block_cache: Option<BlockCache>,

    index_cache: Option<IndexCache>,
}

impl TableOption {
    pub(crate) async fn new(
        key_registry: &KeyRegistry,
        opt: &Arc<Options>,
        block_cache: &Option<BlockCache>,
        index_cache: &Option<IndexCache>,
    ) -> Self {
        let mut registry_w = key_registry.write().await;
        let data_key = registry_w.latest_datakey().await.unwrap();
        drop(registry_w);
        Self {
            read_only: opt.read_only,
            metrics_enabled: opt.metrics_enabled,
            table_size: opt.base_table_size as u64,
            table_capacity: Default::default(),
            chk_mode: opt.checksum_verification_mode,
            bloom_false_positive: opt.bloom_false_positive,
            block_size: opt.block_size,
            datakey: data_key,
            compression: opt.compression,
            zstd_compression_level: opt.zstd_compression_level,
            block_cache: block_cache.clone(),
            index_cache: index_cache.clone(),
        }
    }
}

impl Table {
    pub(crate) async fn open(mmap_f: MmapFile, opt: TableOption) -> anyhow::Result<Self> {
        if opt.block_size == 0 && opt.compression != CompressionType::None {
            bail!("Block size cannot be zero");
        }
        let id = parse_file_id(&mmap_f.file_path, SSTABLE_FILE_EXT).ok_or(anyhow!(
            "Invalid filename: {:?} for mmap_file",
            &mmap_f.file_path
        ))?;

        let table_size = mmap_f.get_file_size()?;
        let created_at = mmap_f.get_modified_time()?;

        let mut cipher = None;
        if let Some(data_key) = opt.datakey.clone() {
            cipher = AesCipher::new(data_key.data.as_ref(), true)?.into();
        }
        let (index_buf, cheap_index) = TableInner::init_index(table_size, &mmap_f, &cipher)?;

        let mut inner = TableInner {
            lock: Default::default(),
            mmap_f,
            table_size,
            smallest: Default::default(),
            biggest: Default::default(),
            id,
            checksum: Default::default(),
            created_at,
            index_start: Default::default(),
            index_len: Default::default(),
            has_bloom_filter: Default::default(),
            opt,
            cipher,
            index_buf,
            cheap_index,
        };
        let table_index = inner.index_buf.to_table_index();
        let block_offset = table_index.offsets().unwrap().get(0);
        if let Some(k) = block_offset.key() {
            inner.smallest = k.bytes().to_vec();
        };
        let table = Table(Arc::new(inner));
        let mut iter = TableIter::new(table.clone(), true, false);

        iter.rewind().await.map_err(|e| {
            anyhow!(
                "Failed to initialize biggest for table {:?} for {}",
                &table.0.get_file_path(),
                e
            )
        })?;
        let mut biggest_w = table.0.biggest.write().await;
        *biggest_w = iter.get_key().unwrap().to_vec();
        drop(biggest_w);

        match table.0.opt.chk_mode {
            ChecksumVerificationMode::OnTableRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {}
            _ => {
                table.0.verify().await?;
            }
        }
        Ok(table)
    }

    // #[inline]
    // pub(crate) fn get_id(&self) -> u64 {
    //     self.0.id
    // }

    #[inline]
    pub(crate) async fn get_block(&self, idx: u32, use_cache: bool) -> anyhow::Result<Block> {
        self.0.get_block(idx, use_cache).await
    }

    #[inline]
    pub(crate) fn sync_mmap(&self) -> Result<(), std::io::Error> {
        self.0.mmap_f.sync()
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.0.table_size
    }
    #[inline]
    pub(crate) fn stale_data_size(&self) -> u32 {
        let table_index = self.0.index_buf.to_table_index();
        table_index.stale_data_size()
    }
    #[inline]
    pub(crate) fn id(&self) -> u64 {
        self.0.id
    }
    #[inline]
    pub(crate) fn smallest(&self) -> &[u8] {
        self.0.smallest.as_ref()
    }
    #[inline]
    pub(crate) fn created_at(&self) -> SystemTime {
        self.0.created_at
    }
    #[inline]
    pub(crate) fn max_version(&self) -> u64 {
        self.0.cheap_index.max_version
    }

    // #[inline]
    // pub(crate) fn biggest(&self)->&[u8]{
    //     self.0.b
    // }
}

//    index_data+index_len(4B u32)+checksum+checksum_len(4B u32)
//
impl TableInner {
    #[inline]
    pub(crate) fn get_offsets_len(&self) -> u32 {
        self.cheap_index.offsets_len as u32
    }
    // fn read_mmap(offset: u64, len: u32) {}
    async fn verify(&self) -> anyhow::Result<()> {
        for i in 0..self.get_offsets_len() {
            let block = self.get_block(i, true).await.map_err(|e| {
                anyhow!(
                    "checksum validation failed for table:{:?}, block:{} for {}",
                    self.get_file_path(),
                    i,
                    e
                )
            })?;
            // OnBlockRead or OnTableAndBlockRead, we don't need to call verify checksum
            // on block, verification would be done while reading block itself.
            match self.opt.chk_mode {
                ChecksumVerificationMode::OnBlockRead
                | ChecksumVerificationMode::OnTableAndBlockRead => {}
                _ => {
                    block.verify().map_err(|e| {
                        anyhow!(
                            "checksum validation failed for table:{:?}, block:{}, offset:{} for {}",
                            self.get_file_path(),
                            i,
                            block.get_offset(),
                            e
                        )
                    })?;
                }
            }
        }
        Ok(())
        // if let Some(offsets) = table_index.offsets() {
        //     for i in 0..offsets.len(){
        //         self.get_block(idx, use_cache);
        //     }
        //     // let p = offsets.len();
        // }
    }
    // async fn init_biggest_smallest(&mut self) {
    //     let table_index = self.index_buf.to_table_index();
    //     let block_offset = table_index.offsets().unwrap().get(0);
    //     if let Some(k) = block_offset.key() {
    //         self.smallest = k.bytes().to_vec();
    //     };
    //     Table::
    //     let iter = TableIter::new(self.clone(), true, false);
    //     iter.rewind().await;

    // }
    fn init_index(
        table_size: usize,
        mmap_f: &MmapFile,
        cipher: &Option<AesCipher>,
    ) -> anyhow::Result<(TableIndexBuf, CheapIndex)> {
        let mut read_pos = table_size;

        //read checksum len from the last 4 bytes
        read_pos -= 4;
        let mut buf = mmap_f.read_slice(read_pos as usize, 4)?;
        let checksum_len = buf.get_u32() as i32;
        if checksum_len < 0 {
            bail!("checksum length less than zero. Data corrupted");
        }

        //read checksum
        read_pos -= checksum_len as usize;
        let buf = mmap_f.read_slice(read_pos, checksum_len as usize)?;
        let checksum = Checksum::decode(buf)?;

        //read index size from the footer
        read_pos -= 4;
        let mut buf = mmap_f.read_slice(read_pos, 4)?;
        let index_len = buf.get_u32() as usize;

        //read index
        read_pos -= index_len;
        // let index_start = read_pos;
        let data = mmap_f.read_slice(read_pos, index_len)?;

        checksum.verify(data).map_err(|e| {
            anyhow!(
                "failed to verify checksum for table:{:?} for {}",
                &mmap_f.file_path,
                e
            )
        })?;
        let index_buf = TableIndexBuf::from_vec(try_decrypt(cipher, data)?)?;

        let cheap_index = CheapIndex::new(&index_buf);

        debug_assert!(index_buf.to_table_index().offsets().is_some());

        Ok((index_buf, cheap_index))
    }

    async fn get_block(&self, idx: u32, use_cache: bool) -> anyhow::Result<Block> {
        if idx >= self.get_offsets_len() {
            bail!("block out of index");
        }

        let key = self.get_block_cache_key(idx);

        if let Some(block_cache) = &self.opt.block_cache {
            if let Some(blk) = block_cache.get(&key).await {
                return Ok(blk.value().clone());
            };
        }

        let table_index = self.index_buf.to_table_index();
        let blk_offset = table_index.offsets().unwrap().get(idx as usize);

        let raw_data_ref = self
            .mmap_f
            .read_slice(blk_offset.offset() as usize, blk_offset.len() as usize)
            .map_err(|e| {
                anyhow!(
                    "Failed to read from file: {:?} at offset: {}, len: {} for {}",
                    &self.mmap_f.file_path,
                    blk_offset.offset(),
                    blk_offset.len(),
                    e
                )
            })?;

        let de_raw_data = try_decrypt(&self.cipher, raw_data_ref)?;
        let raw_data = self.decompress(de_raw_data).map_err(|e| {
            anyhow!(
                "Failed to decode compressed data in file: {:?} at offset: {}, len: {} for {}",
                &self.mmap_f.file_path,
                blk_offset.offset(),
                blk_offset.len(),
                e
            )
        })?;

        let block = Block::new(blk_offset.offset(), raw_data)?;

        match self.opt.chk_mode {
            ChecksumVerificationMode::OnBlockRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {
                block.verify()?;
            }
            _ => {}
        }

        if use_cache {
            if let Some(block_cache) = &self.opt.block_cache {
                block_cache
                    .insert(key, block.clone(), mem::size_of::<Block>() as i64)
                    .await;
            }
        }

        Ok(block)
    }

    //if cipher.is_some() than use cipher decrypt data and return de_data
    //else return data
    #[inline]
    fn get_block_cache_key(&self, idx: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8);
        buf.put_u32(self.id as u32);
        buf.put_u32(idx);
        buf
    }
    #[inline]
    fn get_file_path(&self) -> &PathBuf {
        &self.mmap_f.file_path
    }
    #[inline]
    fn decompress(&self, block_data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let data = match self.opt.compression {
            CompressionType::None => block_data,
            CompressionType::Snappy => Decoder::new()
                .decompress_vec(&block_data)
                .map_err(|e| anyhow!("fail to decompress for {}", e))?,
            CompressionType::ZSTD => {
                let data_u8: &[u8] = block_data.as_ref();
                zstd::decode_all(data_u8).map_err(|e| anyhow!("Failed to decompress for {}", e))?
            }
        };
        Ok(data)
    }
}
fn try_decrypt(cipher: &Option<AesCipher>, data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let data = match cipher {
        Some(c) => {
            let nonce = &data[data.len() - NONCE_SIZE..];
            let plaintext = &data[..data.len() - NONCE_SIZE];
            c.decrypt_with_slice(nonce, plaintext)
                .ok_or(anyhow!("while decrypt"))?
        }
        None => data.to_vec(),
    };
    Ok(data)
}
#[inline(always)]
pub(crate) fn vec_u32_to_bytes(s: Vec<u32>) -> Vec<u8> {
    let mut r = Vec::<u8>::with_capacity(s.len() * 4);
    for ele in s {
        r.put_u32(ele);
    }
    r
}

#[inline(always)]
pub(crate) fn bytes_to_vec_u32(src: &[u8]) -> Vec<u32> {
    let capacity = src.len() / 4;
    let mut s = src;
    let mut v = Vec::<u32>::with_capacity(capacity);
    for _ in 0..capacity {
        v.push(s.get_u32());
    }
    v
}
#[derive(Debug)]
struct TableIndexBuf(Vec<u8>);

impl TableIndexBuf {
    #[inline]
    pub(crate) fn from_vec(data: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<TableIndex>(&data)?;
        Ok(Self(data))
    }
    pub(crate) fn from_slice(data: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<TableIndex>(data)?;
        Ok(Self(data.to_vec()))
    }

    #[inline]
    pub(crate) fn to_table_index(&self) -> TableIndex<'_> {
        unsafe { flatbuffers::root_unchecked::<TableIndex>(self.0.as_ref()) }
    }
}
