pub(crate) mod iter;
pub(crate) mod merge;
pub(crate) mod read;
pub(crate) mod write;
use std::io::{self};
use std::ops::Deref;
use std::path::PathBuf;
use std::{sync::Arc, time::SystemTime};

use aes_gcm_siv::Nonce;
use anyhow::anyhow;
use anyhow::bail;
use bytes::{Buf, BufMut, Bytes};
use flatbuffers::InvalidFlatbuffer;
use prost::Message;

use self::read::SinkBlockIter;
use crate::fb::fb::TableIndex;
use crate::iter::{DoubleEndedSinkIterator, KvSinkIter};
use crate::key_registry::NONCE_SIZE;
use crate::key_registry::{AesCipher, KeyRegistry};
use crate::kv::{KeyTs, TxnTs};
use crate::pb::badgerpb4::{self, Checksum};
use crate::util::bloom::BloomBorrow;
use crate::util::cache::{BlockCache, BlockCacheKey, IndexCache};
#[cfg(feature = "metrics")]
use crate::util::metrics::{add_num_bloom_not_exist, add_num_bloom_use};
use crate::util::{DBFileId, SSTableId};
use crate::{config::CompressionType, util::mmap::MmapFile};

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
#[derive(Debug, Clone)]
pub struct TableConfig {
    // Open tables in read only mode.
    // Maximum size of the table.
    table_size: usize,
    table_capacity: usize, // 0.9x TableSize.

    // ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
    checksum_verify_mode: ChecksumVerificationMode,
    checksum_algo: badgerpb4::checksum::Algorithm,
    // BloomFalsePositive is the false positive probabiltiy of bloom filter.
    bloom_false_positive: f64,

    // BlockSize is the size of each block inside SSTable in bytes.
    block_size: usize,

    // Compression indicates the compression algorithm used for block compression.
    compression: CompressionType,

    zstd_compression_level: i32,
}
impl Default for TableConfig {
    fn default() -> Self {
        Self {
            table_size: 2 << 20,
            table_capacity: ((2 << 20) as f64 * 0.95) as usize,
            checksum_verify_mode: ChecksumVerificationMode::default(),
            bloom_false_positive: 0.01,
            block_size: 4 * 1024,
            compression: CompressionType::default(),
            zstd_compression_level: 1,
            checksum_algo: badgerpb4::checksum::Algorithm::Crc32c,
        }
    }
}
impl TableConfig {
    pub fn set_table_size(&mut self, table_size: usize) {
        self.table_size = table_size;
        self.table_capacity = (self.table_size as f64 * 0.95) as usize;
    }

    pub fn set_checksum_verify_mode(&mut self, checksum_verify_mode: ChecksumVerificationMode) {
        self.checksum_verify_mode = checksum_verify_mode;
    }

    pub fn set_checksum_algo(&mut self, checksum_algo: badgerpb4::checksum::Algorithm) {
        self.checksum_algo = checksum_algo;
    }

    pub fn set_bloom_false_positive(&mut self, bloom_false_positive: f64) {
        self.bloom_false_positive = bloom_false_positive;
    }

    pub fn set_block_size(&mut self, block_size: usize) {
        self.block_size = block_size;
    }

    pub fn set_compression(&mut self, compression: CompressionType) {
        self.compression = compression;
    }

    pub fn set_zstd_compression_level(&mut self, zstd_compression_level: i32) {
        self.zstd_compression_level = zstd_compression_level;
    }

    pub fn table_size(&self) -> usize {
        self.table_size
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn compression(&self) -> CompressionType {
        self.compression
    }
}
impl TableConfig {
    pub(crate) async fn open(
        self,
        mmap_f: MmapFile,
        key_registry: &KeyRegistry,
        index_cache: IndexCache,
        block_cache: Option<BlockCache>,
    ) -> anyhow::Result<Table> {
        let cipher = key_registry.latest_cipher().await;
        if self.block_size == 0 && self.compression != CompressionType::None {
            bail!("Block size cannot be zero");
        }
        let table_id = SSTableId::parse(mmap_f.path())?;

        let table_size = mmap_f.get_file_size()? as usize;
        let created_at: SystemTime = mmap_f.get_modified_time()?;

        let (index_buf, index_start, index_len) = Self::init_index(&mmap_f, cipher.as_ref())?;
        let table_index = index_buf.to_table_index();

        let (smallest, biggest) = self.get_smallest_biggest(table_index, &mmap_f, &cipher)?;

        let inner = TableInner {
            mmap_f,
            table_size,
            smallest,
            biggest,
            table_id,
            created_at,
            config: self,
            block_cache,
            index_cache,
            cipher,
            index_start,
            index_len,
            cheap_index: table_index.into(),
        };

        match inner.config.checksum_verify_mode {
            ChecksumVerificationMode::OnTableRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {}
            _ => {
                inner.verify().await?;
            }
        }
        Ok(inner.into())
    }

    fn get_smallest_biggest(
        &self,
        table_index: TableIndex<'_>,
        mmap_f: &MmapFile,
        cipher: &Option<AesCipher>,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        //get smallest
        let first_block_offset = table_index.offsets().unwrap().get(0);
        let first_block_base_key = first_block_offset.key().unwrap();
        let smallest = first_block_base_key.bytes().to_vec();

        //get biggest
        let last_block_id = table_index.offsets().unwrap().len() - 1;
        let last_block_offset = table_index.offsets().unwrap().get(last_block_id);
        let raw_data = mmap_f.read_slice_ref(
            last_block_offset.offset() as usize,
            last_block_offset.len() as usize,
        )?;

        let plaintext = try_decrypt(cipher.as_ref(), raw_data)?;
        let uncompress_data = self.compression.decompress(plaintext)?;

        let block = BlockInner::deserialize(
            0.into(), // don't care about it
            last_block_id.into(),
            last_block_offset.offset(),
            uncompress_data,
        )?;
        block.verify()?;
        let mut block_iter = block.iter();
        assert!(block_iter.next_back()?);
        let biggest = block_iter.key().unwrap().to_vec();
        Ok((smallest, biggest))
    }
    fn init_index(
        mmap_f: &MmapFile,
        cipher: Option<&AesCipher>,
    ) -> anyhow::Result<(TableIndexBuf, usize, usize)> {
        let mut read_pos = mmap_f.get_file_size()? as usize;

        //read checksum len from the last 4 bytes
        read_pos -= 4;
        // let mut buf = [0; 4];
        // mmap_f.read_slice(read_pos, &mut buf[..])?;
        let mut buf = mmap_f.read_slice_ref(read_pos as usize, 4)?;
        let checksum_len = buf.get_u32() as usize;

        //read checksum
        read_pos -= checksum_len as usize;
        // let mut buf = vec![0; checksum_len];
        // mmap_f.read_slice_ref(read_pos, &mut buf)?;
        let buf = mmap_f.read_slice_ref(read_pos, checksum_len as usize)?;
        let checksum = Checksum::decode(buf)?;

        //read index size from the footer
        read_pos -= 4;
        // let mut buf = [0; 4];
        // mmap_f.read_slice(read_pos, &mut buf);
        let mut buf = mmap_f.read_slice_ref(read_pos, 4)?;
        let index_len = buf.get_u32() as usize;

        //read index
        let index_start = read_pos - index_len;
        // let mut data = vec![0; index_len];
        // mmap_f.read_slice(read_pos, &mut data);
        let data = mmap_f.read_slice_ref(index_start, index_len)?;

        checksum.verify(data)?;

        let index_buf = TableIndexBuf::from_vec(try_decrypt(cipher, data.as_ref())?)?;

        debug_assert!(index_buf.to_table_index().offsets().is_some());

        Ok((index_buf, index_start, index_len))
    }
}

#[derive(Debug)]
pub(crate) struct TableInner {
    table_id: SSTableId,
    mmap_f: MmapFile,
    table_size: usize,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    cheap_index: CheapTableIndex,
    block_cache: Option<BlockCache>,
    index_cache: IndexCache,
    cipher: Option<AesCipher>,
    created_at: SystemTime,
    index_start: usize,
    index_len: usize,
    config: TableConfig,
}
#[derive(Debug, Clone)]
pub(crate) struct Table(Arc<TableInner>);
impl Deref for Table {
    type Target = TableInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<TableInner> for Table {
    fn from(value: TableInner) -> Self {
        Self(Arc::new(value))
    }
}
impl Drop for TableInner {
    fn drop(&mut self) {
        if let Err(e) = self.mmap_f.raw_sync() {
            panic!("{}", e)
        }
    }
}

//    index_data+index_len(4B u32)+checksum+checksum_len(4B u32)
//
impl TableInner {
    pub(crate) fn block_offsets_len(&self) -> usize {
        self.cheap_index.offsets_len
    }

    pub(crate) fn max_version(&self) -> TxnTs {
        self.cheap_index.max_version
    }

    #[inline]
    pub(crate) fn stale_data_size(&self) -> u32 {
        self.cheap_index.stale_data_size
    }

    pub(crate) fn created_at(&self) -> SystemTime {
        self.created_at
    }
    pub(crate) fn smallest(&self) -> &[u8] {
        &self.smallest
    }
    pub(crate) fn table_id(&self) -> SSTableId {
        self.table_id
    }
    pub(crate) fn biggest(&self) -> &[u8] {
        &self.biggest
    }
    pub(crate) fn sync_mmap(&self) -> io::Result<()> {
        self.mmap_f.raw_sync()
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.table_size
    }

    // fn read_mmap(offset: u64, len: u32) {}
    async fn verify(&self) -> anyhow::Result<()> {
        for i in 0..self.block_offsets_len() {
            let block = self.get_block(i.into(), true).await.map_err(|e| {
                anyhow!(
                    "checksum validation failed for table:{:?}, block:{} for {}",
                    self.get_file_path(),
                    i,
                    e
                )
            })?;
            // OnBlockRead or OnTableAndBlockRead, we don't need to call verify checksum
            // on block, verification would be done while reading block itself.
            match self.config.checksum_verify_mode {
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
    }
    pub(crate) async fn may_contain_key(&self, key: &KeyTs) -> anyhow::Result<bool> {
        if self.cheap_index.bloom_filter == 0 {
            return Ok(true);
        }
        #[cfg(feature = "metrics")]
        add_num_bloom_use(1);
        let table_index_buf = self.get_index().await?;
        let table_index = table_index_buf.to_table_index();
        let bloom: BloomBorrow = table_index.bloom_filter().unwrap().bytes().into();
        let may_contain = bloom.may_contain_key(&key.key());
        if !may_contain {
            #[cfg(feature = "metrics")]
            add_num_bloom_not_exist(1);
        }
        Ok(may_contain)
    }
    async fn get_index(&self) -> anyhow::Result<TableIndexBuf> {
        if let Some(s) = self.index_cache.get(&self.table_id).await {
            return Ok(s.value().clone());
        }
        let data = self
            .mmap_f
            .read_slice_ref(self.index_start, self.index_len)?;

        let index_buf = TableIndexBuf::from_vec(try_decrypt(self.cipher.as_ref(), data.as_ref())?)?;
        self.index_cache
            .insert(self.table_id, index_buf.clone())
            .await;
        return Ok(index_buf);
    }
    async fn get_block(&self, block_index: BlockIndex, use_cache: bool) -> anyhow::Result<Block> {
        if block_index >= self.block_offsets_len().into() {
            bail!("block out of index");
        }

        // let key: Vec<u8> = self.get_block_cache_key(idx);
        let key: BlockCacheKey = (self.table_id, block_index).into();

        if let Some(block_cache) = &self.block_cache {
            if let Some(blk) = block_cache.get(key).await {
                return Ok(blk.value().clone());
            };
        }

        let table_index_buf = self.get_index().await?;
        let table_index = table_index_buf.to_table_index();
        let block = table_index.offsets().unwrap().get(block_index.into());
        let block_offset = block.offset() as usize;
        let block_len = block.len() as usize;

        let raw_data_ref = self
            .mmap_f
            .read_slice_ref(block_offset, block_len)
            .map_err(|e| {
                anyhow!(
                    "Failed to read from file: {:?} at offset: {}, len: {} for {}",
                    &self.mmap_f.path(),
                    block_offset,
                    block_len,
                    e
                )
            })?;

        let de_raw_data = try_decrypt(self.cipher.as_ref(), raw_data_ref)?;
        let raw_data = self
            .config
            .compression
            .decompress(de_raw_data)
            .map_err(|e| {
                anyhow!(
                    "Failed to decode compressed data in file: {:?} at offset: {}, len: {} for {}",
                    &self.mmap_f.path(),
                    block_offset,
                    block_len,
                    e
                )
            })?;

        let block = Block::deserialize(self.table_id, block_index, block_offset as u32, raw_data)?;

        match self.config.checksum_verify_mode {
            ChecksumVerificationMode::OnBlockRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {
                block.verify()?;
            }
            _ => {}
        }

        if use_cache {
            if let Some(block_cache) = &self.block_cache {
                block_cache.insert(key, block.clone()).await;
            }
        }

        Ok(block)
    }

    #[inline]
    fn get_file_path(&self) -> &PathBuf {
        &self.mmap_f.path()
    }
}
fn try_decrypt(cipher: Option<&AesCipher>, data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let data = match cipher {
        Some(c) => {
            let nonce = &data[data.len() - NONCE_SIZE..];
            let ciphertext = &data[..data.len() - NONCE_SIZE];
            c.decrypt_with_slice(nonce, ciphertext)
                .ok_or(anyhow!("while decrypt"))?
        }
        None => data.to_vec(),
    };
    Ok(data)
}
fn try_encrypt(cipher: Option<&AesCipher>, data: &[u8]) -> anyhow::Result<Vec<u8>> {
    if let Some(c) = cipher {
        let nonce: Nonce = AesCipher::generate_nonce();
        let mut ciphertext = c.encrypt(&nonce, data).ok_or(anyhow!("while encrypt"))?;
        ciphertext.extend_from_slice(nonce.as_ref());
        return Ok(ciphertext);
    }
    Ok(data.to_vec())
}
#[inline(always)]
pub(crate) fn vec_u32_to_bytes(s: &Vec<u32>) -> Vec<u8> {
    let mut r = Vec::<u8>::with_capacity(s.len() * 4);
    for ele in s {
        r.put_u32(*ele);
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
#[derive(Debug, Clone)]
pub(crate) struct TableIndexBuf(Bytes);
impl Deref for TableIndexBuf {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl TableIndexBuf {
    #[deny(unused)]
    #[inline]
    pub(crate) fn from_vec(data: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<TableIndex>(&data)?;
        Ok(Self(data.into()))
    }

    #[inline]
    pub(crate) fn to_table_index(&self) -> TableIndex<'_> {
        unsafe { flatbuffers::root_unchecked::<TableIndex>(self.0.as_ref()) }
    }
}
#[derive(Debug)]
struct CheapTableIndex {
    max_version: TxnTs,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    stale_data_size: u32,
    offsets_len: usize,
    bloom_filter: usize,
}
impl From<TableIndex<'_>> for CheapTableIndex {
    fn from(value: TableIndex<'_>) -> Self {
        Self {
            max_version: value.max_version().into(),
            key_count: value.key_count(),
            uncompressed_size: value.uncompressed_size(),
            on_disk_size: value.on_disk_size(),
            stale_data_size: value.stale_data_size(),
            offsets_len: if let Some(offsets) = value.offsets() {
                offsets.len()
            } else {
                0
            },
            bloom_filter: if let Some(bloom) = value.bloom_filter() {
                bloom.len()
            } else {
                0
            },
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BlockIndex(u32);
impl Deref for BlockIndex {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<u32> for BlockIndex {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Into<u32> for BlockIndex {
    fn into(self) -> u32 {
        self.0
    }
}
impl From<usize> for BlockIndex {
    fn from(value: usize) -> Self {
        Self(value as u32)
    }
}
impl Into<usize> for BlockIndex {
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
    block_index: BlockIndex,
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
        block_id: BlockIndex,
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
        block_id: BlockIndex,
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
            block_index: block_id,
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
#[derive(Debug, Default)]
pub(crate) struct EntryHeader {
    overlap: u16,
    diff: u16,
}
// Header + base_key (diff bytes)
pub(crate) const HEADER_SIZE: usize = 4;
impl EntryHeader {
    pub(crate) fn new(overlap: u16, diff: u16) -> Self {
        Self { overlap, diff }
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
        debug_assert!(data.len() >= 4);
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
