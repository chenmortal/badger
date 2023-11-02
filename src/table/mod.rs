pub(crate) mod block;
pub(crate) mod index;
pub(crate) mod iter;
pub(crate) mod merge;
pub(crate) mod read;
pub(crate) mod write;
use std::io::{self};
use std::mem;
use std::ops::Deref;
use std::path::PathBuf;
use std::{sync::Arc, time::SystemTime};

use aes_gcm_siv::Nonce;
use anyhow::anyhow;
use anyhow::bail;
use bytes::{Buf, BufMut};
use flatbuffers::InvalidFlatbuffer;
use prost::Message;

use self::block::{Block, BlockId};
use crate::fb::fb::TableIndex;
use crate::iter::{DoubleEndedSinkIterator, KvSinkIter};
use crate::key_registry::NONCE_SIZE;
use crate::key_registry::{AesCipher, KeyRegistry};
use crate::kv::TxnTs;
use crate::pb::badgerpb4::{self, Checksum};
use crate::table::block::BlockInner;
use crate::util::cache::{BlockCache, IndexCache};
use crate::util::{DBFileId, SSTableId};
use crate::{options::CompressionType, util::mmap::MmapFile};

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

    // ChkMode is the checksum verification mode for Table.
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
}
impl TableConfig {
    pub(crate) async fn open(
        self,
        mut mmap_f: MmapFile,
        key_registry: &KeyRegistry,
        index_cache: Option<IndexCache>,
        block_cache: Option<BlockCache>,
    ) -> anyhow::Result<Table> {
        let cipher = key_registry.latest_cipher().await;
        if self.block_size == 0 && self.compression != CompressionType::None {
            bail!("Block size cannot be zero");
        }
        let table_id = SSTableId::parse(mmap_f.path())?;

        let table_size = mmap_f.get_file_size()? as usize;
        let created_at: SystemTime = mmap_f.get_modified_time()?;

        let index_buf = TableIndexBuf::open(&mmap_f, cipher.as_ref())?;
        let table_index = index_buf.to_table_index();

        let (smallest, biggest) = self.get_smallest_biggest(table_index, &mmap_f, &cipher)?;

        let checksum_mode = self.checksum_verify_mode;
        let inner = TableInner {
            mmap_f,
            table_size,
            smallest,
            biggest,
            table_id,
            created_at,
            config: self,
            index_buf,
            block_cache,
            index_cache,
            cipher,
        };

        match checksum_mode {
            ChecksumVerificationMode::OnTableRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {}
            _ => {
                inner.verify().await?;
            }
        }
        Ok(inner.into())
    }
    fn get_smallest(index_buf: &TableIndexBuf) -> Vec<u8> {
        let table_index = index_buf.to_table_index();
        let first_block_offset = table_index.offsets().unwrap().get(0);
        let first_block_base_key = first_block_offset.key();

        assert!(
            first_block_base_key.is_some(),
            "TableIndex first block base key can't be none"
        );

        first_block_base_key.unwrap().bytes().to_vec()
    }
    fn get_smallest_biggest(
        &self,
        table_index: TableIndex<'_>,
        mmap_f: &MmapFile,
        cipher: &Option<AesCipher>,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
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
        let mut block_iter = block.iter();
        assert!(block_iter.next_back()?);
        let biggest = block_iter.key().unwrap().to_vec();
        Ok((smallest, biggest))
    }
}

#[derive(Debug)]
pub(crate) struct TableInner {
    table_id: SSTableId,
    mmap_f: MmapFile,
    table_size: usize,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    index_buf: TableIndexBuf,
    block_cache: Option<BlockCache>,
    index_cache: Option<IndexCache>,
    cipher: Option<AesCipher>,
    created_at: SystemTime,
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
        unsafe { self.index_buf.to_table_index().offsets().unwrap_unchecked() }.len()
    }

    pub(crate) fn max_version(&self) -> TxnTs {
        self.index_buf.to_table_index().max_version().into()
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
    #[inline]
    pub(crate) fn stale_data_size(&self) -> u32 {
        let table_index = self.index_buf.to_table_index();
        table_index.stale_data_size()
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

    async fn get_block(&self, block_id: BlockId, use_cache: bool) -> anyhow::Result<Block> {
        if block_id >= self.block_offsets_len().into() {
            bail!("block out of index");
        }

        // let key: Vec<u8> = self.get_block_cache_key(idx);
        let key = (self.table_id, block_id).into();

        if let Some(block_cache) = &self.block_cache {
            if let Some(blk) = block_cache.get(key).await {
                return Ok(blk.value().clone());
            };
        }

        let table_index = self.index_buf.to_table_index();
        let blk_offset = table_index.offsets().unwrap().get(block_id.into());

        let raw_data_ref = self
            .mmap_f
            .read_slice_ref(blk_offset.offset() as usize, blk_offset.len() as usize)
            .map_err(|e| {
                anyhow!(
                    "Failed to read from file: {:?} at offset: {}, len: {} for {}",
                    &self.mmap_f.path(),
                    blk_offset.offset(),
                    blk_offset.len(),
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
                    blk_offset.offset(),
                    blk_offset.len(),
                    e
                )
            })?;

        let block = Block::deserialize(self.table_id, block_id, blk_offset.offset(), raw_data)?;

        match self.config.checksum_verify_mode {
            ChecksumVerificationMode::OnBlockRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {
                block.verify()?;
            }
            _ => {}
        }

        if use_cache {
            if let Some(block_cache) = &self.block_cache {
                block_cache
                    .insert(key, block.clone(), mem::size_of::<Block>() as i64)
                    .await;
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
#[derive(Debug)]
struct TableIndexBuf(Vec<u8>);

impl TableIndexBuf {
    fn open(mmap_f: &MmapFile, cipher: Option<&AesCipher>) -> anyhow::Result<TableIndexBuf> {
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
        read_pos -= index_len;
        // let index_start = read_pos;
        // let mut data = vec![0; index_len];
        // mmap_f.read_slice(read_pos, &mut data);
        let data = mmap_f.read_slice_ref(read_pos, index_len)?;

        checksum.verify(data)?;

        let index_buf = TableIndexBuf::from_vec(try_decrypt(cipher, data.as_ref())?)?;

        debug_assert!(index_buf.to_table_index().offsets().is_some());

        Ok(index_buf)
    }
    #[deny(unused)]
    #[inline]
    pub(crate) fn from_vec(data: Vec<u8>) -> Result<Self, InvalidFlatbuffer> {
        flatbuffers::root::<TableIndex>(&data)?;
        Ok(Self(data))
    }

    #[inline]
    pub(crate) fn to_table_index(&self) -> TableIndex<'_> {
        unsafe { flatbuffers::root_unchecked::<TableIndex>(self.0.as_ref()) }
    }
}
