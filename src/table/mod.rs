use std::{sync::Arc, time::SystemTime};

use anyhow::anyhow;
use anyhow::bail;
use bytes::Buf;
use prost::Message;
use stretto::AsyncCache;
use tokio::sync::Mutex;
pub(crate) mod block;
pub(crate) mod index;
use self::block::Block;
use crate::fb::fb::TableIndex;
use crate::pb;
use crate::pb::badgerpb4::Checksum;
use crate::{
    db::DB, default::SSTABLE_FILE_EXT, lsm::mmap::MmapFile, options::CompressionType,
    pb::badgerpb4::DataKey, util::parse_file_id,
};
#[derive(Debug)]
pub(crate) struct TableInner {
    lock: Mutex<()>,
    mmap_f: MmapFile,
    table_size: usize,
    smallest: Vec<u8>,
    biggest: Vec<u8>,

    id: u64,
    checksum: Vec<u8>,
    created_at: SystemTime,
    index_start: usize,
    index_len: usize,
    has_bloom_filter: bool,
    opt: TableOption,
}
pub(crate) struct Table(Arc<TableInner>);
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
    datakey: Option<DataKey>,

    // Compression indicates the compression algorithm used for block compression.
    compression: CompressionType,

    zstd_compression_level: isize,

    block_cache: Option<AsyncCache<Vec<u8>, Block>>,

    index_cache: Option<AsyncCache<u64, Vec<u8>>>,
}

impl TableOption {
    pub(crate) async fn new(db: &Arc<DB>) -> Self {
        let mut registry_w = db.key_registry.write().await;
        let data_key = registry_w.latest_datakey().await.unwrap();
        let opt = &db.opt;
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
            block_cache: db.block_cache.clone(),
            index_cache: db.index_cache.clone(),
        }
    }
}

impl Table {
    pub(crate) fn open(mmap_f: MmapFile, opt: TableOption) -> anyhow::Result<()> {
        if opt.block_size == 0 && opt.compression != CompressionType::None {
            bail!("Block size cannot be zero");
        }
        let id = parse_file_id(&mmap_f.file_path, SSTABLE_FILE_EXT).ok_or(anyhow!(
            "Invalid filename: {:?} for mmap_file",
            &mmap_f.file_path
        ))?;

        let table_size = mmap_f.get_file_size()?;
        let created_at = mmap_f.get_modified_time()?;
        let inner = TableInner {
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
        };
        // inner.init_index();

        Ok(())
    }
    fn init_biggest_smallest() {}
}

//    index_data+index_len(4B u32)+checksum+checksum_len(4B u32)
//
impl TableInner {
    fn read_mmap(offset: u64, len: u32) {}
    fn init_index(&mut self) -> anyhow::Result<()> {
        let mut read_pos = self.table_size;

        //read checksum len from the last 4 bytes
        read_pos -= 4;
        let mut buf = self.mmap_f.read_slice(read_pos as usize, 4)?;
        let checksum_len = buf.get_u32() as i32;
        if checksum_len < 0 {
            bail!("checksum length less than zero. Data corrupted");
        }

        //read checksum
        read_pos -= checksum_len as usize;
        let mut buf = self.mmap_f.read_slice(read_pos, checksum_len as usize)?;
        let checksum = Checksum::decode(buf)?;

        //read index size from the footer
        read_pos -= 4;
        let mut buf = self.mmap_f.read_slice(read_pos, 4)?;
        self.index_len = buf.get_u32() as usize;

        //read index
        read_pos -= self.index_len;
        self.index_start = read_pos;
        let data = self.mmap_f.read_slice(read_pos, self.index_len)?;

        checksum.verify(data).map_err(|e| {
            anyhow!(
                "failed to verify checksum for table:{:?} for {}",
                &self.mmap_f.file_path,
                e
            )
        })?;

        // let mmap = self.mmap_f.as_ref();
        // let p = &mmap[0..];
        // mmap.as_ref()[read_pos..];
        // let buf = self.mmap_f[read_pos..read_pos+4];
        Ok(())
    }
    fn read_table_index<'b: 'a, 'a>(&self, data: &'b [u8]) -> anyhow::Result<TableIndex<'a>> {
        if let Some(data_key) = &self.opt.datakey {}
        let t = flatbuffers::root::<TableIndex>(data)?;
        Ok(t)
    }
}
struct TableIndexBuf(Arc<Vec<u8>>);
impl From<&[u8]> for TableIndexBuf {
    fn from(value: &[u8]) -> Self {
        Self(Arc::new(value.to_vec()))
    }
}
impl TableIndexBuf {
    pub(crate) fn to_table_index(&self) -> Result<TableIndex<'_>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root::<TableIndex>(self.0.as_ref())
    }
}
