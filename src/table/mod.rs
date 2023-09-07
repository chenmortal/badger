use std::{time::SystemTime, sync::Arc};

use tokio::sync::Mutex;

use crate::{lsm::mmap::MmapFile, options::CompressionType, pb::badgerpb4::DataKey, db::DB};

pub(crate) struct Table {
    lock: Mutex<()>,
    mmap: MmapFile,
    table_size: i32,
    id: u64,
    checksum: Vec<u8>,
    created_at: SystemTime,
    index_start: i32,
    index_len: i32,
    has_bloom_filter: bool,
}

// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
#[derive(Debug,Clone,Copy)]
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
    block_size: i32,

    // DataKey is the key used to decrypt the encrypted text.
    datakey: Option<DataKey>,

    // Compression indicates the compression algorithm used for block compression.
    compression: CompressionType,


}

impl TableOption {
    pub(crate) async fn new(db:&Arc<DB>)->Self{
        let mut registry_w = db.key_registry.write().await;
        let data_key = registry_w.latest_datakey().await.unwrap();
        let opt = &db.opt;
        Self{
            read_only: opt.read_only,
            metrics_enabled: opt.metrics_enabled,
            table_size: opt.base_table_size as u64,
            table_capacity: Default::default(),
            chk_mode: opt.checksum_verification_mode,
            bloom_false_positive: todo!(),
            block_size: todo!(),
            datakey: todo!(),
            compression: todo!(),
        }
    }
}
