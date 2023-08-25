use std::{
    path::{Path, PathBuf},
    string,
};

use anyhow::{bail, Error};
use log::LevelFilter;

use crate::{
    default::{DEFAULT_DIR, DEFAULT_VALUE_DIR, MAX_VALUE_THRESHOLD, SKL_MAX_NODE_SIZE},
    errors::DBError,
};
#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
    None = 0,
    Snappy = 1,
    ZSTD = 2,
}
impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}
impl From<u32> for CompressionType {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Snappy,
            2 => Self::ZSTD,
            _ => Self::None,
        }
    }
}
// const MAX_VALUE_THRESHOLD: i64 = 1 << 20;
#[derive(Debug)]
pub struct Options {
    // Required options.
    pub(crate) dir: PathBuf,
    pub(crate) value_dir: PathBuf,

    // Usually modified options.
    sync_writes: bool,
    num_versions_to_keep: isize,
    pub(crate) read_only: bool,
    pub(crate) log_level: LevelFilter,
    pub(crate) compression: CompressionType,
    // pub(crate) in_memory: bool,
    metrics_enabled: bool,
    // Sets the Stream.numGo field
    num_goroutines: usize,

    // Fine tuning options.
    pub(crate) memtable_size: u64,
    base_table_size: u64,
    base_level_size: u64,
    level_size_multiplier: isize,
    table_size_multiplier: isize,
    max_levels: usize,

    pub(crate) vlog_percentile: f64,
    pub(crate) value_threshold: i64,
    num_memtables: usize,
    // Changing BlockSize across DB runs will not break badger. The block size is
    // read from the block index stored at the end of the table.
    block_size: usize,
    bloom_false_positive: f64,
    pub(crate) block_cache_size: u64,
    index_cache_size: i64,

    num_level_zero_tables: usize,
    num_level_zero_tables_stall: isize,

    pub(crate) valuelog_file_size: u64,
    valuelog_max_entries: u32,

    pub(crate) num_compactors: usize,
    pub(crate) compactl0_on_close: bool,
    lmax_compaction: bool,
    // zstd_compression_level: isize,

    // When set, checksum will be validated for each entry read from the value log file.
    verify_value_checksum: bool,

    // Encryption related options.
    // EncryptionKey                 []byte        // encryption key
    // EncryptionKeyRotationDuration time.Duration // key rotation duration

    // BypassLockGuard will bypass the lock guard on badger. Bypassing lock
    // guard can cause data corruption if multiple badger instances are using
    // the same directory. Use this options with caution.
    pub(crate) bypass_lock_guard: bool,

    // ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
    // ChecksumVerificationMode options.ChecksumVerificationMode

    // DetectConflicts determines whether the transactions would be checked for
    // conflicts. The transactions can be processed at a higher rate when
    // conflict detection is disabled.
    detect_conflicts: bool,

    // NamespaceOffset specifies the offset from where the next 8 bytes contains the namespace.
    name_space_offset: isize,

    // Magic version used by the application using badger to ensure that it doesn't open the DB
    // with incompatible data format.
    pub(crate) external_magic_version: u16,

    // Transaction start and commit timestamps are managed by end-user.
    // This is only useful for databases built on top of Badger (like Dgraph).
    // Not recommended for most users.
    managed_txns: bool,

    // 4. Flags for testing purposes
    // ------------------------------
    pub(crate) max_batch_count: u64, // max entries in batch
    pub(crate) max_batch_size: u64,  // max batch size in bytes

    pub(crate) max_value_threshold: f64,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            dir: PathBuf::from(DEFAULT_DIR),
            value_dir: PathBuf::from(DEFAULT_VALUE_DIR),
            sync_writes: false,
            num_versions_to_keep: 1,
            read_only: false,
            log_level: log::LevelFilter::Info,
            compression: Default::default(),
            // in_memory: Default::default(),
            metrics_enabled: true,
            num_goroutines: 8,
            memtable_size: 64 << 20,
            base_table_size: 2 << 20,
            base_level_size: 10 << 20,
            level_size_multiplier: 10,
            table_size_multiplier: 2,
            max_levels: 7,
            vlog_percentile: 0.0,
            value_threshold: MAX_VALUE_THRESHOLD,
            num_memtables: 5,
            block_size: 4 * 1024,
            bloom_false_positive: 0.01,
            block_cache_size: 256 << 20,
            index_cache_size: 0,
            num_level_zero_tables: 5,
            num_level_zero_tables_stall: 15,
            valuelog_file_size: 1 << 30 - 1,
            valuelog_max_entries: 1000_000,
            num_compactors: 4,
            compactl0_on_close: false,
            lmax_compaction: Default::default(),
            // zstd_compression_level: 1,
            verify_value_checksum: false,
            bypass_lock_guard: Default::default(),
            detect_conflicts: true,
            name_space_offset: -1,
            external_magic_version: Default::default(),
            managed_txns: Default::default(),
            max_batch_count: Default::default(),
            max_batch_size: Default::default(),
            max_value_threshold: Default::default(),
        }
    }
}
impl Options {
    pub fn set_dir(mut self, dir: String) -> Self {
        assert_ne!(dir,"");
        self.dir = dir.into();
        self
    }
    pub fn set_value_dir(mut self, value_dir: String) -> Self {
        assert_ne!(value_dir,"");
        self.value_dir = value_dir.into();
        self
    }
    pub fn set_sync_writess(mut self, sync_writes: bool) -> Self {
        self.sync_writes = sync_writes;
        self
    }
    pub fn set_num_versions_to_keep(mut self, num_versions_to_keep: isize) -> Self {
        self.num_versions_to_keep = num_versions_to_keep;
        self
    }
    pub fn set_num_goroutines(mut self, routines: usize) -> Self {
        self.num_goroutines = routines;
        self
    }
    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
    pub fn set_metrics_enabled(mut self, metrics_enabled: bool) -> Self {
        self.metrics_enabled = metrics_enabled;
        self
    }
    pub fn set_log_level(mut self, log_level: LevelFilter) -> Self {
        self.log_level = log_level;
        self
    }
    pub fn set_base_table_size(mut self, base_table_size: u64) -> Self {
        self.base_table_size = base_table_size;
        self
    }
    pub fn set_level_size_multiplier(mut self, level_size_multiplier: isize) -> Self {
        self.level_size_multiplier = level_size_multiplier;
        self
    }
    pub fn set_max_levels(mut self, max_levels: usize) -> Self {
        self.max_levels = max_levels;
        self
    }
    pub fn set_value_threshold(mut self, value_threshold: i64) -> Self {
        self.value_threshold = value_threshold;
        self
    }
    pub fn set_vlog_percentile(mut self, vlog_percentile: f64) -> Self {
        self.vlog_percentile = vlog_percentile;
        self
    }
    pub fn set_num_memtables(mut self, num_memtables: usize) -> Self {
        self.num_memtables = num_memtables;
        self
    }
    pub fn set_memtable_size(mut self, memtable_size: u64) -> Self {
        self.memtable_size = memtable_size;
        self
    }
    pub fn set_bloom_false_positive(mut self, bloom_false_positive: f64) -> Self {
        self.bloom_false_positive = bloom_false_positive;
        self
    }
    pub fn set_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
    pub fn set_num_level_zero_tables(mut self, num_level_zero_tables: usize) -> Self {
        self.num_level_zero_tables = num_level_zero_tables;
        self
    }
    pub fn set_num_level_zero_tables_stall(mut self, num_level_zero_tables_stall: isize) -> Self {
        self.num_level_zero_tables_stall = num_level_zero_tables_stall;
        self
    }
    pub fn set_base_level_size(mut self, base_level_size: u64) -> Self {
        self.base_level_size = base_level_size;
        self
    }
    pub fn set_valuelog_file_size(mut self, valuelog_file_size: u64) -> Self {
        self.valuelog_file_size = valuelog_file_size;
        self
    }
    pub fn set_valuelog_max_entries(mut self, valuelog_max_entries: u32) -> Self {
        self.valuelog_max_entries = valuelog_max_entries;
        self
    }
    pub fn set_num_compactors(mut self, num_compactors: usize) -> Self {
        self.num_compactors = num_compactors;
        self
    }
    pub fn set_compactl0_on_close(mut self, compactl0_on_close: bool) -> Self {
        self.compactl0_on_close = compactl0_on_close;
        self
    }
    pub fn set_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }
    pub fn set_verify_value_checksum(mut self, verify_value_checksum: bool) -> Self {
        self.verify_value_checksum = verify_value_checksum;
        self
    }
    pub fn set_block_cache_size(mut self, block_cache_size: u64) -> Self {
        self.block_cache_size = block_cache_size;
        self
    }
    // pub fn set_in_memory(mut self, in_memory: bool) -> Self {
    //     self.in_memory = in_memory;
    //     self
    // }
    // pub fn zstd_compression_level(mut self,level:isize)->Self{
    //     self
    // }
    pub fn set_bypass_lock_guard(mut self, bypass_lock_guard: bool) -> Self {
        self.bypass_lock_guard = bypass_lock_guard;
        self
    }
    pub fn set_index_cache_size(mut self, index_cache_size: i64) -> Self {
        self.index_cache_size = index_cache_size;
        self
    }
    pub fn set_name_space_offset(mut self, offset: isize) -> Self {
        self.name_space_offset = offset;
        self
    }
    pub fn set_detect_conflicts(mut self, detect_conflicts: bool) -> Self {
        self.detect_conflicts = detect_conflicts;
        self
    }
    pub fn set_external_magic_version(mut self, magic: u16) -> Self {
        self.external_magic_version = magic;
        self
    }
}
impl Options {}
