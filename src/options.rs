use std::string;

use anyhow::bail;
use log::LevelFilter;

use crate::default::{MAX_VALUE_THRESHOLD, DEFAULT_DIR, DEFAULT_VALUE_DIR};
pub enum CompressionType {
    Snappy,
    ZSTD { level: isize },
}
// const MAX_VALUE_THRESHOLD: i64 = 1 << 20;
pub struct Options {
    // Required options.
    dir: String,
    value_dir: String,

    // Usually modified options.
    sync_writes: bool,
    num_versions_to_keep: isize,
    read_only: bool,
    log_level: LevelFilter,
    compression: Option<CompressionType>,
    in_memory: bool,
    metrics_enabled: bool,
    // Sets the Stream.numGo field
    num_goroutines: usize,

    // Fine tuning options.
    memtable_size: u64,
    base_table_size: u64,
    base_level_size: u64,
    level_size_multiplier: isize,
    table_size_multiplier: isize,
    max_levels: usize,

    vlog_percentile: f64,
    value_threshold: i64,
    num_memtables: usize,
    // Changing BlockSize across DB runs will not break badger. The block size is
    // read from the block index stored at the end of the table.
    block_size: usize,
    bloom_false_positive: f64,
    block_cache_size: u64,
    index_cache_size: i64,

    num_level_zero_tables: usize,
    num_level_zero_tables_stall: isize,

    valuelog_file_size: u64,
    valuelog_max_entries: u32,

    num_compactors: usize,
    compactl0_on_close: bool,
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
    bypass_lock_guard: bool,

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
    external_magic_version: u16,

    // Transaction start and commit timestamps are managed by end-user.
    // This is only useful for databases built on top of Badger (like Dgraph).
    // Not recommended for most users.
    managed_txns: bool,

    // 4. Flags for testing purposes
    // ------------------------------
    max_batch_count: i64, // max entries in batch
    max_batch_size: i64,  // max batch size in bytes

    max_value_threshold: f64,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            dir: DEFAULT_DIR.to_string(),
            value_dir: DEFAULT_VALUE_DIR.to_string(),
            sync_writes: false,
            num_versions_to_keep: 1,
            read_only: false,
            log_level: log::LevelFilter::Info,
            compression: Some(CompressionType::Snappy),
            in_memory: Default::default(),
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
    pub fn dir(mut self, dir: String) -> Self {
        self.dir = dir;
        self
    }
    pub fn value_dir(mut self, value_dir: String) -> Self {
        self.value_dir = value_dir;
        self
    }
    pub fn sync_writess(mut self, sync_writes: bool) -> Self {
        self.sync_writes = sync_writes;
        self
    }
    pub fn num_versions_to_keep(mut self, num_versions_to_keep: isize) -> Self {
        self.num_versions_to_keep = num_versions_to_keep;
        self
    }
    pub fn num_goroutines(mut self, routines: usize) -> Self {
        self.num_goroutines = routines;
        self
    }
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
    pub fn metrics_enabled(mut self, metrics_enabled: bool) -> Self {
        self.metrics_enabled = metrics_enabled;
        self
    }
    pub fn log_level(mut self, log_level: LevelFilter) -> Self {
        self.log_level = log_level;
        self
    }
    pub fn base_table_size(mut self, base_table_size: u64) -> Self {
        self.base_table_size = base_table_size;
        self
    }
    pub fn level_size_multiplier(mut self, level_size_multiplier: isize) -> Self {
        self.level_size_multiplier = level_size_multiplier;
        self
    }
    pub fn max_levels(mut self, max_levels: usize) -> Self {
        self.max_levels = max_levels;
        self
    }
    pub fn value_threshold(mut self, value_threshold: i64) -> Self {
        self.value_threshold = value_threshold;
        self
    }
    pub fn vlog_percentile(mut self, vlog_percentile: f64) -> Self {
        self.vlog_percentile = vlog_percentile;
        self
    }
    pub fn num_memtables(mut self, num_memtables: usize) -> Self {
        self.num_memtables = num_memtables;
        self
    }
    pub fn memtable_size(mut self, memtable_size: u64) -> Self {
        self.memtable_size = memtable_size;
        self
    }
    pub fn bloom_false_positive(mut self, bloom_false_positive: f64) -> Self {
        self.bloom_false_positive = bloom_false_positive;
        self
    }
    pub fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
    pub fn num_level_zero_tables(mut self, num_level_zero_tables: usize) -> Self {
        self.num_level_zero_tables = num_level_zero_tables;
        self
    }
    pub fn num_level_zero_tables_stall(mut self, num_level_zero_tables_stall: isize) -> Self {
        self.num_level_zero_tables_stall = num_level_zero_tables_stall;
        self
    }
    pub fn base_level_size(mut self, base_level_size: u64) -> Self {
        self.base_level_size = base_level_size;
        self
    }
    pub fn valuelog_file_size(mut self, valuelog_file_size: u64) -> Self {
        self.valuelog_file_size = valuelog_file_size;
        self
    }
    pub fn valuelog_max_entries(mut self, valuelog_max_entries: u32) -> Self {
        self.valuelog_max_entries = valuelog_max_entries;
        self
    }
    pub fn num_compactors(mut self, num_compactors: usize) -> Self {
        self.num_compactors = num_compactors;
        self
    }
    pub fn compactl0_on_close(mut self, compactl0_on_close: bool) -> Self {
        self.compactl0_on_close = compactl0_on_close;
        self
    }
    pub fn compression(mut self,compression:Option<CompressionType>)->Self{
        self.compression=compression;
        self
    }
    pub fn verify_value_checksum(mut self,verify_value_checksum:bool)->Self{
        self.verify_value_checksum=verify_value_checksum;
        self
    }
    pub fn block_cache_size(mut self,block_cache_size: u64)->Self{
        self.block_cache_size=block_cache_size;
        self
    }
    pub fn in_memory(mut self,in_memory:bool)->Self{
        self.in_memory=in_memory;
        self
    }
    // pub fn zstd_compression_level(mut self,level:isize)->Self{
    //     self
    // }
    pub fn bypass_lock_guard(mut self,bypass_lock_guard:bool)->Self{
        self.bypass_lock_guard=bypass_lock_guard;
        self
    }
    pub fn index_cache_size(mut self,index_cache_size:i64)->Self{
        self.index_cache_size=index_cache_size;
        self
    }
    pub fn name_space_offset(mut self,offset:isize)->Self{
        self.name_space_offset=offset;
        self
    }
    pub fn detect_conflicts(mut self,detect_conflicts:bool)->Self{
        self.detect_conflicts=detect_conflicts;
        self
    }
    pub fn external_magic_version(mut self,magic:u16)->Self{
        self.external_magic_version=magic;
        self
    }
}
impl Options {
    pub(crate) fn check_set_options(&mut self)->anyhow::Result<()>{
        if self.num_compactors==1{
            bail!("Cannot have 1 compactor. Need at least 2");
        }
        if self.in_memory && (self.dir!="" || self.value_dir!=""){
            bail!("Cannot use badger in Disk-less mode with Dir or ValueDir set");
        }
        log::set_max_level(self.log_level);
        self.max_batch_size=(15*self.memtable_size as i64)/100;

        // bail!();
        Ok(())
    }
}