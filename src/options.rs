use std::string;

use log::LevelFilter;
pub enum CompressionType {
    Snappy,
    ZSTD { level: u16 },
}
const max_value_threshold: i64 = 1 << 20;
pub struct Options {
    // Required options.
    dir: String,
    value_dir: String,

    // Usually modified options.
    sync_writes: bool,
    num_versions_to_keep: isize,
    read_only: bool,
    // Logger            Logger
    log_level: log::LevelFilter,
    compression: Option<CompressionType>,
    in_memory: bool,
    metrics_enabled: bool,
    // Sets the Stream.numGo field
    num_goroutines: usize,

    // Fine tuning options.
    memtable_size: i64,
    base_table_size: u64,
    base_level_size: i64,
    level_size_multiplier: isize,
    table_size_multiplier: isize,
    max_levels: usize,

    vlog_percentile: f64,
    value_threshold: i64,
    num_memtables: usize,
    // Changing BlockSize across DB runs will not break badger. The block size is
    // read from the block index stored at the end of the table.
    block_size: isize,
    bloom_false_positive: f64,
    block_cache_size: i64,
    index_cache_size: i64,

    num_level_zero_tables: isize,
    num_level_zero_tables_stall: isize,

    valuelog_file_size: i64,
    valuelog_max_entries: u32,

    num_compactors: isize,
    compactl0_on_close: bool,
    lmax_compaction: bool,
    zstd_compression_level: isize,

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
            dir: "./tmp/badger".to_string(),
            value_dir: "./tmp/badger".to_string(),
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
            value_threshold: max_value_threshold,
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
            zstd_compression_level: 1,
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
    pub fn sync_writess(mut self,sync_writes:bool)->Self{
        self.sync_writes=sync_writes;
        self
    }
    pub fn num_versions_to_keep(mut self,num_versions_to_keep:isize)->Self{
        self.num_versions_to_keep=num_versions_to_keep;
        self
    }
    pub fn num_goroutines(mut self,routines:usize)->Self{
        self.num_goroutines=routines;
        self
    }
    pub fn read_only(mut self,read_only:bool)->Self{
        self.read_only=read_only;
        self
    }
    pub fn metrics_enabled(mut self,metrics_enabled:bool)->Self{
        self.metrics_enabled=metrics_enabled;
        self
    }
    pub fn log_level(mut self,log_level:LevelFilter)->Self{
        self.log_level=log_level;
        self
    }
    pub fn base_table_size(mut self,base_table_size:u64)->Self{
        self.base_table_size=base_table_size;
        self
    }
    pub fn level_size_multiplier(mut self,level_size_multiplier:isize)->Self{
        self.level_size_multiplier=level_size_multiplier;
        self
    }
    pub fn max_levels(mut self,max_levels:usize)->Self{
        self.max_levels=max_levels;
        self
    }
    pub fn value_threshold(mut self,value_threshold:i64)->Self{
        self.value_threshold=value_threshold;
        self
    }
    pub fn vlog_percentile(mut self,vlog_percentile:f64)->Self{
        self.vlog_percentile=vlog_percentile;
        self
    }
    pub fn num_memtables(mut self,num_memtables:usize)->Self{
        self.num_memtables=num_memtables;
        self
    }

}
