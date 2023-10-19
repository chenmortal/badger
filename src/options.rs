use std::fs::{create_dir_all, set_permissions, Permissions};
use std::os::unix::prelude::PermissionsExt;
use std::{path::PathBuf, time::Duration};

use crate::default::{DEFAULT_DIR, DEFAULT_VALUE_DIR, MAX_VALUE_THRESHOLD};
use crate::errors::DBError;
use crate::skl::skip_list::SKL_MAX_NODE_SIZE;
use crate::table::opt::ChecksumVerificationMode;
use anyhow::anyhow;
use anyhow::bail;
use log::LevelFilter;
use once_cell::sync::OnceCell;
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd,Eq)]
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
// static OPT:Options=Options::default();
// const MAX_VALUE_THRESHOLD: i64 = 1 << 20;
#[derive(Debug, Clone)]
pub struct Options {
    // Required options.
    dir: PathBuf,
    value_dir: PathBuf,

    // Usually modified options.
    sync_writes: bool,
    num_versions_to_keep: isize,
    read_only: bool,
    log_level: LevelFilter,
    compression: CompressionType,
    // pub(crate) in_memory: bool,
    metrics_enabled: bool,
    // Sets the Stream.numGo field
    num_goroutines: usize,

    // Fine tuning options.
    memtable_size: u32,
    base_table_size: usize,
    base_level_size: usize,
    level_size_multiplier: usize,
    table_size_multiplier: usize,
    max_levels: usize,

    vlog_percentile: f64,
    value_threshold: u32,
    num_memtables: usize,
    // Changing BlockSize across DB runs will not break badger. The block size is
    // read from the block index stored at the end of the table.
    block_size: usize,
    bloom_false_positive: f64,
    block_cache_size: usize,
    index_cache_size: i64,

    num_level_zero_tables: usize,
    num_level_zero_tables_stall: usize,

    vlog_file_size: usize,
    vlog_max_entries: usize,

    num_compactors: usize,
    compactl0_on_close: bool,
    lmax_compaction: bool,
    zstd_compression_level: i32,

    // When set, checksum will be validated for each entry read from the value log file.
    verify_value_checksum: bool,

    // Encryption related options.
    encryption_key: Vec<u8>,                    // encryption key
    encryption_key_rotation_duration: Duration, // key rotation duration

    // BypassLockGuard will bypass the lock guard on badger. Bypassing lock
    // guard can cause data corruption if multiple badger instances are using
    // the same directory. Use this options with caution.
    bypass_lock_guard: bool,

    // ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
    checksum_verification_mode: ChecksumVerificationMode,

    // DetectConflicts determines whether the transactions would be checked for
    // conflicts. The transactions can be processed at a higher rate when
    // conflict detection is disabled.
    detect_conflicts: bool,

    // NamespaceOffset specifies the offset from where the next 8 bytes contains the namespace.
    name_space_offset: Option<usize>,

    // Magic version used by the application using badger to ensure that it doesn't open the DB
    // with incompatible data format.
    external_magic_version: u16,

    // Transaction start and commit timestamps are managed by end-user.
    // This is only useful for databases built on top of Badger (like Dgraph).
    // Not recommended for most users.
    managed_txns: bool,

    // 4. Flags for testing purposes
    // ------------------------------
    max_batch_count: u32, // max entries in batch
    max_batch_size: u32,  // max batch size in bytes

    max_value_threshold: f64,
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
            vlog_file_size: 1 << 30 - 1,
            vlog_max_entries: 1000_000,
            num_compactors: 4,
            compactl0_on_close: false,
            lmax_compaction: Default::default(),
            zstd_compression_level: 1,
            verify_value_checksum: false,
            bypass_lock_guard: Default::default(),
            detect_conflicts: true,
            name_space_offset: None,
            external_magic_version: Default::default(),
            managed_txns: Default::default(),
            max_batch_count: Default::default(),
            max_batch_size: Default::default(),
            max_value_threshold: Default::default(),
            encryption_key: Vec::new(),
            encryption_key_rotation_duration: Duration::from_secs(10 * 24 * 60 * 60),
            checksum_verification_mode: Default::default(),
        }
    }
}
impl Options {
    pub fn set_dir(mut self, dir: String) -> Self {
        assert_ne!(dir, "");
        self.dir = dir.into();
        self
    }
    pub fn set_value_dir(mut self, value_dir: String) -> Self {
        assert_ne!(value_dir, "");
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
    pub fn set_base_table_size(mut self, base_table_size: usize) -> Self {
        self.base_table_size = base_table_size;
        self
    }
    pub fn set_level_size_multiplier(mut self, level_size_multiplier: usize) -> Self {
        self.level_size_multiplier = level_size_multiplier;
        self
    }
    pub fn set_max_levels(mut self, max_levels: usize) -> Self {
        self.max_levels = max_levels;
        self
    }
    pub fn set_value_threshold(mut self, value_threshold: u32) -> Self {
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
    pub fn set_memtable_size(mut self, memtable_size: u32) -> Self {
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
    pub fn set_num_level_zero_tables_stall(mut self, num_level_zero_tables_stall: usize) -> Self {
        self.num_level_zero_tables_stall = num_level_zero_tables_stall;
        self
    }
    pub fn set_base_level_size(mut self, base_level_size: usize) -> Self {
        self.base_level_size = base_level_size;
        self
    }
    pub fn set_vlog_file_size(mut self, vlog_file_size: usize) -> Self {
        self.vlog_file_size = vlog_file_size;
        self
    }
    pub fn set_valuelog_max_entries(mut self, vlog_max_entries: usize) -> Self {
        self.vlog_max_entries = vlog_max_entries;
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
    pub fn set_block_cache_size(mut self, block_cache_size: usize) -> Self {
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
    pub fn set_name_space_offset(mut self, offset: usize) -> Self {
        self.name_space_offset = offset.into();
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

    pub fn set_encryption_key(mut self, encryption_key: Vec<u8>) -> Self {
        self.encryption_key = encryption_key;
        self
    }

    pub fn set_encryption_key_rotation_duration(
        mut self,
        encryption_key_rotation_duration: Duration,
    ) -> Self {
        self.encryption_key_rotation_duration = encryption_key_rotation_duration;
        self
    }
    // ChecksumVerificationMode indicates when the db should verify checksums for SSTable blocks.
    //
    // The default value of VerifyValueChecksum is options.NoVerification.
    pub fn set_checksum_verification_mode(
        mut self,
        checksum_verification_mode: ChecksumVerificationMode,
    ) -> Self {
        self.checksum_verification_mode = checksum_verification_mode;
        self
    }

    pub fn set_table_size_multiplier(mut self, table_size_multiplier: usize) -> Self {
        self.table_size_multiplier = table_size_multiplier;
        self
    }
}
static OPT: OnceCell<Options> = once_cell::sync::OnceCell::new();
impl Options {
    pub(crate) fn check_set_options(&mut self) -> anyhow::Result<()> {
        if self.num_compactors == 1 {
            bail!("Cannot have 1 compactor. Need at least 2");
        }
        // if self.in_memory && (self.dir != PathBuf::from("") || self.value_dir != PathBuf::from(""))
        // {
        //     bail!("Cannot use badger in Disk-less mode with Dir or ValueDir set");
        // }
        log::set_max_level(self.log_level);
        self.max_batch_size = (15 * self.memtable_size) / 100;
        self.max_batch_count = self.max_batch_size / (SKL_MAX_NODE_SIZE);
        self.max_value_threshold = MAX_VALUE_THRESHOLD.min(self.max_batch_size) as f64;
        if self.vlog_percentile < 0.0 || self.vlog_percentile > 1.0 {
            bail!("vlog_percentile must be within range of 0.0-1.0")
        }
        if self.value_threshold > MAX_VALUE_THRESHOLD {
            bail!(
                "Invalid ValueThreshold, must be less or equal to {}",
                MAX_VALUE_THRESHOLD
            );
        }
        if self.value_threshold > self.max_batch_size {
            bail!("Valuethreshold {} greater than max batch size of {}. Either reduce Valuethreshold or increase max_table_size",self.value_threshold,self.max_batch_size);
        }
        if !(self.vlog_file_size >= 1 << 20 && self.vlog_file_size < 2 << 30) {
            bail!(DBError::ValuelogSize);
        }
        if self.read_only {
            self.compactl0_on_close = false;
        }
        match self.compression {
            _ => {}
        }
        let need_cache = match self.compression {
            crate::options::CompressionType::None => true,
            _ => false,
        };
        if need_cache && self.block_cache_size == 0 {
            panic!("Block_Cache_Size should be set since compression are enabled")
        }
        Ok(())
    }
    fn create_dirs(&self) -> anyhow::Result<()> {
        for path in [&self.dir, &self.value_dir] {
            if !path
                .try_exists()
                .map_err(|e| anyhow!("Invalid Dir : {}", e))?
            {
                if self.read_only {
                    bail!("Cannot find directory {:?} for read-only open", path)
                }
                create_dir_all(path)
                    .map_err(|e| anyhow!("Error Creating Dir: {:?} : {}", path, e))?;
                set_permissions(path, Permissions::from_mode(0o700))
                    .map_err(|e| anyhow!("Error Set Permissions 0o700: {:?} : {}", path, e))?;
            };
        }

        Ok(())
    }
    pub(super) fn init(mut self) -> anyhow::Result<()> {
        self.check_set_options()?;
        self.create_dirs()?;
        match OPT.set(self) {
            Ok(_) => {}
            Err(e) => {
                panic!("cannot set static OPT with {:?}", e);
            }
        };
        Ok(())
    }
    fn get_static() -> &'static Self {
        unsafe { OPT.get_unchecked() }
    }

    pub(crate) fn dir() -> &'static PathBuf {
        &Self::get_static().dir
    }

    pub(crate) fn value_dir() -> &'static PathBuf {
        &Self::get_static().value_dir
    }

    pub(crate) fn sync_writes() -> bool {
        Self::get_static().sync_writes
    }

    pub(crate) fn read_only() -> bool {
        Self::get_static().read_only
    }

    pub(crate) fn log_level() -> LevelFilter {
        Self::get_static().log_level
    }

    pub(crate) fn compression() -> CompressionType {
        Self::get_static().compression
    }

    pub(crate) fn metrics_enabled() -> bool {
        Self::get_static().metrics_enabled
    }

    pub(crate) fn memtable_size() -> u32 {
        Self::get_static().memtable_size
    }

    pub(crate) fn base_table_size() -> usize {
        Self::get_static().base_table_size
    }

    pub(crate) fn base_level_size() -> usize {
        Self::get_static().base_level_size
    }

    pub(crate) fn level_size_multiplier() -> usize {
        Self::get_static().level_size_multiplier
    }

    pub(crate) fn max_levels() -> usize {
        Self::get_static().max_levels
    }

    pub(crate) fn vlog_percentile() -> f64 {
        Self::get_static().vlog_percentile
    }

    pub(crate) fn value_threshold() -> usize {
        Self::get_static().value_threshold as _
    }

    pub(crate) fn num_memtables() -> usize {
        Self::get_static().num_memtables
    }

    pub(crate) fn block_size() -> usize {
        Self::get_static().block_size
    }

    pub(crate) fn bloom_false_positive() -> f64 {
        Self::get_static().bloom_false_positive
    }

    pub(crate) fn block_cache_size() -> usize {
        Self::get_static().block_cache_size
    }

    pub(crate) fn index_cache_size() -> i64 {
        Self::get_static().index_cache_size
    }

    pub(crate) fn num_level_zero_tables() -> usize {
        Self::get_static().num_level_zero_tables
    }

    pub(crate) fn num_level_zero_tables_stall() -> usize {
        Self::get_static().num_level_zero_tables_stall
    }

    pub(crate) fn vlog_file_size() -> usize {
        Self::get_static().vlog_file_size
    }

    pub(crate) fn vlog_max_entries() -> usize {
        Self::get_static().vlog_max_entries
    }

    pub(crate) fn num_compactors() -> usize {
        Self::get_static().num_compactors
    }

    pub(crate) fn compactl0_on_close() -> bool {
        Self::get_static().compactl0_on_close
    }

    pub(crate) fn zstd_compression_level() -> i32 {
        Self::get_static().zstd_compression_level
    }

    pub(crate) fn encryption_key() -> &'static [u8] {
        Self::get_static().encryption_key.as_ref()
    }

    pub(crate) fn encryption_key_rotation_duration() -> Duration {
        Self::get_static().encryption_key_rotation_duration
    }

    pub(crate) fn bypass_lock_guard() -> bool {
        Self::get_static().bypass_lock_guard
    }

    pub(crate) fn checksum_verification_mode() -> ChecksumVerificationMode {
        Self::get_static().checksum_verification_mode
    }

    pub(crate) fn detect_conflicts() -> bool {
        Self::get_static().detect_conflicts
    }

    pub(crate) fn name_space_offset() -> Option<usize> {
        Self::get_static().name_space_offset
    }

    pub(crate) fn external_magic_version() -> u16 {
        Self::get_static().external_magic_version
    }

    pub(crate) fn managed_txns() -> bool {
        Self::get_static().managed_txns
    }

    pub(crate) fn max_batch_count() -> u32 {
        Self::get_static().max_batch_count
    }

    pub(crate) fn max_batch_size() -> u32 {
        Self::get_static().max_batch_size
    }

    pub(crate) fn max_value_threshold() -> f64 {
        Self::get_static().max_value_threshold
    }
    pub(crate) fn table_size_multiplier() -> usize {
        Self::get_static().table_size_multiplier
    }
}
