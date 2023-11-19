use std::fs::{create_dir_all, set_permissions, Permissions};
use std::os::unix::prelude::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;

use crate::key_registry::KeyRegistryConfig;
use crate::level::levels::LevelsControllerConfig;
use crate::manifest::ManifestConfig;
use crate::memtable::MemTableConfig;
use crate::table::TableConfig;
use crate::txn::TxnConfig;
use crate::util::cache::{BlockCacheConfig, IndexCacheConfig};
use crate::util::lock::DBLockGuardConfig;
use crate::util::skip_list::SKL_MAX_NODE_SIZE;
use crate::vlog::threshold::VlogThresholdConfig;
use crate::vlog::ValueLogConfig;
use anyhow::anyhow;
use anyhow::bail;
use log::LevelFilter;
use snap::raw::Decoder;
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq)]
pub enum CompressionType {
    None,
    Snappy,
    ZSTD(i32),
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
            2 => Self::ZSTD(1),
            _ => Self::None,
        }
    }
}
impl Into<u32> for CompressionType {
    fn into(self) -> u32 {
        match self {
            CompressionType::None => 0,
            CompressionType::Snappy => 1,
            CompressionType::ZSTD(_) => 2,
        }
    }
}
impl CompressionType {
    #[inline]
    pub(crate) fn compress(&self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        match self {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Snappy => Ok(snap::raw::Encoder::new().compress_vec(data)?),
            CompressionType::ZSTD(level) => Ok(zstd::encode_all(data, *level)?),
        }
    }
    #[inline]
    pub(crate) fn decompress(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        match self {
            CompressionType::None => Ok(data),
            CompressionType::Snappy => Ok(Decoder::new().decompress_vec(&data)?),
            CompressionType::ZSTD(_) => Ok(zstd::decode_all(data.as_slice())?),
        }
    }
}
impl CompressionType {
    pub(crate) fn is_none(&self) -> bool {
        match self {
            CompressionType::None => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    // Usually modified Config.
    sync_writes: bool,
    num_versions_to_keep: isize,
    read_only: bool,
    log_level: LevelFilter,
    num_memtables: usize,
    // NamespaceOffset specifies the offset from where the next 8 bytes contains the namespace.
    name_space_offset: Option<usize>,
    // When set, checksum will be validated for each entry read from the value log file.
    verify_value_checksum: bool,

    pub memtable: MemTableConfig,
    pub block_cache: BlockCacheConfig,
    pub index_cache: IndexCacheConfig,
    pub level_controller: LevelsControllerConfig,
    pub table: TableConfig,
    pub vlog: ValueLogConfig,

    pub key_registry: KeyRegistryConfig,
    pub lock_guard: DBLockGuardConfig,

    pub vlog_threshold: VlogThresholdConfig,
    pub txn: TxnConfig,
    pub manifest: ManifestConfig,

    // 4. Flags for testing purposes
    // ------------------------------
    max_batch_count: usize, // max entries in batch
    max_batch_size: usize,  // max batch size in bytes

                            // max_value_threshold: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            sync_writes: false,
            num_versions_to_keep: 1,
            read_only: false,
            log_level: log::LevelFilter::Info,
            num_memtables: 5,
            verify_value_checksum: false,
            name_space_offset: None,
            max_batch_count: Default::default(),
            max_batch_size: Default::default(),
            manifest: Default::default(),
            lock_guard: Default::default(),
            block_cache: Default::default(),
            index_cache: Default::default(),
            key_registry: Default::default(),
            memtable: Default::default(),
            level_controller: Default::default(),
            table: Default::default(),
            vlog_threshold: Default::default(),
            vlog: Default::default(),
            txn: Default::default(),
        }
    }
}
impl Config {
    #[deny(unused)]
    pub(crate) fn init_lock_guard(&mut self) {
        for path in [
            self.manifest.dir(),
            self.key_registry.dir(),
            self.memtable.dir(),
            self.level_controller.dir(),
            self.vlog.value_dir(),
        ] {
            if !self.lock_guard.contains(path) {
                self.lock_guard.insert(path.clone());
            };
        }
    }

    pub(crate) fn max_batch_count(&self) -> usize {
        self.max_batch_count
    }

    pub(crate) fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub fn sync_writes(&self) -> bool {
        self.sync_writes
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    pub fn num_memtables(&self) -> usize {
        self.num_memtables
    }

    pub fn name_space_offset(&self) -> Option<usize> {
        self.name_space_offset
    }
}
impl Config {
    pub fn set_dir<P: AsRef<Path>>(mut self, dir: P) -> Self {
        let p = dir.as_ref().to_path_buf();
        assert_ne!(p, PathBuf::from(""));
        self.manifest.set_dir(p.clone());
        self.key_registry.set_dir(p.clone());
        self.level_controller.set_dir(p.clone());
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

    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self.manifest.set_read_only(read_only);
        self.lock_guard.set_read_only(read_only);
        self.key_registry.set_read_only(read_only);
        self.memtable.set_read_only(read_only);
        self.vlog.set_read_only(read_only);
        self
    }

    pub fn set_log_level(mut self, log_level: LevelFilter) -> Self {
        self.log_level = log_level;
        self
    }

    pub fn set_num_memtables(mut self, num_memtables: usize) -> Self {
        self.num_memtables = num_memtables;
        self
    }

    pub fn set_verify_value_checksum(mut self, verify_value_checksum: bool) -> Self {
        self.verify_value_checksum = verify_value_checksum;
        self
    }

    pub fn set_name_space_offset(mut self, offset: usize) -> Self {
        self.name_space_offset = offset.into();
        self
    }
}
impl Config {
    pub(crate) fn check_set_config(&mut self) -> anyhow::Result<()> {
        self.level_controller.check_levels_controller_config()?;
        log::set_max_level(self.log_level);
        self.max_batch_size = (15 * self.memtable.memtable_size()) / 100;
        self.max_batch_count = self.max_batch_size / (SKL_MAX_NODE_SIZE);
        self.memtable.set_arena_size(
            self.memtable.memtable_size()
                + self.max_batch_size
                + self.max_batch_count * SKL_MAX_NODE_SIZE,
        );

        self.vlog_threshold
            .check_threshold_config(self.max_batch_size)?;

        self.block_cache.set_block_size(self.table.block_size());

        self.vlog.check_vlog_config()?;

        let need_cache =
            !self.table.compression().is_none() || self.key_registry.encrypt_key().len() > 0;

        if need_cache && self.block_cache.block_cache_size() == 0 {
            panic!("Block_Cache_Size should be set since compression are enabled")
        }
        Ok(())
    }
    fn create_dirs(&self) -> anyhow::Result<()> {
        for path in [
            self.manifest.dir(),
            self.key_registry.dir(),
            self.memtable.dir(),
            self.level_controller.dir(),
            self.vlog.value_dir(),
        ] {
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
    #[deny(unused)]
    pub(super) fn init(&mut self) -> anyhow::Result<()> {
        self.check_set_config()?;
        self.create_dirs()?;
        self.init_lock_guard();
        self.index_cache.init(self.memtable.memtable_size());
        Ok(())
    }
}
