use std::{
    fs::{create_dir_all, set_permissions, Permissions},
    os::unix::prelude::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::anyhow;
use anyhow::bail;
use tokio::sync::RwLock;

use crate::{
    default::{LOCK_FILE, MAX_VALUE_THRESHOLD, SKL_MAX_NODE_SIZE},
    errors::DBError,
    lock::{self, DirLockGuard},
    manifest::open_create_manifestfile,
    options::Options,
};
pub struct DB {
    lock: RwLock<()>,
}
impl DB {
    pub fn open(opt: &mut Options) -> anyhow::Result<()> {
        opt.check_set_options()?;
        let mut dir_lock_guard = None;
        let mut value_dir_lock_guard = None;
        if !opt.in_memory {
            opt.create_dirs()?;
            if !opt.bypass_lock_guard {
                dir_lock_guard =
                    DirLockGuard::acquire_lock(&opt.dir, LOCK_FILE, opt.read_only)?.into();
                if opt.value_dir.canonicalize()? != opt.dir.canonicalize()? {
                    value_dir_lock_guard =
                        DirLockGuard::acquire_lock(&opt.value_dir, LOCK_FILE, opt.read_only)?
                            .into();
                };
            }
        }
        open_create_manifestfile(&opt);
        drop(value_dir_lock_guard);
        drop(dir_lock_guard);
        Ok(())
    }
}
impl Options {
    pub(crate) fn check_set_options(&mut self) -> anyhow::Result<()> {
        if self.num_compactors == 1 {
            bail!("Cannot have 1 compactor. Need at least 2");
        }
        if self.in_memory && (self.dir != PathBuf::from("") || self.value_dir != PathBuf::from(""))
        {
            bail!("Cannot use badger in Disk-less mode with Dir or ValueDir set");
        }
        log::set_max_level(self.log_level);
        self.max_batch_size = (15 * self.memtable_size as i64) / 100;
        self.max_batch_count = self.max_batch_size / (SKL_MAX_NODE_SIZE as i64);
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
        if !(self.valuelog_file_size >= 1 << 20 && self.valuelog_file_size < 2 << 30) {
            bail!(DBError::ValuelogSize);
        }
        if self.read_only {
            self.compactl0_on_close = false;
        }
        let need_cache = self.compression.is_some();
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
}
