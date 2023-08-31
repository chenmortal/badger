use std::{
    fs::{create_dir_all, set_permissions, Permissions},
    os::unix::prelude::PermissionsExt,
    sync::atomic::AtomicU32,
};

use crate::{
    default::{LOCK_FILE, MAX_VALUE_THRESHOLD},
    errors::DBError,
    lock::DirLockGuard,
    lsm::memtable::MemTable,
    manifest::open_create_manifestfile,
    options::Options,
    skl::skip_list::SKL_MAX_NODE_SIZE,
    value::threshold::VlogThreshold, key_registry::KeyRegistryOptions,
};
use anyhow::anyhow;
use anyhow::bail;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
#[derive(Debug, Default)]
pub struct DB {
    lock: RwLock<()>,
    pub(crate) opt: Options,
    next_mem_fid: AtomicU32,
    // imm:Vec<>
}
impl DB {
    pub fn open(opt: &mut Options) -> anyhow::Result<()> {
        opt.check_set_options()?;
        let mut dir_lock_guard = None;
        let mut value_dir_lock_guard = None;
        // if !opt.in_memory {
        opt.create_dirs()?;
        if !opt.bypass_lock_guard {
            dir_lock_guard = DirLockGuard::acquire_lock(&opt.dir, LOCK_FILE, opt.read_only)?.into();
            if opt.value_dir.canonicalize()? != opt.dir.canonicalize()? {
                value_dir_lock_guard =
                    DirLockGuard::acquire_lock(&opt.value_dir, LOCK_FILE, opt.read_only)?.into();
            };
        }
        // }
        let (manifest_file, manifest) = open_create_manifestfile(&opt)?;
        let imm = Vec::<MemTable>::with_capacity(opt.num_memtables);
        let (sender, receiver) = mpsc::channel::<MemTable>(opt.num_memtables);
        let threshold = VlogThreshold::new(&opt);

        if opt.block_cache_size > 0 {
            let mut num_in_cache = opt.block_cache_size / opt.block_size;
            if num_in_cache == 0 {
                num_in_cache = 1;
            }
            // let block_cache = stretto::AsyncCacheBuilder::new(num_in_cache * 8, opt.block_cache_size as i64)
            // .set_buffer_items(64)
            // .set_metrics(true);;
        }
        if opt.index_cache_size > 0 {
            let index_sz = (opt.memtable_size as f64 * 0.05) as usize;
            let mut num_in_cache = opt.index_cache_size as usize / index_sz;
            if num_in_cache == 0 {
                num_in_cache = 1;
            }
            // let index_cache = stretto::AsyncCacheBuilder::new(num_in_cache * 8, opt.index_cache_size)
                            // .set_buffer_items(64)
                            // .set_metrics(true);;
        }

        // KeyRegistryOptions{
        //     dir: opt.dir.clone(),
        //     read_only: opt.read_only,
        //     encryption_key: todo!(),
        //     encryption_key_rotation_duration: todo!(),
        // };


        drop(value_dir_lock_guard);
        drop(dir_lock_guard);
        Ok(())
    }
    #[inline]
    pub(crate) fn get_next_mem_fid(&mut self) -> u32 {
        self.next_mem_fid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
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
        self.max_value_threshold = MAX_VALUE_THRESHOLD.min(self.max_batch_size as i64) as f64;
        if self.vlog_percentile < 0.0 || self.vlog_percentile > 1.0 {
            bail!("vlog_percentile must be within range of 0.0-1.0")
        }
        if self.value_threshold > MAX_VALUE_THRESHOLD {
            bail!(
                "Invalid ValueThreshold, must be less or equal to {}",
                MAX_VALUE_THRESHOLD
            );
        }
        if self.value_threshold > self.max_batch_size as i64 {
            bail!("Valuethreshold {} greater than max batch size of {}. Either reduce Valuethreshold or increase max_table_size",self.value_threshold,self.max_batch_size);
        }
        if !(self.valuelog_file_size >= 1 << 20 && self.valuelog_file_size < 2 << 30) {
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
}
