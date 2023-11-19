use std::{
    collections::{BTreeMap, HashSet},
    fs::{read_dir, OpenOptions},
    path::PathBuf,
    sync::{
        atomic::{AtomicI32, AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::anyhow;
use anyhow::bail;
use log::info;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    default::DEFAULT_VALUE_DIR,
    errors::{err_file, DBError},
    key_registry::KeyRegistry,
    util::{log_file::LogFile, DBFileId, VlogId},
    vlog::read::LogFileIter,
};

use self::{discard::DiscardStats, threshold::VlogThreshold};

pub(crate) mod discard;
pub(crate) mod header;
pub(crate) mod read;
pub(crate) mod threshold;
pub(crate) mod write;
mod histogram;
// size of vlog header.
// +----------------+------------------+
// | keyID(8 bytes) |  baseIV(12 bytes)|
// +----------------+------------------+
pub(crate) const VLOG_HEADER_SIZE: usize = 20;
pub(crate) const MAX_HEADER_SIZE: usize = 22;
// pub(crate) const BIT_DELETE: u8 = 1 << 0;
// pub(crate) const BIT_VALUE_POINTER: u8 = 1 << 1;
// pub(crate) const BIT_DISCARD_EARLIER_VERSIONS: u8 = 1 << 2;
// pub(crate) const BIT_MERGE_ENTRY: u8 = 1 << 3;
// pub(crate) const BIT_TXN: u8 = 1 << 6;
// pub(crate) const BIT_FIN_TXN: u8 = 1 << 7;

pub(crate) const MAX_VLOG_FILE_SIZE: usize = u32::MAX as usize;
#[derive(Debug, Error)]
pub enum VlogError {
    #[error("Do truncate")]
    Truncate,
    #[error("Stop iteration")]
    Stop,
}
#[derive(Debug, Clone)]
pub struct ValueLogConfig {
    read_only: bool,
    value_dir: PathBuf,
    vlog_file_size: usize,
    vlog_max_entries: usize,
    sync_writes: bool,
}
impl Default for ValueLogConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            value_dir: PathBuf::from(DEFAULT_VALUE_DIR),
            vlog_file_size: 1 << 30 - 1,
            sync_writes: false,
            vlog_max_entries: 1_000_000,
        }
    }
}
impl ValueLogConfig {
    pub(crate) fn check_vlog_config(&self) -> Result<(), DBError> {
        if !(self.vlog_file_size >= 1 << 20 && self.vlog_file_size < 2 << 30) {
            return Err(DBError::ValuelogSize);
        }
        Ok(())
    }

    pub(crate) fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn set_value_dir(&mut self, value_dir: PathBuf) {
        self.value_dir = value_dir;
    }

    pub fn set_vlog_file_size(&mut self, vlog_file_size: usize) {
        self.vlog_file_size = vlog_file_size;
    }

    pub fn vlog_file_size(&self) -> usize {
        self.vlog_file_size
    }

    pub fn value_dir(&self) -> &PathBuf {
        &self.value_dir
    }
}
#[derive(Debug)]
pub(crate) struct ValueLog {
    fid_logfile: RwLock<BTreeMap<VlogId, Arc<RwLock<LogFile<VlogId>>>>>,
    max_fid: AtomicU32,
    files_to_be_deleted: Vec<u32>,
    num_active_iter: AtomicI32,
    writable_log_offset: AtomicUsize,
    num_entries_written: AtomicUsize,
    discard_stats: DiscardStats,
    threshold: VlogThreshold,
    key_registry: KeyRegistry,
    config: ValueLogConfig,
}

impl ValueLog {
    pub(crate) fn new(
        threshold: VlogThreshold,
        key_registry: KeyRegistry,
        config: ValueLogConfig,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            fid_logfile: Default::default(),
            max_fid: Default::default(),
            files_to_be_deleted: Default::default(),
            num_active_iter: Default::default(),
            writable_log_offset: Default::default(),
            num_entries_written: Default::default(),
            discard_stats: discard::DiscardStats::new(&config.value_dir)?,
            threshold,
            key_registry,
            config,
        })
    }
    pub(crate) async fn open(&mut self) -> anyhow::Result<()> {
        self.populate_files_map(self.key_registry.clone()).await?;

        let fid_logfile_r = self.fid_logfile.read().await;
        let fid_logfile_len = fid_logfile_r.len();
        drop(fid_logfile_r);

        if self.config.read_only {
            return Ok(());
        }
        if fid_logfile_len == 0 {
            self.create_vlog_file()
                .await
                .map_err(|e| anyhow!("Error while creating log file in vlog.open for {}", e))?;
            return Ok(());
        }

        let last_logfile = self.get_latest_logfile().await?;
        let mut last_logfile_w = last_logfile.write().await;
        let mut last_log_file_iter = LogFileIter::new(&last_logfile_w, VLOG_HEADER_SIZE);
        loop {
            if let Some(_) = last_log_file_iter.next()? {
                continue;
            };
            break;
        }
        let file_len = last_log_file_iter.valid_end_offset();
        drop(last_log_file_iter);
        last_logfile_w.truncate(file_len)?;

        self.create_vlog_file()
            .await
            .map_err(|e| anyhow!("Error while creating log file in vlog.open for {}", e))?;
        Ok(())
    }

    async fn populate_files_map(&mut self, key_registry: KeyRegistry) -> anyhow::Result<()> {
        let dir = &self.config.value_dir;
        let read_only = self.config.read_only;
        let mut fp_open_opt = OpenOptions::new();
        fp_open_opt.read(true).write(!read_only);
        let mut found = HashSet::new();
        let mut fid_logfile_w = self.fid_logfile.write().await;
        for ele in read_dir(dir).map_err(|e| err_file(e, dir, "Unable to open log dir."))? {
            let entry = ele.map_err(|e| err_file(e, dir, "Unable to read dir entry"))?;
            let path = entry.path();
            if let Ok(fid) = VlogId::parse(&path) {
                if !found.insert(fid) {
                    bail!("Duplicate file found. Please delete one.")
                };
                let (log_file, _) = LogFile::open(
                    fid,
                    &path,
                    fp_open_opt.clone(),
                    2 * self.config.vlog_file_size,
                    key_registry.clone(),
                )
                .await
                .map_err(|e| anyhow!("Open existing file: {:?} for {}", path, e))?;

                // delete empty fid
                if log_file.get_size() == VLOG_HEADER_SIZE {
                    info!("Deleting empty file: {:?}", path);
                    log_file.delete().map_err(|e| {
                        anyhow!("While trying to delete empty file: {:?} for {}", &path, e)
                    })?;
                }
                fid_logfile_w.insert(fid, Arc::new(RwLock::new(log_file)));
                self.max_fid.fetch_max(fid.into(), Ordering::SeqCst);
            };
        }
        drop(fid_logfile_w);
        Ok(())
    }
    async fn create_vlog_file(&self) -> anyhow::Result<Arc<RwLock<LogFile<VlogId>>>> {
        let fid: VlogId = (self.max_fid.fetch_add(1, Ordering::SeqCst) + 1).into();
        let file_path = fid.join_dir(&self.config.value_dir);
        let mut fp_open_opt = OpenOptions::new();
        fp_open_opt.read(true).write(true).create_new(true);
        let (log_file, _) = LogFile::open(
            fid,
            &file_path,
            fp_open_opt,
            2 * self.config.vlog_file_size,
            self.key_registry.clone(),
        )
        .await?;
        let mut fid_logfile_w = self.fid_logfile.write().await;
        let new_logfile = Arc::new(RwLock::new(log_file));
        fid_logfile_w.insert(fid, new_logfile.clone());
        // debug_assert!(fid > self.max_fid);
        // self.max_fid = fid;
        self.writable_log_offset
            .store(VLOG_HEADER_SIZE, Ordering::SeqCst);
        self.num_entries_written.store(0, Ordering::SeqCst);
        drop(fid_logfile_w);
        Ok(new_logfile)
    }

    #[inline]
    pub(crate) async fn get_latest_logfile(&self) -> anyhow::Result<Arc<RwLock<LogFile<VlogId>>>> {
        let p = self.fid_logfile.read().await;
        let vlog_id = self.max_fid.load(Ordering::SeqCst).into();
        let r = if let Some(s) = p.get(&vlog_id) {
            s.clone()
        } else {
            bail!("Failed to get latest_logfile");
        };
        drop(p);
        Ok(r)
    }
}
