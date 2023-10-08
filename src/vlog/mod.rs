use std::{
    collections::{BTreeMap, HashSet},
    fs::{read_dir, OpenOptions},
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
    default::VLOG_FILE_EXT,
    errors::err_file,
    key_registry::KeyRegistry,
    lsm::log_file::LogFile,
    options::Options,
    util::{dir_join_id_suffix, parse_file_id},
    vlog::read::LogFileIter,
};

use self::{discard::DiscardStats, threshold::VlogThreshold};

pub(crate) mod discard;
pub(crate) mod header;
pub(crate) mod read;
pub(crate) mod threshold;
pub(crate) mod write;
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
#[derive(Debug)]
pub(crate) struct ValueLog {
    fid_logfile: RwLock<BTreeMap<u32, Arc<RwLock<LogFile>>>>,
    max_fid: AtomicU32,
    files_to_be_deleted: Vec<u32>,
    num_active_iter: AtomicI32,
    writable_log_offset: AtomicUsize,
    num_entries_written: AtomicUsize,
    discard_stats: DiscardStats,
    threshold: VlogThreshold,
    key_registry: KeyRegistry,
}
impl ValueLog {
    pub(crate) fn new(
        // opt: Arc<Options>,
        threshold: VlogThreshold,
        key_registry: KeyRegistry,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            fid_logfile: Default::default(),
            max_fid: Default::default(),
            files_to_be_deleted: Default::default(),
            num_active_iter: Default::default(),
            writable_log_offset: Default::default(),
            num_entries_written: Default::default(),
            discard_stats: discard::DiscardStats::new()?,
            // opt,
            threshold,
            key_registry,
        })
    }
    pub(crate) async fn open(&mut self) -> anyhow::Result<()> {
        self.populate_files_map(self.key_registry.clone()).await?;

        let fid_logfile_r = self.fid_logfile.read().await;
        let fid_logfile_len = fid_logfile_r.len();
        drop(fid_logfile_r);

        if Options::read_only() {
            return Ok(());
        }
        if fid_logfile_len == 0 {
            self.create_vlog_file()
                .await
                .map_err(|e| anyhow!("Error while creating log file in vlog.open for {}", e))?;
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
        let dir = Options::value_dir();
        let read_only = Options::read_only();
        let mut fp_open_opt = OpenOptions::new();
        fp_open_opt.read(true).write(!read_only);
        let mut found = HashSet::new();
        let mut fid_logfile_w = self.fid_logfile.write().await;
        for ele in read_dir(dir).map_err(|e| err_file(e, dir, "Unable to open log dir."))? {
            let entry = ele.map_err(|e| err_file(e, dir, "Unable to read dir entry"))?;
            let path = entry.path();
            if let Some(fid) = parse_file_id(&path, VLOG_FILE_EXT) {
                let fid = fid as u32;
                if !found.insert(fid) {
                    bail!("Duplicate file found. Please delete one.")
                };
                let (log_file, _) = LogFile::open(
                    fid,
                    &path,
                    fp_open_opt.clone(),
                    2 * Options::vlog_file_size(),
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
                self.max_fid.fetch_max(fid, Ordering::SeqCst);
            };
        }
        drop(fid_logfile_w);
        Ok(())
    }
    async fn create_vlog_file(&self) -> anyhow::Result<Arc<RwLock<LogFile>>> {
        let fid = self.max_fid.fetch_add(1, Ordering::SeqCst) + 1;
        let file_path = dir_join_id_suffix(Options::value_dir(), fid, VLOG_FILE_EXT);
        let mut fp_open_opt = OpenOptions::new();
        fp_open_opt.read(true).write(true).create_new(true);
        let (log_file, _) = LogFile::open(
            fid,
            &file_path,
            fp_open_opt,
            2 * Options::vlog_file_size(),
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
    pub(crate) async fn get_logfile(&self, fid: u32) -> Option<Arc<RwLock<LogFile>>> {
        let p = self.fid_logfile.read().await;
        let r = if let Some(s) = p.get(&fid) {
            Some(s.clone())
        } else {
            None
        };
        drop(p);
        r
    }
    #[inline]
    pub(crate) async fn get_latest_logfile(&self) -> anyhow::Result<Arc<RwLock<LogFile>>> {
        let p = self.fid_logfile.read().await;
        let r = if let Some(s) = p.get(&self.max_fid.load(Ordering::SeqCst)) {
            s.clone()
        } else {
            bail!("Failed to get latest_logfile");
        };
        drop(p);
        Ok(r)
    }
}
