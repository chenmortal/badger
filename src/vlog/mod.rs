use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{read_dir, OpenOptions},
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::anyhow;
use anyhow::bail;
use tokio::sync::RwLock;

use crate::{
    default::VALUELOG_FILE_EXT,
    errors::err_file,
    key_registry::KeyRegistry,
    lsm::wal::LogFile,
    options::Options,
    util::{dir_join_id_suffix, parse_file_id},
};

use self::discard::DiscardStats;

pub(crate) mod discard;
pub(crate) mod threshold;
// size of vlog header.
// +----------------+------------------+
// | keyID(8 bytes) |  baseIV(12 bytes)|
// +----------------+------------------+
pub(crate) const VLOG_HEADER_SIZE: usize = 20;
pub(crate) const MAX_HEADER_SIZE: usize = 22;
pub(crate) const BIT_DELETE: u8 = 1 << 0;
pub(crate) const BIT_VALUE_POINTER: u8 = 1 << 1;
pub(crate) const BIT_DISCARD_EARLIER_VERSIONS: u8 = 1 << 2;
pub(crate) const BIT_MERGE_ENTRY: u8 = 1 << 3;
pub(crate) const BIT_TXN: u8 = 1 << 6;
pub(crate) const BIT_FIN_TXN: u8 = 1 << 7;
#[derive(Debug)]
pub(crate) struct ValueLog {
    // dir_path: PathBuf,
    fid_logfile: RwLock<BTreeMap<u32, Arc<LogFile>>>,
    max_fid: u32,
    files_to_be_deleted: Vec<u32>,
    num_active_iter: AtomicI32,
    writable_log_offset: AtomicUsize,
    num_entries_written: u32,
    discard_stats: DiscardStats,
    opt: Arc<Options>,
}
impl ValueLog {
    pub(crate) fn new(opt: Arc<Options>) -> anyhow::Result<Self> {
        Ok(Self {
            fid_logfile: Default::default(),
            max_fid: Default::default(),
            files_to_be_deleted: Default::default(),
            num_active_iter: Default::default(),
            writable_log_offset: Default::default(),
            num_entries_written: Default::default(),
            discard_stats: discard::DiscardStats::new(opt.clone())?,
            opt,
        })
    }
    pub(crate) async fn open(&mut self, key_registry: KeyRegistry) -> anyhow::Result<()> {
        self.populate_files_map(key_registry.clone()).await?;
        let fid_logfile_r = self.fid_logfile.read().await;
        if fid_logfile_r.len() == 0 {
            if self.opt.read_only {
                return Ok(());
            }
            drop(fid_logfile_r);
            self.create_vlog_file(key_registry)
                .await
                .map_err(|e| anyhow!("Error while creating log file in vlog.open for {}", e))?;
        } else {
            drop(fid_logfile_r);
        }
        let fid_logfile_r = self.fid_logfile.read().await;
        for (fid, log_file) in fid_logfile_r
            .iter()
            .filter(|(fid, _)| !self.files_to_be_deleted.contains(*fid))
        {}
        // for fid in self.sorted_fids().await {}

        // drop(fid_logfile_r);

        // let mut value_log = ValueLog::default();
        // value_log.dir_path = opt.value_dir;
        // value_log.populate_files_map(opt.read_only, key_registry);
        Ok(())
    }
    async fn populate_files_map(&mut self, key_registry: KeyRegistry) -> anyhow::Result<()> {
        let dir = &self.opt.value_dir;
        let read_only = self.opt.read_only;
        let mut fp_open_opt = OpenOptions::new();
        fp_open_opt.read(true).write(!read_only);
        let mut found = HashSet::new();
        let mut fid_logfile_w = self.fid_logfile.write().await;
        for ele in read_dir(dir).map_err(|e| err_file(e, dir, "Unable to open log dir."))? {
            let entry = ele.map_err(|e| err_file(e, dir, "Unable to read dir entry"))?;
            let path = entry.path();
            if let Some(fid) = parse_file_id(&path, VALUELOG_FILE_EXT) {
                let fid = fid as u32;
                if !found.insert(fid) {
                    bail!("Duplicate file found. Please delete one.")
                };
                let (log_file, _) = LogFile::open(
                    fid as u32,
                    &path,
                    read_only,
                    fp_open_opt.clone(),
                    2 * self.opt.valuelog_file_size,
                    key_registry.clone(),
                )
                .await
                .map_err(|e| anyhow!("Open existing file: {:?} for {}", path, e))?;
                // log_file.size.load(Or);
                fid_logfile_w.insert(fid, Arc::new(log_file));
                self.max_fid = self.max_fid.max(fid);
            };
        }
        drop(fid_logfile_w);
        Ok(())
    }
    async fn create_vlog_file(&mut self, key_registry: KeyRegistry) -> anyhow::Result<()> {
        let fid = self.max_fid + 1;
        let file_path = dir_join_id_suffix(&self.opt.value_dir, fid, VALUELOG_FILE_EXT);
        let mut fp_open_opt = OpenOptions::new();
        fp_open_opt.read(true).write(true).create_new(true);
        let (log_file, _) = LogFile::open(
            fid,
            &file_path,
            true,
            fp_open_opt,
            2 * self.opt.valuelog_file_size,
            key_registry,
        )
        .await?;
        let mut fid_logfile_w = self.fid_logfile.write().await;
        fid_logfile_w.insert(fid, Arc::new(log_file));
        debug_assert!(fid > self.max_fid);
        self.max_fid = fid;
        self.writable_log_offset
            .store(VLOG_HEADER_SIZE, Ordering::SeqCst);
        self.num_entries_written = 0;
        drop(fid_logfile_w);
        Ok(())
    }
    async fn sorted_fids(&self) -> Vec<u32> {
        let to_be_deleted = self
            .files_to_be_deleted
            .iter()
            .map(|x| *x)
            .collect::<HashSet<_>>();
        let fid_logfile_r = self.fid_logfile.read().await;
        let mut r = fid_logfile_r
            .iter()
            .filter_map(|(fid, _)| {
                if !to_be_deleted.contains(fid) {
                    Some(*fid)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        drop(fid_logfile_r);
        r.sort();
        r
    }
}
#[test]
fn test_map() {
    let mut p = std::collections::BTreeMap::new();
    p.insert(2, "b");
    p.insert(3, "c");
    p.insert(1, "d");
    for ele in p.iter() {
        dbg!(ele);
    }
}
