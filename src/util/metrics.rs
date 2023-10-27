use log::debug;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fs::{metadata, read_dir},
    path::PathBuf,
    sync::{atomic::AtomicUsize, atomic::Ordering, Arc},
};
use tokio::{select, sync::Semaphore};

use crate::options::Options;
lazy_static! {
    static ref LSM_SIZE: RwLock<HashMap<PathBuf, u64>> = RwLock::new(HashMap::new());
    static ref VLOG_SIZE: RwLock<HashMap<PathBuf, u64>> = RwLock::new(HashMap::new());
    static ref PENDING_WRITES: RwLock<HashMap<PathBuf, Arc<AtomicUsize>>> =
        RwLock::new(HashMap::new());
    static ref NUM_COMPACTION_TABLES: AtomicUsize = AtomicUsize::new(0);
    static ref NUM_BYTES_WRITTEN_USER: AtomicUsize = AtomicUsize::new(0);
    static ref NUM_WRITES_VLOG: AtomicUsize = AtomicUsize::new(0);
    static ref NUM_BYTES_VLOG_WRITTEN: AtomicUsize = AtomicUsize::new(0);
    static ref NUM_PUTS: AtomicUsize = AtomicUsize::new(0);
    static ref NUM_GETS: AtomicUsize = AtomicUsize::new(0);
    static ref NUM_MEMTABLE_GETS: AtomicUsize = AtomicUsize::new(0);
    static ref NUM_BYTES_WRITTEN_TO_L0: AtomicUsize = AtomicUsize::new(0);
}

#[inline]
pub(crate) fn get_metrics_enabled() -> bool {
    Options::metrics_enabled()
}
#[inline]
pub(crate) async fn set_lsm_size(k: &PathBuf, v: u64) {
    if !get_metrics_enabled() {
        return;
    }
    let mut lsm_size_w = LSM_SIZE.write();
    lsm_size_w.insert(k.clone(), v);
    drop(lsm_size_w)
}
#[inline]
pub(crate) async fn set_pending_writes(dir: PathBuf, req_len: Arc<AtomicUsize>) {
    if !get_metrics_enabled() {
        return;
    }
    let mut pending_writes_w = PENDING_WRITES.write();
    pending_writes_w.insert(dir, req_len);
    drop(pending_writes_w);
}
#[inline]
pub(crate) async fn set_vlog_size(k: &PathBuf, v: u64) {
    if !get_metrics_enabled() {
        return;
    }
    let mut vlog_size_w = VLOG_SIZE.write();
    vlog_size_w.insert(k.clone(), v);
    drop(vlog_size_w)
}
#[inline]
pub(crate) fn add_num_bytes_written_user(size: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_BYTES_WRITTEN_USER.fetch_add(size, std::sync::atomic::Ordering::SeqCst);
}
#[inline]
pub(crate) fn add_num_writes_vlog(size: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_WRITES_VLOG.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub(crate) fn add_num_bytes_vlog_written(size: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_BYTES_VLOG_WRITTEN.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub(crate) fn add_num_compaction_tables(val: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_COMPACTION_TABLES.fetch_add(val, std::sync::atomic::Ordering::SeqCst);
}
#[inline]
pub(crate) fn add_num_puts(size: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_PUTS.fetch_add(size, Ordering::Acquire);
}
#[inline]
pub(crate) fn add_num_gets(size: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_GETS.fetch_add(size, Ordering::Acquire);
}
#[inline]
pub(crate) fn add_num_memtable_gets(size: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_MEMTABLE_GETS.fetch_add(size, Ordering::Acquire);
}
#[inline]
pub(crate) fn add_num_bytes_written_to_l0(size: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_BYTES_WRITTEN_TO_L0.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub(crate) fn sub_num_compaction_tables(val: usize) {
    if !get_metrics_enabled() {
        return;
    }
    NUM_COMPACTION_TABLES.fetch_sub(val, std::sync::atomic::Ordering::SeqCst);
}

#[inline]
pub(crate) async fn calculate_size() {
    let dir = Options::dir();
    let value_dir = Options::value_dir();
    let (lsm_size, mut vlog_size) = match total_size(dir) {
        Ok(r) => r,
        Err(e) => {
            debug!("Cannot calculate_size {:?} for {}", dir, e);
            (0, 0)
        }
    };
    set_lsm_size(dir, lsm_size).await;
    if value_dir != dir {
        match total_size(value_dir) {
            Ok((_, v)) => {
                vlog_size = v;
            }
            Err(e) => {
                debug!("Cannot calculate_size {:?} for {}", value_dir, e);
                vlog_size = 0;
            }
        };
    }
    set_vlog_size(value_dir, vlog_size).await;
}

pub(crate) async fn update_size(sem: Arc<Semaphore>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
    loop {
        select! {
            _instant=interval.tick() =>{
                calculate_size().await;
            },
            _=sem.acquire()=>{
                break;
            }
        }
    }
}
fn total_size(dir: &PathBuf) -> anyhow::Result<(u64, u64)> {
    let mut lsm_size = 0;
    let mut vlog_size = 0;
    let read_dir = read_dir(dir)?;
    for ele in read_dir {
        let entry = ele?;
        let path = entry.path();
        if path.is_dir() {
            match total_size(&path) {
                Ok((sub_lsm, sub_vlog)) => {
                    lsm_size += sub_lsm;
                    vlog_size += sub_vlog;
                }
                Err(e) => {
                    debug!(
                        "Got error while calculating total size of directory: {:?} for {}",
                        path, e
                    );
                }
            }
        } else if path.is_file() {
            let meta_data = metadata(&path)?;
            let size = meta_data.len();
            let path = path.to_string_lossy();

            if path.ends_with(".sst") {
                lsm_size += size;
            } else if path.ends_with(".vlog") {
                vlog_size += size;
            }
        }
    }
    Ok((lsm_size, vlog_size))
}
