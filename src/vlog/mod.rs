use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicI32, AtomicU32},
        Arc,
    }, fs::read_dir,
};

use crate::{lsm::wal::LogFile, options::Options};

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

pub(crate) struct ValueLog {
    dir_path: PathBuf,
    files_log: HashMap<u32, Arc<LogFile>>,
    max_fid: u32,
    files_to_be_deleted: Vec<u32>,
    num_active_iter: AtomicI32,
    writable_log_offset: AtomicU32,
    num_entries_written: u32,
    opt: Arc<Options>,
}
impl ValueLog {
    pub(crate) fn open(opt:Arc<Options>){

    }
    fn populate_files_map(dir:&PathBuf,){
        let p = read_dir(dir);
    }
}
