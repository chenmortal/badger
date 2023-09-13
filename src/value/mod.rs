use std::{path::PathBuf, collections::HashMap, sync::{Arc, atomic::{AtomicI32, AtomicU32}}};

use crate::{lsm::wal::LogFile, options::Options};

pub(crate) mod threshold;
pub(crate) mod discard;
	// size of vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
pub(crate) const VLOG_HEADER_SIZE:usize=20;
pub(crate) const MAX_HEADER_SIZE:usize=22;

pub(crate) struct ValueLog{
	dir_path:PathBuf,
	files_log:HashMap<u32,Arc<LogFile>>,
	max_fid:u32,
	files_to_be_deleted:Vec<u32>,
	num_active_iter:AtomicI32,
	writable_log_offset:AtomicU32,
	num_entries_written:u32,
	opt:Arc<Options>,
}
impl ValueLog {
	pub(crate) fn init(opt:&Arc<Options>){
		
	}
}