use std::time::SystemTime;

use tokio::sync::Mutex;

use crate::lsm::mmap::MmapFile;

pub(crate) struct Table{
    lock:Mutex<()>,
    mmap:MmapFile,
    table_size:i32,
    id:u64,
    checksum:Vec<u8>,
    created_at:SystemTime,
    index_start:i32,
    index_len:i32,
    has_bloom_filter:bool,
}