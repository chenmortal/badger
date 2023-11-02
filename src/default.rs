pub(crate) const MAX_VALUE_THRESHOLD: u32 = 1 << 20;
pub(crate) const DEFAULT_DIR: &str = "./tmp/badger";
pub(crate) const DEFAULT_VALUE_DIR: &str = "./tmp/badger";
pub(crate) const LOCK_FILE: &str = "LOCK";
pub(crate) const MANIFEST_FILE_NAME: &str = "MANIFEST";
pub(crate) const MANIFEST_REWRITE_FILE_NAME: &str = "MANIFEST-REWEITE";
// pub(crate) const MEM_FILE_EXT: &str = ".mem";
// pub(crate) const SSTABLE_FILE_EXT: &str = ".sst";
// pub(crate) const VLOG_FILE_EXT: &str = ".vlog";
pub(crate) const MANIFEST_DELETIONS_REWRITE_THRESHOLD: i32 = 10000;
pub(crate) const KV_WRITES_ENTRIES_CHANNEL_CAPACITY: usize = 1000;
// pub(crate) const DEFAULT_IS_SIV: bool = false;
lazy_static! {
    pub(crate) static ref DEFAULT_PAGE_SIZE: usize =
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
}
