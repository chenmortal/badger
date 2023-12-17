pub(crate) const MAX_VALUE_THRESHOLD: usize = 1 << 20;
pub(crate) const DEFAULT_DIR: &str = "./tmp/badger";
pub(crate) const DEFAULT_VALUE_DIR: &str = "./tmp/badger";
pub(crate) const LOCK_FILE: &str = "LOCK";
pub(crate) const KV_WRITES_ENTRIES_CHANNEL_CAPACITY: usize = 1000;

lazy_static! {
    pub(crate) static ref DEFAULT_PAGE_SIZE: usize =
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
}
