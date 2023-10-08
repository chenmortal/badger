use crate::txn::entry::{Entry, EntryMeta};

use super::{log_file::LogFile, memtable::MemTable};

impl MemTable {
    #[inline]
    pub(crate) fn push(&mut self, entry: &Entry) -> anyhow::Result<()> {
        // self.wal.write_entry(buf, entry);
        // let buf=self.buf_mut();
        // let log = self.wal_mut();
        // log.write_entry(buf, entry);
        self.wal.write_entry(&mut self.buf, entry)?;
        if entry.meta().contains(EntryMeta::FIN_TXN) {
            return Ok(());
        }
        Ok(())
        // self.wal_mut().write_entry(buf, entry);
    }
}
impl LogFile {
    #[tracing::instrument]
    fn write_entry(&mut self, buf: &mut Vec<u8>, entry: &Entry) -> std::io::Result<()> {
        buf.clear();
        let offset = self.write_offset();
        let size = self.encode_entry(buf, entry, offset);
        self.write_slice(offset, &buf[..size])
    }
}
