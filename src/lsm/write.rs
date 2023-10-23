use crate::{
    metrics::add_num_bytes_written_to_l0,
    txn::entry::{DecEntry, Entry, EntryMeta},
};

use super::{log_file::LogFile, memtable::MemTable};

impl MemTable {
    #[inline]
    pub(crate) fn push(&mut self, entry: &DecEntry) -> anyhow::Result<()> {
        self.wal.write_entry(&mut self.buf, entry)?;
        if entry.meta().contains(EntryMeta::FIN_TXN) {
            return Ok(());
        }
        self.skip_list.push(
            &entry.key_ts().get_bytes(),
            entry.value_meta().serialize()?.as_ref(),
        );
        self.max_version = self.max_version.max(entry.version());
        add_num_bytes_written_to_l0(entry.estimate_size());
        Ok(())
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
