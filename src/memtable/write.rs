use crate::{
    kv::{Entry, Meta},
    util::{log_file::LogFile, metrics::add_num_bytes_written_to_l0},
};

use super::MemTable;

impl MemTable {
    #[inline]
    pub(crate) fn push(&mut self, entry: &Entry) -> anyhow::Result<()> {
        self.wal.write_entry(&mut self.buf, entry)?;
        if entry.meta().contains(Meta::FIN_TXN) {
            return Ok(());
        }
        self.skip_list.push(
            &entry.key_ts().serialize().as_ref(),
            entry.value_meta().serialize().as_ref(),
        );
        self.max_version = self.max_version.max(entry.version());
        add_num_bytes_written_to_l0(entry.estimate_size(entry.value_threshold()));
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
