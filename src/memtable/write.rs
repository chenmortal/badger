use anyhow::bail;

use super::MemTable;
#[cfg(feature = "metrics")]
use crate::util::metrics::add_num_bytes_written_to_l0;
use crate::{
    errors::DBError,
    kv::{Entry, Meta},
    util::log_file::LogFile,
    vlog::{read::LogFileIter, VLOG_HEADER_SIZE},
};

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
        #[cfg(feature = "metrics")]
        add_num_bytes_written_to_l0(entry.estimate_size(entry.value_threshold()));
        Ok(())
    }
    pub(crate) fn reload(&mut self) -> anyhow::Result<()> {
        let mut wal_iter = LogFileIter::new(&self.wal, VLOG_HEADER_SIZE);
        while let Some(next) = wal_iter.next()? {
            for (entry, _vptr) in next {
                self.max_version = self.max_version.max(entry.version());
                self.skip_list.push(
                    &entry.key_ts().serialize().as_ref(),
                    &entry.value_meta().serialize().as_ref(),
                )
            }
        }
        let end_offset = wal_iter.valid_end_offset();
        if end_offset < self.wal().get_size() && self.read_only {
            bail!(DBError::TruncateNeeded(end_offset, self.wal().get_size()));
        }
        self.wal_mut().truncate(end_offset)?;
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
