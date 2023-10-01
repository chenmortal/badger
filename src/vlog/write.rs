use std::{hash::Hasher, io::Write, mem, sync::atomic::Ordering};

use bytes::BufMut;

use crate::{
    default::DEFAULT_PAGE_SIZE,
    kv::ValuePointer,
    lsm::wal::LogFile,
    metrics::{add_num_bytes_vlog_written, add_num_writes_vlog},
    options::Options,
    txn::{entry::DecEntry, TxnTs},
    vlog::{BIT_FIN_TXN, BIT_TXN},
    write::WriteReq,
};

use super::{header::EntryHeader, ValueLog, MAX_HEADER_SIZE, MAX_VLOG_FILE_SIZE};
use anyhow::bail;
pub(crate) struct HashWriter<'a, T: Hasher> {
    writer: &'a mut Vec<u8>,
    hasher: T,
}

impl<T: Hasher> Write for HashWriter<'_, T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.put_slice(buf);
        self.hasher.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ValueLog {
    pub(crate) async fn write(&self, reqs: &mut Vec<WriteReq>) -> anyhow::Result<()> {
        self.validate_write(reqs)?;

        let mut buf = Vec::with_capacity(DEFAULT_PAGE_SIZE.to_owned());

        let sender = self.threshold.sender();
        let mut cur_logfile = self.get_latest_logfile().await?;

        for req in reqs.iter_mut() {
            let mut cur_logfile_w = cur_logfile.write().await;
            let entries_vptrs = req.entries_vptrs_mut();
            let mut value_sizes = Vec::with_capacity(entries_vptrs.len());
            let mut written = 0;
            let mut bytes_written = 0;
            for (dec_entry, vptr) in entries_vptrs {
                buf.clear();
                value_sizes.push(dec_entry.value().len());
                dec_entry.try_set_value_threshold(self.threshold.value_threshold());
                if dec_entry.value().len() < dec_entry.value_threshold() {
                    if !vptr.is_empty() {
                        *vptr = ValuePointer::default();
                    }
                    continue;
                }
                let fid = cur_logfile_w.fid();
                let offset = self.writable_log_offset();

                let tmp_meta = dec_entry.meta();
                dec_entry.clean_meta_bit(BIT_TXN | BIT_FIN_TXN);

                let len = cur_logfile_w.encode_entry(&mut buf, &dec_entry, offset);

                dec_entry.set_meta(tmp_meta);
                *vptr = ValuePointer::new(fid, len, offset);

                if buf.len() != 0 {
                    let buf_len = buf.len();
                    let start_offset = self.writable_log_offset_fetch_add(buf_len);
                    let end_offset = start_offset + buf_len;
                    if end_offset >= cur_logfile_w.mmap.len() {
                        cur_logfile_w.truncate(end_offset)?;
                    };
                    cur_logfile_w.mmap[start_offset..end_offset].copy_from_slice(&buf);
                }
                written += 1;
                bytes_written += buf.len();
            }
            add_num_writes_vlog(written);
            add_num_bytes_vlog_written(bytes_written);
            self.num_entries_written
                .fetch_add(written, Ordering::SeqCst);
            sender.send(value_sizes).await?;
            let w_offset = self.writable_log_offset();
            if w_offset > Options::vlog_file_size()
                || self.num_entries_written.load(Ordering::SeqCst) > Options::vlog_max_entries()
            {
                if Options::sync_writes() {
                    cur_logfile_w.mmap.flush()?;
                }
                cur_logfile_w.truncate(w_offset)?;
                let new = self.create_vlog_file().await?; //new logfile will be latest logfile
                drop(cur_logfile_w);
                cur_logfile = new;
            };
        }
        //wait for async closure trait
        let mut cur_logfile_w = cur_logfile.write().await;
        let w_offset = self.writable_log_offset();
        if w_offset > Options::vlog_file_size()
            || self.num_entries_written.load(Ordering::SeqCst) > Options::vlog_max_entries()
        {
            if Options::sync_writes() {
                cur_logfile_w.mmap.flush()?;
            }
            cur_logfile_w.truncate(w_offset)?;
            let _ = self.create_vlog_file().await?; //new logfile will be latest logfile
        };
        Ok(())
    }
    fn validate_write(&self, reqs: &Vec<WriteReq>) -> anyhow::Result<()> {
        let mut vlog_offset = self.writable_log_offset();
        for req in reqs {
            let mut size = 0;
            req.entries_vptrs().iter().for_each(|(x, _)| {
                size += MAX_HEADER_SIZE
                    + x.entry.key().len()
                    + mem::size_of::<TxnTs>()
                    + x.entry.value().len()
                    + mem::size_of::<u32>()
            });
            let estimate = vlog_offset + size;
            if estimate > MAX_VLOG_FILE_SIZE {
                bail!(
                    "Request size offset {} is bigger than maximum offset {}",
                    estimate,
                    MAX_VLOG_FILE_SIZE
                )
            }

            if estimate >= Options::vlog_file_size() {
                vlog_offset = 0;
                continue;
            }
            vlog_offset = estimate;
        }
        Ok(())
    }
    #[inline]
    pub(crate) fn writable_log_offset(&self) -> usize {
        self.writable_log_offset.load(Ordering::SeqCst)
    }
    #[inline]
    pub(crate) fn writable_log_offset_fetch_add(&self, size: usize) -> usize {
        self.writable_log_offset.fetch_add(size, Ordering::SeqCst)
    }
}
impl LogFile {
    fn encode_entry(&self, buf: &mut Vec<u8>, entry: &DecEntry, offset: usize) -> usize {
        let header = EntryHeader::new(&entry);
        let mut hash_writer = HashWriter {
            writer: buf,
            hasher: crc32fast::Hasher::new(),
        };
        let header_encode = header.encode();
        let header_len = hash_writer.write(&header_encode).unwrap();

        let mut kv_buf = entry.key_ts().get_bytes();
        kv_buf.extend_from_slice(entry.value());
        if let Some(e) = self.try_encrypt(&kv_buf, offset) {
            kv_buf = e;
        };
        let kv_len = hash_writer.write(&kv_buf).unwrap();

        let crc = hash_writer.hasher.finalize();
        let buf = hash_writer.writer;
        buf.put_u32(crc);
        header_len + kv_len + mem::size_of::<u32>()
    }
}
