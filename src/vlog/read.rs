use std::{
    hash::Hasher,
    io::{self, BufRead, BufReader, Read},
};

use crate::{
    kv::{Entry, Meta, TxnTs, ValuePointer},
    util::{log_file::LogFile, DBFileId},
};

use super::header::VlogEntryHeader;
use anyhow::bail;
use bytes::Buf;
#[derive(Debug)]
pub(crate) struct LogFileIter<'a, F: DBFileId> {
    log_file: &'a LogFile<F>,
    record_offset: usize,
    reader: BufReader<&'a [u8]>,
    entries_vptrs: Vec<(Entry, ValuePointer)>,
    valid_end_offset: usize,
}
impl<'a, F: DBFileId> LogFileIter<'a, F> {
    pub(crate) fn new(log_file: &'a LogFile<F>, offset: usize) -> Self {
        let buf_reader = BufReader::new(&log_file.as_ref()[offset..]);
        Self {
            log_file,
            record_offset: offset,
            reader: buf_reader,
            entries_vptrs: Vec::new(),
            valid_end_offset: offset,
        }
    }

    pub(crate) fn read_entry(&mut self) -> std::io::Result<(Entry, ValuePointer)> {
        let mut hash_reader = HashReader {
            reader: &mut self.reader,
            hasher: crc32fast::Hasher::new(),
            len: 0,
        };

        let entry_header = VlogEntryHeader::decode_from(&mut hash_reader)?;
        let header_len = hash_reader.len;
        if entry_header.key_len() > 1 << 16 as u32 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "key length must be below u16",
            ));
        }

        let key_len = entry_header.key_len() as usize;
        let value_len = entry_header.value_len() as usize;

        let mut kv_buf = vec![0; key_len + value_len];
        hash_reader.read_exact(&mut kv_buf)?;

        if kv_buf.len() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "kv len can't be zero",
            ));
        }
        if let Some(s) = self.log_file.try_decrypt(&kv_buf, self.record_offset) {
            kv_buf = s;
        };

        let entry = Entry::new_ts(
            &kv_buf[..key_len],
            &kv_buf[key_len..],
            &entry_header,
            self.record_offset,
            header_len,
        );

        let mut crc_buf = (0 as u32).to_be_bytes();

        hash_reader.read_exact(&mut crc_buf)?;

        let crc = crc_buf.as_slice().get_u32();
        if hash_reader.hasher.finalize() != crc {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to checksum crc32",
            ));
        };

        let size = header_len + key_len + value_len + crc_buf.len();
        debug_assert!(size == hash_reader.len);

        let v_ptr = ValuePointer::new(self.log_file.fid().into(), size, self.record_offset);
        self.record_offset += size;
        Ok((entry, v_ptr))
    }
    #[tracing::instrument]
    pub(crate) fn next(&mut self) -> anyhow::Result<Option<&Vec<(Entry, ValuePointer)>>> {
        let mut last_commit = TxnTs::default();
        self.entries_vptrs.clear();
        loop {
            match self.read_entry() {
                Ok((entry, v_ptr)) => {
                    if entry.meta().contains(Meta::TXN) {
                        let txn_ts = entry.version();
                        if last_commit == TxnTs::default() {
                            last_commit = txn_ts;
                        }
                        if last_commit != txn_ts {
                            break;
                        }
                        self.entries_vptrs.push((entry, v_ptr));
                    } else if entry.meta().contains(Meta::FIN_TXN) {
                        let txn_ts = entry.version();
                        if last_commit != txn_ts {
                            break;
                        }
                        self.valid_end_offset = self.record_offset;
                        return Ok(Some(&self.entries_vptrs));
                    } else {
                        if last_commit != TxnTs::default() {
                            break;
                        }
                        self.valid_end_offset = self.record_offset;
                        return Ok(Some(&self.entries_vptrs));
                    }
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => {
                        bail!(e)
                    }
                },
            }
        }
        Ok(None)
    }

    pub(crate) fn valid_end_offset(&self) -> usize {
        self.valid_end_offset
    }
}

pub(crate) struct HashReader<'a, B: BufRead, T: Hasher> {
    reader: &'a mut BufReader<B>,
    hasher: T,
    len: usize,
}

impl<B: BufRead, T: Hasher> Read for HashReader<'_, B, T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let size = self.reader.read(buf)?;
        self.len += size;
        self.hasher.write(&buf[..size]);
        Ok(size)
    }
}
