use std::{
    hash::Hasher,
    io::{self, BufRead, BufReader, Read},
};

use crate::{kv::ValuePointer, lsm::wal::LogFile, txn::entry::Entry};

use super::{header::EntryHeader, BIT_FIN_TXN, BIT_TXN};
use anyhow::bail;
use bytes::Buf;
pub(crate) struct SafeRead<'a> {
    key: Vec<u8>,
    value: Vec<u8>,
    record_offset: usize,
    log_file: &'a LogFile,
}
impl SafeRead<'_> {
    pub(crate) fn read_entry(
        &mut self,
        reader: &mut BufReader<&[u8]>,
    ) -> std::io::Result<(Entry, usize)> {
        let mut hash_reader = HashReader {
            reader,
            hasher: crc32fast::Hasher::new(),
            len: 0,
        };

        let entry_header = EntryHeader::decode_from(&mut hash_reader)?;
        let header_len = hash_reader.len;
        if entry_header.key_len() > 1 << 16 as u32 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "key length must be below u16",
            ));
        }

        let key_len = entry_header.key_len() as usize;
        if self.key.capacity() < key_len {
            self.key = Vec::with_capacity(2 * key_len);
        }

        let value_len = entry_header.value_len() as usize;
        if self.value.capacity() < value_len {
            self.value = Vec::with_capacity(2 * value_len);
        }

        let offset = self.record_offset;

        let mut kv_buf = vec![0; key_len + value_len];
        hash_reader.read_exact(&mut kv_buf)?;

        if let Some(s) = self.log_file.try_decrypt(&kv_buf, self.record_offset) {
            kv_buf = s;
        };

        let mut entry = Entry::new(&kv_buf[..key_len], &kv_buf[key_len..]);
        let mut crc_buf = (0 as u32).to_be_bytes();

        hash_reader.read_exact(&mut crc_buf)?;

        let crc = crc_buf.as_slice().get_u32();
        if hash_reader.hasher.finalize() != crc {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to checksum crc32",
            ));
        };

        entry.set_meta(entry_header.meta());
        entry.set_user_meta(entry_header.user_meta());
        entry.set_expires_at(entry_header.expires_at());
        entry.set_offset(offset);
        entry.set_header_len(header_len);

        let size = header_len + key_len + value_len + crc_buf.len();
        Ok((entry, size))
    }
}

impl LogFile {
    pub(crate) fn iterate(&self, read_only: bool, offset: usize) -> anyhow::Result<()> {
        let mut buf_reader = BufReader::new(&self.mmap[offset..]);
        let mut safe_read = SafeRead {
            key: Vec::with_capacity(10),
            value: Vec::with_capacity(10),
            record_offset: offset,
            log_file: self,
        };
        loop {
            match safe_read.read_entry(&mut buf_reader) {
                Ok((mut entry, len)) => {
                    if entry.key().len() == 0 {
                        break;
                    }
                    let pointer = ValuePointer::new(self.fid(), len, entry.offset());
                    safe_read.record_offset += entry.offset();

                    if entry.meta() & BIT_TXN > 0 {
                    } else if entry.meta() & BIT_FIN_TXN > 0 {
                    } else {
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
            };
        }
        Ok(())
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
