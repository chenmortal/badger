use log::error;
use memmap2::MmapRaw;
use std::fs::{remove_file, File};
use std::io::{self, Read, Write};
use std::ops::{Deref, DerefMut};
use std::slice;
use std::time::SystemTime;
use std::{fs::OpenOptions, path::PathBuf};

use crate::default::DEFAULT_PAGE_SIZE;
use crate::util::sys::sync_dir;
#[derive(Debug)]
pub(crate) struct MmapFile {
    /// like std::io::BufWriter
    w_buf: Vec<u8>,
    /// point to actual file
    w_pos: usize,

    ///like std::io::BufReader
    r_buf: Vec<u8>,
    ///point to actual file , already read from actual file
    r_pos: usize,
    ///point to r_buf，already read from r_buf, must <= r_buf.len()
    r_buf_pos: usize,

    last_flush_pos: usize,
    panicked: bool,
    raw: MmapRaw,
    path: PathBuf,
    fd: File,
}
impl AsRef<[u8]> for MmapFile {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.raw.as_ptr() as _, self.raw.len()) }
    }
}
impl AsMut<[u8]> for MmapFile {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.raw.as_mut_ptr() as _, self.raw.len()) }
    }
}
impl Deref for MmapFile {
    type Target = MmapRaw;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}
impl DerefMut for MmapFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw
    }
}

impl MmapFile {
    #[tracing::instrument]
    pub(crate) fn open(
        file_path: &PathBuf,
        fp_open_opt: OpenOptions,
        max_file_size: usize,
    ) -> io::Result<(Self, bool)> {
        let fd = fp_open_opt.open(file_path)?;
        let metadata = fd.metadata()?;
        let file_size = metadata.len();
        let mut is_new_file = false;
        if max_file_size > 0 && file_size == 0 {
            fd.set_len(max_file_size as u64)?;
            is_new_file = true;
        }
        let mmap_raw = memmap2::MmapRaw::map_raw(&fd)?;
        let mmap_file = MmapFile {
            w_pos: 0,
            w_buf: Vec::with_capacity(DEFAULT_PAGE_SIZE.to_owned()),
            last_flush_pos: 0,
            panicked: false,
            raw: mmap_raw,
            path: file_path.to_owned(),
            fd,
            r_buf: Vec::with_capacity(DEFAULT_PAGE_SIZE.to_owned()),
            r_pos: 0,
            r_buf_pos: 0,
        };

        if let Some(dir) = file_path.parent() {
            let dir = PathBuf::from(dir);
            tokio::spawn(async move {
                match sync_dir(&dir) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("cannot sync dir {:?} for {}", dir, e);
                    }
                };
            });
        }
        Ok((mmap_file, is_new_file))
    }

    #[inline]
    #[tracing::instrument]
    pub fn raw_sync(&self) -> io::Result<()> {
        self.raw.flush()
    }

    #[inline]
    #[tracing::instrument]
    pub(crate) fn get_file_size(&self) -> io::Result<u64> {
        Ok(self.fd.metadata()?.len())
    }

    #[inline]
    #[tracing::instrument]
    pub(crate) fn get_modified_time(&self) -> io::Result<SystemTime> {
        self.fd.metadata()?.modified()
    }

    /// offset for sequential writeing
    #[inline]
    pub(crate) fn write_offset(&self) -> usize {
        self.w_pos + self.w_buf.len()
    }

    /// offset for sequential reading
    #[inline]
    pub(crate) fn read_offset(&self) -> usize {
        self.r_pos - (self.r_buf.len() - self.r_buf_pos)
    }

    pub(crate) fn read_slice_ref(&self, offset: usize, len: usize) -> Result<&[u8], io::Error> {
        let p = self.as_ref();
        if p[offset..].len() < len {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        };
        Ok(&p[offset..offset + len])
    }

    #[inline]
    #[tracing::instrument]
    pub(crate) fn delete(&self) -> io::Result<()> {
        self.munmap()?;
        self.fd.set_len(0)?;
        remove_file(&self.path)?;
        Ok(())
    }

    #[tracing::instrument]
    pub(crate) fn ready_to_close(&mut self, max_sz: u64) -> io::Result<()> {
        self.flush()?;
        self.raw_sync()?;
        self.munmap()?;
        self.fd.set_len(max_sz as u64)?;
        Ok(())
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) fn w_pos(&self) -> usize {
        self.w_pos
    }

    pub(crate) fn set_w_pos(&mut self, w_pos: usize) {
        self.w_pos = w_pos;
    }
}

// impl Seek for MmapWriter {
//     fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
//         self.flush();
//         let (base_pos, offset) = match pos {
//             SeekFrom::Start(s) => {
//                 self.pos = s;
//                 return Ok(s);
//             }
//             SeekFrom::End(e) => (self.mmap.len() as u64, e),
//             SeekFrom::Current(c) => (self.pos, c),
//         };
//         match base_pos.checked_add_signed(offset) {
//             Some(n) => {
//                 self.pos = n;
//                 Ok(self.pos)
//             }
//             None => Err(io::Error::new(
//                 ErrorKind::InvalidInput,
//                 "invalid seek to a negative or overflowing position",
//             )),
//         }
//     }
// }

impl MmapFile {
    #[tracing::instrument]
    pub(crate) fn munmap(&self) -> io::Result<()> {
        let result = unsafe { libc::munmap(self.raw.as_mut_ptr() as _, self.raw.len() as _) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[tracing::instrument]
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub(crate) fn set_len(&mut self, size: usize) -> io::Result<()> {
        self.raw.flush_range(0, size)?;
        self.munmap()?;
        self.fd.set_len(size as u64)?;
        self.raw = memmap2::MmapRaw::map_raw(&self.fd)?;
        Ok(())
    }

    #[tracing::instrument]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn set_len(&mut self, size: usize) -> io::Result<()> {
        self.flush(0, size);
        self.file_handle.set_len(0)?;
        let opt = RemapOptions::new();
        self.mmap.remap(size, opt)?;
    }

    #[tracing::instrument]
    fn check_len_satisfied(&mut self, buf_len: usize) -> io::Result<()> {
        let write_at = self.w_pos as usize;
        let new_write_at = write_at + buf_len;
        if new_write_at >= self.raw.len() {
            let align = new_write_at % DEFAULT_PAGE_SIZE.to_owned();
            let new_len = new_write_at - align + 2 * DEFAULT_PAGE_SIZE.to_owned();
            self.set_len(new_len)?;
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    #[tracing::instrument]
    fn write_cold(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() + self.w_buf.len() > self.w_buf.capacity() {
            self.flush_buf()?;
        }
        if buf.len() >= self.w_buf.capacity() {
            self.panicked = true;
            let r = self.check_len_satisfied(buf.len());
            self.panicked = false;
            if let Err(e) = r {
                return Err(e);
            }
            // self.as_mut()[self.w_pos as usize..].copy_from_slice(&buf);
            unsafe {
                Self::raw_write(&self.raw, self.w_pos, buf)?;
            };

            self.w_pos += buf.len();
        } else {
            unsafe { write_to_buffer_unchecked(&mut self.w_buf, buf) }
        };
        Ok(buf.len())
    }

    #[tracing::instrument]
    fn flush_buf(&mut self) -> io::Result<()> {
        self.panicked = true;
        let r = self.check_len_satisfied(self.w_buf.len());
        self.panicked = false;
        if let Err(e) = r {
            match e.kind() {
                io::ErrorKind::Interrupted => {}
                _ => {
                    return Err(e);
                }
            }
        }
        self.panicked = true;
        let r = unsafe { Self::raw_write(&self.raw, self.w_pos, &self.w_buf) };
        self.panicked = false;
        if let Err(e) = r {
            match e.kind() {
                io::ErrorKind::Interrupted => {}
                _ => {
                    return Err(e);
                }
            }
        }

        self.w_pos += self.w_buf.len();
        self.w_buf.clear();

        Ok(())
    }
}
impl Read for MmapFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.r_buf_pos == self.r_buf.len() {
            if buf.len() >= self.r_buf.capacity() {
                self.r_buf.clear();
                self.r_buf_pos = 0;

                let size = unsafe { Self::raw_read(&self.raw, self.r_pos, buf) };
                self.r_pos += size;
                return Ok(size);
            }

            let remain = (self.raw.len() - self.r_pos).min(self.r_buf.capacity());
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.raw.as_ptr().add(self.r_pos),
                    self.r_buf.as_mut_ptr(),
                    remain,
                );
                self.r_buf.set_len(remain);
            }
            self.r_pos += remain;
            self.r_buf_pos = 0;
        }
        let size = self.r_buf[self.r_buf_pos..].as_ref().read(buf)?;
        self.r_buf_pos += size;
        Ok(size)
    }
}
impl Write for MmapFile {
    ///copy from std::io::Bufwriter
    #[tracing::instrument]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.w_buf.len() + buf.len() < self.w_buf.capacity() {
            unsafe {
                write_to_buffer_unchecked(&mut self.w_buf, buf);
            }
            Ok(buf.len())
        } else {
            self.write_cold(buf)
        }
    }

    #[tracing::instrument]
    fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        let (offset, len) = if self.w_pos < self.last_flush_pos {
            (self.w_pos, self.last_flush_pos - self.w_pos)
        } else if self.w_pos > self.last_flush_pos {
            (self.last_flush_pos, self.w_pos - self.last_flush_pos)
        } else {
            return Ok(());
        };
        self.raw.flush_range(offset as usize, len as usize)?;
        self.last_flush_pos = self.w_pos;
        Ok(())
    }
}
impl MmapFile {
    #[inline(always)]
    unsafe fn raw_write(raw: &MmapRaw, offset: usize, data: &[u8]) -> io::Result<()> {
        std::ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr().add(offset), data.len());
        raw.flush_async_range(offset, data.len())?;
        Ok(())
    }

    #[inline]
    unsafe fn raw_read(raw: &MmapRaw, offset: usize, buf: &mut [u8]) -> usize {
        let buf_len = buf.len().min(raw.len() - offset);
        let s = slice::from_raw_parts(raw.as_mut_ptr().add(offset) as _, buf_len);
        if buf_len == 1 {
            buf[0] = s[0];
        } else {
            buf[..buf_len].copy_from_slice(s);
        }
        buf_len
    }

    #[inline]
    pub(crate) fn write_slice(&mut self, mut offset: usize, mut data: &[u8]) -> io::Result<()> {
        let buf_offset = self.w_pos + self.w_buf.len();
        if offset == buf_offset {
            self.write(data)?;
        } else if offset > buf_offset || data.len() >= self.w_buf.capacity() {
            self.panicked = true;
            let r = self.check_len_satisfied(data.len());
            self.panicked = false;
            r?;
            self.panicked = true;
            let r = unsafe { Self::raw_write(&self.raw, offset, data) };
            self.panicked = false;
            r?;
        } else {
            if offset < self.w_pos {
                let s = self.w_pos - offset;
                let (l_data, r_data) = data.split_at(s);
                self.panicked = true;
                let r = unsafe { Self::raw_write(&self.raw, offset, l_data) };
                self.panicked = false;
                r?;
                data = r_data;
                offset += s;
            }
            let end_offset = offset + data.len();
            if buf_offset < end_offset {
                let s = end_offset - buf_offset;
                let (l_data, r_data) = data.split_at(s);
                self.panicked = true;
                let r = self.check_len_satisfied(end_offset);
                self.panicked = false;
                r?;
                self.panicked = true;
                let r = unsafe { Self::raw_write(&self.raw, offset, r_data) };
                self.panicked = false;
                r?;
                data = l_data;
            }
            let start_offset = offset - self.w_pos;
            let end_offset = start_offset + data.len();
            self.w_buf[start_offset..end_offset].copy_from_slice(data);
        };
        Ok(())
    }
    #[inline]
    pub(crate) fn read_slice(&mut self, mut offset: usize, mut data: &mut [u8]) -> io::Result<()> {
        let buf_end_offset = self.read_offset();
        if buf_end_offset == offset {
            self.read_exact(data)?; //sequential reading
        } else if offset > self.r_pos || data.len() >= self.r_buf.capacity() {
            let read_size = unsafe { Self::raw_read(&self.raw, offset, data) };
            if read_size < data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "failed to read whole data, only read {}, need read {}",
                        read_size,
                        data.len()
                    ),
                ));
            }
        } else {
            //random reading
            let mut read_size = 0;
            let need_size = data.len();
            let buf_start_offset = self.r_pos - self.r_buf.len();
            if offset < buf_start_offset {
                let (l_data, r_data) = data.split_at_mut(buf_start_offset - offset);
                read_size = unsafe { Self::raw_read(&self.raw, offset, l_data) };
                if read_size < l_data.len() {
                    // return Ok(read_size);
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "failed to read whole data, only read {}, need read {}",
                            read_size, need_size
                        ),
                    ));
                }
                data = r_data;
                offset += read_size;
            }
            let end_offset = offset + data.len();
            if self.r_pos >= end_offset {
                data.copy_from_slice(
                    &self.r_buf[offset - buf_start_offset..end_offset - buf_start_offset],
                );
                read_size += data.len();
            } else {
                let (l_data, r_data) = data.split_at_mut(self.r_pos - offset);
                l_data.copy_from_slice(&self.r_buf[offset - buf_start_offset..]);
                read_size += l_data.len();
                read_size += unsafe { Self::raw_read(&self.raw, self.r_pos, r_data) };
            }
            if read_size < need_size {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "failed to read whole data, only read {}, need read {}",
                        read_size, need_size
                    ),
                ));
            }
        }
        Ok(())
    }
}
impl Drop for MmapFile {
    fn drop(&mut self) {
        if !self.panicked {
            let _r = self.flush_buf();
        }
        let _ = self.raw_sync();
    }
}
#[inline]
unsafe fn write_to_buffer_unchecked(buffer: &mut Vec<u8>, buf: &[u8]) {
    debug_assert!(buffer.len() + buf.len() <= buffer.capacity());
    let old_len = buffer.len();
    let buf_len = buf.len();
    let src = buf.as_ptr();
    let dst = buffer.as_mut_ptr().add(old_len);
    std::ptr::copy_nonoverlapping(src, dst, buf_len);

    buffer.set_len(old_len + buf_len);
}
