use bytes::BufMut;
use core::slice;
use log::error;
use memmap2::MmapRaw;
use std::fs::{remove_file, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::ops::{Deref, DerefMut};
use std::time::SystemTime;
use std::{fs::OpenOptions, path::PathBuf};

use crate::default::DEFAULT_PAGE_SIZE;
use crate::sys::sync_dir;
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
    pub fn sync_all(&self) -> io::Result<()> {
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
    pub(crate) fn read_slice(&self, offset: usize, len: usize) -> Result<&[u8], io::Error> {
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
        self.sync_all()?;
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
                Self::raw_write(&self.raw, self.w_pos, buf);
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

        unsafe {
            Self::raw_write(&self.raw, self.w_pos, &self.w_buf);
        }

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
        // BufReader
        if self.r_buf_pos == self.r_buf.len() {
            if buf.len() >= self.r_buf.capacity() {
                self.r_buf_pos = 0;
                self.r_buf.clear();

                if self.r_pos + buf.len() <= self.raw.len() {
                    unsafe {
                        Self::raw_read(&self.raw, self.r_pos, buf);
                    };
                    self.r_pos += buf.len();
                    return Ok(buf.len());
                } else {
                    let buf_len = self.raw.len() - self.r_pos;
                    unsafe {
                        Self::raw_read(&self.raw, self.r_pos, &mut buf[..buf_len]);
                    }
                    self.r_pos += buf_len;
                    return Ok(buf_len);
                }
            } else {
                let remain = self.raw.len() - self.r_pos;
                if remain >= self.r_buf.capacity() {
                    unsafe {
                        Self::raw_read(&self.raw, self.r_pos, &mut self.r_buf);
                    }
                    // self.r_pos+=self.
                } else {
                    unsafe {
                        Self::raw_read(&self.raw, self.r_pos, &mut self.r_buf[..remain]);
                    }
                }
            }
        }

        todo!()
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
    #[inline]
    unsafe fn raw_write(raw: &MmapRaw, offset: usize, data: &[u8]) {
        std::ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr().add(offset), data.len());
    }
    #[inline]
    unsafe fn raw_read(raw: &MmapRaw, offset: usize, buf: &mut [u8]) {
        std::ptr::copy_nonoverlapping(raw.as_ptr().add(offset), buf.as_mut_ptr(), buf.len());
    }
    #[inline]
    pub(crate) fn write_slice(&mut self, offset: usize, data: &[u8]) -> io::Result<()> {
        let buf_offset = self.w_pos + self.w_buf.len();
        if offset == buf_offset {
            self.write(data);
        } else if offset < self.w_pos || offset > buf_offset {
            self.check_len_satisfied(data.len())?;
            unsafe {
                Self::raw_write(&self.raw, offset, data);
            };
        } else {
            unsafe {
                self.w_buf.set_len(offset - self.w_pos);
            }
            self.write(data);
        };
        Ok(())
    }
}
impl Drop for MmapFile {
    fn drop(&mut self) {
        if !self.panicked {
            let _r = self.flush_buf();
        }
        self.sync_all();
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
#[tokio::test]
async fn test_raw() {
    // BufWriter;
    let file_path = PathBuf::from("tmp/a.x");
    let mut fp_open_opt = OpenOptions::new();
    fp_open_opt.create(true).read(true).write(true);
    let mut mmap = MmapFile::open(&file_path, fp_open_opt, 10).unwrap().0;
    let s = String::from("abc");
    let data = s.as_bytes();
    unsafe {
        MmapFile::raw_write(&mmap.raw, 0, data);
    }
    // let mut buf: Vec<u8> = Vec::with_capacity(10);
    let mut buf = vec![0 as u8; 10];
    buf.clear();
    let mut buffer=&mut buf;
    unsafe {
        // write_to_buffer_unchecked(&mut buffer, data);
        std::ptr::copy_nonoverlapping(data.as_ptr(), buffer.as_mut_ptr().add(buffer.len()), data.len());
        buffer.set_len(data.len());
    }
    // buf.put_slice();
    // unsafe{

    // std::ptr::write_bytes(buf.as_mut_ptr(), mmap.raw.as_ptr(), 3);
    // std::ptr::write_volatile(dst, src)
    // }
    // let buf = buf.as_mut_slice();
    // unsafe {
    // MmapFile::raw_read(&mmap.raw, 0, buf);
    // };
    dbg!(buffer);
}
#[test]
fn test_a() {
    let mut v: Vec<u8> = Vec::with_capacity(10);
    // let p: &mut [u8] = &mut v;
    // let p=v.as_mut_ptr();
    let k = v.as_mut_slice();
    let s = String::from("abc");
    let a = s.as_bytes();
    // let a=a.as_ptr();
    // dbg!(p[..8].len());
    unsafe {
        std::ptr::copy_nonoverlapping(a.as_ptr(), k.as_mut_ptr(), a.len());
    };
    dbg!(v);
    // dbg!(String::from_utf8_lossy(v.as_ref()));
}
