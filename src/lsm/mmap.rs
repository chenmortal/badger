use anyhow::anyhow;
use core::slice;
use log::error;
use memmap2::MmapRaw;
use std::fs::{remove_file, File};
use std::io::{self, Write, SeekFrom, Seek};
use std::ops::{Deref, DerefMut};
use std::time::SystemTime;
use std::{fs::OpenOptions, path::PathBuf};

use crate::sys::sync_dir;

#[derive(Debug)]
pub(crate) struct MmapFile {
    read_only: bool,
    mmap: MmapRaw,
    write_at: usize,
    pub(crate) file_path: PathBuf,
    pub(crate) file_handle: File,
}
impl Deref for MmapFile {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.mmap.as_ptr() as *const u8, self.mmap.len()) }
    }
}
impl Write for MmapFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self[self.write_at..self.write_at + buf.len()].copy_from_slice(buf);
        self.write_at += buf.len();
        // std::io::Seek;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush()
    }
}
impl DerefMut for MmapFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.mmap.as_mut_ptr() as *mut u8, self.mmap.len()) }
    }
}
impl Seek for MmapFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        todo!()
    }
}

// impl MmapFile {
//     fn truncate(&mut self) {
//         match self.file_handle.set_len(0) {
//             Ok(_) => {}
//             Err(e) => {
//                 error!("while truncate mmap_file {:?} for {}", self.file_path, e);
//             }
//         }
//         // drop(x)
//     }
// }
impl Drop for MmapFile {
    fn drop(&mut self) {
        self.munmap();
    }
}
// fn page_size() -> usize {
//     static PAGE_SIZE: AtomicUsize = AtomicUsize::new(0);

//     match PAGE_SIZE.load(Ordering::Relaxed) {
//         0 => {
//             let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };

//             PAGE_SIZE.store(page_size, Ordering::Relaxed);

//             page_size
//         }
//         page_size => page_size,
//     }
// }
impl MmapFile {
    // pub(crate) fn lock(&self) -> io::Result<()> {
    //     unsafe {
    //         if libc::mlock(self.ptr, self.len) != 0 {
    //             Err(io::Error::last_os_error())
    //         } else {
    //             Ok(())
    //         }
    //     }
    // }

    // pub(crate) fn unlock(&self) -> io::Result<()> {
    //     unsafe {
    //         if libc::munlock(self.ptr, self.len) != 0 {
    //             Err(io::Error::last_os_error())
    //         } else {
    //             Ok(())
    //         }
    //     }
    // }
    fn munmap(&self) -> io::Result<()> {
        let result =
            unsafe { libc::munmap(self.mmap.as_mut_ptr() as _, self.mmap.len() as libc::size_t) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    // #[inline]
    // pub fn sync(&self) -> anyhow::Result<()> {
    //     self.flush(0, self.len)
    //         .map_err(|e| anyhow!("while sync file:{:?}, for {}", self.file_path, e))
    // }
    // #[inline]
    // pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
    //     let alignment = (self.ptr as usize + offset) % DEFAULT_PAGE_SIZE.to_owned();
    //     let offset = offset as isize - alignment as isize;
    //     let len = len + alignment;
    //     let result =
    //         unsafe { libc::msync(self.ptr.offset(offset), len as libc::size_t, libc::MS_SYNC) };
    //     if result == 0 {
    //         Ok(())
    //     } else {
    //         Err(io::Error::last_os_error())
    //     }
    // }

    // pub fn flush_async(&self, offset: usize, len: usize) -> io::Result<()> {
    //     let alignment = (self.ptr as usize + offset) % DEFAULT_PAGE_SIZE.to_owned();
    //     let offset = offset as isize - alignment as isize;
    //     let len = len + alignment;
    //     let result =
    //         unsafe { libc::msync(self.ptr.offset(offset), len as libc::size_t, libc::MS_ASYNC) };
    //     if result == 0 {
    //         Ok(())
    //     } else {
    //         Err(io::Error::last_os_error())
    //     }
    // }

    pub(crate) fn get_file_size(&self) -> anyhow::Result<usize> {
        let p = self.file_handle.metadata()?;
        Ok(p.len() as usize)
    }
    pub(crate) fn get_modified_time(&self) -> anyhow::Result<SystemTime> {
        let meta = self.file_handle.metadata()?;
        let m = meta.modified()?;
        Ok(m)
    }
    pub(crate) fn read_slice(&self, offset: usize, len: usize) -> Result<&[u8], io::Error> {
        let p = self.as_ref();
        if p[offset..].len() < len {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        };
        Ok(&p[offset..offset + len])
    }
    pub(crate) fn delete(&self) -> anyhow::Result<()> {
        // self.munmap()?;
        // drop();
        self.file_handle
            .set_len(0)
            .map_err(|e| anyhow!("while truncate file:{:?}, error: {},", self.file_path, e))?;
        remove_file(&self.file_path)
            .map_err(|e| anyhow!("while remove file:{:?}, error:{}", self.file_path, e))?;
        Ok(())
    }
    pub(crate) fn ready_to_close(&self, max_sz: u64) -> anyhow::Result<()> {
        self.mmap
            .flush()
            .map_err(|e| anyhow!("while sync file:{:?}, for {}", self.file_path, e))?;
        self.munmap()
            .map_err(|e| anyhow!("while munmap file:{:?}, for {}", self.file_path, e))?;
        self.file_handle
            .set_len(max_sz as u64)
            .map_err(|e| anyhow!("while truncate file:{:?}, for {}", self.file_path, e))?;
        Ok(())
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn remap(&mut self, size: usize) -> io::Result<()> {
        unsafe {
            let new_ptr = libc::mremap(self.ptr, self.len, size, 0);

            if new_ptr == libc::MAP_FAILED {
                Err(io::Error::last_os_error())
            } else {
                // We explicitly don't drop self since the pointer within is no longer valid.
                // ptr::write(self, Self::from_raw_parts(new_ptr, new_len, offset));
                self.ptr = new_ptr;
                self.len = size;
                Ok(())
            }
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn truncate(&mut self, size: usize) -> anyhow::Result<()> {
        self.flush(0, size);
        self.file_handle
            .set_len(0)
            .map_err(|e| anyhow!("while truncate file:{:?}, error: {},", self.file_path, e))?;
        self.remap(size)?;
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub(crate) fn truncate(&mut self, size: usize) -> anyhow::Result<()> {
        // self.flush(0, size)?;
        self.mmap.flush_range(0, size);
        self.munmap()?;
        self.file_handle
            .set_len(0)
            .map_err(|e| anyhow!("while truncate file:{:?}, error: {},", self.file_path, e))?;
        let raw = memmap2::MmapRaw::map_raw(&self.file_handle)?;
        self.mmap = raw;
        Ok(())
    }
}
unsafe impl Send for MmapFile {}
unsafe impl Sync for MmapFile {}

pub(crate) fn open_mmap_file(
    file_path: &PathBuf,
    fp_open_opt: OpenOptions,
    read_only: bool,
    max_file_size: usize,
) -> anyhow::Result<(MmapFile, bool)> {
    let mut fd = fp_open_opt
        .open(file_path)
        .map_err(|e| anyhow!("unable to open: {:?} :{}", file_path, e))?;
    let metadata = fd
        .metadata()
        .map_err(|e| anyhow!("cannot get metadata file:{:?} :{}", file_path, e))?;
    let mut file_size = metadata.len();
    let mut is_new_file = false;
    if max_file_size > 0 && file_size == 0 {
        fd.set_len(max_file_size as u64).map_err(|e| {
            anyhow!(
                "cannot truncate {:?} to {} : {}",
                file_path,
                max_file_size,
                e
            )
        })?;
        file_size = max_file_size as u64;
        is_new_file = true;
    }
    let mmap_raw = memmap2::MmapRaw::map_raw(&fd)?;

    let mmap_file = MmapFile {
        file_handle: fd,
        file_path: file_path.clone(),
        read_only,
        mmap: mmap_raw,
        write_at: 0,
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
