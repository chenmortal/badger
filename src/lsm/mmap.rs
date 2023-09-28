#[cfg(not(any(
    target_os = "android",
    all(target_os = "linux", not(target_env = "musl"))
)))]
use libc::mmap;
#[cfg(any(
    target_os = "android",
    all(target_os = "linux", not(target_env = "musl"))
))]
use libc::{mmap64 as mmap, off64_t as off_t};

use anyhow::{anyhow, bail};
use core::slice;
use log::error;
use std::fs::{remove_file, File};
use std::ops::{Deref, DerefMut};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;
use std::{fs::OpenOptions, path::PathBuf};
use std::{io, ptr};

use crate::default::DEFAULT_PAGE_SIZE;
use crate::sys::sync_dir;

#[cfg(any(target_os = "linux", target_os = "android"))]
const MAP_POPULATE: libc::c_int = libc::MAP_POPULATE;

#[cfg(not(any(target_os = "linux", target_os = "android")))]
const MAP_POPULATE: libc::c_int = 0;
#[derive(Debug)]
pub(crate) struct MmapFile {
    ptr: *mut libc::c_void,
    len: usize,
    pub(crate) file_path: PathBuf,
    pub(crate) file_handle: File,
}
impl Deref for MmapFile {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr as *const u8, self.len as usize) }
    }
}

impl DerefMut for MmapFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.ptr as *mut u8, self.len) }
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
    pub(crate) fn lock(&self) -> io::Result<()> {
        unsafe {
            if libc::mlock(self.ptr, self.len) != 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }

    pub(crate) fn unlock(&self) -> io::Result<()> {
        unsafe {
            if libc::munlock(self.ptr, self.len) != 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }
    fn munmap(&self) -> io::Result<()> {
        let result = unsafe { libc::munmap(self.ptr, self.len as libc::size_t) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[inline]
    pub fn sync(&self) -> anyhow::Result<()> {
        self.flush(0, self.len)
            .map_err(|e| anyhow!("while sync file:{:?}, for {}", self.file_path, e))
    }
    #[inline]
    pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
        let alignment = (self.ptr as usize + offset) % DEFAULT_PAGE_SIZE.to_owned();
        let offset = offset as isize - alignment as isize;
        let len = len + alignment;
        let result =
            unsafe { libc::msync(self.ptr.offset(offset), len as libc::size_t, libc::MS_SYNC) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn flush_async(&self, offset: usize, len: usize) -> io::Result<()> {
        let alignment = (self.ptr as usize + offset) % DEFAULT_PAGE_SIZE.to_owned();
        let offset = offset as isize - alignment as isize;
        let len = len + alignment;
        let result =
            unsafe { libc::msync(self.ptr.offset(offset), len as libc::size_t, libc::MS_ASYNC) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

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
        self.munmap()?;
        self.file_handle
            .set_len(0)
            .map_err(|e| anyhow!("while truncate file:{:?}, error: {},", self.file_path, e))?;
        remove_file(&self.file_path)
            .map_err(|e| anyhow!("while remove file:{:?}, error:{}", self.file_path, e))?;
        Ok(())
    }
    pub(crate) fn ready_to_close(&self, max_sz: u64) -> anyhow::Result<()> {
        self.sync()
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
        self.flush(0, size)?;
        self.munmap()?;
        self.file_handle
            .set_len(0)
            .map_err(|e| anyhow!("while truncate file:{:?}, error: {},", self.file_path, e))?;
        let ptr = unsafe {
            let prot = libc::PROT_READ | libc::PROT_WRITE;
            let flags = libc::MAP_SHARED | MAP_POPULATE;
            let ptr = mmap(
                ptr::null_mut(),
                size as libc::size_t,
                prot,
                flags,
                self.file_handle.as_raw_fd(),
                0,
            );
            if ptr == libc::MAP_FAILED {
                bail!(
                    "cannot get mmap from {:?} :{}",
                    self.file_path,
                    io::Error::last_os_error()
                );
            }
            ptr
        };
        self.ptr = ptr;
        self.len = size;
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
    let fd = fp_open_opt
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

    let ptr = unsafe {
        let mut prot = libc::PROT_READ;
        if !read_only {
            prot |= libc::PROT_WRITE;
        }
        let flags = libc::MAP_SHARED | MAP_POPULATE;
        let ptr = mmap(
            ptr::null_mut(),
            file_size as libc::size_t,
            prot,
            flags,
            fd.as_raw_fd(),
            0,
        );
        if ptr == libc::MAP_FAILED {
            bail!(
                "cannot get mmap from {:?} :{}",
                file_path,
                io::Error::last_os_error()
            );
        }
        ptr
    };
    let mmap_file = MmapFile {
        ptr,
        len: file_size as usize,
        file_handle: fd,
        file_path: file_path.clone(),
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
#[tokio::test]
// #[test]
async fn test_a() {
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
    dbg!(p);
    // let file_path = PathBuf::from("tt.txt");
    // let mut fp_open_opt = OpenOptions::new();
    // fp_open_opt.read(true).write(true).create(true);
    // let s = "hello world";
    // dbg!(s.len());
    // let (mut mmap, is_new) =
    //     open_mmap_file(&file_path, fp_open_opt, false, (s.len() + 10) ).unwrap();
    // // mmap.munmap();
    // dbg!(is_new);
    // // mmap[]

    // mmap[0..s.len()].copy_from_slice(s.as_bytes());
    // mmap.munmap();
    // mmap[0..s.len()].fill(0);
}
