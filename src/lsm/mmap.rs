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
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs::OpenOptions, path::PathBuf};
use std::{io, ptr};

use crate::sys::sync_dir;
#[derive(Debug)]
pub(crate) struct MmapFile {
    ptr: *mut libc::c_void,
    len: usize,
    file_handle: File,
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
impl Drop for MmapFile {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.ptr, self.len as libc::size_t) };
    }
}
fn page_size() -> usize {
    static PAGE_SIZE: AtomicUsize = AtomicUsize::new(0);

    match PAGE_SIZE.load(Ordering::Relaxed) {
        0 => {
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };

            PAGE_SIZE.store(page_size, Ordering::Relaxed);

            page_size
        }
        page_size => page_size,
    }
}
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
    pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
        let alignment = (self.ptr as usize + offset) % page_size();
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
        let alignment = (self.ptr as usize + offset) % page_size();
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
}
unsafe impl Send for MmapFile {}
unsafe impl Sync for MmapFile {}

pub(crate) fn open_mmap_file(
    file_path: &PathBuf,
    fp_open_opt: OpenOptions,
    read_only: bool,
    max_file_size: u64,
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
        fd.set_len(max_file_size).map_err(|e| {
            anyhow!(
                "cannot truncate {:?} to {} : {}",
                file_path,
                max_file_size,
                e
            )
        })?;
        file_size = max_file_size;
        is_new_file = true;
    }

    let ptr = unsafe {
        let mut prot = libc::PROT_READ;
        if !read_only {
            prot |= libc::PROT_WRITE;
        }
        let flags = libc::MAP_SHARED;
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
