use std::{
    ffi::CString,
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
    path::PathBuf,
};

use anyhow::anyhow;
use anyhow::bail;
pub(crate) fn open_with_libc(dir: &PathBuf, oflag: i32) -> anyhow::Result<File> {
    unsafe {
        match CString::new(dir.to_string_lossy().as_bytes()) {
            Ok(path) => {
                let fd = libc::open(path.as_ptr(), oflag);
                drop(path);
                if fd != -1 {
                    return Ok(File::from_raw_fd(fd));
                }
            }
            Err(_) => {}
        };
    }

    bail!("cannot open {:?}", dir);
}
pub(crate) fn flock(f: &File, operation: i32) -> anyhow::Result<()> {
    unsafe {
        let r = libc::flock(f.as_raw_fd(), operation);
        if r == -1 {
            bail!("another process is using it")
        }
    }
    Ok(())
}
pub(crate) fn sync_dir(dir: &PathBuf) -> anyhow::Result<()> {
    let f = open_with_libc(dir, libc::O_RDONLY)?;
    f.sync_all()
        .map_err(|e| anyhow!("cannot sync dir {:?} : {}", dir, e))?;
    Ok(())
}
