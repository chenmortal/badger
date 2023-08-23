use anyhow::{anyhow, bail};
use std::{
    ffi::CString,
    fs::{File, OpenOptions, Permissions},
    io::Write,
    os::{
        fd::{AsFd, AsRawFd, FromRawFd},
        unix::prelude::PermissionsExt,
    },
    path::{Path, PathBuf},
};
pub(crate) struct DirLockGuard {
    dir_fd: File,
    abs_pid_path: PathBuf,
    read_only: bool,
}
///we need lock on dir , but rust std::fs cannot provide dir's fd for 'flock',so use libc;
fn open_with_libc(dir: &PathBuf, oflag: i32) -> anyhow::Result<File> {
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
fn flock(f: &File, operation: i32) -> anyhow::Result<()> {
    unsafe {
        let r = libc::flock(f.as_raw_fd(), operation);
        if r == -1 {
            bail!("another process is using it")
        }
    }
    Ok(())
}
impl DirLockGuard {
    pub(crate) fn acquire_lock(
        dir: &PathBuf,
        pid_file_name: String,
        read_only: bool,
    ) -> anyhow::Result<Self> {
        let mut abs_pid_path = dir.canonicalize().expect("cannot get absolute path");
        abs_pid_path.push(pid_file_name);
        let dir_fd = open_with_libc(dir, libc::O_RDONLY)?;
        flock(
            &dir_fd,
            (if read_only {
                libc::LOCK_SH
            } else {
                libc::LOCK_EX
            }) | libc::LOCK_NB,
        )
        .map_err(|_| {
            anyhow!(
                "cannot acquire dir lock on {:?}. Another process is using this Badger database",
                dir
            )
        })?;
        if !read_only {
            let mut pid_f = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&abs_pid_path)
                .map_err(|e| anyhow!("cannot open pid lock file {:?} : {}", abs_pid_path, e))?;
            pid_f
                .set_permissions(Permissions::from_mode(0o666))
                .map_err(|e| {
                    anyhow!(
                        "cannot set permission 0o666 for pid lock file {:?} : {}",
                        abs_pid_path,
                        e
                    )
                })?;
            pid_f
                .write_all(format!("{}", unsafe { libc::getpid() }).as_bytes())
                .map_err(|e| anyhow!("cannot write pid lock file {:?} : {}", abs_pid_path, e))?;
        }
        Ok(Self {
            dir_fd,
            abs_pid_path,
            read_only,
        })
    }
}

