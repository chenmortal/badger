use anyhow::anyhow;
use log::error;
use std::{
    fs::{File, OpenOptions, Permissions},
    io::Write,
    os::unix::prelude::PermissionsExt,
    path::PathBuf,
};

use crate::sys::{flock, open_with_libc};
pub(crate) struct DirLockGuard {
    dir_fd: File,
    abs_pid_path: PathBuf,
    read_only: bool,
}
///we need lock on dir , but rust std::fs cannot provide dir's fd for 'flock',so use libc;

impl DirLockGuard {
    pub(crate) fn acquire_lock(
        dir: &PathBuf,
        pid_file_name: &str,
        read_only: bool,
    ) -> anyhow::Result<Self> {
        let abs_pid_path = dir
            .canonicalize()
            .expect("cannot get absolute path")
            .join(pid_file_name);
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
impl Drop for DirLockGuard {
    fn drop(&mut self) {
        if !self.read_only {
            match std::fs::remove_file(&self.abs_pid_path) {
                Ok(_) => {}
                Err(e) => {
                    error!("cannot remove file {:?} : {}", &self.abs_pid_path, e);
                }
            };
        }
        match flock(&self.dir_fd, libc::LOCK_UN) {
            Ok(_) => {}
            Err(_) => {
                error!("cannot release lock on opt.dir or opt.value_dir");
            }
        };
    }
}
