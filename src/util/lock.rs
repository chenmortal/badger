use anyhow::anyhow;
use log::error;
use std::{
    collections::HashSet,
    fs::{File, OpenOptions, Permissions},
    io::Write,
    os::unix::prelude::PermissionsExt,
    path::PathBuf,
};

use crate::{
    default::LOCK_FILE,
    util::sys::{flock, open_with_libc},
};
#[derive(Debug)]
pub(crate) struct DBLockGuard {
    lock_guards: Vec<DirLockGuard>,
}
#[derive(Debug, Clone)]
pub struct DBLockGuardConfig {
    dirs: HashSet<PathBuf>,
    // BypassLockGuard will bypass the lock guard on badger. Bypassing lock
    // guard can cause data corruption if multiple badger instances are using
    // the same directory. Use this options with caution.
    bypass_lock_guard: bool,
    read_only: bool,
}
impl Default for DBLockGuardConfig {
    fn default() -> Self {
        Self {
            dirs: Default::default(),
            bypass_lock_guard: false,
            read_only: false,
        }
    }
}
impl DBLockGuardConfig {
    pub(crate) fn try_build(&self) -> anyhow::Result<Option<DBLockGuard>> {
        if !self.bypass_lock_guard {
            let mut lock_guards = Vec::with_capacity(self.dirs.len());
            for dir in self.dirs.iter() {
                let dir_gurard = DirLockGuard::acquire_lock(dir, LOCK_FILE, self.read_only)?;
                lock_guards.push(dir_gurard);
            }
            return Ok(DBLockGuard { lock_guards }.into());
        }
        Ok(None)
    }

    pub fn set_bypass_lock_guard(&mut self, bypass_lock_guard: bool) {
        self.bypass_lock_guard = bypass_lock_guard;
    }

    pub(crate) fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }
    pub(crate) fn insert(&mut self, p: PathBuf) {
        self.dirs.insert(p);
    }
}
#[derive(Debug)]
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
            if let Err(e) = std::fs::remove_file(&self.abs_pid_path) {
                error!("cannot remove file {:?} : {}", &self.abs_pid_path, e);
            }
        }
        if let Err(e) = flock(&self.dir_fd, libc::LOCK_UN) {
            error!("cannot release lock on opt.dir or opt.value_dir : {}", e);
        }
    }
}
