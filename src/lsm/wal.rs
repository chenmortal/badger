use std::{fs::OpenOptions, os::unix::prelude::OpenOptionsExt, path::PathBuf, sync::Arc};

use crate::{
    key_registry::{self, KeyRegistry},
    lsm::mmap::{open_mmap_file, MmapFile},
    options::Options,
};

pub(crate) struct LogFile {
    fid: u32,
    file_path: PathBuf,
    opt: Arc<Options>,
    key_registry: Arc<KeyRegistry>,
}

impl LogFile {
    pub(crate) fn new(
        fid: u32,
        file_path: PathBuf,
        opt: Arc<Options>,
        key_registry: Arc<KeyRegistry>,
    ) -> Self {
        Self {
            fid,
            file_path,
            opt,
            key_registry,
        }
    }
    pub(crate) fn open(
        &self,
        read_only: bool,
        fp_open_opt: OpenOptions,
        fsize: u64,
    ) -> anyhow::Result<()> {
        let (mmap_file, is_new) = open_mmap_file(&self.file_path, fp_open_opt, read_only, fsize)?;
        if is_new {}
        Ok(())
    }
    // bootstrap will initialize the log file with key id and baseIV.
    // The below figure shows the layout of log file.
    // +----------------+------------------+------------------+
    // | keyID(8 bytes) |  baseIV(12 bytes)|	 entry...     |
    // +----------------+------------------+------------------+
    fn bootstrap(&self) {}
}
