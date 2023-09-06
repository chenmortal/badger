use std::{
    fs::{remove_file, OpenOptions},
    ops::{Deref, DerefMut},
    os::unix::prelude::OpenOptionsExt,
    path::PathBuf,
    sync::{
        atomic::{self, AtomicU32, Ordering},
        Arc,
    },
};

use crate::{
    key_registry::{self, AesCipher, KeyRegistry},
    lsm::mmap::{open_mmap_file, MmapFile},
    options::Options,
    pb::badgerpb4::DataKey,
    value::{threshold::VlogThreshold, MAX_HEADER_SIZE, VLOG_HEADER_SIZE},
};
use aes_gcm_siv::Nonce;
use anyhow::{anyhow, bail};
use bytes::{Buf, BufMut};
use tokio::sync::RwLock;
#[derive(Debug)]
pub(crate) struct LogFile {
    fid: u32,
    opt: Arc<Options>,
    key_registry: Arc<RwLock<KeyRegistry>>,
    datakey: Option<DataKey>,
    pub(crate) mmap: MmapFile,
    size: AtomicU32,
    base_nonce: Vec<u8>,
    write_at: usize,
}

impl LogFile {
    pub(crate) async fn open(
        // &self,
        fid: u32,
        file_path: PathBuf,
        read_only: bool,
        fp_open_opt: OpenOptions,
        fsize: u64,
        opt: Arc<Options>,
        key_registry: Arc<RwLock<KeyRegistry>>,
    ) -> anyhow::Result<(LogFile, bool)> {
        let (mmap, is_new) = open_mmap_file(&file_path, fp_open_opt, read_only, fsize)
            .map_err(|e| anyhow!("while opening file: {:?} for {}", &file_path, e))?;
        let mut log_file = Self {
            fid,
            // file_path,
            opt,
            key_registry,
            datakey: None,
            mmap,
            size: AtomicU32::new(0),
            base_nonce: Vec::new(),
            write_at: VLOG_HEADER_SIZE,
        };

        if is_new {
            match log_file.bootstrap().await {
                Ok(_) => {
                    log_file
                        .size
                        .store(VLOG_HEADER_SIZE as u32, Ordering::SeqCst);
                }
                Err(e) => {
                    match remove_file(&log_file.mmap.file_path) {
                        Ok(_) => {
                            bail!(
                                "Cannot logfile.boostrap {:?} for {}",
                                &log_file.mmap.file_path,
                                e
                            );
                        }
                        Err(error) => {
                            bail!(
                                "Cannot boostrap {:?} for {} and failed to remove this mmap_file  for {}",
                                &log_file.mmap.file_path,
                                e,
                                error
                            )
                        }
                    };
                }
            };
        }
        log_file
            .size
            .store(log_file.mmap.len() as u32, Ordering::SeqCst);

        if log_file.size.load(Ordering::SeqCst) < VLOG_HEADER_SIZE as u32 {
            return Ok((log_file, is_new));
        }

        let mut buf = Vec::with_capacity(VLOG_HEADER_SIZE);
        buf.put(&log_file.mmap[0..VLOG_HEADER_SIZE]);
        debug_assert_eq!(buf.len(), VLOG_HEADER_SIZE);

        let mut buf_ref: &[u8] = buf.as_ref();
        let key_id = buf_ref.get_u64();

        let registry_r = log_file.key_registry.read().await;
        let datakeys_r = registry_r.data_keys.read().await;
        if let Some(dk) = datakeys_r.get(&key_id) {
            log_file.datakey = Some(dk.clone());
        }
        drop(datakeys_r);
        drop(registry_r);
        let nonce = buf_ref.get(0..12);
        log_file.base_nonce = nonce.unwrap().to_vec();

        Ok((log_file, is_new))
    }
    fn delete(&self) {}
    // bootstrap will initialize the log file with key id and baseIV.
    // The below figure shows the layout of log file.
    // +----------------+------------------+------------------+
    // | keyID(8 bytes) |  baseIV(12 bytes)|	 entry...     |
    // +----------------+------------------+------------------+
    async fn bootstrap(&mut self) -> anyhow::Result<()> {
        let mut key_registry_w = self.key_registry.write().await;
        let datakey = key_registry_w
            .latest_datakey()
            .await
            .map_err(|e| anyhow!("Error while retrieving datakey in LogFile.bootstarp {}", e))?;
        drop(key_registry_w);
        self.datakey = datakey;
        self.base_nonce = AesCipher::generate_nonce().to_vec();

        let mut buf = Vec::with_capacity(VLOG_HEADER_SIZE);
        buf.put_u64(self.get_key_id());
        buf.put(self.base_nonce.as_ref());

        debug_assert_eq!(buf.len(), VLOG_HEADER_SIZE);
        self.mmap[0..buf.len()].copy_from_slice(&buf);
        self.zero_next_entry();
        Ok(())
    }
    #[inline]
    fn get_key_id(&self) -> u64 {
        match self.datakey {
            Some(ref k) => k.key_id,
            None => 0,
        }
    }
    #[inline]
    fn zero_next_entry(&mut self) {
        let start = self.write_at;
        let mut end = self.write_at + MAX_HEADER_SIZE;
        let len = self.mmap.len();
        if start >= len {
            return;
        }
        if end >= len {
            end = len;
        }
        self.mmap[start..end].fill(0);
    }
}

#[test]
fn test_a() {
    let s: Vec<u8> = vec![0, 0, 0, 0, 1, 2, 3, 4];
    let mut m: &[u8] = s.as_ref();
    let i = m.get_u32();
    let mut p = vec![0, 0, 0, 0];
    dbg!(m.len());
    p.copy_from_slice(m);
    dbg!(p);
    // let a = m.get(0..4);
    // dbg!(a);
}
