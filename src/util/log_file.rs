use std::{
    fs::{remove_file, OpenOptions},
    io::{self},
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    default::DEFAULT_IS_SIV,
    key_registry::{AesCipher, KeyRegistry},
    util::mmap::MmapFile,
    options::Options,
    pb::badgerpb4::DataKey,
    vlog::VLOG_HEADER_SIZE,
};
use anyhow::{anyhow, bail};
use bytes::{Buf, BufMut};

#[derive(Debug)]
pub(crate) struct LogFile {
    fid: u32,
    key_registry: KeyRegistry,
    datakey: Option<DataKey>,
    cipher: Option<AesCipher>,
    mmap: MmapFile,
    size: AtomicUsize,
    base_nonce: Vec<u8>,
    // write_at: usize,
}

impl Deref for LogFile {
    type Target = MmapFile;

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}
impl DerefMut for LogFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mmap
    }
}
impl LogFile {
    pub(crate) async fn open(
        fid: u32,
        file_path: &PathBuf,
        fp_open_opt: OpenOptions,
        fsize: usize,
        key_registry: KeyRegistry,
    ) -> anyhow::Result<(LogFile, bool)> {
        let (mmap, is_new) = MmapFile::open(file_path, fp_open_opt, fsize)?;

        let mut log_file = Self {
            fid,
            key_registry,
            datakey: None,
            mmap,
            size: AtomicUsize::new(0),
            base_nonce: Vec::new(),
            // write_at: VLOG_HEADER_SIZE,
            cipher: None,
        };

        if is_new {
            match log_file.bootstrap().await {
                Ok(_) => {
                    log_file.set_size(VLOG_HEADER_SIZE);
                }
                Err(e) => {
                    match remove_file(&log_file.path()) {
                        Ok(_) => {
                            bail!("Cannot logfile.boostrap {:?} for {}", &log_file.path(), e);
                        }
                        Err(error) => {
                            bail!(
                                "Cannot boostrap {:?} for {} and failed to remove this mmap_file  for {}",
                                &log_file.path(),
                                e,
                                error
                            )
                        }
                    };
                }
            };
        }
        log_file.set_size(log_file.mmap.len());

        if log_file.get_size() < VLOG_HEADER_SIZE {
            return Ok((log_file, is_new));
        }

        let mut buf = Vec::with_capacity(VLOG_HEADER_SIZE);
        buf.put(&log_file.mmap.as_ref()[0..VLOG_HEADER_SIZE]);
        debug_assert_eq!(buf.len(), VLOG_HEADER_SIZE);

        let mut buf_ref: &[u8] = buf.as_ref();
        let key_id = buf_ref.get_u64();

        let registry_r = log_file.key_registry.read().await;
        if let Some(dk) = registry_r.get_data_key(key_id).await? {
            log_file.cipher = AesCipher::new(dk.data.as_slice(), Options::aes_is_siv())?.into();
            log_file.datakey = Some(dk);
        }
        drop(registry_r);
        let nonce = buf_ref.get(0..12);
        log_file.base_nonce = nonce.unwrap().to_vec();

        Ok((log_file, is_new))
    }
    #[tracing::instrument]
    pub(crate) fn delete(&self) -> io::Result<()> {
        self.mmap.delete()
    }
    pub(crate) fn truncate(&mut self, end_offset: usize) -> io::Result<()> {
        let file_size = self.mmap.get_file_size()? as usize;
        if file_size == end_offset {
            return Ok(());
        }
        self.set_size(end_offset);
        self.mmap.set_len(end_offset)
    }
    // bootstrap will initialize the log file with key id and baseIV.
    // The below figure shows the layout of log file.
    // +----------------+------------------+------------------+
    // | keyID(8 bytes) |  baseIV(12 bytes)|	 entry...     |
    // +----------------+------------------+------------------+
    #[tracing::instrument]
    async fn bootstrap(&mut self) -> anyhow::Result<()> {
        let mut key_registry_w = self.key_registry.write().await;
        let datakey = key_registry_w
            .latest_datakey()
            .await
            .map_err(|e| anyhow!("Error while retrieving datakey in LogFile.bootstarp {}", e))?;
        drop(key_registry_w);
        self.datakey = datakey;
        if let Some(dk) = &self.datakey {
            self.cipher = AesCipher::new(&dk.data, Options::aes_is_siv())?.into();
        }
        self.base_nonce = AesCipher::generate_nonce().to_vec();

        let mut buf = Vec::with_capacity(VLOG_HEADER_SIZE);
        buf.put_u64(self.get_key_id());
        buf.put(self.base_nonce.as_ref());

        debug_assert_eq!(buf.len(), VLOG_HEADER_SIZE);
        self.mmap.write_slice(0, &buf);
        // self.mmap[0..buf.len()].copy_from_slice(&buf);
        // self.zero_next_entry();
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
    fn generate_nonce(&self, offset: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(12);
        let p = offset.to_ne_bytes();
        v.extend_from_slice(&self.base_nonce[..12 - p.len()]);
        v.extend_from_slice(&p);
        v
    }
    #[inline]
    pub(crate) fn try_decrypt(&self, ciphertext: &[u8], offset: usize) -> Option<Vec<u8>> {
        if let Some(c) = &self.cipher {
            let nonce = self.generate_nonce(offset);
            return c.decrypt_with_slice(nonce.as_slice(), ciphertext);
        } else {
            None
        }
    }
    #[inline]
    pub(crate) fn try_encrypt(&self, plaintext: &[u8], offset: usize) -> Option<Vec<u8>> {
        if let Some(c) = &self.cipher {
            let nonce = self.generate_nonce(offset);
            return c.encrypt_with_slice(nonce.as_slice(), plaintext);
        } else {
            None
        }
    }
    #[inline]
    fn zero_next_entry(&mut self) {
        // if let Some(s) = &mut self.mmap.right() {
        //     s.write([0 as u8; MAX_HEADER_SIZE].as_slice());
        //     s.seek(io::SeekFrom::Current(0 - MAX_HEADER_SIZE as i64));
        // }
        // let k=try_right!(self.mmap);
        // let start = self.write_at;
        // let mut end = self.write_at + MAX_HEADER_SIZE;
        // let len = self.mmap.len();
        // if start >= len {
        //     return;
        // }
        // if end >= len {
        //     end = len;
        // }
        // self.mmap[start..end].fill(0);
    }
    #[inline]
    pub(crate) fn get_size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }
    #[inline]
    pub(crate) fn set_size(&self, size: usize) {
        self.size.store(size, Ordering::SeqCst)
    }

    pub(crate) fn fid(&self) -> u32 {
        self.fid
    }
}
