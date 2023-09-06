use crate::util::{now_since_unix, secs_to_systime};
use crate::{errors::DBError, options::Options, pb::badgerpb4::DataKey, sys::sync_dir};
use aes_gcm::AeadCore;
use aes_gcm::{aead::Aead, Aes128Gcm, Aes256Gcm, KeyInit};

use aes_gcm::aead::OsRng;
use aes_gcm_siv::{Aes128GcmSiv, Aes256GcmSiv, Nonce};
use anyhow::anyhow;
use anyhow::bail;
use bytes::{Buf, BufMut};
use log::error;
use prost::Message;
use std::time::SystemTime;
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{rename, File, OpenOptions},
    io::{BufReader, Read, Seek, Write},
    os::unix::prelude::OpenOptionsExt,
    path::PathBuf,
    time::Duration,
};
use tokio::sync::RwLock;
const KEY_REGISTRY_FILE_NAME: &str = "KEYREGISTRY";
const KEY_REGISTRY_REWRITE_FILE_NAME: &str = "REWRITE-KEYREGISTRY";
const SANITYTEXT: &[u8] = b"Hello Badger";

#[derive(Debug, Default)]
pub(crate) struct KeyRegistry {
    pub(crate) data_keys: RwLock<HashMap<u64, DataKey>>,
    last_created: u64, //last_created is the timestamp(seconds) of the last data key,
    next_key_id: u64,
    fp: Option<File>,
    cipher: Option<AesCipher>,
    dir: PathBuf,
    read_only: bool,
    cipher_rotation_duration: Duration,
}

struct KeyRegistryIter<'a> {
    reader: BufReader<&'a File>,
    cipher: &'a Option<AesCipher>,
    len_crc_buf: Vec<u8>,
}

impl KeyRegistry {
    fn new(opt: &Options) -> anyhow::Result<Self> {
        let keys_len = opt.encryption_key.len();
        if keys_len > 0 && !vec![16, 32].contains(&keys_len) {
            bail!("{:?} During OpenKeyRegistry", DBError::InvalidEncryptionKey);
        }
        let cipher = if opt.encryption_key.len() > 0 {
            AesCipher::new(&opt.encryption_key, false).ok()
        } else {
            None
        };
        Ok(Self {
            // rw: Default::default(),
            data_keys: Default::default(),
            last_created: 0,
            next_key_id: 0,
            fp: None,
            cipher,
            dir: opt.dir.clone(),
            read_only: opt.read_only,
            cipher_rotation_duration: opt.encryption_key_rotation_duration,
        })
    }
    pub(crate) async fn open(opt: &Options) -> anyhow::Result<Self> {
        let mut key_registry = KeyRegistry::new(opt)?;
        let read_only = key_registry.read_only;
        let key_registry_path = key_registry.dir.join(KEY_REGISTRY_FILE_NAME);
        if !key_registry_path.exists() {
            if read_only {
                return Ok(key_registry);
            }
            key_registry
                .write()
                .await
                .map_err(|e| anyhow!("Error while writing key registry. {}", e))?;
        }

        let key_registry_fp = OpenOptions::new()
            .read(true)
            .write(!read_only)
            .custom_flags(libc::O_DSYNC)
            .open(key_registry_path)
            .map_err(|e| anyhow!("Error while opening key registry. {}", e))?;

        key_registry.read(&key_registry_fp).await?;
        if read_only {
            return Ok(key_registry);
        }
        key_registry.fp = Some(key_registry_fp);
        return Ok(key_registry);
    }
    //     Structure of Key Registry.
    // +-------------------+---------------------+--------------------+--------------+------------------+------------------+------------------+
    // |   Nonce   |  SanityText.len() u32 | e_Sanity Text  | DataKey1(len_crc_buf(e_data_key.len,crc),e_data_key(..,e_data,..))     | DataKey2     | ...              |
    // +-------------------+---------------------+--------------------+--------------+------------------+------------------+------------------+
    async fn write(&mut self) -> anyhow::Result<()> {
        let nonce: Nonce = AesCipher::generate_nonce();
        let mut e_sanity = SANITYTEXT.to_vec();

        if let Some(c) = &self.cipher {
            if let Some(e) = c.encrypt(&nonce, &e_sanity) {
                e_sanity = e;
            };
        }
        // let mut buf = bytes::BytesMut::new();
        let mut buf = Vec::with_capacity(12 + 4 + 12 + 16);
        buf.put_slice(nonce.as_slice());
        buf.put_u32(e_sanity.len() as u32);
        buf.put_slice(&e_sanity);

        for (_, data_key) in self.data_keys.write().await.iter_mut() {
            // let d:DataKey = data_key.;
            Self::store_data_key(&mut buf, &self.cipher, data_key)
                .map_err(|e| anyhow!("Error while storing datakey in WriteKeyRegistry {}", e))?;
        }

        let rewrite_path = self.dir.join(KEY_REGISTRY_REWRITE_FILE_NAME);
        let mut rewrite_fp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .custom_flags(libc::O_DSYNC)
            .open(&rewrite_path)
            .map_err(|e| anyhow!("Error while opening tmp file in WriteKeyRegistry {}", e))?;

        rewrite_fp
            .write_all(&buf)
            .map_err(|e| anyhow!("Error while writing buf in WriteKeyRegistry {}", e))?;

        rename(rewrite_path, self.dir.join(KEY_REGISTRY_FILE_NAME))
            .map_err(|e| anyhow!("Error while renaming file in WriteKeyRegistry {}", e))?;
        sync_dir(&self.dir)?;
        Ok(())
    }

    fn store_data_key(
        buf: &mut Vec<u8>,
        cipher: &Option<AesCipher>,
        data_key: &mut DataKey,
    ) -> anyhow::Result<()> {
        let nonce = Nonce::from_slice(&data_key.iv);
        if let Some(c) = &cipher {
            match c.encrypt(nonce, &data_key.data) {
                Some(e_data) => {
                    data_key.data = e_data;
                }
                None => {
                    bail!("Error while encrypting datakey in storeDataKey")
                }
            };
        }

        let e_data_key = data_key.encode_to_vec();

        let mut len_crc_buf = Vec::with_capacity(8);
        len_crc_buf.put_u32(e_data_key.len() as u32);
        len_crc_buf.put_u32(crc32fast::hash(&e_data_key));

        buf.put(len_crc_buf.as_ref());
        buf.put(e_data_key.as_ref());

        if let Some(c) = &cipher {
            match c.decrypt(nonce, &data_key.data) {
                Some(d_data) => {
                    data_key.data = d_data;
                }
                None => {
                    bail!("Error while decrypting datakey in storeDataKey")
                }
            };
        }
        Ok(())
    }

    async fn read(&mut self, fp: &File) -> anyhow::Result<()> {
        let key_iter = KeyRegistryIter::new(fp, &self.cipher)?;
        for data_key in key_iter {
            if data_key.key_id > self.next_key_id {
                self.next_key_id = data_key.key_id;
            }
            if data_key.created_at > self.last_created {
                self.last_created = data_key.created_at;
            }
            self.data_keys
                .write()
                .await
                .insert(data_key.key_id, data_key);
        }
        Ok(())
    }
    pub(crate) async fn latest_datakey(&mut self) -> anyhow::Result<Option<DataKey>> {
        if self.cipher.is_none() {
            return Ok(None);
        }
        for _ in 0..2 {
            let last = secs_to_systime(self.last_created);

            if let Ok(diff) = SystemTime::now().duration_since(last) {
                if diff < self.cipher_rotation_duration {
                    let data_keys_r = self.data_keys.read().await;
                    let r = match data_keys_r.get(&self.next_key_id) {
                        Some(d) => Some(d.clone()),
                        None => None,
                    };
                    drop(data_keys_r);
                    return Ok(r);
                }
            };
        }

        let cipher = self.cipher.as_ref().unwrap();

        let key = cipher.generate_key();
        let nonce:Nonce = AesCipher::generate_nonce();
        self.next_key_id += 1;
        let mut data_key = DataKey {
            key_id: self.next_key_id,
            data: key,
            iv: nonce.to_vec(),
            created_at: now_since_unix().as_secs(),
        };
        let mut buf = Vec::new();
        Self::store_data_key(&mut buf, &self.cipher, &mut data_key)?;
        if let Some(f) = &mut self.fp {
            f.write_all(&buf)?;
        }

        self.last_created = data_key.created_at;
        let mut data_key_w = self.data_keys.write().await;
        data_key_w.insert(self.next_key_id, data_key.clone());
        drop(data_key_w);
        Ok(Some(data_key))
    }
    // async fn get_data_key(&self,id:u64)->Option<DataKey>{
    // if id==0{
    // return None;
    // }
    //  self.data_keys.read().await.get(&id).;
    // }
}
impl<'a> KeyRegistryIter<'a> {
    fn valid(&mut self) -> anyhow::Result<()> {
        let mut nonce: Nonce = AesCipher::generate_nonce();
        self.reader
            .read_exact(nonce.as_mut())
            .map_err(|e| anyhow!("Error while reading IV for key registry. {}", e))?;

        let mut len_e_saintytext_buf = vec![0 as u8; 4];
        self.reader
            .read_exact(len_e_saintytext_buf.as_mut())
            .map_err(|e| anyhow!("Error while reading saintytext.len for key registry. {}", e))?;
        let mut len_e_saintytext_ref: &[u8] = len_e_saintytext_buf.as_ref();
        let len_e_saintytext = len_e_saintytext_ref.get_u32();

        let mut e_saintytext = vec![0; len_e_saintytext as usize];
        self.reader
            .read_exact(e_saintytext.as_mut())
            .map_err(|e| anyhow!("Error while reading sanity text. {}", e))?;

        let saintytext = match self.cipher {
            Some(c) => match c.decrypt(&nonce, &e_saintytext) {
                Some(d) => d,
                None => {
                    bail!("Cannot decrypt saintytext during valid");
                }
            },
            None => e_saintytext.to_vec(),
        };

        if saintytext != SANITYTEXT {
            bail!(DBError::EncryptionKeyMismatch);
        };
        Ok(())
    }
    fn new(fp: &'a File, cipher: &'a Option<AesCipher>) -> anyhow::Result<Self> {
        let mut reader = BufReader::new(fp);
        reader.seek(std::io::SeekFrom::Start(0))?;
        let mut s = Self {
            reader,
            cipher,
            len_crc_buf: vec![0; 8],
        };
        s.valid()?;
        Ok(s)
    }
}
impl<'a> Iterator for KeyRegistryIter<'a> {
    type Item = DataKey;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_exact(self.len_crc_buf.as_mut()) {
            Ok(_) => {}
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {}
                    _ => {
                        error!(
                            "While reading data_key.len and crc in keyRegistryIter.next {}",
                            e
                        );
                    }
                }
                return None;
            }
        };
        let mut len_crc_buf_ref: &[u8] = self.len_crc_buf.as_ref();
        let e_data_key_len = len_crc_buf_ref.get_u32();
        let e_data_key_crc = len_crc_buf_ref.get_u32();

        let mut e_data_key = vec![0 as u8; e_data_key_len as usize];
        match self.reader.read_exact(e_data_key.as_mut()) {
            Ok(_) => {}
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {}
                    _ => {
                        error!("While reading data in keyRegistryIter.next {}", e);
                    }
                }
                return None;
            }
        };

        if crc32fast::hash(&e_data_key) != e_data_key_crc {
            error!(
                "Error while checking checksum for data key. {:?}",
                e_data_key
            );
            //skip
            return self.next();
        };
        let mut data_key = match DataKey::decode(e_data_key.as_ref()) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "Error while decode protobuf-bytes for data key. {:?} for {:?}",
                    e_data_key, e
                );
                //skip
                return self.next();
            }
        };
        if let Some(c) = self.cipher {
            let nonce: &Nonce = Nonce::from_slice(&data_key.iv);
            match c.decrypt(nonce, &data_key.data) {
                Some(data) => {
                    data_key.data = data;
                }
                None => {
                    error!("Error while use aes cipher to decrypt datakey.data");
                    //skip
                    return self.next();
                }
            };
        }
        Some(data_key)
    }
}
pub(crate) enum AesCipher {
    Aes128(Aes128Gcm),
    Aes128Siv(Aes128GcmSiv),
    Aes256(Aes256Gcm),
    Aes256Siv(Aes256GcmSiv),
}
impl Debug for AesCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aes128(_) => f.debug_tuple("Aes128").finish(),
            Self::Aes128Siv(_) => f.debug_tuple("Aes128Siv").finish(),
            Self::Aes256(_) => f.debug_tuple("Aes256").finish(),
            Self::Aes256Siv(_) => f.debug_tuple("Aes256Siv").finish(),
        }
    }
}

impl AesCipher {
    #[inline]
    fn new(key: &[u8], is_siv: bool) -> anyhow::Result<Self> {
        let cipher = match key.len() {
            16 => {
                if is_siv {
                    Self::Aes128(aes_gcm::Aes128Gcm::new_from_slice(key).unwrap())
                } else {
                    Self::Aes128Siv(aes_gcm_siv::Aes128GcmSiv::new_from_slice(key).unwrap())
                }
            }
            32 => {
                if is_siv {
                    Self::Aes256(aes_gcm::Aes256Gcm::new_from_slice(key).unwrap())
                } else {
                    Self::Aes256Siv(aes_gcm_siv::Aes256GcmSiv::new_from_slice(key).unwrap())
                }
            }
            _ => {
                bail!(
                    "{:?} During Create Aes Cipher",
                    DBError::InvalidEncryptionKey
                );
            }
        };
        Ok(cipher)
    }
    #[inline]
    fn encrypt(&self, nonce: &Nonce, plaintext: &[u8]) -> Option<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher) => cipher.encrypt(nonce, plaintext).ok(),
            AesCipher::Aes128Siv(ref cipher) => cipher.encrypt(nonce, plaintext).ok(),
            AesCipher::Aes256(ref cipher) => cipher.encrypt(nonce, plaintext).ok(),
            AesCipher::Aes256Siv(ref cipher) => cipher.encrypt(nonce, plaintext).ok(),
        }
    }
    #[inline]
    fn decrypt(&self, nonce: &Nonce, plaintext: &[u8]) -> Option<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher) => cipher.decrypt(nonce, plaintext).ok(),
            AesCipher::Aes128Siv(ref cipher) => cipher.decrypt(nonce, plaintext).ok(),
            AesCipher::Aes256(ref cipher) => cipher.decrypt(nonce, plaintext).ok(),
            AesCipher::Aes256Siv(ref cipher) => cipher.decrypt(nonce, plaintext).ok(),
        }
    }
    #[inline]
    fn generate_key(&self) -> Vec<u8> {
        match self {
            AesCipher::Aes128(_) => aes_gcm::Aes128Gcm::generate_key(&mut OsRng).to_vec(),
            AesCipher::Aes128Siv(_) => aes_gcm_siv::Aes128GcmSiv::generate_key(&mut OsRng).to_vec(),
            AesCipher::Aes256(_) => aes_gcm::Aes256Gcm::generate_key(&mut OsRng).to_vec(),
            AesCipher::Aes256Siv(_) => aes_gcm_siv::Aes256GcmSiv::generate_key(&mut OsRng).to_vec(),
        }
    }
    #[inline]
    pub(crate) fn generate_nonce() -> Nonce {
        aes_gcm_siv::Aes128GcmSiv::generate_nonce(&mut OsRng)
    }
}
