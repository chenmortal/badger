use aead::consts::U12;
use aead::generic_array::GenericArray;
use aead::{Aead, AeadCore, KeyInit, OsRng};
#[cfg(feature = "aes-gcm")]
use aes_gcm::{Aes128Gcm, Aes256Gcm};
#[cfg(feature = "aes-gcm-siv")]
use aes_gcm_siv::Aes128GcmSiv as Aes128Gcm;
#[cfg(feature = "aes-gcm-siv")]
use aes_gcm_siv::Aes256GcmSiv as Aes256Gcm;
pub type Nonce = GenericArray<u8, U12>;
use crate::default::DEFAULT_DIR;
use crate::kv::PhyTs;
use crate::util::sys::sync_dir;
use crate::{errors::DBError, pb::badgerpb4::DataKey};

use anyhow::anyhow;
use anyhow::bail;
use bytes::{Buf, BufMut};
use log::error;
use prost::Message;
use std::ops::{Add, AddAssign, Deref};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{rename, File, OpenOptions},
    io::{BufReader, Read, Seek, Write},
    os::unix::prelude::OpenOptionsExt,
};
use tokio::sync::RwLock;
const KEY_REGISTRY_FILE_NAME: &str = "KEYREGISTRY";
const KEY_REGISTRY_REWRITE_FILE_NAME: &str = "REWRITE-KEYREGISTRY";
const SANITYTEXT: &[u8] = b"Hello Badger";
pub const NONCE_SIZE: usize = 12;
#[derive(Debug, Default, Clone)]
pub(crate) struct KeyRegistry(Arc<RwLock<KeyRegistryInner>>);
impl Deref for KeyRegistry {
    type Target = Arc<RwLock<KeyRegistryInner>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CipherKeyId(u64);
impl From<u64> for CipherKeyId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
impl Into<u64> for CipherKeyId {
    fn into(self) -> u64 {
        self.0
    }
}

impl Add<u64> for CipherKeyId {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        (self.0 + rhs).into()
    }
}
impl AddAssign<u64> for CipherKeyId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs
    }
}
#[derive(Debug, Default)]
pub(crate) struct KeyRegistryInner {
    data_keys: HashMap<CipherKeyId, DataKey>,
    last_created: PhyTs, //last_created is the timestamp(seconds) of the last data key,
    next_key_id: CipherKeyId,
    fp: Option<File>,
    cipher: Option<AesCipher>,
    data_key_rotation_duration: Duration,
}
#[derive(Debug, Clone)]
pub struct KeyRegistryConfig {
    // Encryption related options.
    encrypt_key: Vec<u8>,                 // encryption key
    data_key_rotation_duration: Duration, // key rotation duration
    read_only: bool,
    dir: PathBuf,
}
impl Default for KeyRegistryConfig {
    fn default() -> Self {
        Self {
            encrypt_key: Default::default(),
            data_key_rotation_duration: Duration::from_secs(10 * 24 * 60 * 60),
            read_only: false,
            dir: PathBuf::from(DEFAULT_DIR),
        }
    }
}
impl KeyRegistryConfig {
    pub(crate) async fn open(&self) -> anyhow::Result<KeyRegistry> {
        let mut key_registry =
            KeyRegistryInner::new(&self.encrypt_key, self.data_key_rotation_duration)?;

        let key_registry_path = self.dir.join(KEY_REGISTRY_FILE_NAME);
        if !key_registry_path.exists() {
            if self.read_only {
                return Ok(KeyRegistry(
                    Arc::new(RwLock::new(key_registry)), // is_siv: self.is_siv,
                ));
            }
            key_registry
                .write_to_file(&self.dir)
                .await
                .map_err(|e| anyhow!("Error while writing key registry. {}", e))?;
        }

        let key_registry_fp = OpenOptions::new()
            .read(true)
            .write(!self.read_only)
            .custom_flags(libc::O_DSYNC)
            .open(key_registry_path)
            .map_err(|e| anyhow!("Error while opening key registry. {}", e))?;

        key_registry.read(&key_registry_fp).await?;
        if !self.read_only {
            key_registry.fp = Some(key_registry_fp);
        }

        return Ok(KeyRegistry(Arc::new(RwLock::new(key_registry))));
    }

    pub fn set_dir(&mut self, dir: PathBuf) {
        self.dir = dir;
    }
    pub(crate) fn dir(&self) -> &PathBuf {
        &self.dir
    }
    pub(crate) fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn set_data_key_rotation_duration(&mut self, data_key_rotation_duration: Duration) {
        self.data_key_rotation_duration = data_key_rotation_duration;
    }

    pub fn set_encrypt_key(&mut self, encrypt_key: Vec<u8>) {
        self.encrypt_key = encrypt_key;
    }

    pub fn encrypt_key(&self) -> &[u8] {
        self.encrypt_key.as_ref()
    }
}
impl KeyRegistryInner {
    fn new(encrypt_key: &[u8], data_key_rotation_duration: Duration) -> anyhow::Result<Self> {
        let keys_len = encrypt_key.len();
        if keys_len > 0 && !vec![16, 32].contains(&keys_len) {
            bail!("{:?} During OpenKeyRegistry", DBError::InvalidEncryptionKey);
        }
        let cipher: Option<AesCipher> = if keys_len > 0 {
            AesCipher::new(encrypt_key, 0.into()).ok()
        } else {
            None
        };
        Ok(Self {
            data_keys: Default::default(),
            last_created: 0.into(),
            next_key_id: 0.into(),
            fp: None,
            cipher,
            data_key_rotation_duration,
        })
    }
    //     Structure of Key Registry.
    // +-------------------+---------------------+--------------------+--------------+------------------+------------------+------------------+
    // |   Nonce   |  SanityText.len() u32 | e_Sanity Text  | DataKey1(len_crc_buf(e_data_key.len,crc),e_data_key(..,e_data,..))     | DataKey2     | ...              |
    // +-------------------+---------------------+--------------------+--------------+------------------+------------------+------------------+
    async fn write_to_file(&mut self, dir: &PathBuf) -> anyhow::Result<()> {
        let nonce: Nonce = AesCipher::generate_nonce();
        let mut e_sanity = SANITYTEXT.to_vec();

        if let Some(c) = &self.cipher {
            if let Some(e) = c.encrypt(&nonce, &e_sanity) {
                e_sanity = e;
            };
        }
        let mut buf = Vec::with_capacity(12 + 4 + 12 + 16);
        buf.put_slice(nonce.as_slice());
        buf.put_u32(e_sanity.len() as u32);
        buf.put_slice(&e_sanity);

        for (_, data_key) in self.data_keys.iter_mut() {
            Self::store_data_key(&mut buf, &self.cipher, data_key)
                .map_err(|e| anyhow!("Error while storing datakey in WriteKeyRegistry {}", e))?;
        }

        let rewrite_path = dir.join(KEY_REGISTRY_REWRITE_FILE_NAME);
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

        rename(rewrite_path, dir.join(KEY_REGISTRY_FILE_NAME))
            .map_err(|e| anyhow!("Error while renaming file in WriteKeyRegistry {}", e))?;
        sync_dir(dir)?;
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
            self.next_key_id = self.next_key_id.max(data_key.key_id.into());
            self.last_created = self.last_created.max(data_key.created_at.into());
            self.data_keys.insert(data_key.key_id.into(), data_key);
        }
        Ok(())
    }
}
impl KeyRegistry {
    pub(crate) async fn latest_cipher(&self) -> anyhow::Result<Option<AesCipher>> {
        if let Some(data_key) = self.latest_datakey().await? {
            return Ok(AesCipher::new(&data_key.data, data_key.key_id.into())?.into());
        };
        Ok(None)
    }

    async fn latest_datakey(&self) -> anyhow::Result<Option<DataKey>> {
        let inner_r = self.read().await;
        if inner_r.cipher.is_none() {
            return Ok(None);
        }

        let valid_key = |inner: &KeyRegistryInner| {
            let last = inner.last_created.into();
            if let Ok(diff) = SystemTime::now().duration_since(last) {
                if diff < inner.data_key_rotation_duration {
                    return (
                        inner
                            .data_keys
                            .get(&inner.next_key_id)
                            .and_then(|x| x.clone().into()),
                        true,
                    );
                }
            };
            return (None, false);
        };
        let (key, valid) = valid_key(&inner_r);
        if valid {
            return Ok(key);
        }
        drop(inner_r);
        let mut inner_w = self.write().await;
        let (key, valid) = valid_key(&inner_w);
        if valid {
            return Ok(key);
        }

        let cipher = inner_w.cipher.as_ref().unwrap();

        let key = cipher.generate_key();
        let nonce: Nonce = AesCipher::generate_nonce();
        inner_w.next_key_id += 1;
        let key_id = inner_w.next_key_id;
        let created_at = PhyTs::now()?;
        let mut data_key = DataKey {
            key_id: key_id.into(),
            data: key,
            iv: nonce.to_vec(),
            created_at: created_at.to_u64(),
        };
        let mut buf = Vec::new();
        KeyRegistryInner::store_data_key(&mut buf, &inner_w.cipher, &mut data_key)?;
        if let Some(f) = &mut inner_w.fp {
            f.write_all(&buf)?;
        }

        inner_w.last_created = created_at;
        inner_w.data_keys.insert(key_id, data_key.clone());
        Ok(Some(data_key))
    }
    async fn get_data_key(&self, cipher_key_id: CipherKeyId) -> anyhow::Result<Option<DataKey>> {
        let inner_r = self.read().await;
        if cipher_key_id == CipherKeyId::default() {
            return Ok(None);
        }
        match inner_r.data_keys.get(&cipher_key_id) {
            Some(s) => Ok(Some(s.clone())),
            None => {
                bail!(
                    "{} Error for the KEY ID {:?}",
                    DBError::InvalidDataKeyID,
                    cipher_key_id
                )
            }
        }
    }
    pub(crate) async fn get_cipher(
        &self,
        cipher_key_id: CipherKeyId,
    ) -> anyhow::Result<Option<AesCipher>> {
        if let Some(dk) = self.get_data_key(cipher_key_id).await? {
            let cipher = AesCipher::new(&dk.data, cipher_key_id)?;
            return Ok(cipher.into());
        };
        Ok(None)
    }
}
struct KeyRegistryIter<'a> {
    reader: BufReader<&'a File>,
    cipher: &'a Option<AesCipher>,
    len_crc_buf: Vec<u8>,
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
                        error!("While reading data_key.len and crc in keyRegistryIter.next {e}",);
                    }
                }
                return None;
            }
        };
        let mut len_crc_buf_ref: &[u8] = self.len_crc_buf.as_ref();
        let e_data_key_len = len_crc_buf_ref.get_u32();
        let e_data_key_crc: u32 = len_crc_buf_ref.get_u32();

        let mut e_data_key = vec![0 as u8; e_data_key_len as usize];
        match self.reader.read_exact(e_data_key.as_mut()) {
            Ok(_) => {}
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {}
                    _ => {
                        error!("While reading data in keyRegistryIter.next {e}");
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
            match c.decrypt_with_slice(&data_key.iv, &data_key.data) {
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

#[derive(Clone)]
pub(crate) enum AesCipher {
    Aes128(Aes128Gcm, CipherKeyId),
    Aes256(Aes256Gcm, CipherKeyId),
}

impl Debug for AesCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "aes-gcm")]
        match self {
            Self::Aes128(_, id) => f.debug_tuple("Aes128:").field(id).finish(),
            Self::Aes256(_, id) => f.debug_tuple("Aes256:").field(id).finish(),
        }
        #[cfg(feature = "aes-gcm-siv")]
        match self {
            Self::Aes128(_, id) => f.debug_tuple("Aes128Siv:").field(id).finish(),
            Self::Aes256(_, id) => f.debug_tuple("Aes256Siv:").field(id).finish(),
        }
    }
}

impl AesCipher {
    #[inline]
    pub(crate) fn new(key: &[u8], id: CipherKeyId) -> anyhow::Result<Self> {
        let cipher = match key.len() {
            16 => Self::Aes128(Aes128Gcm::new_from_slice(key).unwrap(), id),
            32 => Self::Aes256(Aes256Gcm::new_from_slice(key).unwrap(), id),
            _ => {
                bail!(
                    "{:?} During Create Aes Cipher",
                    DBError::InvalidEncryptionKey
                );
            }
        };
        Ok(cipher)
    }
    pub(crate) fn cipher_key_id(&self) -> CipherKeyId {
        match self {
            AesCipher::Aes128(_, id) => *id,
            AesCipher::Aes256(_, id) => *id,
        }
    }
    #[inline]
    pub(crate) fn encrypt(&self, nonce: &Nonce, plaintext: &[u8]) -> Option<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher, _) => cipher.encrypt(nonce, plaintext).ok(),
            AesCipher::Aes256(ref cipher, _) => cipher.encrypt(nonce, plaintext).ok(),
        }
    }
    #[inline]
    pub(crate) fn encrypt_with_slice(&self, nonce: &[u8], plaintext: &[u8]) -> Option<Vec<u8>> {
        self.encrypt(Nonce::from_slice(nonce), plaintext)
    }
    #[inline]
    pub(crate) fn decrypt(&self, nonce: &Nonce, ciphertext: &[u8]) -> Option<Vec<u8>> {
        match self {
            AesCipher::Aes128(ref cipher, _) => cipher.decrypt(nonce, ciphertext).ok(),
            AesCipher::Aes256(ref cipher, _) => cipher.decrypt(nonce, ciphertext).ok(),
        }
    }
    #[inline]
    pub(crate) fn decrypt_with_slice(&self, nonce: &[u8], ciphertext: &[u8]) -> Option<Vec<u8>> {
        self.decrypt(Nonce::from_slice(nonce), ciphertext)
    }
    #[inline]
    fn generate_key(&self) -> Vec<u8> {
        match self {
            AesCipher::Aes128(_, _) => Aes128Gcm::generate_key(&mut OsRng).to_vec(),
            AesCipher::Aes256(_, _) => Aes256Gcm::generate_key(&mut OsRng).to_vec(),
        }
    }
    #[inline]
    pub(crate) fn generate_nonce() -> Nonce {
        Aes128Gcm::generate_nonce(&mut OsRng)
    }
}
