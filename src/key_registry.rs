use crate::{
    errors::{self, DBError},
    options::Options,
    pb::badgerpb4::DataKey,
};
use aes_gcm::{
    aead::{Aead, AeadMut, AeadMutInPlace},
    aes::Aes128,
    AeadCore, Aes128Gcm, Aes256Gcm, KeyInit,
};

use aes_gcm::aead::OsRng;
use aes_gcm_siv::{Aes128GcmSiv, Aes256GcmSiv, Nonce};
use anyhow::anyhow;
use anyhow::bail;
use bytes::{BufMut, BytesMut};
use prost::Message;
use std::{
    collections::HashMap,
    f32::consts::E,
    fs::{File, OpenOptions},
    path::PathBuf,
    time::Duration,
};
use tokio::sync::RwLock;
const KEY_REGISTRY_FILE_NAME: &str = "KEYREGISTRY";
const KEY_REGISTRY_REWRITE_FILE_NAME: &str = "REWRITE-KEYREGISTRY";
const SANITYTEXT: &[u8] = b"Hello Badger";

#[derive(Debug, Default)]
pub(crate) struct KeyRegistry {
    rw: RwLock<()>,
    data_keys: HashMap<u64, DataKey>,
    last_created: i64, //last_created is the timestamp(seconds) of the last data key,
    next_key_id: u64,
    fp: Option<File>,
    opt: KeyRegistryOptions,
}

#[derive(Debug, Default)]
pub(crate) struct KeyRegistryOptions {
    dir: PathBuf,
    read_only: bool,
    encryption_key: Vec<u8>,
    encryption_key_rotation_duration: Duration,
}
impl KeyRegistryOptions {
    pub(crate) fn new(opt: &Options) -> Self {
        Self {
            dir: opt.dir.clone(),
            read_only: opt.read_only,
            encryption_key: opt.encryption_key.clone(),
            encryption_key_rotation_duration: opt.encryption_key_rotation_duration,
        }
    }
}
impl KeyRegistry {
    fn new(opt: KeyRegistryOptions) -> Self {
        let mut key_registry = KeyRegistry::default();
        key_registry.opt = opt;
        key_registry
    }
    pub(crate) fn open(key_opt: KeyRegistryOptions) -> anyhow::Result<Self> {
        let keys_len = key_opt.encryption_key.len();
        if keys_len > 0 && !vec![16, 32].contains(&keys_len) {
            bail!("{:?} During OpenKeyRegistry", DBError::InvalidEncryptionKey);
        }
        let key_registry_path = key_opt.dir.join(KEY_REGISTRY_FILE_NAME);
        if !key_registry_path.exists() {
            // let mut key_registry = KeyRegistry::new(key_opt);
            // if key_opt.read_only{
            // return Ok(key_registry);
            // }
        }
        // Ok(())
        bail!("a")
    }
    //     Structure of Key Registry.
    // +-------------------+---------------------+--------------------+--------------+------------------+
    // |     IV            | Sanity Text         | DataKey1           | DataKey2     | ...              |
    // +-------------------+---------------------+--------------------+--------------+------------------+
    fn write(&mut self, key_opt: KeyRegistryOptions) -> anyhow::Result<()> {
        let nonce: Nonce = generate_nonce();
        let mut e_sanity = SANITYTEXT.to_vec();
        let cipher = if key_opt.encryption_key.len() > 0 {
            AesCipher::new(&key_opt.encryption_key, false).ok()
        } else {
            None
        };
        if let Some(c) = &cipher {
            if let Some(e) = c.encrypt(&nonce, &e_sanity) {
                e_sanity = e;
            };
        }
        let mut buf = bytes::BytesMut::new();
        buf.put_slice(nonce.as_slice());
        buf.put_slice(&e_sanity);
        for (_, data_key) in self.data_keys.iter_mut() {
            Self::store_data_key(&mut buf, &cipher, data_key)
                .map_err(|e| anyhow!("Error while storing datakey in WriteKeyRegistry {}", e))?;
        }
        let rewrite_path = key_opt.dir.join(KEY_REGISTRY_REWRITE_FILE_NAME);

        Ok(())
    }
    fn store_data_key(
        buf: &mut BytesMut,
        cipher: &Option<AesCipher>,
        data_key: &mut DataKey,
    ) -> anyhow::Result<()> {
        let nonce = Nonce::from_slice(&data_key.iv);
        if let Some(c) = cipher {
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
        let mut len_crc_buf = BytesMut::with_capacity(8);
        len_crc_buf.put_u32(e_data_key.len() as u32);
        len_crc_buf.put_u32(crc32fast::hash(&e_data_key));
        buf.put(len_crc_buf);
        buf.put(e_data_key.as_ref());

        if let Some(c) = cipher {
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
}

enum AesCipher {
    Aes128(Aes128Gcm),
    Aes128Siv(Aes128GcmSiv),
    Aes256(Aes256Gcm),
    Aes256Siv(Aes256GcmSiv),
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
}
#[inline]
fn generate_nonce() -> Nonce {
    aes_gcm_siv::Aes128GcmSiv::generate_nonce(&mut OsRng)
}

#[test]
fn test_aes() {
    let mut key = [0; 16];
    let p = aes_gcm::Aes128Gcm::new_from_slice(&key).unwrap();
    let nonce: Nonce = aes_gcm::Aes128Gcm::generate_nonce(&mut OsRng);
    dbg!(nonce);
    let n: Nonce = aes_gcm::Aes256Gcm::generate_nonce(&mut OsRng);

    let k = p.encrypt(&nonce, b"pp".as_ref()).unwrap();
    let r = p.decrypt(&nonce, k.as_ref()).unwrap();
    dbg!(String::from_utf8(r));
    let mut a = b"cc".to_vec();
    // p.encrypt_in_place(nonce, associated_data, buffer)
    // p.encrypt_in_place(nonce, associated_data, buffer);
    // p.encrypt_in_place_detached(nonce, associated_data, buffer)
    // match k {
    // Ok(_) => {}
    // Err(e) => {
    // dbg!(e);
    // }
    // }
    // let k = p.encrypt(&nonce, b"hha".as_ref());
}
#[test]
fn test_endian() {
    let a: u32 = 1;
    dbg!(a.to_be_bytes());
    dbg!(a.to_le_bytes());
    dbg!(a.to_ne_bytes());
    // let p=a as char;
}
