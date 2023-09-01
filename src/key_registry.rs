use crate::{
    errors::{self, DBError},
    options::Options,
    pb::{self, badgerpb4::DataKey},
};
use aes_gcm::{
    aead::Aead,
    aes::{cipher::BlockEncrypt, Aes192},
    AeadCore, KeyInit, Aes128Gcm, Aes256Gcm,
};

use anyhow::anyhow;
use anyhow::bail;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    path::PathBuf,
    time::Duration,
};
use tokio::sync::RwLock;
const KEY_REGISTRY_FILE_NAME: &str = "KEYREGISTRY";
const KEY_REGISTRY_REWRITE_FILE_NAME: &str = "REWRITE-KEYREGISTRY";
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
        if keys_len > 0 && !vec![16, 24, 32].contains(&keys_len) {
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
    fn write(&self, key_opt: KeyRegistryOptions) {}
}
use aes_gcm::aead::OsRng;
use aes_gcm_siv::{Nonce, Aes128GcmSiv, Aes256GcmSiv};
enum AesAlgo {
    Aes128,
    Aes128Siv,
    Aes256,
    Aes256Siv,
}
impl AesAlgo {
    fn new(key:&[u8]){
        let p:Aes128Gcm = aes_gcm::Aes128Gcm::new_from_slice(key).unwrap();
        let p:Aes256Gcm = aes_gcm::Aes256Gcm::new_from_slice(key).unwrap();
        let p:Aes128GcmSiv = aes_gcm_siv::Aes128GcmSiv::new_from_slice(key).unwrap();
        let p:Aes256GcmSiv = aes_gcm_siv::Aes256GcmSiv::new_from_slice(key).unwrap();
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
    let k = p.encrypt(&nonce, b"hha".as_ref());
}

