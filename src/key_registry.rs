use crate::{
    errors::{self, DBError},
    options::Options,
    pb::{self, badgerpb4::DataKey},
};
// use aes::cipher::{generic_array::GenericArray, KeyInit, BlockEncrypt};
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
fn generate_none(){
    
}
// fn xor_block_stream(key:&[u8]){

    // let p = aes::Aes128::new_from_slice(key).unwrap();
    // p.encrypt_block(block)
    // aes::cipher::StreamCipherCore::apply_keystream_blocks(&mut self, blocks)
    // p.encrypt_with_backend(f)
    // p.encrypt_block_b2b(in_block, out_block)
    // aes::cipher::StreamCipher::apply_keystream(&mut self, buf)
// }
