use crate::options::CompressionType;

use self::badgerpb4::{ManifestChange, manifest_change::Operation, EncryptionAlgo};

pub mod badgerpb4;
impl ManifestChange {
    pub fn new_create_change(id:u64,level:u32,key_id:u64,compression:CompressionType)->Self{
        Self{
            id,
            op: Operation::Create as i32,
            level,
            key_id,
            encryption_algo: EncryptionAlgo::Aes as i32,
            compression:compression as u32,
        }
    }
    
}