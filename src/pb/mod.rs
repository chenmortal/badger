use std::fmt::Display;

use anyhow::bail;

use crate::options::CompressionType;

use self::badgerpb4::{manifest_change::Operation, Checksum, EncryptionAlgo, ManifestChange};
use crate::pb::badgerpb4::checksum::Algorithm;
pub mod badgerpb4;
impl ManifestChange {
    pub fn new_create_change(
        id: u32,
        level: u32,
        key_id: u64,
        compression: CompressionType,
    ) -> Self {
        Self {
            id,
            op: Operation::Create as i32,
            level,
            key_id,
            encryption_algo: EncryptionAlgo::Aes as i32,
            compression: compression.into(),
        }
    }
}
pub(crate) const ERR_CHECKSUM_MISMATCH: &str = "CHECKSUM_MISMATCH";
impl Algorithm {
    pub(crate) fn calculate(&self, data: &[u8]) -> u64 {
        match self {
            Algorithm::Crc32c => crc32fast::hash(data) as u64,
            Algorithm::XxHash64 => xxhash_rust::xxh3::xxh3_64(data),
        }
    }
}
#[derive(Debug)]
pub struct ChecksumError {
    actual: u64,
    expected: u64,
}
impl Display for ChecksumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "checksum mismatch actual: {} , expected: {}",
            self.actual, self.expected
        )
    }
}
impl std::error::Error for ChecksumError {}
impl Checksum {
    pub(crate) fn new(algo: Algorithm, data: &[u8]) -> Self {
        let mut checksum = Checksum::default();
        checksum.set_algo(algo);
        checksum.sum = algo.calculate(data);
        checksum
    }
    pub(crate) fn verify(&self, data: &[u8]) -> Result<(), ChecksumError> {
        let sum = self.algo().calculate(data);
        if self.sum != sum {
            return Err(ChecksumError {
                actual: sum,
                expected: self.sum,
            });
        };
        Ok(())
    }
}
