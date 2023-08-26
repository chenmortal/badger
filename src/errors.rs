use std::{path::PathBuf, io::Error};

use thiserror::Error;
use anyhow::anyhow;
#[derive(Debug,Error)]
pub enum DBError{
    #[error("Invalid ValueLogFileSize, must be in range [1MB,2GB)")]
    ValuelogSize,
    #[error("Key not found")]
    KeyNotFound,
    #[error("Txn is too big to fit into one request")]
    TxnTooBig,
    #[error("Transaction Conflict. Please retry")]
    Conflict,
    #[error("No sets or deletes are allowed in a read-only transaction")]
    ReadOnlyTxn,
    #[error("This transaction has been discarded. Create a new one")]
    DiscardedTxn,
    #[error("Key cannot be empty")]
    EmptyKey,
    #[error("Key is using a reserved !badger! prefix")]
    InvalidKey,
    #[error("Key is using the banned prefix")]
    BannedKey,
    #[error("Value log GC can't run because threshold is set to zero")]
    ThresholdZero
}
pub(crate) fn err_file(err:Error,path:&PathBuf,msg:&str)->anyhow::Error{
    anyhow!("{}. Path={:?}. Error={}",msg,path,err)
}
// #[derive(Debug,Error)]
// pub enum FileSysErr {
//     #[error("Error opening {file_path} : {source}")]
//     CannotOpen{
//         source:std::io::Error,
//         file_path:PathBuf
//     },
//     #[error("Error get absolute path")]
// }

// impl From<std::io::Error> for FileSysErr{
//     fn from(value: std::io::Error) -> Self {
//         todo!()
//     }
// }
// impl From for FileSysErr {
    
// }