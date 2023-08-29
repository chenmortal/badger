use std::path::PathBuf;

use crate::{options::Options, mmap::{MmapFile, open_mmap_file}};

pub(crate) struct LogFile{
    fid:u32,
    file_path:PathBuf,
    opt:Options,

}
impl LogFile {
    pub(crate) fn open(file_path:PathBuf,read_only:bool,create:bool,fsize:u64)->anyhow::Result<()>{
        let (mmap_file,is_new) = open_mmap_file(&file_path, read_only, create, fsize)?;;

        Ok(())
    }
    fn bootstrap(){
        
    }
}