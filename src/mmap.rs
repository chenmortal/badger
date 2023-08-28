use anyhow::anyhow;
use std::{fs::OpenOptions, path::PathBuf};
use memmap2::MmapOptions;
use crate::options::Options;
fn open_mmap_file(
    file_path: &PathBuf,
    read_only: bool,
    create: bool,
    max_file_size: u64,
) -> anyhow::Result<()> {
    let fd = OpenOptions::new()
        .read(true)
        .write(!read_only)
        .create(!read_only && create)
        .open(file_path)
        .map_err(|e| anyhow!("unable to open: {:?} :{}", file_path, e))?;
    let metadata = fd.metadata()
            .map_err(|e| anyhow!("cannot get metadata file:{:?} :{}", file_path, e))?;;
    let mut file_size = metadata.len();
    let mut is_new_file=false;
    if max_file_size >0 && file_size==0{
        fd.set_len(max_file_size).map_err(|e| anyhow!("cannot truncate {:?} to {} : {}",file_path,max_file_size,e))?;
        file_size=max_file_size;
        is_new_file=true;
    }

    MmapOptions::new().map_raw_read_only(file)
    // let opt = OpenOptions::new();

    // open_opt
    // .open(file_path)
    // .map_err(|e| anyhow!("unable to open {:?} : {}", file_path, e))?;

    Ok(())
}
