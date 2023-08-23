use std::{collections::{HashSet, HashMap}, sync::Mutex, fs::OpenOptions};

use anyhow::bail;

use crate::{options::{Options, CompressionType}, default::MANIFEST_FILE_NAME};
#[derive(Debug,Default)]
struct Manifest{
    levels:Vec<LevelManifest>,
    tables:HashMap<u64,TableManifest>,
    creations:isize,
    deletions:isize,
}
#[derive(Debug,Default)]
struct LevelManifest{
    tables:HashSet<u64>, //Set of table id's
}
#[derive(Debug,Default)]
struct TableManifest{
    level:u8,
    keyid:u64,
    compression:Option<CompressionType>
}
#[derive(Debug,Default)]
struct ManifestFile{
    dir:String,
    external_magic:u16,
    manifest:Mutex<Manifest>,
    // in_memory:bool
}
pub(crate) fn open_create_manifestfile(opt:&Options)->anyhow::Result<()>{
    let path = opt.dir.clone().join(MANIFEST_FILE_NAME);
    match OpenOptions::new().read(true).write(!opt.read_only).open(path) {
        Ok(_) => {},
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::NotFound => {
                    if opt.read_only{
                        bail!("no manifest found, required for read-only db");
                    }
                    let manifest = Manifest::default();
                    
                },
                _ => bail!(e),
            }
            // dbg!(e);
        },
    };;
    // open_opt.read(true).write(!opt.read_only);
    Ok(())
}