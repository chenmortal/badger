use std::{
    collections::{HashMap, HashSet},
    fs::{rename, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Mutex, Arc},
};

use anyhow::bail;
use bytes::{BufMut, BytesMut};
use prost::Message;

use crate::{
    default::{MANIFEST_FILE_NAME, MANIFEST_REWRITE_FILE_NAME},
    options::{CompressionType, Options},
    pb::badgerpb4::{ManifestChange, ManifestChangeSet},
    sys::sync_dir,
};
#[derive(Debug, Default)]
struct Manifest {
    levels: Vec<LevelManifest>,
    tables: HashMap<u64, TableManifest>,
    creations: isize,
    deletions: isize,
}
#[derive(Debug, Default)]
struct LevelManifest {
    tables: HashSet<u64>, //Set of table id's
}
#[derive(Debug, Default, Clone, Copy)]
struct TableManifest {
    level: u8,
    keyid: u64,
    compression: CompressionType,
}
#[derive(Debug)]
struct ManifestFile {
    file_path:File,
    dir: PathBuf,
    external_magic: u16,
    manifest: Arc<Mutex<Manifest>>,
    // in_memory:bool
}
pub(crate) fn open_create_manifestfile(opt: &Options) -> anyhow::Result<()> {
    let path = opt.dir.clone().join(MANIFEST_FILE_NAME);
    match OpenOptions::new()
        .read(true)
        .write(!opt.read_only)
        .open(path)
    {
        Ok(_) => {}
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::NotFound => {
                    if opt.read_only {
                        bail!("no manifest found, required for read-only db");
                    }
                    let manifest = Manifest::default();
                    let (file_path, net_creations) =
                        help_rewrite(&opt.dir, &manifest, opt.external_magic_version)?;
                    assert_eq!(net_creations,0) ;
                    ManifestFile{
                        file_path,
                        dir: opt.dir.clone(),
                        external_magic: opt.external_magic_version,
                        manifest:Arc::new(Mutex::new(manifest)),
                    };

                }
                _ => bail!(e),
            }
            // dbg!(e);
        }
    };
    // open_opt.read(true).write(!opt.read_only);
    Ok(())
}
const BADGER_MAGIC_VERSION: u16 = 8;
const MAGIC_TEXT: &[u8; 4] = b"Bdgr";
fn help_rewrite(
    dir: &PathBuf,
    manifest: &Manifest,
    ext_magic: u16,
) -> anyhow::Result<(File, usize)> {
    let rewrite_path = dir.join(MANIFEST_REWRITE_FILE_NAME);
    let mut fp = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&rewrite_path)?;

    // magic bytes are structured as
    // +---------------------+-------------------------+-----------------------+
    // | magicText (4 bytes) | externalMagic (2 bytes) | badgerMagic (2 bytes) |
    // +---------------------+-------------------------+-----------------------+

    let mut buf = BytesMut::with_capacity(8);
    buf.put(&MAGIC_TEXT[..]);
    buf.put_u16(ext_magic);
    buf.put_u16(BADGER_MAGIC_VERSION);

    let net_creations = manifest.tables.len();
    let changes = manifest.as_changes();
    let set = ManifestChangeSet { changes };
    let changebuf = set.encode_to_vec();

    let mut len_crc_buf = BytesMut::with_capacity(8);
    len_crc_buf.put_u32(changebuf.len() as u32);
    len_crc_buf.put_u32(crc32fast::hash(&changebuf));

    buf.extend_from_slice(&len_crc_buf);
    buf.extend_from_slice(&changebuf);
    fp.write_all(&buf)?;
    fp.sync_all()?;
    drop(fp);

    let manifest_path = dir.join(MANIFEST_FILE_NAME);
    rename(rewrite_path, &manifest_path)?;
    let mut fp = OpenOptions::new()
        .read(true)
        .write(true)
        .open(manifest_path)?;
    fp.seek(SeekFrom::End(0))?;
    sync_dir(dir)?;
    Ok((fp, net_creations))
}
impl Manifest {
    fn as_changes(&self) -> Vec<ManifestChange> {
        let mut changes = Vec::<ManifestChange>::with_capacity(self.tables.len());
        for (id, table_manifest) in self.tables.iter() {
            changes.push(ManifestChange::new_create_change(
                *id,
                table_manifest.level as u32,
                table_manifest.keyid,
                table_manifest.compression,
            ));
        }

        changes
        // ManifestChange::default();
    }
}
