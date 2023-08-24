use std::{
    collections::{HashMap, HashSet},
    fs::{rename, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use log::error;
use prost::Message;

use crate::{
    byte_util::{to_u16, to_u32},
    default::{MANIFEST_FILE_NAME, MANIFEST_REWRITE_FILE_NAME},
    options::{CompressionType, Options},
    pb::badgerpb4::{manifest_change, ManifestChange, ManifestChangeSet},
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
    file_path: File,
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
                    assert_eq!(net_creations, 0);
                    ManifestFile {
                        file_path,
                        dir: opt.dir.clone(),
                        external_magic: opt.external_magic_version,
                        manifest: Arc::new(Mutex::new(manifest.clone())),
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
fn replay_manifest_file(fp: &File, ext_magic: u16) -> anyhow::Result<()> {
    let mut reader = BufReader::new(fp);
    let mut magic_buf = [0; 8];
    let mut offset = 0;
    offset += reader
        .read(&mut magic_buf)
        .map_err(|e| anyhow!("manifest has bad magic : {}", e))?;
    if magic_buf[..4] != MAGIC_TEXT[..] {
        bail!("manifest has bad magic");
    }

    let ext_version = to_u16(&magic_buf[4..6]);

    let version = to_u16(&magic_buf[6..8]);
    if version != BADGER_MAGIC_VERSION {
        bail!(
            "manifest has upsupported version: {} (we support {} )",
            version,
            BADGER_MAGIC_VERSION
        );
    }
    if ext_version != ext_magic {
        bail!("cannot open db because the external magic number doesn't match. Expected: {}, version present in manifest: {}",ext_magic,ext_version);
    }
    let fp_szie = fp.metadata()?.len();
    // meta.len();
    let mut manifest = Manifest::default();
    loop {
        let mut len_crc_buf=[0;8];
        match reader.read(&mut len_crc_buf) {
            Ok(size) => {
                offset+=size;
                if size!=8{
                    break;
                }
            },
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => bail!(e),
                }
            },
        };
        let len = to_u32(&len_crc_buf[..4]);
        if (offset + len as usize) as u64 > fp_szie{
            bail!("buffer len too greater, might be corrupted");
        }
        let changebuf=[0;]

        // let len_crc_buf=
    }
    Ok(())
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
    fn apply_change_set(&mut self, change_set: &ManifestChangeSet) -> anyhow::Result<()> {
        for change in change_set.changes.iter() {
            self.apply_manifest_change(change)?;
        }
        Ok(())
    }
    fn apply_manifest_change(&mut self, change: &ManifestChange) -> anyhow::Result<()> {
        match change.op() {
            manifest_change::Operation::Create => {
                if self.tables.get(&change.id).is_some() {
                    bail!("MANIFEST invalid, table {} exists", change.id);
                }
                self.tables.insert(
                    change.id,
                    TableManifest {
                        level: change.level as u8,
                        keyid: change.key_id,
                        compression: CompressionType::from(change.compression),
                    },
                );
                if self.levels.len() <= change.level as usize {
                    self.levels.push(LevelManifest::default());
                }
                self.levels[change.level as usize].tables.insert(change.id);
                self.creations += 1;
            }
            manifest_change::Operation::Delete => {
                if self.tables.get(&change.id).is_none() {
                    bail!("MANIFEST removes non-existing table {}", change.id);
                }
                self.levels[change.level as usize].tables.remove(&change.id);
                self.tables.remove(&change.id);
                self.deletions += 1;
            }
        }
        Ok(())
    }
}
impl Clone for Manifest {
    fn clone(&self) -> Self {
        let change_set = ManifestChangeSet {
            changes: self.as_changes(),
        };
        let mut m = Manifest::default();
        match m.apply_change_set(&change_set) {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
            }
        };
        m
    }
}
