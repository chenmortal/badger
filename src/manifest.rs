use std::{
    collections::{HashMap, HashSet},
    fs::{rename, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{anyhow, bail};
use bytes::{Buf, BufMut};
use parking_lot::Mutex;
use prost::Message;

use crate::{
    config::CompressionType,
    default::DEFAULT_DIR,
    errors::err_file,
    pb::badgerpb4::{manifest_change, ManifestChange, ManifestChangeSet},
    util::{sys::sync_dir, SSTableId}, key_registry::CipherKeyId,
};
const MANIFEST_FILE_NAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILE_NAME: &str = "MANIFEST-REWRITE";
const DELETIONS_REWRITE_THRESHOLD: usize = 10_000;
const DELETIONS_RATIO: usize = 10;
#[derive(Debug, Clone)]
pub(crate) struct Manifest(Arc<Mutex<ManifestInner>>);
impl Deref for Manifest {
    type Target = Arc<Mutex<ManifestInner>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
pub(crate) struct ManifestInner {
    file: File,
    config: ManifestConfig,
    deletions_rewrite_threshold: usize,
    info: ManifestInfo,
}
impl Deref for ManifestInner {
    type Target = ManifestInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}
impl DerefMut for ManifestInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.info
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct ManifestInfo {
    levels: Vec<LevelManifest>,
    pub(crate) tables: HashMap<SSTableId, TableManifest>,
    creations: usize,
    deletions: usize,
}
#[derive(Debug, Default, Clone)]
struct LevelManifest {
    tables: HashSet<SSTableId>, //Set of table id's
}
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct TableManifest {
    pub(crate) level: u8,
    pub(crate) keyid: CipherKeyId,
    pub(crate) compression: CompressionType,
}

#[derive(Debug, Clone)]
pub struct ManifestConfig {
    dir: PathBuf,
    read_only: bool,
    // Magic version used by the application using badger to ensure that it doesn't open the DB
    // with incompatible data format.
    external_magic_version: u16,
}
impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from(DEFAULT_DIR),
            read_only: false,
            external_magic_version: 0,
        }
    }
}
impl ManifestConfig {
    pub fn set_dir(&mut self, dir: PathBuf) {
        self.dir = dir;
    }
    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn set_external_magic_version(&mut self, external_magic_version: u16) {
        self.external_magic_version = external_magic_version;
    }
    pub(crate) fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }
    pub(crate) fn open(&self) -> anyhow::Result<Manifest> {
        let path = self.dir.join(MANIFEST_FILE_NAME);
        match OpenOptions::new()
            .read(true)
            .write(!self.read_only)
            .open(&path)
        {
            Ok(mut file) => {
                let (info, trunc_offset) =
                    replay_manifest_file(&file, self.external_magic_version)?;
                if !self.read_only {
                    file.set_len(trunc_offset)?;
                }
                file.seek(SeekFrom::End(0))?;
                let manifest = Manifest(
                    Mutex::new(ManifestInner {
                        file,
                        info,
                        deletions_rewrite_threshold: DELETIONS_REWRITE_THRESHOLD,
                        config: self.clone(),
                    })
                    .into(),
                );

                Ok(manifest)
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    if self.read_only {
                        bail!(err_file(
                            e,
                            &path,
                            "no manifest found, required for read-only db"
                        ));
                    }
                    let info = ManifestInfo::default();
                    let (file, table_creations) = self.help_rewrite(&info)?;
                    assert_eq!(table_creations, 0);
                    let manifest = Manifest(
                        Mutex::new(ManifestInner {
                            file,
                            info,
                            deletions_rewrite_threshold: DELETIONS_REWRITE_THRESHOLD,
                            config: self.clone(),
                        })
                        .into(),
                    );
                    Ok(manifest)
                }
                _ => bail!(e),
            },
        }
    }
    fn help_rewrite(&self, manifest: &ManifestInfo) -> anyhow::Result<(File, usize)> {
        let rewrite_path = self.dir.join(MANIFEST_REWRITE_FILE_NAME);
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

        let mut buf = Vec::with_capacity(8);
        buf.put(&MAGIC_TEXT[..]);
        buf.put_u16(self.external_magic_version);
        buf.put_u16(BADGER_MAGIC_VERSION);

        let table_creations = manifest.tables.len();
        let changes = manifest.as_changes();
        let set = ManifestChangeSet { changes };
        let change_set_buf = set.encode_to_vec();

        let mut len_crc_buf = Vec::with_capacity(8);
        len_crc_buf.put_u32(change_set_buf.len() as u32);
        len_crc_buf.put_u32(crc32fast::hash(&change_set_buf));

        buf.extend_from_slice(&len_crc_buf);
        buf.extend_from_slice(&change_set_buf);
        fp.write_all(&buf)?;
        fp.sync_all()?;
        drop(fp);

        let manifest_path = self.dir.join(MANIFEST_FILE_NAME);
        rename(rewrite_path, &manifest_path)?;
        let mut fp = OpenOptions::new()
            .read(true)
            .write(true)
            .open(manifest_path)?;
        fp.seek(SeekFrom::End(0))?;
        sync_dir(&self.dir)?;
        Ok((fp, table_creations))
    }
}

const BADGER_MAGIC_VERSION: u16 = 8;
const MAGIC_TEXT: &[u8; 4] = b"Bdgr";

fn replay_manifest_file(fp: &File, ext_magic: u16) -> anyhow::Result<(ManifestInfo, u64)> {
    let mut reader = BufReader::new(fp);
    let mut magic_buf = [0; 8];
    let mut offset: u64 = 0;
    offset += reader
        .read(&mut magic_buf)
        .map_err(|e| anyhow!("manifest has bad magic : {}", e))? as u64;
    if magic_buf[..4] != MAGIC_TEXT[..] {
        bail!("manifest has bad magic");
    }
    let mut buf = &magic_buf[4..];
    let ext_version = buf.get_u16();
    let version = buf.get_u16();
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

    let mut manifest = ManifestInfo::default();
    loop {
        let mut read_size = 0;
        let mut len_crc_buf = [0; 8];
        match reader.read_exact(len_crc_buf.as_mut()) {
            Ok(_) => {
                read_size += 8;
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => break,
                _ => bail!(e),
            },
        };

        let mut len_crc_buf_ref = len_crc_buf.as_ref();

        let change_len = len_crc_buf_ref.get_u32() as usize;
        let crc = len_crc_buf_ref.get_u32();
        if (offset + change_len as u64) > fp_szie {
            bail!("buffer len too greater, Manifest file might be corrupted");
        }

        let mut change_set_buf = vec![0 as u8; change_len];
        match reader.read_exact(&mut change_set_buf) {
            Ok(_) => {
                read_size += change_len;
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => break,
                _ => bail!(e),
            },
        };

        if crc32fast::hash(&change_set_buf) != crc {
            bail!("manifest has checksum mismatch");
        }
        offset += read_size as u64;
        let change_set = ManifestChangeSet::decode(change_set_buf.as_ref())?;
        manifest.apply_change_set(&change_set)?;
    }
    Ok((manifest, offset))
}
impl ManifestInfo {
    fn as_changes(&self) -> Vec<ManifestChange> {
        let mut changes = Vec::<ManifestChange>::with_capacity(self.tables.len());
        for (id, table_manifest) in self.tables.iter() {
            changes.push(ManifestChange::new(
                *id,
                table_manifest.level as u32,
                table_manifest.keyid,
                table_manifest.compression,
            ));
        }

        changes
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
                if self.tables.get(&change.table_id()).is_some() {
                    bail!("MANIFEST invalid, table {:?} exists", change.table_id());
                }
                self.tables.insert(
                    change.table_id(),
                    TableManifest {
                        level: change.level as u8,
                        keyid: change.key_id.into(),
                        compression: CompressionType::from(change.compression),
                    },
                );
                if self.levels.len() <= change.level as usize {
                    self.levels.push(LevelManifest::default());
                }
                self.levels[change.level as usize]
                    .tables
                    .insert(change.table_id());
                self.creations += 1;
            }
            manifest_change::Operation::Delete => {
                if self.tables.get(&change.table_id()).is_none() {
                    bail!("MANIFEST removes non-existing table {:?}", change.table_id());
                }
                self.levels[change.level as usize]
                    .tables
                    .remove(&change.table_id());
                self.tables.remove(&change.table_id());
                self.deletions += 1;
            }
        }
        Ok(())
    }
}
impl Manifest {
    pub(crate) fn push_changes(&self, changes: Vec<ManifestChange>) -> anyhow::Result<()> {
        let mut inner = self.lock();
        let change_set = ManifestChangeSet { changes };
        let serialize_buf = change_set.encode_to_vec();

        inner.apply_change_set(&change_set)?;

        if inner.deletions > inner.deletions_rewrite_threshold
            && inner.deletions > DELETIONS_RATIO * (inner.creations - inner.deletions)
        {
            let (file, table_creations) = inner.config.help_rewrite(&inner.info)?;
            inner.file = file;
            inner.creations = table_creations;
            inner.deletions = 0;
        } else {
            let mut buf = Vec::with_capacity(8 + serialize_buf.len());
            buf.put_u32(serialize_buf.len() as u32);
            buf.put_u32(crc32fast::hash(&serialize_buf));
            buf.put_slice(&serialize_buf);
            inner.file.write_all(&buf)?;
        };
        inner.file.sync_all()?;
        Ok(())
    }
}
