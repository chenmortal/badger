use std::fs::{read_dir, OpenOptions};

use crate::{
    db::NextId,
    default::{DEFAULT_PAGE_SIZE, MEM_FILE_EXT},
    errors::err_file,
    key_registry::KeyRegistry,
    kv::KeyTsBorrow,
    options::Options,
    skl::skip_list::{SkipList, SKL_MAX_NODE_SIZE},
    txn::TxnTs,
    util::{dir_join_id_suffix, parse_file_id},
};
use anyhow::Result;
use anyhow::{anyhow, bail};

use super::log_file::LogFile;
#[derive(Debug)]
pub(crate) struct MemTable {
    pub(super) skip_list: SkipList,
    pub(super) wal: LogFile,
    pub(super) max_version: TxnTs,
    pub(super) buf: Vec<u8>, // buf: BytesMut,
}

pub(crate) async fn open_mem_tables(
    key_registry: &KeyRegistry,
    next_mem_fid: &NextId,
) -> Result<()> {
    let dir = read_dir(Options::dir())
        .map_err(|err| err_file(err, Options::dir(), "Unable to open mem dir"))?;

    let mut mem_file_fids = dir
        .filter_map(|ele| ele.ok())
        .map(|e| e.path())
        .filter_map(|p| parse_file_id(&p, MEM_FILE_EXT))
        .collect::<Vec<_>>();
    mem_file_fids.sort();
    for fid in &mem_file_fids {
        let mut fp_open_opt: OpenOptions = OpenOptions::new();
        fp_open_opt.read(true).write(!Options::read_only());
        open_mem_table(key_registry, *fid as u32, fp_open_opt).await;
    }
    if mem_file_fids.len() != 0 {
        next_mem_fid.store(*mem_file_fids.last().unwrap() as u32);
    }
    next_mem_fid.add_next_id();
    Ok(())
}

async fn open_mem_table(
    key_registry: &KeyRegistry,
    mem_file_fid: u32,
    fp_open_opt: OpenOptions,
) -> anyhow::Result<(MemTable, bool)> {
    let mem_file_path = dir_join_id_suffix(Options::dir(), mem_file_fid as u64, MEM_FILE_EXT);

    let skip_list = SkipList::new(Options::arena_size(), KeyTsBorrow::cmp);

    let (log_file, is_new) = LogFile::open(
        mem_file_fid,
        &mem_file_path,
        fp_open_opt,
        2 * Options::memtable_size() as usize,
        key_registry.clone(),
    )
    .await
    .map_err(|e| anyhow!("While opening memtable: {:?} for {}", &mem_file_path, e))?;

    let mem_table = MemTable {
        skip_list,
        wal: log_file,
        max_version: TxnTs::default(),
        buf: Vec::with_capacity(DEFAULT_PAGE_SIZE.to_owned()),
    };
    if is_new {
        return Ok((mem_table, true));
    }

    Ok((mem_table, false))
}

pub(crate) async fn new_mem_table(
    key_registry: &KeyRegistry,
    next_mem_fid: &NextId,
) -> anyhow::Result<MemTable> {
    let mut open_opt = OpenOptions::new();
    open_opt.read(true).write(true).create(true);
    let mem_file_fid = next_mem_fid.get_next_id();
    let (memtable, is_new) = open_mem_table(key_registry, mem_file_fid, open_opt)
        .await
        .map_err(|e| anyhow!("Gor error: {} for id {}", e, mem_file_fid))?;
    if !is_new {
        bail!("File {:?} already exists", &memtable.wal.path());
    }
    Ok(memtable)
}

impl Options {
    fn arena_size() -> u32 {
        Options::memtable_size()
            + Options::max_batch_size()
            + Options::max_batch_count() * (SKL_MAX_NODE_SIZE)
    }
}

impl MemTable {
    #[inline]
    pub(crate) fn is_full(&self) -> bool {
        if self.skip_list.mem_size() >= Options::memtable_size() {
            return true;
        }
        self.wal.write_offset() as u32 >= Options::memtable_size()
    }

    // #[inline]
    // pub(crate) fn put(&mut self, entry: &Entry) {

    // }

    pub(crate) fn wal(&self) -> &LogFile {
        &self.wal
    }

    pub(crate) fn wal_mut(&mut self) -> &mut LogFile {
        &mut self.wal
    }
}
