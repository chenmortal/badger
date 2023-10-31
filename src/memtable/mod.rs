pub(crate) mod read;
pub(crate) mod write;
use std::{
    collections::VecDeque,
    fs::{read_dir, OpenOptions},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use crate::{
    default::{DEFAULT_DIR, DEFAULT_PAGE_SIZE},
    key_registry::KeyRegistry,
    kv::{KeyTsBorrow, TxnTs},
    util::log_file::LogFile,
    util::{skip_list::SkipList, DBFileId, MemTableId},
};
use anyhow::bail;

#[derive(Debug)]
pub(crate) struct MemTable {
    pub(super) skip_list: SkipList,
    pub(super) wal: LogFile<MemTableId>,
    pub(super) max_version: TxnTs,
    pub(super) buf: Vec<u8>, // buf: BytesMut,
    memtable_size: usize,
    read_only: bool,
}
#[derive(Debug, Clone)]
pub struct MemTableBuilder {
    dir: PathBuf,
    read_only: bool,
    memtable_size: usize,
    arena_size: usize,
    // max_batch_size: usize,
    // max_batch_count: usize,
    num_memtables: usize,
    next_fid: Arc<AtomicU32>,
}
impl Default for MemTableBuilder {
    fn default() -> Self {
        let mem_table_builder = Self {
            dir: PathBuf::from(DEFAULT_DIR),
            read_only: false,
            memtable_size: 64 << 20,
            arena_size: 64 << 20,
            next_fid: Default::default(),
            num_memtables: 5,
        };
        mem_table_builder
    }
}

impl MemTableBuilder {
    pub(crate) async fn open_many(
        &self,
        key_registry: &KeyRegistry,
    ) -> anyhow::Result<VecDeque<Arc<MemTable>>> {
        let dir = read_dir(&self.dir)?;

        let mut fids = dir
            .filter_map(|ele| ele.ok())
            .map(|e| e.path())
            .filter_map(|p| MemTableId::parse(p).ok())
            .collect::<Vec<_>>();
        fids.sort();
        let mut immut_memtable = VecDeque::with_capacity(self.num_memtables);
        for fid in fids.iter() {
            let mut open_opt = OpenOptions::new();
            open_opt.read(true).write(!self.read_only);
            let (memtable, _) = self.open(key_registry, *fid, open_opt).await?;
            if memtable.skip_list.is_empty() {
                continue;
            }
            immut_memtable.push_back(Arc::new(memtable));
        }
        if fids.len() != 0 {
            self.next_fid
                .store((*fids.last().unwrap()).into(), Ordering::SeqCst)
        } else {
            self.next_fid.fetch_add(1, Ordering::SeqCst);
        }

        Ok(immut_memtable)
    }
    pub(crate) async fn open(
        &self,
        key_registry: &KeyRegistry,
        fid: MemTableId,
        open_opt: OpenOptions,
    ) -> anyhow::Result<(MemTable, bool)> {
        let mem_file_path = fid.join_dir(&self.dir);

        let skip_list = SkipList::new(self.arena_size, KeyTsBorrow::cmp);

        let (log_file, is_new) = LogFile::open(
            fid,
            &mem_file_path,
            open_opt,
            2 * self.memtable_size,
            key_registry.clone(),
        )
        .await?;

        let mut mem_table = MemTable {
            skip_list,
            wal: log_file,
            max_version: TxnTs::default(),
            buf: Vec::with_capacity(DEFAULT_PAGE_SIZE.to_owned()),
            memtable_size: self.memtable_size,
            read_only: self.read_only,
        };
        if is_new {
            return Ok((mem_table, true));
        }
        mem_table.reload()?;
        Ok((mem_table, false))
    }
    pub(crate) async fn new(&self, key_registry: &KeyRegistry) -> anyhow::Result<MemTable> {
        let mut open_opt = OpenOptions::new();
        open_opt.read(true).write(true).create(true);

        let fid = self.next_fid.fetch_add(1, Ordering::SeqCst).into();
        let (memtable, is_new) = self.open(key_registry, fid, open_opt).await?;
        if !is_new {
            bail!("File {:?} already exists", &memtable.wal.path());
        }
        Ok(memtable)
    }
    // fn arena_size(&self) -> usize {
    //     self.memtable_size + self.max_batch_size + self.max_batch_count * SKL_MAX_NODE_SIZE
    // }

    pub fn set_dir(&mut self, dir: PathBuf) {
        self.dir = dir;
    }

    pub(crate) fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn set_memtable_size(&mut self, memtable_size: usize) {
        self.memtable_size = memtable_size;
    }
    #[deny(unused)]
    pub(crate) fn set_arena_size(&mut self, arena_size: usize) {
        self.arena_size = arena_size;
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn memtable_size(&self) -> usize {
        self.memtable_size
    }

    pub fn set_num_memtables(&mut self, num_memtables: usize) {
        self.num_memtables = num_memtables;
    }
}

impl MemTable {
    #[inline]
    pub(crate) fn is_full(&self) -> bool {
        if self.skip_list.mem_size() >= self.memtable_size {
            return true;
        }
        self.wal.write_offset() >= self.memtable_size
    }

    pub(crate) fn wal(&self) -> &LogFile<MemTableId> {
        &self.wal
    }

    pub(crate) fn wal_mut(&mut self) -> &mut LogFile<MemTableId> {
        &mut self.wal
    }
}
