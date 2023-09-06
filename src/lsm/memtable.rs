use std::{
    fs::{read_dir, OpenOptions},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    db::DB,
    default::MEM_FILE_EXT,
    errors::err_file,
    options::Options,
    skl::skip_list::{SkipList, SKL_MAX_NODE_SIZE},
    util::{dir_join_id_suffix, parse_file_id},
};
use anyhow::Result;
use anyhow::{anyhow, bail};
use bytes::BytesMut;

use super::wal::LogFile;
#[derive(Debug)]
pub(crate) struct MemTable {
    skip_list: SkipList,
    wal: LogFile,
    opt: Arc<Options>,
    max_version: usize,
    buf: BytesMut,
}

impl DB {
    pub fn open_mem_tables(&self) -> Result<()> {
        let opt = &self.opt;
        let dir =
            read_dir(&opt.dir).map_err(|err| err_file(err, &opt.dir, "Unable to open mem dir"))?;

        let mut mem_file_fids = dir
            .filter_map(|ele| ele.ok())
            .map(|e| e.path())
            .filter_map(|p| parse_file_id(p, MEM_FILE_EXT))
            .collect::<Vec<_>>();
        mem_file_fids.sort();
        for fid in mem_file_fids {
            let mut fp_open_opt = OpenOptions::new();
            fp_open_opt.read(true).write(!self.opt.read_only);
            self.open_mem_table(fid as u32, fp_open_opt);
        }
        Ok(())
    }

    async fn open_mem_table(
        &self,
        mem_file_fid: u32,
        fp_open_opt: OpenOptions,
    ) -> anyhow::Result<(MemTable, bool)> {
        let opt = &self.opt;
        
        let mem_file_path = dir_join_id_suffix(&opt.dir, mem_file_fid, MEM_FILE_EXT);

        let skip_list = SkipList::new(opt.arena_size());

        let (log_file, is_new) = LogFile::open(
            mem_file_fid,
            mem_file_path.clone(),
            opt.read_only,
            fp_open_opt,
            2 * opt.memtable_size as u64,
            opt.clone(),
            self.key_registry.clone(),
        )
        .await
        .map_err(|e| anyhow!("While opening memtable: {:?} for {}", &mem_file_path, e))?;

        let mem_table = MemTable {
            skip_list,
            wal: log_file,
            opt: self.opt.clone(),
            max_version: 0,
            buf: BytesMut::new(),
        };
        if is_new {
            return Ok((mem_table, true));
        }

        Ok((mem_table, false))
        // LogFile::new(mem_file_fid, mem_file_path, opt.clone(), self.key_registry.clone());
    }

    pub(crate) async fn new_mem_table(&mut self) -> anyhow::Result<MemTable> {
        let mut open_opt = OpenOptions::new();
        open_opt.read(true).write(true).create(true);
        let mem_file_fid = self.get_next_mem_fid();
        let (memtable, is_new) = self
            .open_mem_table(mem_file_fid, open_opt)
            .await
            .map_err(|e| anyhow!("Gor error: {} for id {}", e, mem_file_fid))?;
        if !is_new {
            bail!("File {:?} already exists", &memtable.wal.mmap.file_path);
        }
        Ok(memtable)
    }

    // #[inline]
    // fn join_file_ext(&self, mem_fid: u32) -> PathBuf {
    //     self.opt.dir.join(format!("{:05}{}", mem_fid, MEM_FILE_EXT))
    // }
}
impl Options {
    fn arena_size(&self) -> usize {
        self.memtable_size + self.max_batch_size + self.max_batch_count * (SKL_MAX_NODE_SIZE)
    }
}
