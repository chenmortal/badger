use std::{
    fs::{read_dir, OpenOptions},
    path::PathBuf,
};

use crate::{
    db::DB,
    default::MEM_FILE_EXT,
    errors::err_file,
    options::Options,
    skl::skip_list::{SkipList, SKL_MAX_NODE_SIZE},
};
use anyhow::Result;
use bytes::BytesMut;

use super::wal::LogFile;
pub(crate) struct MemTable {
    skip_list: SkipList,
    opt: Options,
    max_version: usize,
    buf: BytesMut,
}
impl MemTable {
    fn new(skip_list: SkipList, opt: Options) -> Self {
        Self {
            skip_list,
            opt,
            max_version: 0,
            buf: BytesMut::with_capacity(4096),
        }
    }
}

impl DB {
    pub fn open_mem_tables(&self) -> Result<()> {
        let opt = &self.opt;
        let dir =
            read_dir(&opt.dir).map_err(|err| err_file(err, &opt.dir, "Unable to open mem dir"))?;

        let mut mem_file_fids = dir
            .filter_map(|ele| ele.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.ends_with(MEM_FILE_EXT))
            .filter_map(|p| {
                if let Some(stem) = p.file_stem() {
                    if let Some(s) = stem.to_str() {
                        if let Ok(id) = s.parse::<usize>() {
                            return Some(id);
                        };
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        mem_file_fids.sort();
        for fid in mem_file_fids {
            let mut fp_open_opt = OpenOptions::new();
            fp_open_opt.read(true).write(!self.opt.read_only);
            self.open_mem_table(fid as u32,fp_open_opt);
        }
        Ok(())
    }

    fn open_mem_table(&self, mem_file_fid: u32, fp_open_opt:OpenOptions) {
        let opt = &self.opt;
        let mem_file_path = self.join_file_ext(mem_file_fid);
        let skip_list = SkipList::new(opt.arena_size());
        // LogFile::new(mem_file_fid, mem_file_path, opt.clone(), self.key_registry.clone()); 

    }

    pub(crate) fn new_mem_table(&mut self) {
        let mut open_opt = OpenOptions::new();
        open_opt.read(true).write(true).create(true);
        let mem_file_fid = self.get_next_mem_fid();
        self.open_mem_table(mem_file_fid, open_opt);
    }

    #[inline]
    fn join_file_ext(&self, mem_fid: u32) -> PathBuf {
        self.opt.dir.join(format!("{:05}{}", mem_fid, MEM_FILE_EXT))
    }
}
impl Options {
    fn arena_size(&self) -> usize {
        self.memtable_size + self.max_batch_size + self.max_batch_count * (SKL_MAX_NODE_SIZE)
    }
}
