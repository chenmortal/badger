use std::{
    fs::{read_dir, DirEntry, OpenOptions},
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
pub(crate) struct MemTable {
    skip_list:SkipList,
    opt:Options,
    max_version:usize,
    buf:BytesMut,
    // wal:
}
impl MemTable {
    fn new(skip_list:SkipList,opt:Options)->Self{
        Self{
            skip_list,
            opt,
            max_version: 0,
            buf: BytesMut::with_capacity(4096),
        }
    }
}
struct LogFile{
    fid:u32,
    file_path:PathBuf,
    opt:Options,

}
impl LogFile {
    fn open(file_path:PathBuf,open_opt:OpenOptions,fsize:u64){

    }
}

impl DB {
    pub fn open_mem_tables(&self) -> Result<()> {
        let opt = &self.opt;
        let dir =
            read_dir(&opt.dir).map_err(|err| err_file(err, &opt.dir, "Unable to open mem dir"))?;

        let mut mem_files = dir
            .filter_map(|ele| ele.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.ends_with(MEM_FILE_EXT))
            .collect::<Vec<_>>();
        mem_files.sort();
        for file_path in mem_files {
            let file_handle = OpenOptions::new()
                .read(true)
                .write(!opt.read_only)
                .open(file_path)?;
        }

        Ok(())
    }
    fn open_mem_table(&self, mem_file_fid: u32,read_only:bool,) {
        let opt = &self.opt;
        let mem_file_path = self.join_file_ext(mem_file_fid);
        let skip_list = SkipList::new(opt.arena_size());

    }
    pub(crate) fn new_mem_table(&mut self) {
        let mut open_opt = OpenOptions::new();
        open_opt.read(true).write(true).create(true);
        // open_opt.write(true);
        // open_opt.create(true);
        let mem_file_fid = self.get_next_mem_fid();
        // self.open_mem_table(mem_file_fid, open_opt);
    }
    #[inline]
    // fn compute_arena_size(opt:&Options)->u64{
    // opt.memtable_size+opt.max_batch_size+opt.max_batch_count*(SKL_MAX_NODE_SIZE as u64)
    // }
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
