use std::{
    fs::{read_dir, DirEntry, OpenOptions},
    path::PathBuf,
};

use crate::{db::DB, default::{MEM_FILE_EXT, SKL_MAX_NODE_SIZE}, errors::err_file, options::Options};
use anyhow::Result;
pub(crate) struct MemTable {
    // skip_list:
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
    fn open_mem_table(&self, mem_fid: u32, open_opt: OpenOptions) {
        let opt = &self.opt;
        let mem_file_path = self.join_file_ext(mem_fid);
        

    }
    pub(crate) fn new_mem_table(&self) {}
    #[inline]
    fn compute_arena_size(opt:&Options)->u64{
        opt.memtable_size+opt.max_batch_size+opt.max_batch_count*(SKL_MAX_NODE_SIZE as u64)
    }
    #[inline]
    fn join_file_ext(&self, mem_fid: u32) -> PathBuf {
        self.opt.dir.join(format!("{:05}{}", mem_fid, MEM_FILE_EXT))
    }
}
