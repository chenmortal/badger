use crate::{
    lsm::mmap::{open_mmap_file, MmapFile},
    options::Options,
};
use anyhow::anyhow;
use bytes::Buf;
use log::info;
use std::{fs::OpenOptions, sync::Arc};
use tokio::sync::Mutex;
const DISCARD_FILE_NAME: &str = "DISCARD";
const DISCARD_FILE_SIZE: usize = 1 << 20; //1MB
const DISCARD_MAX_SLOT: usize = DISCARD_FILE_SIZE / 16; //1MB file can store 65536 discard entries. Each entry is 16 bytes;
#[derive(Debug)]
pub(crate) struct DiscardStats(Arc<Mutex<DiscardStatsInner>>);
#[derive(Debug)]
struct DiscardStatsInner {
    mmap_f: MmapFile,
    next_empty_slot: usize,
}
impl DiscardStats {
    pub(crate) fn new(opt: Arc<Options>) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(DiscardStatsInner::new(opt)?))))
    }
}
impl DiscardStatsInner {
    fn new(opt: Arc<Options>) -> anyhow::Result<Self> {
        let file_path = opt.value_dir.join(DISCARD_FILE_NAME);
        let mut fp_open_opt = OpenOptions::new();
        fp_open_opt.read(true).write(true).create(true);

        let (mmap_f, is_new) = open_mmap_file(
            &file_path,
            fp_open_opt,
            opt.read_only,
            DISCARD_FILE_SIZE as u64,
        )
        .map_err(|e| anyhow!("while openint file: {} for {} \n", DISCARD_FILE_NAME, e))?;
        let mut discard_stats = Self {
            mmap_f,
            next_empty_slot: 0,
        };

        if is_new {
            discard_stats.zero_out();
        }
        for slot in 0..DISCARD_MAX_SLOT {
            if discard_stats.get(slot * 16) == 0 {
                discard_stats.next_empty_slot = slot;
                break;
            }
        }
        discard_stats.sort();
        info!(
            "Discard stats next_empty_slot:{} \n",
            discard_stats.next_empty_slot
        );
        Ok(discard_stats)
    }
    #[inline(always)]
    pub(crate) fn set(&mut self, offset: usize, val: u64) {
        let big_endian = val.to_be_bytes();
        self.mmap_f[offset..offset + 8].copy_from_slice(&big_endian);
    }
    #[inline(always)]
    pub(crate) fn get(&self, offset: usize) -> u64 {
        let mut p = &self.mmap_f[offset..offset + 8];
        // p.sort();
        p.get_u64()
    }
    #[inline]
    pub(crate) fn zero_out(&mut self) {
        let next_offset = self.next_empty_slot * 16;
        self.mmap_f[next_offset..next_offset + 16].fill(0);
    }
    #[inline]
    pub(crate) fn sort(&mut self) {
        let slice = &mut self.mmap_f[..self.next_empty_slot * 8 * 2];
        let chunks = unsafe { slice.as_chunks_unchecked_mut::<16>() };
        chunks.sort_unstable_by(|a, b| a.as_ref().get_u64().cmp(&b.as_ref().get_u64()));
    }
}
