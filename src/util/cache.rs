use bytes::Bytes;
use stretto::AsyncCache;

use crate::table::block::{self, Block};
#[derive(Debug, Clone, Copy)]
pub struct BlockCacheBuilder {
    block_cache_size: usize,
    block_size: usize,
}
#[derive(Debug, Clone)]
pub(crate) struct BlockCache {
    cache: AsyncCache<Vec<u8>, block::Block>,
}
impl Default for BlockCacheBuilder {
    fn default() -> Self {
        Self {
            block_cache_size: 256 << 20,
            block_size: 4 * 1024,
        }
    }
}
impl BlockCacheBuilder {
    pub fn set_block_cache_size(&mut self, block_cache_size: usize) {
        self.block_cache_size = block_cache_size;
    }

    pub(crate) fn block_cache_size(&self) -> usize {
        self.block_cache_size
    }

    #[deny(unused)]
    pub(crate) fn set_block_size(&mut self, block_size: usize) {
        self.block_size = block_size;
    }

    pub(crate) fn try_build(&self) -> anyhow::Result<Option<BlockCache>> {
        if self.block_cache_size > 0 {
            let num_in_cache = (self.block_cache_size / self.block_size).max(1);
            let cache =
                stretto::AsyncCacheBuilder::new(num_in_cache * 8, self.block_cache_size as i64)
                    .set_buffer_items(64)
                    .set_metrics(true)
                    .finalize(tokio::spawn)?;
            return Ok(BlockCache { cache }.into());
        }
        return Ok(None);
    }
}
impl BlockCache {
    pub(crate) async fn get(&self, key: &[u8]) -> Option<stretto::ValueRef<'_, block::Block>> {
        self.cache.get(key).await
    }
    pub(crate) async fn insert(&self, key: Vec<u8>, block: Block, size: i64) -> bool {
        self.cache.insert(key, block, size).await
    }
}
#[derive(Debug, Clone, Copy)]
pub struct IndexCacheBuilder {
    index_cache_size: usize,
    index_size: usize,
}
#[derive(Debug, Clone)]
pub(crate) struct IndexCache {
    cache: AsyncCache<u64, Bytes>,
}
const DEFAULT_INDEX_SIZE: usize = ((64 << 20) as f64 * 0.05) as usize;
impl Default for IndexCacheBuilder {
    fn default() -> Self {
        Self {
            index_cache_size: 0,
            index_size: DEFAULT_INDEX_SIZE,
        }
    }
}

impl IndexCacheBuilder {
    pub fn set_index_cache_size(&mut self, index_cache_size: usize) {
        self.index_cache_size = index_cache_size;
    }

    pub fn set_index_size(&mut self, index_size: usize) {
        self.index_size = index_size;
    }
    #[deny(unused)]
    pub(crate) fn init(&mut self, memtable_size: usize) {
        if self.index_size == DEFAULT_INDEX_SIZE {
            self.index_size = (memtable_size as f64 * 0.05) as usize;
        }
    }
    pub(crate) fn try_build(&self) -> anyhow::Result<Option<IndexCache>> {
        if self.index_cache_size > 0 {
            let num_in_cache = (self.index_cache_size / self.index_size).max(1);
            let cache =
                stretto::AsyncCacheBuilder::new(num_in_cache * 8, self.index_cache_size as i64)
                    .set_buffer_items(64)
                    .set_metrics(true)
                    .finalize(tokio::spawn)?;
            return Ok(IndexCache { cache }.into());
        }
        Ok(None)
    }
}
