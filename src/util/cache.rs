use std::mem;

use bytes::{BufMut, Bytes};
use stretto::AsyncCache;

use crate::table::{TableIndexBuf, BlockIndex, Block};

use super::SSTableId;
#[derive(Debug, Clone, Copy)]
pub struct BlockCacheConfig {
    block_cache_size: usize,
    block_size: usize,
}
#[derive(Debug, Clone)]
pub(crate) struct BlockCache {
    cache: AsyncCache<Vec<u8>, Block>,
}
impl Default for BlockCacheConfig {
    fn default() -> Self {
        Self {
            block_cache_size: 256 << 20,
            block_size: 4 * 1024,
        }
    }
}
impl BlockCacheConfig {
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
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockCacheKey((SSTableId, BlockIndex));
impl From<(SSTableId, BlockIndex)> for BlockCacheKey {
    fn from(value: (SSTableId, BlockIndex)) -> Self {
        Self(value)
    }
}
impl BlockCacheKey {
    fn serialize(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(8);
        v.put_u32(self.0 .0.into());
        v.put_u32(self.0 .1.into());
        v
    }
}
impl BlockCache {
    pub(crate) async fn get(
        &self,
        key: BlockCacheKey,
    ) -> Option<stretto::ValueRef<'_, Block>> {
        self.cache.get(&key.serialize()).await
    }
    pub(crate) async fn insert(&self, key: BlockCacheKey, block: Block) -> bool {
        self.cache
            .insert(key.serialize(), block, mem::size_of::<Block>() as i64)
            .await
    }
}
#[derive(Debug, Clone, Copy)]
pub struct IndexCacheConfig {
    index_cache_size: usize,
    index_size: usize,
}
#[derive(Debug, Clone)]
pub(crate) struct IndexCache {
    cache: AsyncCache<SSTableId, TableIndexBuf>,
}
const DEFAULT_INDEX_SIZE: usize = ((64 << 20) as f64 * 0.05) as usize;
impl Default for IndexCacheConfig {
    fn default() -> Self {
        Self {
            index_cache_size: 16 << 20,
            index_size: DEFAULT_INDEX_SIZE,
        }
    }
}

impl IndexCacheConfig {
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
    pub(crate) fn build(&self) -> anyhow::Result<IndexCache> {
        let num_in_cache = (self.index_cache_size / self.index_size).max(1);
        let cache = stretto::AsyncCacheBuilder::new(num_in_cache * 8, self.index_cache_size as i64)
            .set_buffer_items(64)
            .set_metrics(true)
            .finalize(tokio::spawn)?;
        return Ok(IndexCache { cache });
    }
}
impl IndexCache {
    pub(crate) async fn get(
        &self,
        key: &SSTableId,
    ) -> Option<stretto::ValueRef<'_, TableIndexBuf>> {
        self.cache.get(key).await
    }
    pub(crate) async fn insert(&self, key: SSTableId, val: TableIndexBuf) -> bool {
        let cost = val.len() as i64;
        self.cache.insert(key, val, cost).await
    }
}
