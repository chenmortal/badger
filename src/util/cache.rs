use crate::table::{Block, BlockIndex, TableIndexBuf};
use bytes::BufMut;

use super::SSTableId;
#[cfg(all(feature = "moka", feature = "async_cache"))]
use moka::future::Cache as AsyncCache;
#[cfg(all(feature = "moka", not(feature = "async_cache")))]
use moka::sync::Cache;
#[cfg(all(feature = "stretto", feature = "async_cache"))]
use stretto::AsyncCache;
#[cfg(all(feature = "stretto", not(feature = "async_cache")))]
use stretto::Cache;
#[derive(Debug, Clone, Copy)]
pub struct BlockCacheConfig {
    block_cache_size: usize,
    block_size: usize,
}
#[derive(Debug, Clone)]
pub(crate) struct BlockCache {
    #[cfg(feature = "async_cache")]
    cache: AsyncCache<BlockCacheKey, Block>,
    #[cfg(not(feature = "async_cache"))]
    cache: Cache<BlockCacheKey, Block>,
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
    #[cfg(feature = "stretto")]
    pub(crate) fn try_build(&self) -> anyhow::Result<Option<BlockCache>> {
        if self.block_cache_size > 0 {
            let num_in_cache = (self.block_cache_size / self.block_size).max(1);
            #[cfg(feature = "async_cache")]
            let cache =
                stretto::AsyncCacheBuilder::new(num_in_cache * 8, self.block_cache_size as i64)
                    .set_buffer_items(64)
                    .set_metrics(true)
                    .finalize(tokio::spawn)?;
            #[cfg(not(feature = "async_cache"))]
            let cache = stretto::CacheBuilder::new(num_in_cache * 8, self.block_cache_size as i64)
                .set_buffer_items(64)
                .set_metrics(true)
                .finalize()?;
            return Ok(BlockCache { cache }.into());
        }
        return Ok(None);
    }
    #[cfg(feature = "moka")]
    pub(crate) fn try_build(&self) -> anyhow::Result<Option<BlockCache>> {
        if self.block_cache_size > 0 {
            let num_in_cache = (self.block_cache_size / self.block_size).max(1) as u64;
            #[cfg(not(feature = "async_cache"))]
            let cache = moka::sync::CacheBuilder::new(num_in_cache)
                .initial_capacity(num_in_cache as usize / 2)
                .build();
            #[cfg(feature = "async_cache")]
            let cache = moka::future::CacheBuilder::new(num_in_cache)
                .initial_capacity(num_in_cache as usize / 2)
                .build();
            return Ok(BlockCache { cache }.into());
        }
        return Ok(None);
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    #[cfg(feature = "async_cache")]
    pub(crate) async fn get(&self, key: &BlockCacheKey) -> Option<Block> {
        #[cfg(feature = "stretto")]
        return self
            .cache
            .get(key)
            .await
            .and_then(|x| x.value().clone().into());
        #[cfg(feature = "moka")]
        return self.cache.get(key).await;
    }
    #[cfg(not(feature = "async_cache"))]
    pub(crate) fn get(&self, key: &BlockCacheKey) -> Option<Block> {
        #[cfg(feature = "stretto")]
        return self.cache.get(key).and_then(|x| x.value().clone().into());
        #[cfg(feature = "moka")]
        return self.cache.get(key);
    }
    #[cfg(feature = "async_cache")]
    pub(crate) async fn insert(&self, key: BlockCacheKey, block: Block) -> bool {
        #[cfg(feature = "stretto")]
        return self
            .cache
            .insert(key, block, mem::size_of::<Block>() as i64)
            .await;
        #[cfg(feature = "moka")]
        {
            self.cache.insert(key, block);
            true
        }
    }
    #[cfg(not(feature = "async_cache"))]
    pub(crate) fn insert(&self, key: BlockCacheKey, block: Block) -> bool {
        #[cfg(feature = "stretto")]
        return self
            .cache
            .insert(key, block, mem::size_of::<Block>() as i64);

        #[cfg(feature = "moka")]
        {
            self.cache.insert(key, block);
            return true;
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct IndexCacheConfig {
    index_cache_size: usize,
    index_size: usize,
}
#[derive(Debug, Clone)]
pub(crate) struct IndexCache {
    #[cfg(feature = "async_cache")]
    cache: AsyncCache<SSTableId, TableIndexBuf>,
    #[cfg(not(feature = "async_cache"))]
    cache: Cache<SSTableId, TableIndexBuf>,
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
    #[cfg(feature = "stretto")]
    pub(crate) fn build(&self) -> anyhow::Result<IndexCache> {
        let num_in_cache = (self.index_cache_size / self.index_size).max(1);
        #[cfg(feature = "async_cache")]
        let cache = stretto::AsyncCacheBuilder::new(num_in_cache * 8, self.index_cache_size as i64)
            .set_buffer_items(64)
            .set_metrics(true)
            .finalize(tokio::spawn)?;
        #[cfg(not(feature = "async_cache"))]
        let cache = stretto::CacheBuilder::new(num_in_cache * 8, self.index_cache_size as i64)
            .set_buffer_items(64)
            .set_metrics(true)
            .finalize()?;
        return Ok(IndexCache { cache });
    }
    #[cfg(feature = "moka")]
    pub(crate) fn build(&self) -> anyhow::Result<IndexCache> {
        let num_in_cache = (self.index_cache_size / self.index_size).max(1) as u64;
        #[cfg(not(feature = "async_cache"))]
        let cache = moka::sync::CacheBuilder::new(num_in_cache)
            .initial_capacity(num_in_cache as usize / 2)
            .build();
        #[cfg(feature = "async_cache")]
        let cache = moka::future::CacheBuilder::new(num_in_cache)
            .initial_capacity(num_in_cache as usize / 2)
            .build();
        return Ok(IndexCache { cache }.into());
    }
}
impl IndexCache {
    #[cfg(feature = "async_cache")]
    pub(crate) async fn get(&self, key: &SSTableId) -> Option<TableIndexBuf> {
        #[cfg(feature = "stretto")]
        return self
            .cache
            .get(key)
            .await
            .and_then(|x| x.value().clone().into());
        #[cfg(feature = "moka")]
        return self.cache.get(key).await;
    }
    #[cfg(not(feature = "async_cache"))]
    pub(crate) fn get(&self, key: &SSTableId) -> Option<TableIndexBuf> {
        #[cfg(feature = "stretto")]
        return self.cache.get(key).and_then(|x| x.value().clone().into());
        #[cfg(feature = "moka")]
        return self.cache.get(key);
    }
    #[cfg(feature = "async_cache")]
    pub(crate) async fn insert(&self, key: SSTableId, val: TableIndexBuf) -> bool {
        #[cfg(feature = "stretto")]
        {
            let cost = val.len() as i64;
            self.cache.insert(key, val, cost).await
        }
        #[cfg(feature = "moka")]
        {
            self.cache.insert(key, val);
            true
        }
    }
    #[cfg(not(feature = "async_cache"))]
    pub(crate) fn insert(&self, key: SSTableId, val: TableIndexBuf) -> bool {
        #[cfg(feature = "stretto")]
        {
            let cost = val.len() as i64;
            self.cache.insert(key, val, cost)
        }
        #[cfg(feature = "moka")]
        {
            self.cache.insert(key, val);
            true
        }
    }
}
