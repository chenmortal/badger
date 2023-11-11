#[cfg(test)]
mod test_table_iter {
    use tempfile::TempDir;

    use crate::{
        iter::{
            DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIter, SinkIterator, TestIter,
        },
        table::{write::TableBuilder, Table, TableConfig},
        test_iter_double_ended, test_iter_next, test_iter_next_back, test_iter_rev_double_ended,
        test_iter_rev_next, test_iter_rev_next_back, test_iter_rev_rev_next,
        util::{cache::IndexCacheConfig, DBFileId, SSTableId},
    };

    async fn generate_instance(tmp_dir: &TempDir, len: usize) -> anyhow::Result<Table> {
        let id: SSTableId = 0.into();
        let path = id.join_dir(tmp_dir.path());
        let iter = TestIter::new(len);
        let config = TableConfig::default();
        let mut table_builder = TableBuilder::build_l0_table(iter, vec![], config, None)?;
        let index_cache = IndexCacheConfig::default().build()?;
        let block_cache = None;
        Ok(table_builder.build(path, index_cache, block_cache).await?)
    }

    #[tokio::test]
    async fn test_next() -> anyhow::Result<()> {
        let len = 1_000_000;
        let tmp_dir = tempfile::tempdir()?;
        let table = generate_instance(&tmp_dir, len).await?;
        let mut iter = table.iter(false);
        test_iter_next!(iter, len);
        Ok(())
    }
    #[tokio::test]
    async fn test_next_back() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let len = 1_000_000;
        let table = generate_instance(&tmp_dir, len).await?;
        let mut iter = table.iter(false);
        test_iter_next_back!(iter, len);
        Ok(())
    }
    #[tokio::test]
    async fn test_double_ended() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let len = 1_000_000;
        let split = 500_000;
        let table = generate_instance(&tmp_dir, len).await?;
        let mut iter = table.iter(false);
        test_iter_double_ended!(iter, len, split);
        Ok(())
    }

    #[tokio::test]
    async fn test_rev_next() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let len = 1_000_000;
        let table = generate_instance(&tmp_dir, len).await?;
        let iter = table.iter(false);
        test_iter_rev_next!(iter, len);
        Ok(())
    }
    #[tokio::test]
    async fn test_rev_next_back() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let len = 1_000_000;
        let table = generate_instance(&tmp_dir, len).await?;
        let iter = table.iter(false);
        test_iter_rev_next_back!(iter, len);
        Ok(())
    }
    #[tokio::test]
    async fn test_rev_double_ended() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let len = 1_000_000;
        let split = 500_000;
        let table = generate_instance(&tmp_dir, len).await?;
        let iter = table.iter(false);
        test_iter_rev_double_ended!(iter, len, split);
        Ok(())
    }
    #[tokio::test]
    async fn test_rev_rev() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let len = 1_000_000;
        let table = generate_instance(&tmp_dir, len).await?;
        let iter = table.iter(false);
        test_iter_rev_rev_next!(iter, len);
        Ok(())
    }
}
#[cfg(test)]
mod test_block_iter {

    use crate::{
        iter::{
            DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIter, SinkIterator, TestIter,
        },
        table::{write::BlockBuilder, Block, BlockInner},
        test_iter_double_ended, test_iter_next, test_iter_next_back, test_iter_rev_double_ended,
        test_iter_rev_next, test_iter_rev_next_back, test_iter_rev_rev_next,
    };

    fn generate_instance(len: usize) -> Block {
        let mut block_builder = BlockBuilder::new(4096);
        let mut iter = TestIter::new(len);
        while iter.next().unwrap() {
            block_builder.push_entry(&iter.key().unwrap(), &iter.value().unwrap());
        }
        block_builder.finish_block(crate::pb::badgerpb4::checksum::Algorithm::Crc32c);
        let data = block_builder.data().to_vec();
        let block_inner = BlockInner::deserialize(0.into(), (0 as usize).into(), 0, data).unwrap();
        block_inner.verify().unwrap();
        block_inner.into()
    }
    #[test]
    fn test_next() {
        let len = 1000;
        let block = generate_instance(len);
        let mut iter = block.iter();
        test_iter_next!(iter, len);
    }
    #[test]
    fn test_next_back() {
        let len = 1000;
        let block = generate_instance(len);
        let mut iter = block.iter();
        test_iter_next_back!(iter, len);
    }
    #[test]
    fn test_double_ended() {
        let len = 1000;
        let split = 500;
        let block = generate_instance(len);
        let mut iter = block.iter();
        test_iter_double_ended!(iter, len, split);
    }
    #[test]
    fn test_rev_next() {
        let len = 1000;
        let block = generate_instance(len);
        let iter = block.iter();
        test_iter_rev_next!(iter, len);
    }
    #[test]
    fn test_rev_next_back() {
        let len = 1000;
        let block = generate_instance(len);
        let iter = block.iter();
        test_iter_rev_next_back!(iter, len);
    }
    #[test]
    fn test_rev_double_ended() {
        let len = 1000;
        let split = 500;
        let block = generate_instance(len);
        let iter = block.iter();
        test_iter_rev_double_ended!(iter, len, split);
    }

    #[test]
    fn test_rev_rev() {
        let len = 1000;
        let block = generate_instance(len);
        let iter = block.iter();
        test_iter_rev_rev_next!(iter, len);
    }
}
