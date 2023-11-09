use crate::{
    iter::{
        DoubleEndedSinkIter, DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIter, SinkIter,
        SinkIterator,
    },
    kv::ValueMeta,
};

struct TestIter {
    data: Option<String>,
    len: usize,
    back_data: Option<String>,
}
impl TestIter {
    fn new(len: usize) -> Self {
        Self {
            data: None,
            len,
            back_data: None,
        }
    }
}
impl SinkIter for TestIter {
    type Item = String;

    fn item(&self) -> Option<&Self::Item> {
        self.data.as_ref()
    }
}
impl DoubleEndedSinkIter for TestIter {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.back_data.as_ref()
    }
}
impl SinkIterator for TestIter {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        match self.data.as_mut() {
            Some(d) => {
                let now = d.parse::<usize>().unwrap();
                let next = (now + 1).to_string();
                if now + 1 == self.len || Some(&next) == self.back_data.as_ref() {
                    return Ok(false);
                } else {
                    *d = next;
                    return Ok(true);
                }
            }
            _ => {}
        }
        self.data = Some(0.to_string());
        return Ok(true);
    }
}
impl DoubleEndedSinkIterator for TestIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        match self.back_data.as_mut() {
            Some(d) => {
                let now = d.parse::<usize>().unwrap();
                let back = (now - 1).to_string();
                if now == 0 || Some(&back) == self.data.as_ref() {
                    return Ok(false);
                } else {
                    *d = back;
                    return Ok(true);
                }
            }
            _ => {}
        }
        self.back_data = Some((self.len - 1).to_string());
        return Ok(true);
    }
}
impl KvSinkIter<ValueMeta> for TestIter {
    fn key(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        if let Some(s) = self.item() {
            return Some(s.as_bytes().into());
        }
        return None;
    }

    fn value(&self) -> Option<ValueMeta> {
        if let Some(s) = self.item().cloned() {
            let mut value = ValueMeta::default();
            value.set_value(s.into());
            return Some(value);
        }
        return None;
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for TestIter {
    fn key_back(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        if let Some(s) = self.item_back() {
            return Some(s.as_bytes().into());
        }
        return None;
    }

    fn value_back(&self) -> Option<ValueMeta> {
        if let Some(s) = self.item_back().cloned() {
            let mut value = ValueMeta::default();
            value.set_value(s.into());
            return Some(value);
        }
        return None;
    }
}
#[cfg(test)]
mod test_table_iter {
    use std::path::PathBuf;

    use crate::{
        iter::{DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIter, SinkIterator},
        table::{write::TableBuilder, Table, TableConfig},
        util::{cache::IndexCacheConfig, DBFileId, SSTableId},
    };

    use super::TestIter;

    async fn generate_instance(path: PathBuf, len: usize) -> anyhow::Result<Table> {
        let iter = TestIter::new(len);
        let config = TableConfig::default();
        let mut table_builder = TableBuilder::build_l0_table(iter, vec![], config, None)?;
        let index_cache = IndexCacheConfig::default().build()?;
        let block_cache = None;
        Ok(table_builder.build(path, index_cache, block_cache).await?)
    }
    #[tokio::test]
    async fn test_next() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let id: SSTableId = 0.into();
        let len = 1_000_000;
        let path = id.join_dir(tmp_dir.path());
        let table = generate_instance(path, len).await?;
        let mut iter = table.iter(false);
        let mut test_iter = TestIter::new(len);
        while iter.next()? {
            assert!(test_iter.next()?);
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
        tmp_dir.close()?;
        Ok(())
    }
    #[tokio::test]
    async fn test_next_back() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let id: SSTableId = 1.into();
        let len = 1_000_000;
        let path = id.join_dir(tmp_dir.path());
        let table = generate_instance(path, len).await?;
        let mut iter = table.iter(false);
        let mut test_iter = TestIter::new(len);
        while iter.next_back()? {
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
        tmp_dir.close()?;
        Ok(())
    }
    #[tokio::test]
    async fn test_double_ended() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let id: SSTableId = 1.into();
        let len = 1_000_000;
        let split = 500_000;
        let path = id.join_dir(tmp_dir.path());
        let table = generate_instance(path, len).await?;
        let mut iter = table.iter(false);
        let mut test_iter = TestIter::new(len);
        for _ in 0..split {
            assert!(iter.next()?);
            assert!(test_iter.next()?);
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
        for _ in split..len {
            assert!(iter.next_back()?);
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
        assert!(!iter.next()?);
        assert!(!iter.next_back()?);
        tmp_dir.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_rev_next() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let id: SSTableId = 0.into();
        let len = 1_000_000;
        let path = id.join_dir(tmp_dir.path());
        let table = generate_instance(path, len).await?;
        let mut iter = table.iter(false).rev();
        let mut test_iter = TestIter::new(len);
        while iter.next()? {
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key(), test_iter.key_back());
            assert_eq!(iter.value(), test_iter.value_back());
        }
        tmp_dir.close()?;
        Ok(())
    }
    #[tokio::test]
    async fn test_rev_next_back() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let id: SSTableId = 0.into();
        let len = 1_000_000;
        let path = id.join_dir(tmp_dir.path());
        let table = generate_instance(path, len).await?;
        let mut iter = table.iter(false).rev();
        let mut test_iter = TestIter::new(len);
        while iter.next_back()? {
            assert!(test_iter.next()?);
            assert_eq!(iter.key_back(), test_iter.key());
            assert_eq!(iter.value_back(), test_iter.value());
        }
        tmp_dir.close()?;
        Ok(())
    }
    #[tokio::test]
    async fn test_rev_double_ended() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let id: SSTableId = 1.into();
        let len = 1_000_000;
        let split = 500_000;
        let path = id.join_dir(tmp_dir.path());
        let table = generate_instance(path, len).await?;
        let mut iter = table.iter(false).rev();
        let mut test_iter = TestIter::new(len);
        for _ in 0..split {
            assert!(iter.next()?);
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key(), test_iter.key_back());
            assert_eq!(iter.value(), test_iter.value_back());
        }
        for _ in split..len {
            assert!(iter.next_back()?);
            assert!(test_iter.next()?);
            assert_eq!(iter.key_back(), test_iter.key());
            assert_eq!(iter.value_back(), test_iter.value());
        }
        assert!(!iter.next()?);
        assert!(!iter.next_back()?);
        tmp_dir.close()?;
        Ok(())
    }
    #[tokio::test]
    async fn test_rev_rev() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let id: SSTableId = 0.into();
        let len = 1_000_000;
        let path = id.join_dir(tmp_dir.path());
        let table = generate_instance(path, len).await?;
        let mut iter = table.iter(false).rev().rev();
        let mut test_iter = TestIter::new(len);
        while iter.next()? {
            assert!(test_iter.next()?);
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
        tmp_dir.close()?;
        Ok(())
    }
}
#[cfg(test)]
mod test_block_iter {
    use crate::{
        iter::{DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIter, SinkIterator},
        table::{write::BlockBuilder, Block, BlockInner},
    };

    use super::TestIter;

    fn generate_instance(len: usize) -> anyhow::Result<Block> {
        let mut block_builder = BlockBuilder::new(4096);
        let mut iter = TestIter::new(len);
        while iter.next()? {
            block_builder.push_entry(&iter.key().unwrap(), &iter.value().unwrap());
        }
        block_builder.finish_block(crate::pb::badgerpb4::checksum::Algorithm::Crc32c);
        let data = block_builder.data().to_vec();
        let block_inner = BlockInner::deserialize(0.into(), (0 as usize).into(), 0, data)?;
        block_inner.verify()?;
        Ok(block_inner.into())
    }
    #[test]
    fn test_next() -> anyhow::Result<()> {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut iter = block.iter();
        let mut test_iter = TestIter::new(len);
        while iter.next()? {
            assert!(test_iter.next()?);
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), iter.value());
        }
        Ok(())
    }
    #[test]
    fn test_next_back() -> anyhow::Result<()> {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut iter = block.iter();
        let mut test_iter = TestIter::new(len);
        while iter.next_back()? {
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
        Ok(())
    }
    #[test]
    fn test_double_ended() -> anyhow::Result<()> {
        let len = 1000;
        let split = 500;
        let block = generate_instance(len).unwrap();
        let mut iter = block.iter();
        let mut test_iter = TestIter::new(len);
        for _ in 0..split {
            assert!(iter.next()?);
            assert!(test_iter.next()?);
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
        for _ in split..len {
            assert!(iter.next_back()?);
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
        assert!(!iter.next()?);
        assert!(!iter.next_back()?);
        Ok(())
    }
    #[test]
    fn test_rev_next() -> anyhow::Result<()> {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut iter = block.iter().rev();
        let mut test_iter = TestIter::new(len);
        while iter.next()? {
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key(), test_iter.key_back());
            assert_eq!(iter.value(), test_iter.value_back());
        }
        Ok(())
    }
    #[test]
    fn test_rev_next_back() -> anyhow::Result<()> {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut iter = block.iter().rev();
        let mut test_iter = TestIter::new(len);
        while iter.next_back()? {
            assert!(test_iter.next()?);
            assert_eq!(iter.key_back(), test_iter.key());
            assert_eq!(iter.value_back(), test_iter.value());
        }
        Ok(())
    }
    #[test]
    fn test_rev_double_ended() -> anyhow::Result<()> {
        let len = 1000;
        let split = 500;
        let block = generate_instance(len).unwrap();
        let mut iter = block.iter().rev();

        let mut test_iter = TestIter::new(len);
        for _ in 0..split {
            assert!(iter.next()?);
            assert!(test_iter.next_back()?);
            assert_eq!(iter.key(), test_iter.key_back());
            assert_eq!(iter.value(), test_iter.value_back());
        }
        for _ in split..len {
            assert!(iter.next_back()?);
            assert!(test_iter.next()?);
            assert_eq!(iter.key_back(), test_iter.key());
            assert_eq!(iter.value_back(), test_iter.value());
        }
        assert!(!iter.next()?);
        assert!(!iter.next_back()?);
        Ok(())
    }

    #[test]
    fn test_rev_rev() -> anyhow::Result<()> {
        let len = 1000;
        let block = generate_instance(len).unwrap();
        let mut iter = block.iter().rev().rev();
        let mut test_iter = TestIter::new(len);
        while iter.next()? {
            assert!(test_iter.next()?);
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
        Ok(())
    }
}
