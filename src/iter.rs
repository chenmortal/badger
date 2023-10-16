// here use async fn look at https://blog.rust-lang.org/inside-rust/2022/11/17/async-fn-in-trait-nightly.html
pub(crate) trait Iter {
    async fn rewind(&mut self) -> Result<(), anyhow::Error>;
    fn next(&self) {}
    fn get_key(&self) -> Option<&[u8]>;
}
pub(crate) struct SinkIterRev<T> {
    iter: T,
}
impl<T> SinkIter for SinkIterRev<T>
where
    T: SinkIter,
{
    type Item = <T as SinkIter>::Item;

    fn item(&self) -> Option<&Self::Item> {
        self.iter.item()
    }
}
impl<T> DoubleEndedSinkIter for SinkIterRev<T>
where
    T: DoubleEndedSinkIter,
{
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.iter.item_back()
    }
}
impl<T> AsyncSinkIterator for SinkIterRev<T>
where
    T: AsyncDoubleEndedSinkIterator,
{
    async fn next(&mut self) -> Result<(), anyhow::Error> {
        self.iter.next_back().await
    }
    async fn seek_to_last(&mut self) -> Result<(), anyhow::Error> {
        self.iter.seek_to_first().await
    }

    async fn seek_to_first(&mut self) -> Result<(), anyhow::Error> {
        self.iter.seek_to_last().await
    }
}
impl<T> SinkIterator for SinkIterRev<T>
where
    T: DoubleEndedSinkIterator,
{
    fn next(&mut self) -> Result<(), anyhow::Error> {
        self.iter.next_back()
    }

    fn seek_to_first(&mut self) -> Result<(), anyhow::Error> {
        self.iter.seek_to_last()
    }

    fn seek_to_last(&mut self) -> Result<(), anyhow::Error> {
        self.iter.seek_to_first()
    }
}
impl<T> DoubleEndedSinkIterator for SinkIterRev<T>
where
    T: DoubleEndedSinkIterator,
{
    fn next_back(&mut self) -> Result<(), anyhow::Error> {
        self.iter.next()
    }
}
impl<T> AsyncDoubleEndedSinkIterator for SinkIterRev<T>
where
    T: AsyncDoubleEndedSinkIterator,
{
    async fn next_back(&mut self) -> Result<(), anyhow::Error> {
        self.iter.next().await
    }
}

pub(crate) trait SinkIter {
    type Item;
    fn item(&self) -> Option<&Self::Item>;
}
pub(crate) trait DoubleEndedSinkIter: SinkIter {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item>;
}
pub(crate) trait AsyncSinkIterator: SinkIter {
    async fn next(&mut self) -> Result<(), anyhow::Error>;
    async fn seek_to_first(&mut self) -> Result<(), anyhow::Error>;
    async fn seek_to_last(&mut self) -> Result<(), anyhow::Error>;
    async fn rev(mut self) -> Result<SinkIterRev<Self>, anyhow::Error>
    where
        Self: Sized,
    {
        self.seek_to_last().await?;
        Ok(SinkIterRev { iter: self })
    }
}
pub(crate) trait SinkIterator: SinkIter {
    fn next(&mut self) -> Result<(), anyhow::Error>;
    fn seek_to_first(&mut self) -> Result<(), anyhow::Error>;
    fn seek_to_last(&mut self) -> Result<(), anyhow::Error>;
    fn rev(mut self) -> Result<SinkIterRev<Self>, anyhow::Error>
    where
        Self: Sized + DoubleEndedSinkIterator,
    {
        self.seek_to_last()?;
        Ok(SinkIterRev { iter: self })
    }
}
pub(crate) trait DoubleEndedSinkIterator: SinkIterator + DoubleEndedSinkIter {
    fn next_back(&mut self) -> Result<(), anyhow::Error>;
}
pub(crate) trait AsyncDoubleEndedSinkIterator:
    AsyncSinkIterator + DoubleEndedSinkIter
{
    async fn next_back(&mut self) -> Result<(), anyhow::Error>;
}
pub(crate) trait KvSinkIterator: SinkIter {
    type Key;
    type Value;
    fn key_ref(&self) -> Option<&Self::Key> {
        if let Some(item) = self.item() {
            return Self::parse_key(item);
        } else {
            None
        }
    }
    fn value_ref(&self) -> Option<&Self::Value> {
        if let Some(item) = self.item() {
            return Self::parse_value(item);
        } else {
            None
        }
    }
    fn parse_key(item: &<Self as SinkIter>::Item) -> Option<&Self::Key>;
    fn parse_value(item: &<Self as SinkIter>::Item) -> Option<&Self::Value>;
}
