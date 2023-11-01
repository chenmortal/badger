use crate::kv::{KeyTsBorrow, ValueMeta};

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
    T: DoubleEndedSinkIter,
{
    type Item = <T as SinkIter>::Item;

    fn item(&self) -> Option<&Self::Item> {
        self.iter.item_back()
    }
}
impl<T> DoubleEndedSinkIter for SinkIterRev<T>
where
    T: SinkIter + DoubleEndedSinkIter,
{
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.iter.item()
    }
}
impl<T> AsyncSinkIterator for SinkIterRev<T>
where
    T: AsyncDoubleEndedSinkIterator,
{
    async fn next(&mut self) -> Result<(), anyhow::Error> {
        self.iter.next_back().await
    }
}
impl<T> SinkIterator for SinkIterRev<T>
where
    T: DoubleEndedSinkIterator,
{
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        self.iter.next_back()
    }
}
impl<T> DoubleEndedSinkIterator for SinkIterRev<T>
where
    T: DoubleEndedSinkIterator,
{
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
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
impl<'a, T, V> KvSinkIter<V> for SinkIterRev<T>
where
    T: KvDoubleEndedSinkIter< V>,
    V: Into<ValueMeta>,
{
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        self.iter.key_back()
    }


    fn value(&self) -> Option<V> {
        self.iter.value_back()
    }
}
impl<T,V> KvDoubleEndedSinkIter<V> for SinkIterRev<T>
where
    T: KvSinkIter< V> + KvDoubleEndedSinkIter<V>,
    V: Into<ValueMeta>,
{
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        self.iter.key()
    }

    fn value_back(&self) -> Option<V> {
        self.iter.value()
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
    async fn rev(self) -> SinkIterRev<Self>
    where
        Self: Sized + AsyncDoubleEndedSinkIterator,
    {
        SinkIterRev { iter: self }
    }
}
pub(crate) trait SinkIterator: SinkIter {
    fn next(&mut self) -> Result<bool, anyhow::Error>;
    fn rev(self) -> SinkIterRev<Self>
    where
        Self: Sized + DoubleEndedSinkIterator,
    {
        SinkIterRev { iter: self }
    }
}
pub(crate) trait DoubleEndedSinkIterator: SinkIterator + DoubleEndedSinkIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error>;
}
pub(crate) trait AsyncDoubleEndedSinkIterator:
    AsyncSinkIterator + DoubleEndedSinkIter
{
    async fn next_back(&mut self) -> Result<(), anyhow::Error>;
}
pub(crate) trait KvSinkIter<V>: SinkIter
where
    V: Into<ValueMeta>,
{
    fn key(&self) -> Option<KeyTsBorrow<'_>>;
    fn value(&self) -> Option<V>;
}
pub(crate) trait KvDoubleEndedSinkIter<V>: DoubleEndedSinkIter
where
    V: Into<ValueMeta>,
{
    fn key_back(&self) -> Option<KeyTsBorrow<'_>>;
    fn value_back(&self) -> Option<V>;
}
