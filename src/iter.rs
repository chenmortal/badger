use bytes::Buf;

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
    T: KvDoubleEndedSinkIter<V>,
    V: Into<ValueMeta>,
{
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        self.iter.key_back()
    }

    fn value(&self) -> Option<V> {
        self.iter.value_back()
    }
}
impl<T, V> KvDoubleEndedSinkIter<V> for SinkIterRev<T>
where
    T: KvSinkIter<V> + KvDoubleEndedSinkIter<V>,
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
// if true then KvSinkIter.key() >= k
pub(crate) trait KvSeekIter: SinkIterator {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> anyhow::Result<bool>;
}

pub(crate) struct TestIter {
    data: Option<[u8; 8]>,
    len: u64,
    back_data: Option<[u8; 8]>,
}
impl TestIter {
    pub(crate) fn new(len: usize) -> Self {
        Self {
            data: None,
            len: len as u64,
            back_data: None,
        }
    }
}
impl SinkIter for TestIter {
    type Item = [u8; 8];

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
                let now = (*d).as_ref().get_u64();
                if now + 1 == self.len {
                    return Ok(false);
                }
                if let Some(back_data) = self.back_data.as_ref() {
                    let mut b = back_data.as_ref();
                    let b = b.get_u64();
                    if now + 1 == b {
                        return Ok(false);
                    };
                };
                *d = (now + 1).to_be_bytes();
                return Ok(true);
            }
            _ => {}
        }
        self.data = 0u64.to_be_bytes().into();
        return Ok(true);
    }
}

impl DoubleEndedSinkIterator for TestIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        match self.back_data.as_mut() {
            Some(d) => {
                let now = (*d).as_ref().get_u64();
                if now == 0 {
                    return Ok(false);
                }
                if let Some(data) = self.data.as_ref() {
                    let mut b = data.as_ref();
                    let s = b.get_u64();
                    if now - 1 == s {
                        return Ok(false);
                    }
                }

                *d = (now - 1).to_be_bytes();
                return Ok(true);
            }
            _ => {}
        }
        self.back_data = (self.len - 1).to_be_bytes().into();
        return Ok(true);
    }
}
impl KvSinkIter<ValueMeta> for TestIter {
    fn key(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        if let Some(s) = self.item() {
            return Some(s.as_ref().into());
        }
        return None;
    }

    fn value(&self) -> Option<ValueMeta> {
        if let Some(s) = self.item().cloned() {
            let mut value = ValueMeta::default();
            value.set_value(s.to_vec().into());
            return Some(value);
        }
        return None;
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for TestIter {
    fn key_back(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        if let Some(s) = self.item_back() {
            return Some(s.as_ref().into());
        }
        return None;
    }

    fn value_back(&self) -> Option<ValueMeta> {
        if let Some(s) = self.item_back().cloned() {
            let mut value = ValueMeta::default();
            value.set_value(s.to_vec().into());
            return Some(value);
        }
        return None;
    }
}
impl KvSeekIter for TestIter {
    fn seek(&mut self, k: crate::kv::KeyTsBorrow<'_>) -> anyhow::Result<bool> {
        let key = k.as_ref().get_u64();
        if key >= self.len {
            return Ok(false);
        }
        self.data = Some(key.to_be_bytes());
        return Ok(true);
    }
}

#[test]
fn test_next() {
    let len = 3;
    let split = 2;
    let mut iter = TestIter::new(len);
    crate::test_iter_next!(iter, len);

    let iter = TestIter::new(len);
    crate::test_iter_rev_next!(iter, len);

    let iter = TestIter::new(len);
    crate::test_iter_rev_rev_next!(iter, len);

    let mut iter = TestIter::new(len);
    crate::test_iter_next_back!(iter, len);

    let iter = TestIter::new(len);
    crate::test_iter_rev_next_back!(iter, len);

    let mut iter = TestIter::new(len);
    crate::test_iter_double_ended!(iter, len, split);

    let iter = TestIter::new(len);
    crate::test_iter_rev_double_ended!(iter, len, split);
}
#[macro_export]
macro_rules! test_iter_next {
    ($iter:ident, $len:expr) => {
        let mut test_iter = TestIter::new($len);
        while $iter.next().unwrap() {
            assert!(test_iter.next().unwrap());
            assert_eq!($iter.key(), test_iter.key());
            assert_eq!($iter.value(), test_iter.value());
        }
    };
}
#[macro_export]
macro_rules! test_iter_rev_next {
    ($iter:ident, $len:expr) => {
        let mut iter = $iter.rev();
        let mut test_iter = TestIter::new($len).rev();
        while iter.next().unwrap() {
            assert!(test_iter.next().unwrap());
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
    };
}
#[macro_export]
macro_rules! test_iter_rev_rev_next {
    ($iter:expr, $len:expr) => {
        let mut iter = $iter.rev().rev();
        let mut test_iter = TestIter::new($len);
        while iter.next().unwrap() {
            assert!(test_iter.next().unwrap());
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
    };
}
#[macro_export]
macro_rules! test_iter_next_back {
    ($iter:expr, $len:expr) => {
        let mut test_iter = TestIter::new($len);
        while $iter.next_back().unwrap() {
            assert!(test_iter.next_back().unwrap());
            assert_eq!($iter.key_back(), test_iter.key_back());
            assert_eq!($iter.value_back(), test_iter.value_back());
        }
    };
}
#[macro_export]
macro_rules! test_iter_rev_next_back {
    ($iter:expr, $len:expr) => {
        let mut iter = $iter.rev();
        let mut test_iter = TestIter::new($len).rev();
        while iter.next_back().unwrap() {
            assert!(test_iter.next_back().unwrap());
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
    };
}
#[macro_export]
macro_rules! test_iter_double_ended {
    ($iter:expr, $len:expr,$split:expr) => {
        let mut test_iter = TestIter::new($len);
        for _ in 0..$split {
            assert!($iter.next().unwrap());
            assert!(test_iter.next().unwrap());
            assert_eq!($iter.key(), test_iter.key());
            assert_eq!($iter.value(), test_iter.value());
        }
        for _ in $split..$len {
            assert!($iter.next_back().unwrap());
            assert!(test_iter.next_back().unwrap());
            assert_eq!($iter.key_back(), test_iter.key_back());
            assert_eq!($iter.value_back(), test_iter.value_back());
        }
        assert!(!$iter.next().unwrap());
        assert!(!$iter.next_back().unwrap());
    };
}
#[macro_export]
macro_rules! test_iter_rev_double_ended {
    ($iter:expr, $len:expr,$split:expr) => {
        let mut iter = $iter.rev();
        let mut test_iter = TestIter::new($len).rev();
        for _ in 0..$split {
            assert!(iter.next().unwrap());
            assert!(test_iter.next().unwrap());
            assert_eq!(iter.key(), test_iter.key());
            assert_eq!(iter.value(), test_iter.value());
        }
        for _ in $split..$len {
            assert!(iter.next_back().unwrap());
            assert!(test_iter.next_back().unwrap());
            assert_eq!(iter.key_back(), test_iter.key_back());
            assert_eq!(iter.value_back(), test_iter.value_back());
        }
        assert!(!iter.next().unwrap());
        assert!(!iter.next_back().unwrap());
    };
}
