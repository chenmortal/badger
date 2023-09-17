use std::future::Future;

// here use async fn look at https://blog.rust-lang.org/inside-rust/2022/11/17/async-fn-in-trait-nightly.html
pub(crate) trait Iter {
    async fn rewind(&mut self) -> Result<(), anyhow::Error>;
    // fn seek(key: &[u8]);
    fn get_key(&self) -> Option<&[u8]>;
    // fn valid() -> bool;
}
