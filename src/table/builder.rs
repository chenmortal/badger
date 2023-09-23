use bytes::{Buf, BufMut};
#[derive(Debug)]
pub(crate) struct Header {
    overlap: u16,
    diff: u16,
}
// Header + base_key (diff bytes)
pub(crate) const HEADER_SIZE: usize = 4;
impl Header {
    #[inline]
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(HEADER_SIZE);
        v.put_u16(self.overlap);
        v.put_u16(self.diff);
        v
    }
    #[inline]
    pub(crate) fn deserialize(mut data: &[u8]) -> Self {
        Header {
            overlap: data.get_u16(),
            diff: data.get_u16(),
        }
    }
    #[inline]
    pub(crate) fn get_diff(&self) -> usize {
        self.diff as usize
    }
    #[inline]
    pub(crate) fn get_overlap(&self) -> usize {
        self.overlap as usize
    }
}
