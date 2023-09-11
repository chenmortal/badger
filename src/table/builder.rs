use bytes::{Buf, BufMut};
#[derive(Debug)]
pub(crate) struct Header {
    overlap: u16,
    diff: u16,
}
// Header + base_key (diff bytes)
pub(crate) const HEADER_SIZE:usize=4;
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
    pub(crate) fn get_diff(&self)->usize{
        self.diff as usize
    }
    #[inline]
    pub(crate) fn get_overlap(&self)->usize{
        self.overlap as usize
    }
}
// #[test]
// fn test_a() {
//     let header = Header {
//         overlap: 1,
//         diff: 2,
//     };
//     // for _ in 0..10_000_000 {

//         let p = bincode::serialize(&header).unwrap();
//         dbg!(p.len());
//         let d = bincode::deserialize::<Header>(&p).unwrap();
//     // }
// }
#[test]
fn test_b() {
    let header = Header {
        overlap: 1,
        diff: 2,
    };
    // for _ in 0..10_000_000 {
    let mut v = Vec::with_capacity(4);
    v.put_u16(header.overlap);
    v.put_u16(header.diff);

    let mut p: &[u8] = v.as_ref();
    let header = Header {
        overlap: p.get_u16(),
        diff: p.get_u16(),
    };
    dbg!(header);
    // }
}
