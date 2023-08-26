#[inline]
pub(crate) fn to_u16(src:&[u8])->u16{
    assert_eq!(src.len(),2);
    let mut bytes=[0;2];
    bytes.copy_from_slice(src);
    u16::from_be_bytes(bytes)
}
#[inline]
pub(crate) fn to_u32(src:&[u8])->u32{
    assert_eq!(src.len(),4);
    let mut bytes=[0;4];
    bytes.copy_from_slice(src);
    u32::from_be_bytes(bytes)
}
