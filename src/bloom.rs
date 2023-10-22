//copy from LevelDB-Go
pub(crate) struct Bloom;
impl Bloom {
    pub(crate) fn hash(mut bytes: &[u8]) -> u32 {
        const SEED: u32 = 0xbc9f1d34;
        const M: u32 = 0xc6a4a793;
        let mut hash = SEED ^ unsafe { (bytes.len() as u32).unchecked_mul(M) };
        while bytes.len() >= 4 {
            hash = unsafe {
                hash.unchecked_add(
                    bytes[0] as u32
                        | (bytes[1] as u32) << 8
                        | (bytes[2] as u32) << 16
                        | (bytes[3] as u32) << 24,
                )
            };
            hash = unsafe { hash.unchecked_mul(M) };
            hash ^= hash >> 16;
            bytes = &bytes[4..];
        }
        let len = bytes.len();
        if len == 3 {
            hash = unsafe {
                hash.unchecked_add(
                    (bytes[0] as u32) | (bytes[1] as u32) << 8 | (bytes[2] as u32) << 16,
                )
            }
        } else if len == 2 {
            hash = unsafe { hash.unchecked_add((bytes[0] as u32) | (bytes[1] as u32) << 8) }
        } else if len == 1 {
            hash = unsafe { hash.unchecked_add(bytes[0] as u32) }
        };
        if len != 0 {
            hash = unsafe { hash.unchecked_mul(M) };
            hash ^= hash >> 24;
        }
        hash
    }
}
#[cfg(test)]
mod tests {
    use super::Bloom;

    #[test]
    fn test_hash() {
        assert_eq!(Bloom::hash("".as_ref()), 0xbc9f1d34);
        assert_eq!(Bloom::hash("a".as_ref()), 0x286e9db0);
        assert_eq!(Bloom::hash("ab".as_ref()), 0x39aca330);
        assert_eq!(Bloom::hash("abc".as_ref()), 0x855d012f);
        assert_eq!(Bloom::hash("abcd".as_ref()), 0xb9c83353);
        assert_eq!(Bloom::hash("abcde".as_ref()), 0x41d2c26d);
        assert_eq!(Bloom::hash("abcdefghi".as_ref()), 0xf065ec74);
        assert_eq!(Bloom::hash("你好".as_ref()), 0x6466387);
    }
}
