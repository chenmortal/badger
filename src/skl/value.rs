use bincode::{DefaultOptions, Options};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
struct ValueInner {
    meta: u8,
    user_meta: u8,
    expires_at: u64,
    value: Vec<u8>,
}
struct Value {
    inner: ValueInner,
    version: u64,
}
impl Value {
    fn encode(&self) -> Result<Vec<u8>, Box<bincode::ErrorKind>> {
        DefaultOptions::new()
            .with_varint_encoding()
            .serialize(&self.inner)
    }
}
