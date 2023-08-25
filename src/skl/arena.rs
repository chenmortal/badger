use std::sync::Arc;
use bytes::BytesMut;
pub(crate) struct Arena(Arc<BytesMut>);
impl Arena {
    pub(crate) fn new(size:u64){
        
    }
}