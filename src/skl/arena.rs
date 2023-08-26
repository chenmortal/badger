use bumpalo::Bump;
use bytes::BytesMut;
use std::sync::Arc;
pub(crate) struct Arena(Bump);
impl Arena {
    pub(crate) fn new(size: usize) -> Self {
        let bump = Bump::new();
        bump.set_allocation_limit(Some(size));
        Arena(bump)
    }
    #[inline]
    pub(crate) fn alloc<T>(&self, data: T) -> bumpalo::boxed::Box<'_, T> {
        bumpalo::boxed::Box::new_in(data, &self.0)
    }
    #[inline]
    pub(crate) fn alloc_bytes(&self, data: &[u8]) -> bumpalo::boxed::Box<'_, [u8]> {
        unsafe { bumpalo::boxed::Box::from_raw(self.0.alloc_slice_copy(data)) }
    }
    pub(crate) fn put_node() {}
}
// impl Arena {
//     pub(crate) fn new(size:u64){

//     }
// }
