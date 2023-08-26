use crate::default::SKL_MAX_HEIGHT;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64};

use super::arena::Arena;
use bumpalo::boxed::Box;
pub(crate) struct Node<'a> {
    value: AtomicU64,
    key: Option<bumpalo::boxed::Box<'a, [u8]>>,
    height: u16,
    // tower:[AtomicU32;SKL_MAX_HEIGHT]
}
pub(crate) struct SkipList<'a,'b:'a>{
    height: AtomicI32,
    head: Box<'a, Node<'b>>,
    arena: Arena,
}
// impl<'a,'b:'a> SkipList<'a,'b> {
//     fn new(arena_size: u64) -> SkipList<'a,'b> {
//         let arena = Arena::new(arena_size as usize);
//         // let tow=[]
//         let head = arena.alloc(Node {
//             value: AtomicU64::new(0),
//             key: None,
//             height: 2,
//             // tower: [None;SKL_MAX_HEIGHT],
//         });
//         Self {
//             height: AtomicI32::new(0),
//             head,
//             arena,
//         }
//     }
// }
