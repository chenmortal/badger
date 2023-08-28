// mod tests {
//     #![feature(ptr_internals)]
//     use std::{sync::atomic::{AtomicPtr, AtomicU64}, mem::size_of};
//     const MAX_HIGHT: usize = 8;
//     // use bumpalo::Bump;
//     struct Arena {
//         len: usize,
//         bytes: Vec<u8>,
//     }
//     struct SkipListImpl {
//         // arena: Bump,
//         inner: SkipListInner,
//     }
//     struct SkipListInner {
//         hight: usize,
//         head: AtomicPtr<Node>,
//     }
//     struct Node {
//         value: AtomicU64,
//         key: Option<u64>,
//         height: u16,
//         tower: [Option<AtomicPtr<Node>>; MAX_HIGHT],
//     }
//     fn default_tower() -> [Option<AtomicPtr<Node>>; 8] {
//         [None, None, None, None, None, None, None, None]
//     }
//     impl SkipListImpl {
//         fn new(size: usize) -> Self {
//             // let tower:[Option<AtomicPtr<Node>>;2]=[None]
//             let tower = default_tower();
//             let node = Node {
//                 value: AtomicU64::new(0),
//                 key: None,
//                 height: 2,
//                 tower,
//             };
//             let bump = Bump::new();
//             bump.set_allocation_limit(Some(size));
//             let a = bump.alloc(node);

//             let head = AtomicPtr::new(a);
//             let b= bump.alloc_slice_copy::<u8>(&b"hello world"[..]);
//             // AtomicPtr::new(unsafe{ Unique::new(b).unwrap()});
//             // unsafe{AtomicPtr::from_ptr(b)}

            
//             let inner = SkipListInner { hight: 8, head };
//             SkipListImpl { arena: bump, inner }
//             // Box::from_raw(raw)
//             // bumpalo::vec![]
//             // bumpalo::collections::Vec::new_in(bump)
//             // Vec::new_in(alloc);
//         }
//     }
//     #[test]
//     fn test_a() {
//         let p = SkipListImpl::new(1000);
//     }
//     #[test]
//     fn test_b(){
//         dbg!(size_of::<&mut [u8]>());
//     }
// }
// mod test_arena{
//     use generational_arena::Arena;
//     #[test]
//     fn test_fg(){
//         let mut arena = Arena::new();
//         // arena.
//         let indexa = arena.insert(42);
//     }
// }