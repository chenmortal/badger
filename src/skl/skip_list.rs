use crate::default::SKL_MAX_HEIGHT;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64};

// use super::arena::Arena;
use bumpalo::boxed::Box;
use bumpalo::Bump;

 struct Node {
    value: AtomicU64,
    key: Option<u64>,
    height: u16,
    // tower:[AtomicU32;SKL_MAX_HEIGHT]
}
 struct SkipListInner{
    height: AtomicI32,
    // head: Box<'a, Node<'a>>,
    // arena: Arena,
}
 struct SkipList{
    arena:Bump,
    skip_list:SkipListInner,
}
impl<'a> SkipList {
    fn new(size:usize){
        // let bump = Bump::new();
        // bump.set_allocation_limit(Some(size));
        // let node=Node{
        //     value: AtomicU64::new(0),
        //     key: None,
        //     height: 0,
        // };
        // let node_ptr = bump.alloc(node);
        
        // bump.alloc(va);
        // let arena = Arena::new(size);
        // let node = arena.alloc(Node{
        //             value: AtomicU64::new(0),
        //             key: None,
        //             height: 0,
        //         });
        // let inner = SkipListInner{
        //             height:AtomicI32::new(1),
        //             head: node,
        //         };
        //         SkipList{
        //             arena,
        //             skip_list: inner,
        //         }        


    }
}
struct A<'a>{
    b:B,
    name:&'a str
}
struct B{
    mem:String,
    name:String,
    a:i32,
}
impl<'a> A<'a> {
    pub fn new(){
        // let b = B{
        //             mem: String::from("aa"),
        //             name: String::from("bb"),
        //             a: 0,
        //         };
        //         let name = b.name.as_str();
        // A{
        //     b,
        //     name,
        // };
    }
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
