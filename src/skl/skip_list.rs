use std::{
    mem::size_of,
    sync::{
        atomic::{AtomicI32, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use crate::options::Options;

use super::arena::{Arena, ArenaSlice};
const SKL_MAX_HEIGHT: usize = 8;
#[derive(Debug, Default)]
#[repr(align(8))]
struct Node {
    value: Option<ArenaSlice<u8>>,
    key: Option<ArenaSlice<u8>>,
    height: usize,
    tower: [Option<NodePtr>; SKL_MAX_HEIGHT],
}
impl Node {
    fn new(
        arena: &Arena,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        height: usize,
    ) -> AtomicPtr<Node> {
        let mut node = Node::default();
        node.key = key.and_then(|src| Some(arena.alloc_slice_copy(src)));
        node.value = value.and_then(|src| Some(arena.alloc_slice_copy(src)));
        node.height = height;
        AtomicPtr::new(arena.alloc(node))
    }
}
#[derive(Debug)]
struct NodePtr(AtomicPtr<Node>);
impl NodePtr {
    fn get_key(&self) -> Option<&ArenaSlice<u8>> {
        unsafe {
            match self.0.load(Ordering::SeqCst).as_ref() {
                Some(s) => match s.key {
                    Some(ref p) => Some(p),
                    None => None,
                },
                None => None,
            }
        }
    }
}
impl From<AtomicPtr<Node>> for NodePtr {
    fn from(value: AtomicPtr<Node>) -> Self {
        Self(value)
    }
}
pub(crate) const SKL_MAX_NODE_SIZE: usize = size_of::<Node>();
struct SkipListInner {
    height: AtomicUsize,
    head: NodePtr,
    arena: Arena,
}
impl SkipListInner {
    fn new(arena_size: usize) -> Self {
        let arena = Arena::new(arena_size);
        let head = Node::new(&arena, None, None, SKL_MAX_HEIGHT).into();
        Self {
            height: AtomicUsize::new(1),
            head,
            arena,
        }
    }
    fn push() {}
}
pub(crate) struct SkipList {
    skip_list: Arc<SkipListInner>,
}
impl SkipList {
    pub(crate) fn new(arena_size: usize) -> Self {
        SkipList {
            skip_list: Arc::new(SkipListInner::new(arena_size)),
        }
    }
}

#[test]
fn test_node() {
    let node = Node::default();
    dbg!(node);
    dbg!(size_of::<Option<ArenaSlice<u8>>>());
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
