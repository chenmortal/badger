use std::{
    mem::size_of,
    sync::{
        atomic::{AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
};

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
        arena: &mut Arena,
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
#[derive(Debug)]
struct SkipListInner {
    height: AtomicUsize,
    head: NodePtr,
    arena: Arena,
}
impl SkipListInner {
    fn new(arena_size: usize) -> Self {
        let mut arena = Arena::new(arena_size);
        let head = Node::new(&mut arena, None, None, SKL_MAX_HEIGHT).into();
        Self {
            height: AtomicUsize::new(1),
            head,
            arena,
        }
    }
}
#[derive(Debug)]
pub(crate) struct SkipList {
    skip_list: Arc<SkipListInner>,
}
impl SkipList {
    pub(crate) fn new(arena_size: usize) -> Self {
        SkipList {
            skip_list: Arc::new(SkipListInner::new(arena_size)),
        }
    }

    #[inline]
    pub(crate) fn mem_size(&self) -> usize {
        self.skip_list.arena.len()
    }
}
