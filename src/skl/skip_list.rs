use crate::kv::{KeyTs, ValueInner};

/// <head> --> [1] --> [2] --> [3] --> [4] --> [5] --> [6] --> [7] --> [8] --> [9] --> [10] ->
/// <head> ----------> [2] ----------> [4] ------------------> [7] ----------> [9] --> [10] ->
/// <head> ----------> [2] ------------------------------------[7] ----------> [9] ---------->
/// <head> ----------> [2] --------------------------------------------------> [9] ---------->
use super::arena::Arena;
use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{
        atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

const SKL_MAX_HEIGHT: usize = 20; //<20 !=20
#[derive(Debug, Default)]
struct Tower([NodeOffset; SKL_MAX_HEIGHT]);
impl Deref for Tower {
    type Target = [NodeOffset; SKL_MAX_HEIGHT];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Tower {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
#[derive(Debug, Default)]
#[repr(C, align(8))]
struct Node {
    value_slice: AtomicU64,
    key_offset: u32,
    key_len: u16,
    height: u16,
    tower: Tower,
}
#[test]
fn test_size() {
    // let p = size_of::<Node<KeyTs, ValueInner>>();

    let mut v = ValueInner::default();
    let k = AtomicPtr::new(&mut v as *mut _);
    // k.compare_exchange(current, new, success, failure)
    // k.compare_exchange_weak(current, new, success, failure)
    // dbg!(p);
    dbg!(size_of::<Node>());
    // size_of::<Arc<>>();
}
impl Node {
    fn new<'a>(arena: &'a Arena, key: &[u8], value: &[u8], height: usize) -> &'a mut Self {
        let node = arena.alloc_with(Self::default);
        let key_p = arena.alloc_slice_copy(key);
        node.key_offset = arena.offset_slice(key_p);
        node.key_len = key.len() as u16;
        node.height = height as u16;
        node.set_value(arena, value);
        node
    }
    fn set_value(&self, arena: &Arena, value: &[u8]) {
        let value_p = arena.alloc_slice_copy(value);
        let offset = arena.offset_slice(value_p);
        let v = (offset as u64) << 32 | value.len() as u64;
        self.value_slice.store(v, Ordering::SeqCst)
    }

    fn value_slice(&self) -> (u32, u32) {
        let v = self.value_slice.load(Ordering::SeqCst);
        ((v >> 32) as u32, v as u32)
    }
    #[inline]
    fn get_next_node(&self, height: usize) -> Option<&Node> {
        // let node_ptr = &self.tower.0[height];
        // let p = node_ptr.load();
        // if p.is_null() {
        //     None
        // } else {
        //     Some(unsafe { &*p })
        // }
        todo!()
    }
    // #[inline]
    // fn get_key<'a>(&self, arena: &'a Arena) -> Option<&[u8]> {
    //     arena.get(self.key_offset)
    // }
}
#[test]
fn test_v() {
    let offset = 2;
    let l = u32::MAX - 2;
    let v = (offset as u64) << 32 | l as u64;
    println!("{:064b}", v);
    println!("{:032b}", v >> 32 as u32);
    println!("{:032b}", v as u32);
    // dbg!(((v >> 32) as u32, v as u32));
}
#[derive(Debug, Default)]
struct NodeOffset(AtomicU32);
impl NodeOffset {
    fn new(arena: &Arena, node: &mut Node) -> Self {
        Self(AtomicU32::new(arena.offset(node as *const _)))
    }
    fn get_node<'a>(&self, arena: &'a Arena) -> Option<&'a Node> {
        let offset = self.0.load(Ordering::SeqCst);
        arena.get(offset)
    }
}

// impl Clone for NodePtr {
//     fn clone(&self) -> Self {

//         // Self(AtomicPtr::new(self.load_mut()))
//     }
// }
// #[test]
// fn test_node() {
//     let n = NodePtr::default();
//     let mut p: Node<KeyTs, ValueInner> = Node {
//         key_offset: 1,
//         value: AtomicPtr::default(),
//         height: 2,
//         tower: Tower::default(),
//         key_len: todo!(),
//     };
//     let a: NodePtr<KeyTs, ValueInner> = AtomicPtr::new(&mut p as *mut _).into();
//     dbg!(n.is_empty());
//     dbg!(a.is_empty());
//     for i in (0..2).rev() {
//         dbg!(i);
//     }
// }

// const K: fn() -> usize = ;
// const V: fn() -> usize = ;
pub(crate) const SKL_MAX_NODE_SIZE: u32 = size_of::<Node>() as u32;
#[derive(Debug)]
struct SkipListInner {
    height: AtomicUsize,
    head: NodeOffset,
    arena: Arena,
}
impl SkipListInner {
    fn new(arena_size: u32) -> Self {
        let mut arena = Arena::new(arena_size);
        // let head = Node::new(&arena, None, None, SKL_MAX_HEIGHT);
        // Self {
        //     height: AtomicUsize::new(1),
        //     // head,
        //     arena,
        // }
        todo!()
    }

    pub fn push(&self, key: &[u8], value: &[u8]) {
        let mut prev: Tower = Tower::default();
        let mut next: Tower = Tower::default();
        let height = self.height();
        // prev[height] = self.head.clone();
        for h in (0..height).rev() {}
        // let prev:[NodePtr<K,V>;21]=Default::default();
        // let prev = [AtomicPtr::<K, V>::default(); SKL_MAX_HEIGHT + 1];
    }
    // fn find_splice_for_level(
    //     &self,
    //     key: &,
    //     before: NodePtr<K, V>,
    //     height: usize,
    // ) -> (NodePtr<K, V>, Option<NodePtr<K, V>>) {
    //     loop {
    //         match before.load_ref().get_next_node(height) {
    //             Some(next) => {}
    //             None => return (before, None),
    //         };
    //     }
    // }
    fn height(&self) -> usize {
        self.height.load(Ordering::Acquire)
    }
}
#[derive(Debug)]
pub(crate) struct SkipList {
    skip_list: Arc<SkipListInner>,
}
impl SkipList {
    pub(crate) fn new(arena_size: u32) -> Self {
        SkipList {
            skip_list: Arc::new(SkipListInner::new(arena_size)),
        }
    }

    #[inline]
    pub(crate) fn mem_size(&self) -> u32 {
        self.skip_list.arena.len() as u32
    }
}
