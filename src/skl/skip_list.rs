use rand::Rng;

use crate::kv::KeyTsBorrow;

/// <head> --> [1] --> [2] --> [3] --> [4] --> [5] --> [6] --> [7] --> [8] --> [9] --> [10] ->
/// <head> ----------> [2] ----------> [4] ------------------> [7] ----------> [9] --> [10] ->
/// <head> ----------> [2] ------------------------------------[7] ----------> [9] ---------->
/// <head> ----------> [2] --------------------------------------------------> [9] ---------->
use super::arena::Arena;
use std::{
    mem::size_of,
    ops::{Deref, DerefMut},
    ptr::Unique,
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
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
    fn get_key<'a>(&self, arena: &'a Arena) -> Option<&'a [u8]> {
        arena.get_slice::<u8>(self.key_offset, self.key_len as u32)
    }
    fn get_value<'a>(&self, arena: &'a Arena) -> Option<&'a [u8]> {
        let (offset, len) = self.value_slice();
        arena.get_slice::<u8>(offset, len)
    }
    #[inline]
    fn get_next_node<'a>(&self, arena: &'a Arena, height: usize) -> Option<&'a Node> {
        self.tower[height].get_node(arena)
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
impl Deref for NodeOffset {
    type Target = AtomicU32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl NodeOffset {
    fn new(arena: &Arena, node: &mut Node) -> Self {
        Self(AtomicU32::new(
            arena.offset(node as *const _).unwrap_or_default(),
        ))
    }
    fn get_node<'a>(&self, arena: &'a Arena) -> Option<&'a Node> {
        let offset = self.0.load(Ordering::SeqCst);
        arena.get(offset)
    }
}

pub(crate) const SKL_MAX_NODE_SIZE: u32 = size_of::<Node>() as u32;
#[derive(Debug)]
pub(crate) struct SkipListInner {
    height: AtomicUsize,
    head: Unique<Node>,
    arena: Arena,
}
impl SkipListInner {
    fn new(arena_size: u32) -> Self {
        let arena = Arena::new(arena_size);
        let head = arena.alloc_with(Node::default);
        head.height = SKL_MAX_HEIGHT as u16;
        let head = Unique::new(head as *mut _).unwrap();
        Self {
            height: AtomicUsize::new(1),
            head,
            arena,
        }
    }

    pub fn push(&self, key: &[u8], value: &[u8]) {
        let mut height = self.height();
        let mut prev = [std::ptr::null::<Node>(); SKL_MAX_HEIGHT + 1];
        let mut next = [std::ptr::null::<Node>(); SKL_MAX_HEIGHT + 1];
        prev[height] = self.head.as_ptr();
        for h in (0..height).rev() {
            let (p, n) = self.find_splice_for_level(key.into(), prev[h + 1], h);
            prev[h] = p;
            next[h] = n;
            if prev[h] == next[h] {
                self.try_set_value(prev[h], value);
                return;
            }
        }
        let random_height = Self::random_height();
        let node = Node::new(&self.arena, key, value, random_height);
        while random_height > height {
            match self.height.compare_exchange(
                height,
                random_height,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    break;
                }
                Err(h) => {
                    height = h;
                }
            };
        }
        let node_offset = self.arena.offset(node).unwrap();
        for h in 0..random_height {
            loop {
                let prev_node = match unsafe { prev[h].as_ref() } {
                    Some(prev_node) => prev_node,
                    None => {
                        assert!(h > 1);
                        let (p, n) = self.find_splice_for_level(key.into(), self.head.as_ptr(), h);
                        prev[h] = p;
                        next[h] = n;
                        assert_ne!(prev[h], next[h]);
                        unsafe { &*prev[h] }
                    }
                };
                let next_offset = self.arena.offset(next[h]).unwrap_or_default();
                node.tower[h].store(next_offset, Ordering::SeqCst);
                match prev_node.tower[h].compare_exchange(
                    next_offset,
                    node_offset,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        break;
                    }
                    Err(_) => {
                        let (p, n) = self.find_splice_for_level(key.into(), prev_node as _, h);
                        prev[h] = p;
                        next[h] = n;
                        if prev[h] == next[h] {
                            assert!(h == 0);
                            self.try_set_value(prev[h], value);
                            return;
                        }
                    }
                };
            }
        }
    }
    fn find_splice_for_level<'a>(
        &self,
        key: KeyTsBorrow<'a>,
        mut before_ptr: *const Node,
        height: usize,
    ) -> (*const Node, *const Node) {
        loop {
            if let Some(before) = unsafe { before_ptr.as_ref() } {
                if let Some(next) = before.get_next_node(&self.arena, height) {
                    if let Some(next_key_slice) = next.get_key(&self.arena) {
                        let next_key: KeyTsBorrow = next_key_slice.into();
                        let next_ptr = next as *const _;
                        match key.cmp(&next_key) {
                            std::cmp::Ordering::Less => return (before_ptr, next_ptr),
                            std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                            std::cmp::Ordering::Greater => {
                                before_ptr = next_ptr;
                                continue;
                            }
                        }
                    }
                }
            };
            return (before_ptr, std::ptr::null());
        }
    }
    fn height(&self) -> usize {
        self.height.load(Ordering::Acquire)
    }
    #[inline]
    fn random_height() -> usize {
        let mut rng = rand::thread_rng();
        let mut h = 1;
        while h < SKL_MAX_HEIGHT && rng.gen_ratio(u32::MAX / 3, u32::MAX) {
            h += 1;
        }
        return h;
    }
    fn try_set_value(&self, ptr: *const Node, value: &[u8]) {
        if let Some(node) = unsafe { ptr.as_ref() } {
            if let Some(v) = node.get_value(&self.arena) {
                if v == value {
                    return;
                }
            };
            node.set_value(&self.arena, value);
        } else {
            unreachable!()
        }
    }
}
struct SkipListLevelIter {
    skip_list: SkipList,
    height: usize,
    cursor: *const Node,
}
impl SkipListLevelIter {
    fn new(skip_list: SkipList, height: usize) -> Self {
        let cursor = skip_list.head.as_ptr();
        Self {
            skip_list,
            height,
            cursor,
        }
    }
}
impl Iterator for SkipListLevelIter {
    type Item = *const Node;

    fn next<'a>(&'a mut self) -> Option<Self::Item> {
        if let Some(node) = unsafe { self.cursor.as_ref() } {
            match node.tower[self.height].get_node(&self.skip_list.arena) {
                Some(next) => {
                    self.cursor = next;
                }
                None => {
                    self.cursor = std::ptr::null();
                }
            };
            Some(node as _)
        } else {
            None
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) struct SkipList {
    skip_list: Arc<SkipListInner>,
}
impl Deref for SkipList {
    type Target = SkipListInner;

    fn deref(&self) -> &Self::Target {
        &self.skip_list
    }
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
#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::{SkipList, SkipListLevelIter, SKL_MAX_HEIGHT};

    #[test]
    fn test_init() {
        let skip_list = SkipList::new(10000);
        for i in 1..10 {
            skip_list.push(i.to_string().as_bytes(), (i.to_string() + "abc").as_bytes());
        }

        for i in 0..SKL_MAX_HEIGHT {
            let mut iter = SkipListLevelIter::new(skip_list.clone(), i);
            print!("h{:<5}", i);
            iter.next();
            while let Some(node) = iter.next() {
                let n = unsafe { &*node };
                print!(
                    "---{}:{}  ",
                    String::from_utf8_lossy(n.get_key(&skip_list.arena).unwrap()),
                    String::from_utf8_lossy(n.get_value(&skip_list.arena).unwrap())
                );
            }
            println!()
        }
        // skip_list.push(key, value)
    }
    #[test]
    fn test_rng() {
        let mut rng = rand::thread_rng();
        let mut count:usize=0;
        let mut c=1000;
        // let mut res = Vec::with_capacity(100);
        for i in 0..c {
            if rng.gen_ratio(u32::MAX / 3, u32::MAX){
                count+=1;
            }
        }
        println!("{}",count);
        println!("{}",count as f32/c as f32);
    }
}
