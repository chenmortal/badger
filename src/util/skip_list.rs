use rand::Rng;

use crate::{
    iter::{
        DoubleEndedSinkIter, DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIterator,
        SinkIter, SinkIterator,
    },
    kv::ValueMeta,
    util::arena::Arena,
};

/// <head> --> [1] --> [2] --> [3] --> [4] --> [5] --> [6] --> [7] --> [8] --> [9] --> [10] ->
/// <head> ----------> [2] ----------> [4] ------------------> [7] ----------> [9] --> [10] ->
/// <head> ----------> [2] ------------------------------------[7] ----------> [9] ---------->
/// <head> ----------> [2] --------------------------------------------------> [9] ---------->
use std::{
    mem::{replace, size_of},
    ops::{Deref, DerefMut},
    ptr::Unique,
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

const SKL_MAX_HEIGHT: usize = 20; //<20 !=20

///a probability of `numerator/denominator`.
///for example,the probability of node.height==1 is 1/3
///the probability of node.height==2 is (1/3)^2, node.height==3 is (1/3)^3;
const RANDOM_HEIGHT_NUMERATOR: u32 = 1;
const RANDOM_HEIGHT_DENOMINATOR: u32 = 3;

//
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
pub(crate) struct Node {
    value_slice: AtomicU64,
    key_offset: u32,
    key_len: u16,
    height: u16,
    prev: NodeOffset,
    tower: Tower,
}

impl Node {
    fn new<'a>(arena: &'a Arena, key: &[u8], value: &[u8], height: usize) -> &'a mut Self {
        let node = arena.alloc_with(Self::default);
        let key_p = arena.alloc_slice_copy(key);
        node.key_offset = arena.offset_slice(key_p);
        node.key_len = key.len() as u16;
        node.height = height as u16;
        node.prev = NodeOffset(AtomicU32::new(8));
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
    pub(crate) fn next<'a>(&self, arena: &'a Arena, level: usize) -> Option<&'a Node> {
        self.tower[level].get_node(arena)
    }
    pub(crate) fn prev<'a>(&self, arena: &'a Arena) -> Option<&'a Node> {
        self.prev.get_node(arena)
    }
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

pub(crate) const SKL_MAX_NODE_SIZE: usize = size_of::<Node>();
#[derive(Debug)]
pub(crate) struct SkipListInner {
    height: AtomicUsize,
    head: Unique<Node>,
    arena: Arena,
    cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering,
}
impl SkipListInner {
    fn new(arena_size: usize, cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering) -> Self {
        let arena = Arena::new(arena_size);
        let head: &mut Node = arena.alloc_with(Node::default);
        head.height = SKL_MAX_HEIGHT as u16;
        let head = Unique::new(head as *mut _).unwrap();
        Self {
            height: AtomicUsize::new(1),
            head,
            arena,
            cmp,
        }
    }
    #[inline]
    fn head_offset(&self) -> u32 {
        self.arena.offset(self.head.as_ptr()).unwrap()
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
        height = random_height;
        let node_offset = self.arena.offset(node).unwrap();
        for h in 0..height {
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
                let mut next_offset = self.arena.offset(next[h]).unwrap_or_default();
                node.tower[h].store(next_offset, Ordering::SeqCst);
                if h == 0 {
                    loop {
                        let next_node = next[0];
                        let prev_offset = self.arena.offset(prev[0]).unwrap_or(self.head_offset());
                        node.prev.store(prev_offset, Ordering::SeqCst);
                        if !next_node.is_null() {
                            let next_node = unsafe { &*next_node };
                            match next_node.prev.compare_exchange(
                                prev_offset,
                                node_offset,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => {
                                    break;
                                }
                                Err(_) => {
                                    let (p, n) = self.find_splice_for_level(key.into(), prev[0], 0);
                                    if p == n {
                                        self.try_set_value(prev[0], value);
                                        return;
                                    }
                                    prev[0] = p;
                                    next[0] = n;
                                    next_offset = self.arena.offset(next[0]).unwrap_or_default();
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }

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
        key: &[u8],
        mut before_ptr: *const Node,
        height: usize,
    ) -> (*const Node, *const Node) {
        loop {
            if let Some(before) = unsafe { before_ptr.as_ref() } {
                if let Some(next) = before.next(&self.arena, height) {
                    if let Some(next_key_slice) = next.get_key(&self.arena) {
                        let next_ptr = next as *const _;
                        match (self.cmp)(key, next_key_slice) {
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
        while h < SKL_MAX_HEIGHT
            && rng.gen_ratio(RANDOM_HEIGHT_NUMERATOR, RANDOM_HEIGHT_DENOMINATOR)
        {
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
    fn find_or_near(&self, key: &[u8], allow_near: bool) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let mut level = self.height() - 1;
        loop {
            match node.next(&self.arena, level) {
                Some(next) => {
                    let next_key = next.get_key(&self.arena).unwrap();
                    match (self.cmp)(key, next_key) {
                        std::cmp::Ordering::Less => {
                            if level > 0 {
                                level -= 1;
                                continue;
                            } else {
                                if allow_near {
                                    return next.into();
                                }
                                return None;
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            return next.into();
                        }
                        std::cmp::Ordering::Greater => {
                            node = next;
                            continue;
                        }
                    }
                }
                None => {
                    if level > 0 {
                        level -= 1;
                    } else {
                        return None;
                    }
                }
            }
        }
    }
    fn find_next(&self, key: &[u8]) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let mut level = self.height() - 1;
        loop {
            match node.next(&self.arena, level) {
                Some(next) => {
                    let next_key = next.get_key(&self.arena).unwrap();
                    match (self.cmp)(key, next_key) {
                        std::cmp::Ordering::Less => {
                            if level > 0 {
                                level -= 1;
                                continue;
                            } else {
                                return next.into();
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            return next.next(&self.arena, 0);
                        }
                        std::cmp::Ordering::Greater => {
                            node = next;
                            continue;
                        }
                    }
                }
                None => {
                    if level > 0 {
                        level -= 1;
                    } else {
                        return None;
                    }
                }
            }
        }
    }
    fn find_prev(&self, key: &[u8]) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let head_ptr = node as *const _;
        let mut level = self.height() - 1;
        loop {
            match node.next(&self.arena, level) {
                Some(next) => {
                    let next_key = next.get_key(&self.arena).unwrap();
                    match (self.cmp)(key, next_key) {
                        std::cmp::Ordering::Greater => {
                            //node.key <next.key < key
                            node = next;
                            continue;
                        }
                        _ => {}
                    }
                }
                None => {}
            }
            if level > 0 {
                level -= 1;
            } else {
                if head_ptr == node as *const _ {
                    return None;
                } else {
                    return node.into();
                }
            }
        }
    }
    fn find_last(&self) -> Option<&Node> {
        let mut node = unsafe { self.head.as_ref() };
        let mut level = self.height() - 1;
        loop {
            match node.next(&self.arena, level) {
                Some(next) => {
                    node = next;
                }
                None => {
                    if level > 0 {
                        level -= 1;
                    } else {
                        return node.into();
                    }
                }
            }
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
    pub(crate) fn new(arena_size: usize, cmp: fn(&[u8], &[u8]) -> std::cmp::Ordering) -> Self {
        SkipList {
            skip_list: Arc::new(SkipListInner::new(arena_size, cmp)),
        }
    }

    #[inline]
    pub(crate) fn mem_size(&self) ->usize{
        self.skip_list.arena.len()
    }
    #[inline]
    pub(crate) fn iter(&self) -> SkipListIter<'_> {
        SkipListIter::new(&self)
    }
    #[inline]
    pub(crate) fn get(&self, key: &[u8], allow_near: bool) -> Option<&[u8]> {
        if let Some(node) = self.find_or_near(key, allow_near) {
            return node.get_value(&self.arena);
        };
        None
    }
    pub(crate) fn get_key_value(&self, key: &[u8], allow_near: bool) -> Option<(&[u8], &[u8])> {
        if let Some(node) = self.find_or_near(key, allow_near) {
            return Some((
                node.get_key(&self.arena).unwrap(),
                node.get_value(&self.arena).unwrap(),
            ));
        };
        None
    }
}
pub(crate) struct SkipListIter<'a> {
    inner: &'a SkipListInner,
    node: Option<&'a Node>,
    value: Option<ValueMeta>,
    node_back: Option<&'a Node>,
    value_back: Option<ValueMeta>,
}
impl<'a> SkipListIter<'a> {
    fn new(inner: &'a SkipListInner) -> Self {
        let node = unsafe { inner.head.as_ref() }.into();
        let node_back = None;
        Self {
            inner,
            node,
            node_back,
            value: None,
            value_back: None,
        }
    }
}
impl<'a> SinkIter for SkipListIter<'a> {
    type Item = Node;

    fn item(&self) -> Option<&Self::Item> {
        if let Some(node) = self.node {
            if node as *const _ == self.inner.head.as_ptr() {
                return None;
            };
        }
        self.node
    }
}
impl<'a> DoubleEndedSinkIter for SkipListIter<'a> {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.node_back
    }
}
impl<'a> SinkIterator for SkipListIter<'a> {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        if let Some(now) = self.node {
            if let Some(new) = now.next(&self.inner.arena, 0) {
                if let Some(back) = self.node_back {
                    if new as *const _ == back as *const _ {
                        return Ok(false);
                    }
                }
                self.node = new.into();
                return Ok(true);
            };
        }
        Ok(false)
    }
}
impl<'a> DoubleEndedSinkIterator for SkipListIter<'a> {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        if let Some(now) = self.node_back {
            if let Some(new) = now.prev(&self.inner.arena) {
                if let Some(node) = self.node {
                    if new as *const _ == node as *const _ {
                        return Ok(false);
                    }
                }
                self.node_back = new.into();
                return Ok(true);
            }
        } else {
            if let Some(last) = self.inner.find_last() {
                if last as *const _ != self.inner.head.as_ptr() {
                    self.node_back = last.into();
                    return Ok(true);
                }
            };
        }
        Ok(false)
    }
}
impl<'a> KvSinkIterator<'a, &'a [u8], ValueMeta> for SkipListIter<'a> {
    fn key(&self) -> Option<&'a [u8]> {
        if let Some(item) = self.item() {
            item.get_key(&self.inner.arena)
        } else {
            None
        }
    }

    fn value_ref(&mut self) -> Option<&ValueMeta> {
        if let None = self.value {
            if let Some(item) = self.item() {
                if let Some(data) = item.get_value(&self.inner.arena) {
                    self.value = ValueMeta::deserialize(data).into();
                }
            }
        }
        self.value.as_ref()
    }

    fn take_value(&mut self) -> Option<ValueMeta> {
        if let None = self.value {
            if let Some(item) = self.item() {
                if let Some(data) = item.get_value(&self.inner.arena) {
                    return ValueMeta::deserialize(data).into();
                }
            }
        } else {
            return replace(&mut self.value, None);
        }
        None
    }
}
impl<'a> KvDoubleEndedSinkIter<'a, &'a [u8], ValueMeta> for SkipListIter<'a> {
    fn key_back(&self) -> Option<&'a [u8]> {
        if let Some(item) = self.item_back() {
            item.get_key(&self.inner.arena)
        } else {
            None
        }
    }

    fn value_back_ref(&mut self) -> Option<&ValueMeta> {
        if let None = self.value {
            if let Some(item) = self.item_back() {
                if let Some(data) = item.get_value(&self.inner.arena) {
                    self.value = ValueMeta::deserialize(data).into();
                }
            }
        }
        self.value.as_ref()
    }

    fn take_value_back(&mut self) -> Option<ValueMeta> {
        if let None = self.value {
            if let Some(item) = self.item_back() {
                if let Some(data) = item.get_value(&self.inner.arena) {
                    return ValueMeta::deserialize(data).into();
                }
            }
        } else {
            return replace(&mut self.value, None);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, mem::size_of, time::SystemTime};

    use rand::Rng;

    use crate::{
        iter::{
            DoubleEndedSinkIter, DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSinkIterator,
            SinkIter, SinkIterator,
        },
        kv::KeyTsBorrow,
    };

    use super::{Node, SkipList, SkipListIter, RANDOM_HEIGHT_DENOMINATOR, RANDOM_HEIGHT_NUMERATOR};
    #[test]
    fn test_random() {
        let mut rng = rand::thread_rng();
        let count = 100_000;
        let mut n = 0;
        for _i in 0..count {
            if rng.gen_ratio(RANDOM_HEIGHT_NUMERATOR, RANDOM_HEIGHT_DENOMINATOR) {
                n += 1;
            }
        }
        let two_decimal = |a: u32, b: u32| {
            let ratio = (a as f32) / (b as f32);
            (ratio * 100 as f32) as u32
        };

        assert_eq!(
            two_decimal(n, count),
            two_decimal(RANDOM_HEIGHT_NUMERATOR, RANDOM_HEIGHT_DENOMINATOR)
        )
    }
    #[test]
    fn test_iter_next() {
        let end = 1000;
        //key and value only have 4 bytes,but align is 8 bytes,so actually write 8 bytes
        let arena_size = (size_of::<Node>() + 8 * 2) * (end + 1);
        let skip_list = SkipList::new(arena_size, KeyTsBorrow::cmp);
        for i in 0..end {
            skip_list.push(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
        }
        let mut iter = SkipListIter::new(&skip_list);
        assert!(iter.item().is_none());
        for i in 0..end {
            assert!(iter.next().unwrap());
            assert_eq!(iter.key().unwrap(), i.to_be_bytes());
        }
        assert_eq!(iter.next().unwrap(), false);
    }

    #[test]
    fn test_iter_next_back() {
        let end = 1000;
        //key and value only have 4 bytes,but align is 8 bytes,so actually write 8 bytes
        let arena_size = (size_of::<Node>() + 8 * 2) * (end + 1);
        let skip_list = SkipList::new(arena_size, KeyTsBorrow::cmp);
        for i in 0..end {
            skip_list.push(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
        }
        let mut iter = SkipListIter::new(&skip_list);
        assert!(iter.item_back().is_none());
        for i in (0..end).rev() {
            assert!(iter.next_back().unwrap());
            assert_eq!(iter.key_back().unwrap(), i.to_be_bytes())
        }
        assert_eq!(iter.next_back().unwrap(), false);
    }
    #[test]
    fn test_iter_double_ended() {
        let end = 1000;
        let split = 500;
        assert!(split < end);
        //key and value only have 4 bytes,but align is 8 bytes,so actually write 8 bytes
        let arena_size = (size_of::<Node>() + 8 * 2) * (end + 1);
        let skip_list = SkipList::new(arena_size, KeyTsBorrow::cmp);
        for i in 0..end {
            skip_list.push(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
        }
        let mut iter = SkipListIter::new(&skip_list);
        for i in 0..split {
            assert!(iter.next().unwrap());
            assert_eq!(iter.key().unwrap(), i.to_be_bytes());
        }
        for i in (split..end).rev() {
            assert!(iter.next_back().unwrap());
            assert_eq!(iter.key_back().unwrap(), i.to_be_bytes());
        }
        assert_eq!(iter.next().unwrap(), false);
        assert_eq!(iter.next_back().unwrap(), false);
    }
    #[test]
    fn test_iter_rev_next() {
        let end = 1000;

        //key and value only have 4 bytes,but align is 8 bytes,so actually write 8 bytes
        let arena_size = (size_of::<Node>() + 8 * 2) * (end + 1);
        let skip_list = SkipList::new(arena_size, KeyTsBorrow::cmp);
        for i in 0..end {
            skip_list.push(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
        }
        let iter = SkipListIter::new(&skip_list);
        let mut iter_rev = iter.rev();

        assert!(iter_rev.item().is_none());
        for i in (0..end).rev() {
            assert!(iter_rev.next().unwrap());
            assert_eq!(iter_rev.key().unwrap(), i.to_be_bytes());
        }
        assert_eq!(iter_rev.next().unwrap(), false);
    }
    #[test]
    fn test_iter_rev_next_back() {
        let end = 1000;

        //key and value only have 4 bytes,but align is 8 bytes,so actually write 8 bytes
        let arena_size = (size_of::<Node>() + 8 * 2) * (end + 1);
        let skip_list = SkipList::new(arena_size, KeyTsBorrow::cmp);
        for i in 0..end {
            skip_list.push(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
        }
        let iter = SkipListIter::new(&skip_list);
        let mut iter_rev = iter.rev();

        assert!(iter_rev.item().is_none());
        for i in 0..end {
            assert!(iter_rev.next_back().unwrap());
            assert_eq!(iter_rev.key_back().unwrap(), i.to_be_bytes());
        }
        assert_eq!(iter_rev.next_back().unwrap(), false);
    }
    #[test]
    fn test_iter_rev_double_ended() {
        let end = 1000;
        let split = 500;
        assert!(split < end);
        //key and value only have 4 bytes,but align is 8 bytes,so actually write 8 bytes
        let arena_size = (size_of::<Node>() + 8 * 2) * (end + 1);
        let skip_list = SkipList::new(arena_size, KeyTsBorrow::cmp);
        for i in 0..end {
            skip_list.push(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
        }
        let iter = SkipListIter::new(&skip_list);
        let mut iter_rev = iter.rev();
        for i in 0..split {
            assert!(iter_rev.next_back().unwrap());
            assert_eq!(iter_rev.key_back().unwrap(), i.to_be_bytes());
        }
        for i in (split..end).rev() {
            assert!(iter_rev.next().unwrap());
            assert_eq!(iter_rev.key().unwrap(), i.to_be_bytes());
        }
        assert_eq!(iter_rev.next().unwrap(), false);
        assert_eq!(iter_rev.next_back().unwrap(), false);
    }
    #[test]
    fn test_iter_rev_rev() {
        let end = 1000;
        //key and value only have 4 bytes,but align is 8 bytes,so actually write 8 bytes
        let arena_size = (size_of::<Node>() + 8 * 2) * (end + 1);
        let skip_list = SkipList::new(arena_size, KeyTsBorrow::cmp);
        for i in 0..end {
            skip_list.push(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
        }
        let iter = SkipListIter::new(&skip_list);
        let mut iter = iter.rev().rev();
        assert!(iter.item().is_none());
        for i in 0..end {
            assert!(iter.next().unwrap());
            assert_eq!(iter.key().unwrap(), i.to_be_bytes());
        }
        assert_eq!(iter.next().unwrap(), false);
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
    #[test]
    fn test_init() {
        let skip_list = SkipList::new(100_000_000, KeyTsBorrow::cmp);
        let start = SystemTime::now();
        for i in 1..100_000 {
            skip_list.push(i.to_string().as_bytes(), (i.to_string() + "abc").as_bytes());
        }
        let e = SystemTime::now().duration_since(start).unwrap();
        dbg!(e);

        assert_eq!(
            skip_list
                .get(49.to_string().as_bytes(), false)
                .and_then(|x| String::from_utf8_lossy(x).to_string().into()),
            Some(49.to_string() + "abc")
        );
        assert_eq!(
            skip_list
                .get(0.to_string().as_bytes(), true)
                .and_then(|x| String::from_utf8_lossy(x).to_string().into()),
            Some(1.to_string() + "abc")
        );
        assert_ne!(
            skip_list
                .get(0.to_string().as_bytes(), false)
                .and_then(|x| String::from_utf8_lossy(x).to_string().into()),
            Some(1.to_string() + "abc")
        );
        // for i in 0..SKL_MAX_HEIGHT {
        //     let mut iter = SkipListLevelIter::new(skip_list.clone(), i);
        //     print!("h{:<5}", i);
        //     iter.next();
        //     while let Some(node) = iter.next() {
        //         let n = unsafe { &*node };

        //         print!(
        //             "---{}:{}:prev{}  ",
        //             String::from_utf8_lossy(n.get_key(&skip_list.arena).unwrap()),
        //             String::from_utf8_lossy(n.get_value(&skip_list.arena).unwrap()),
        //             String::from_utf8_lossy(
        //                 if let Some(s) = n.prev(&skip_list.arena) {
        //                     s.get_key(&skip_list.arena).unwrap_or_default()
        //                 }else {
        //                     b""
        //                 }
        //             ),
        //         );
        //     }
        //     println!()
        // }
        // skip_list.push(key, value)
    }
}
