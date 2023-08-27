use std::{
    alloc::Layout,
    mem::size_of,
    ptr::{self, NonNull, Unique},
    sync::{
        atomic::{AtomicPtr, AtomicU64},
        Arc,
    },
};

use bumpalo::Bump;

pub(crate) mod arena;
pub(crate) mod skip_list;
const MAX_HIGHT: usize = 8;
struct Arena {
    len: usize,
    bytes: Vec<u8>,
}
struct SkipListImpl {
    arena: Bump,
    inner: SkipListInner,
}
struct SkipListInner {
    hight: usize,
    head: AtomicPtr<Node>,
}
struct Node {
    value: AtomicU64,
    // key: Option<Arc<&'a mut [u8]>>,
    height: u16,
    tower: [Option<AtomicPtr<Node>>; MAX_HIGHT],
}
fn default_tower() -> [Option<AtomicPtr<Node>>; 8] {
    [None, None, None, None, None, None, None, None]
}
use bytes::Bytes;
impl SkipListImpl {
    fn new(size: usize) -> Self {
        // let tower:[Option<AtomicPtr<Node>>;2]=[None]
        let tower = default_tower();
        let node = Node {
            value: AtomicU64::new(0),
            // key: None,
            height: 2,
            tower,
        };
        let bump = Bump::new();
        bump.set_allocation_limit(Some(size));
        let a = bump.alloc(node);

        let head = AtomicPtr::new(a);
        let b = bump.alloc_slice_copy::<u8>(&b"hello world"[..]);

        let p = Arc::new(b);
        String::from_utf8_lossy(p.as_ref());
        // b.is_null();
        // let p = NonNull::new(b).unwrap();
        // Arc::new(p);
        // AtomicPtr::new(p);
        // let p = Arc::new(b);
        // NonNull::new(b);
        // AtomicPtr::new(b);
        // let data = AtomicPtr::new(b.as_mut_ptr());

        // AtomicPtr::new(unsafe{ Unique::new(b).unwrap()});
        // AtomicPtr::from_mut_slice(b);
        // AtomicPtr::from_mut_slice(v)
        // unsafe{
        //     AtomicPtr::from_ptr(b)
        // }

        // unsafe{AtomicPtr::from_ptr(b)}

        let inner = SkipListInner { hight: 8, head };
        SkipListImpl { arena: bump, inner }
        // Box::from_raw(raw)
        // bumpalo::vec![]
        // bumpalo::collections::Vec::new_in(bump)
        // Vec::new_in(alloc);
    }
    fn alloc_bytes(arena: &Bump, bytes: &[u8]) -> (AtomicPtr<u8>, usize) {
        //         let layout = Layout::for_value(src);
        // let dst = self.alloc_layout(layout).cast::<T>();

        // unsafe {
        //     ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), src.len());
        //     slice::from_raw_parts_mut(dst.as_ptr(), src.len())
        // }
        let layout = Layout::for_value(bytes);

        let dst_ptr = arena.alloc_layout(layout).as_ptr();
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst_ptr, bytes.len());
        }
        // let ptr = p.as_ptr();
        // let k = self.arena.alloc(p);
        let p = AtomicPtr::new(dst_ptr);
        (p, bytes.len())
        // let k = p.load(std::sync::atomic::Ordering::SeqCst);

        // let k = p.load(std::sync::atomic::Ordering::Relaxed);
        // *k;
        // AtomicPtr::from_ptr(ptr);
        // let k = AtomicPtr::new(ptr);
        // unsafe{
        // AtomicPtr::from_ptr(ptr)
        // };

        // core::slice::from_raw_parts_mut(ptr, len);
        // let p= k.get_mut();
        // unsafe{
        //     ptr::copy_from_nonoverlapping();
        // }
    }
    fn get_bytes(ptr: &AtomicPtr<u8>, len: usize) -> &[u8] {
        let ptr_raw = ptr.load(std::sync::atomic::Ordering::SeqCst);
        let p = unsafe { std::slice::from_raw_parts(ptr_raw, len) };
        p
    }
}
#[test]
fn test_a() {
    let bump = Bump::with_capacity(1000);
    // let (ptr, len) = SkipListImpl::alloc_bytes(&bump, &b"hello world"[..]);
    // ptr.fetch_ptr_add(1, std::sync::atomic::Ordering::SeqCst);
    // dbg!(String::from_utf8_lossy(SkipListImpl::get_bytes(&ptr, len-1)));;
    // head = AtomicPtr::new(a);
    let t = bump.alloc("hello");
    let b= bump.alloc_slice_copy::<u8>(&b"hello world hah"[..]);
    // let p = Arc::new(b);;

    // dbg!(String::from_utf8_lossy(p.as_ref()));
    dbg!(size_of::<Arc<&mut [u8]>>());
    dbg!(size_of::<usize>());
}
