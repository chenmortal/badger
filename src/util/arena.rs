use std::alloc::{alloc, dealloc, Layout};

use std::ptr::{self, NonNull, Unique};
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize};

use crate::default::DEFAULT_PAGE_SIZE;

const CHUNK_ALIGN: usize = 16;
// const PAGE_CUTOFF: usize = 4096;
const DEFAULT_ALIGN: usize = 8;

// #[derive(Debug)]
// pub(crate) struct ArenaSlice<T> {
//     ptr: AtomicPtr<T>,
//     len: usize,
// }
// impl<T> ArenaSlice<T> {
//     pub(crate) fn get(&self) -> &[T] {
//         let ptr_raw = self.ptr.load(std::sync::atomic::Ordering::Acquire);
//         unsafe { slice::from_raw_parts(ptr_raw, self.len) }
//     }
//     pub(crate) fn get_mut(&mut self) -> &mut [T] {
//         let ptr_raw = self.ptr.load(std::sync::atomic::Ordering::Acquire);
//         unsafe { slice::from_raw_parts_mut(ptr_raw, self.len) }
//     }
// }

///If T contains elements such as String that contain Pointers,
///make sure that the memory pointed to by the Pointers is also allocated by Arena,
///otherwise you may end up destroying only the Pointers and not the memory pointed to by the Pointers,
///causing a memory leak
#[derive(Debug)]
pub(crate) struct Arena {
    start: Unique<u8>,
    ptr_offset: AtomicUsize,
    end: Unique<u8>,
    layout: Layout,
}
impl Arena {
    pub(crate) fn new(size: u32) -> Arena {
        let chunk_align = CHUNK_ALIGN;
        let size = size as usize + 8;
        let mut request_size = Self::round_up_to(size, chunk_align).unwrap();
        if request_size >= DEFAULT_PAGE_SIZE.to_owned() {
            request_size = Self::round_up_to(request_size, DEFAULT_PAGE_SIZE.to_owned()).unwrap();
        }
        // debug_assert_eq!(chunk_align % CHUNK_ALIGN, 0);
        debug_assert_eq!(request_size % CHUNK_ALIGN, 0);
        let layout = Layout::from_size_align(request_size, chunk_align).unwrap();
        let (data, end) = unsafe {
            let data_ptr = alloc(layout);
            let data = Unique::new(data_ptr).unwrap();
            let end_ptr = data.as_ptr().add(layout.size());
            let end = Unique::new(end_ptr).unwrap();
            (data, end)
        };
        debug_assert_eq!((data.as_ptr() as usize) % layout.align(), 0);
        debug_assert_eq!((end.as_ptr() as usize) % CHUNK_ALIGN, 0);
        let ptr_offset = AtomicUsize::new(0);
        // let ptr = AtomicPtr::new(NonNull::new(data.as_ptr()).unwrap().as_ptr());
        let s = Self {
            start: data,
            // ptr,
            ptr_offset,
            end,
            layout,
        };
        s.alloc(0 as u64);
        s
    }
    pub(crate) fn alloc<T>(&self, value: T) -> &mut T {
        self.alloc_with(|| value)
    }
    pub(crate) fn alloc_with<F, T>(&self, f: F) -> &mut T
    where
        F: FnOnce() -> T,
    {
        #[inline(always)]
        unsafe fn inner_write<T, F>(dst: *mut T, f: F)
        where
            F: FnOnce() -> T,
        {
            ptr::write(dst, f())
        }
        let layout = Layout::new::<T>();
        let p = self.alloc_layout(layout);
        let dst = p.as_ptr() as *mut T;
        unsafe {
            inner_write(dst, f);
            &mut *dst
        }
    }
    pub(crate) unsafe fn get_mut<T>(&self, offset: u32) -> Option<&mut T> {
        if offset < 8 {
            None
        } else {
            Some(&mut *(self.start.as_ptr().add(offset as usize) as *mut T))
        }
    }
    pub(crate) fn get<T>(&self, offset: u32) -> Option<&T> {
        if offset < 8 {
            None
        } else {
            unsafe { &*(self.start.as_ptr().add(offset as usize) as *const T) }.into()
        }
    }
    pub(crate) fn get_slice<T>(&self, offset: u32, len: u32) -> Option<&[T]> {
        if offset < 8 {
            None
        } else {
            unsafe {
                let ptr_raw = self.start.as_ptr().add(offset as usize) as *const T;
                std::slice::from_raw_parts(ptr_raw, len as usize)
            }
            .into()
        }
    }
    pub(crate) fn offset<N>(&self, ptr: *const N) -> Option<u32> {
        if ptr.is_null() {
            return None;
        }

        let ptr_addr = ptr as usize;
        let start_addr = self.start.as_ptr() as usize;
        debug_assert!(ptr_addr > start_addr);
        Some((ptr_addr - start_addr) as u32)
    }
    fn alloc_layout(&self, layout: Layout) -> NonNull<u8> {
        debug_assert!(DEFAULT_ALIGN.is_power_of_two());
        let layout = layout.align_to(DEFAULT_ALIGN).unwrap();
        let end_ptr = self.end.as_ptr();
        let start_ptr = self.start.as_ptr();
        let alloc_size = Self::round_up_to(layout.size(), layout.align()).unwrap();
        let old_ptr = unsafe {
            self.start.as_ptr().add(
                self.ptr_offset
                    .fetch_add(alloc_size, std::sync::atomic::Ordering::AcqRel)
                    as usize,
            )
        };
        debug_assert_eq!(old_ptr as usize % 8, 0);
        unsafe {
            let new_ptr = old_ptr.add(alloc_size);
            if new_ptr > end_ptr {
                let new_total = new_ptr.sub_ptr(start_ptr);
                panic!(
                    "Arena too small, toWrite:{}, newTotal:{}, limit:{}",
                    layout.size(),
                    new_total,
                    self.layout.size()
                );
            }
            NonNull::new_unchecked(old_ptr)
        }
    }
    #[inline(always)]
    pub(crate) fn alloc_slice_copy<T: Copy>(&self, src: &[T]) -> NonNull<T> {
        let layout = Layout::for_value(src);
        let dst = self.alloc_layout(layout).cast::<T>();
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), src.len());
        }
        dst
    }
    pub(crate) fn offset_slice<T>(&self, ptr: NonNull<T>) -> u32 {
        let ptr_addr = ptr.as_ptr() as usize;
        let start_addr = self.start.as_ptr() as usize;
        debug_assert!(ptr_addr > start_addr);
        (ptr_addr - start_addr) as u32
    }
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.ptr_offset.load(std::sync::atomic::Ordering::Acquire)
    }
    #[inline(always)]
    pub(crate) fn alloc_slice_clone<T: Clone>(&self, src: &[T]) -> NonNull<T> {
        let layout = Layout::for_value(src);
        let dst = self.alloc_layout(layout).cast::<T>();
        unsafe {
            for (i, val) in src.iter().cloned().enumerate() {
                ptr::write(dst.as_ptr().add(i), val);
            }
        }
        dst
    }

    #[inline(always)]
    fn round_up_to(n: usize, divisor: usize) -> Option<usize> {
        debug_assert!(divisor > 0);
        debug_assert!(divisor.is_power_of_two());
        Some(n.checked_add(divisor - 1)? & !(divisor - 1))
    }
}
impl Drop for Arena {
    fn drop(&mut self) {
        unsafe {
            // 因为在这里指向的元素是u8 实现了 Trait Copy , 所以 drop_in_place 在这里不会有任何操作,所以直接用dealloc
            // Because the element pointed to here is u8 that implements the Trait Copy, drop_in_place does nothing here,so use dealloc
            // ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
            //     self.start.as_ptr(),
            //     self.layout.size(),
            // ));

            dealloc(self.start.as_ptr(), self.layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::size_of, sync::Arc};
    struct Ar {
        p: Option<AtomicPtr<u8>>,
        len: usize,
    }
    use crate::kv::KeyTs;

    use super::*;
    #[derive(Debug)]
    struct Node {
        a: u64,
        b: u16,
        c: u32,
    }
    #[test]
    fn test_slice_size() {
        dbg!(size_of::<Option<AtomicPtr<u8>>>());
        dbg!(size_of::<usize>());
        dbg!(size_of::<Ar>());
    }
    // #[test]
    // fn test_round() {
    //     let p = &b"hello world"[..];
    //     let k = &b"rust nb"[..];
    //     let mut arena = Arena::new(100);
    //     let slice = arena.alloc_slice_copy(p);
    //     let slice_a = arena.alloc_slice_copy(k);
    //     let node = arena.alloc(Node { a: 1, b: 2, c: 3 });
    //     // drop(node);
    //     dbg!(String::from_utf8_lossy(slice.get()));
    //     dbg!(String::from_utf8_lossy(slice_a.get()));
    //     // dbg!(node);
    //     // let k = Layout::for_value(p).align_to(8).unwrap();
    //     // dbg!(k);
    //     // let (size, align) = dbg!((mem::size_of_val(p), mem::align_of_val(p)));
    //     // dbg!(Arena::round_up_to(1000, 8));
    // }
    #[test]
    fn test_offset() {
        let arena = Arena::new(100);
        let keyts = KeyTs::new("abc".into(), 1.into());

        let p = arena.alloc(keyts);
        dbg!(&p);
        // let t = p as *const u8;
        let k = p as *const KeyTs;
        let p=k as *const u8;
        let offset = arena.offset(k);
        dbg!(&offset);
        let p = unsafe { arena.get_mut::<KeyTs>(offset.unwrap()) }.unwrap();
        dbg!(p);

        // Arena::offset(&self, ptr)
    }
    #[test]
    fn test_slice() {
        let s = String::from("12,3,4");
        let slice = s.as_bytes();
        let arena = Arena::new(100);
        let p = arena.alloc_slice_copy(slice);
        let offset = arena.offset_slice(p);
        let k = arena.get_slice::<u8>(offset, slice.len() as u32).unwrap();
        dbg!(String::from_utf8_lossy(k));
    }
    #[tokio::test]
    async fn test_sync() {
        let arena = Arc::new(Arena::new(1000));
        let mut handles = Vec::new();
        for i in 0..3 {
            let arena_clone = arena.clone();
            handles.push(tokio::spawn(async move {
                let s = i.to_string() + "abc";
                let k = KeyTs::new(s.into(), i.into());
                let k_clone = k.clone();
                let offset = arena_clone.offset(arena_clone.alloc(k) as _);
                let p = unsafe { arena_clone.get_mut::<KeyTs>(offset.unwrap()) }.unwrap();
                assert_eq!(*p, k_clone);
            }));
        }
        for ele in handles {
            ele.await;
        }
    }
    #[test]
    fn test_mutl() {
        let arena = Arc::new(Arena::new(1000));
        let mut handles = Vec::new();
        for i in 0..3 {
            let arena_clone = arena.clone();
            // std::thread::spawn(f)
            handles.push(std::thread::spawn(move || {
                let s = i.to_string() + "abc";
                let k = KeyTs::new(s.into(), i.into());
                let k_clone = k.clone();
                let offset = arena_clone.offset(arena_clone.alloc(k) as _);
                let p = unsafe { arena_clone.get_mut::<KeyTs>(offset.unwrap()) }.unwrap();
                assert_eq!(*p, k_clone);
            }));
        }
        for ele in handles {
            ele.join();
        }
    }
}
