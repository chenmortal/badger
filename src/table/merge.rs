use crate::iter::Iter;

use super::iter::ConcatIter;
#[derive(Debug)]
struct Node<T: Iter> {
    valid: bool,
    key: Vec<u8>,
    merge: Option<Box<MergeIter<T>>>,
    concat: Option<ConcatIter<T>>,
}
impl<T: Iter> Default for Node<T> {
    fn default() -> Self {
        Self {
            valid: Default::default(),
            key: Default::default(),
            merge: Default::default(),
            concat: Default::default(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct MergeIter<T: Iter> {
    left: Node<T>,
    right: Node<T>,
    small: *mut Node<T>,
    cur_key: Vec<u8>,
    reverse: bool,
}

impl<T: Iter> MergeIter<T> {
    pub(crate) fn new(mut iters: Vec<ConcatIter<T>>, reverse: bool) -> Self {
        debug_assert!(iters.len() >= 2);

        let len = iters.len();
        let mut gen_pair = || {
            let mut right = Node::default();
            let mut left = Node::default();
            right.concat = iters.pop();
            left.concat = iters.pop();
            let mut s = Self {
                left,
                right,
                small: std::ptr::null_mut(),
                cur_key: Vec::new(),
                reverse,
            };
            s.small = &mut s.left as *mut Node<T>;
            s
        };

        match len {
            2 => gen_pair().into(),
            3 => {
                let right_merge = gen_pair();
                let mut right = Node::default();
                right.merge = Some(Box::new(right_merge));
                let mut left = Node::default();
                left.concat = iters.pop();
                let small = &mut left as *mut Node<T>;
                Self {
                    left,
                    right,
                    small,
                    cur_key: Vec::new(),
                    reverse,
                }
            }
            _ => {
                let mid = len / 2;

                let iters2 = iters.split_off(mid);
                let left_m = MergeIter::new(iters, reverse);
                let right_m = MergeIter::new(iters2, reverse);
                let mut left = Node::default();
                let mut right = Node::default();
                left.merge = Some(Box::new(left_m));
                right.merge = Some(Box::new(right_m));
                let mut s = Self {
                    left,
                    right,
                    small: std::ptr::null_mut(),
                    cur_key: Vec::new(),
                    reverse,
                };
                s.small = &mut s.left as *mut Node<T>;
                s
            }
        }
    }
}
