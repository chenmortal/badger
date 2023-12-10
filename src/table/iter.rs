use crate::{
    iter::{
        DoubleEndedSinkIter, DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSeekIter,
        KvSinkIter, SinkIter, SinkIterator,
    },
    kv::ValueMeta,
};

use super::{read::SinkTableIter, Table};

pub(crate) struct SinkTableConcatIter {
    index: Option<usize>,
    back_index: Option<usize>,
    iters: Vec<Option<SinkTableIter>>,
    tables: Vec<Table>,
    use_cache: bool,
}
impl SinkTableConcatIter {
    pub(crate) fn new(tables: Vec<Table>, use_cache: bool) -> Self {
        let mut iters = Vec::with_capacity(tables.len());
        iters.resize_with(tables.len(), Default::default);
        Self {
            index: None,
            back_index: None,
            iters,
            tables,
            use_cache,
        }
    }
    fn double_ended_eq(&self) -> bool {
        self.key() == self.key_back() && self.value() == self.value_back()
    }
}
impl SinkIter for SinkTableConcatIter {
    type Item = SinkTableIter;

    fn item(&self) -> Option<&Self::Item> {
        if let Some(index) = self.index {
            if let Some(s) = self.iters.get(index) {
                return s.as_ref();
            };
        }
        None
    }
}
impl DoubleEndedSinkIter for SinkTableConcatIter {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        if let Some(index) = self.back_index {
            if let Some(s) = self.iters.get(index) {
                return s.as_ref();
            };
        }
        None
    }
}
impl SinkIterator for SinkTableConcatIter {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        if self.double_ended_eq() {
            return Ok(false);
        }
        let new_index = match self.index {
            Some(index) => {
                if let Some(cur) = self.iters[index].as_mut() {
                    if cur.next()? {
                        return Ok(!self.double_ended_eq());
                    };
                    if index == self.tables.len() - 1 {
                        return Ok(false);
                    }
                    index + 1
                } else {
                    index
                }
            }
            None => {
                if self.tables.len() == 0 {
                    return Ok(false);
                }
                0
            }
        };
        let mut iter = self.tables[new_index].iter(self.use_cache);
        if !iter.next()? {
            return Ok(false);
        };
        self.iters[new_index] = Some(iter);
        Ok(!self.double_ended_eq())
    }
}
impl DoubleEndedSinkIterator for SinkTableConcatIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        if self.double_ended_eq() {
            return Ok(false);
        }
        let new_index = match self.back_index {
            Some(back_index) => {
                if let Some(cur) = self.iters[back_index].as_mut() {
                    if cur.next()? {
                        return Ok(!self.double_ended_eq());
                    };
                    if back_index == 0 {
                        return Ok(false);
                    }
                    0
                } else {
                    back_index
                }
            }
            None => {
                if self.tables.len() == 0 {
                    return Ok(false);
                }
                self.tables.len() - 1
            }
        };
        let mut iter = self.tables[new_index].iter(self.use_cache);
        if !iter.next_back()? {
            return Ok(false);
        };
        self.iters[new_index] = Some(iter);
        Ok(!self.double_ended_eq())
    }
}
impl KvSinkIter<ValueMeta> for SinkTableConcatIter {
    fn key(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        self.item().and_then(|x| x.key())
    }

    fn value(&self) -> Option<ValueMeta> {
        self.item().and_then(|x| x.value())
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for SinkTableConcatIter {
    fn key_back(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        self.item_back().and_then(|x| x.key())
    }

    fn value_back(&self) -> Option<ValueMeta> {
        self.item_back().and_then(|x| x.value())
    }
}
impl KvSeekIter for SinkTableConcatIter {
    fn seek(&mut self, k: crate::kv::KeyTsBorrow<'_>) -> anyhow::Result<bool> {
        let index = match self
            .tables
            .binary_search_by(|t| t.biggest().partial_cmp(&k).unwrap())
        {
            Ok(index) => index,  // t.biggest() ==k
            Err(index) => index, // t.biggest() > k and (t-1).biggest() < k
        };
        if index == self.tables.len() {
            return Ok(false);
        }

        if let Some(cur) = self.iters[index].as_mut() {
            return cur.seek(k);
        } else {
            let mut iter = self.tables[index].iter(self.use_cache);
            if iter.seek(k)? {
                self.index = Some(index);
                self.iters[index] = Some(iter);
                return Ok(true);
            };
            return Ok(false);
        }
    }
}
pub(crate) enum SinkMergeNodeIter {
    Table(SinkTableIter),
    TableConcat(SinkTableConcatIter),
    Merge(Box<SinkMergeIter>),
}
impl From<SinkTableIter> for SinkMergeNodeIter {
    fn from(value: SinkTableIter) -> Self {
        Self::Table(value)
    }
}
impl From<SinkTableConcatIter> for SinkMergeNodeIter {
    fn from(value: SinkTableConcatIter) -> Self {
        Self::TableConcat(value)
    }
}
impl From<SinkMergeIter> for SinkMergeNodeIter {
    fn from(value: SinkMergeIter) -> Self {
        Self::Merge(Box::new(value))
    }
}

impl SinkIterator for SinkMergeNodeIter {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        match self {
            SinkMergeNodeIter::Table(iter) => iter.next(),
            SinkMergeNodeIter::TableConcat(iter) => iter.next(),
            SinkMergeNodeIter::Merge(iter) => iter.next(),
        }
    }
}
impl DoubleEndedSinkIterator for SinkMergeNodeIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        match self {
            SinkMergeNodeIter::Table(iter) => iter.next_back(),
            SinkMergeNodeIter::TableConcat(iter) => iter.next_back(),
            SinkMergeNodeIter::Merge(iter) => iter.next_back(),
        }
    }
}
impl KvSinkIter<ValueMeta> for SinkMergeNodeIter {
    fn key(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        match self {
            SinkMergeNodeIter::Table(iter) => iter.key(),
            SinkMergeNodeIter::TableConcat(iter) => iter.key(),
            SinkMergeNodeIter::Merge(iter) => iter.key(),
        }
    }

    fn value(&self) -> Option<ValueMeta> {
        match self {
            SinkMergeNodeIter::Table(iter) => iter.value(),
            SinkMergeNodeIter::TableConcat(iter) => iter.value(),
            SinkMergeNodeIter::Merge(iter) => iter.value(),
        }
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for SinkMergeNodeIter {
    fn key_back(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        match self {
            SinkMergeNodeIter::Table(iter) => iter.key_back(),
            SinkMergeNodeIter::TableConcat(iter) => iter.key_back(),
            SinkMergeNodeIter::Merge(iter) => iter.key_back(),
        }
    }

    fn value_back(&self) -> Option<ValueMeta> {
        match self {
            SinkMergeNodeIter::Table(iter) => iter.value_back(),
            SinkMergeNodeIter::TableConcat(iter) => iter.value_back(),
            SinkMergeNodeIter::Merge(iter) => iter.value_back(),
        }
    }
}
impl KvSeekIter for SinkMergeNodeIter {
    fn seek(&mut self, k: crate::kv::KeyTsBorrow<'_>) -> anyhow::Result<bool> {
        match self {
            SinkMergeNodeIter::Table(iter) => iter.seek(k),
            SinkMergeNodeIter::TableConcat(iter) => iter.seek(k),
            SinkMergeNodeIter::Merge(iter) => iter.seek(k),
        }
    }
}
struct SinkMergeNode {
    valid: bool,
    valid_back: bool,
    iter: SinkMergeNodeIter,
}

impl<S> From<S> for SinkMergeNode
where
    S: Into<SinkMergeNodeIter>,
{
    fn from(value: S) -> Self {
        Self {
            valid: true,
            valid_back: true,
            iter: value.into(),
        }
    }
}
impl SinkIterator for SinkMergeNode {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        match self.iter.next() {
            Ok(b) => {
                if !b {
                    self.valid = false;
                }
                Ok(b)
            }
            Err(e) => {
                self.valid = false;
                Err(e)
            }
        }
    }
}
impl DoubleEndedSinkIterator for SinkMergeNode {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        match self.iter.next_back() {
            Ok(b) => {
                if !b {
                    self.valid_back = false;
                }
                Ok(b)
            }
            Err(e) => {
                self.valid_back = false;
                Err(e)
            }
        }
    }
}
impl KvSinkIter<ValueMeta> for SinkMergeNode {
    fn key(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        if self.valid {
            self.iter.key()
        } else {
            None
        }
    }

    fn value(&self) -> Option<ValueMeta> {
        if self.valid {
            self.iter.value()
        } else {
            None
        }
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for SinkMergeNode {
    fn key_back(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        if self.valid_back {
            self.iter.key_back()
        } else {
            None
        }
    }

    fn value_back(&self) -> Option<ValueMeta> {
        if self.valid_back {
            self.iter.value_back()
        } else {
            None
        }
    }
}
impl KvSeekIter for SinkMergeNode {
    fn seek(&mut self, k: crate::kv::KeyTsBorrow<'_>) -> anyhow::Result<bool> {
        self.iter.seek(k)
    }
}

pub(crate) struct SinkMergeIter {
    left: SinkMergeNode,
    right: Option<SinkMergeNode>,
    temp_key: Vec<u8>,
    back_temp_key: Vec<u8>,
    left_small: bool, // if true small is left node else small is right node
    back_left_big: bool,
}

impl SinkMergeIter {
    pub(crate) fn new(mut iters: Vec<SinkMergeNodeIter>) -> Option<Self> {
        let new = |left: SinkMergeNode, right: Option<SinkMergeNode>| Self {
            left,
            right,
            temp_key: vec![],
            back_temp_key: vec![],
            left_small: true,
            back_left_big: true,
        };
        match iters.len() {
            0 => None,
            1 => new(iters.pop().unwrap().into(), None).into(),
            2 => {
                let right = iters.pop().unwrap().into();
                let left = iters.pop().unwrap().into();
                new(left, Some(right)).into()
            }
            len => {
                let mid = len / 2;
                let right = iters.drain(mid..).collect::<Vec<_>>();
                let left = iters;
                new(
                    Self::new(left).unwrap().into(),
                    Some(Self::new(right).unwrap().into()),
                )
                .into()
            }
        }
    }

    fn smaller(&self) -> &SinkMergeNode {
        if self.left_small {
            &self.left
        } else {
            self.right.as_ref().unwrap()
        }
    }
    fn smaller_mut(&mut self) -> &mut SinkMergeNode {
        if self.left_small {
            &mut self.left
        } else {
            self.right.as_mut().unwrap()
        }
    }
    fn back_smaller(&self) -> &SinkMergeNode {
        if self.back_left_big {
            self.right.as_ref().unwrap()
        } else {
            &self.left
        }
    }
    fn back_smaller_mut(&mut self) -> &mut SinkMergeNode {
        if self.back_left_big {
            self.right.as_mut().unwrap()
        } else {
            &mut self.left
        }
    }
    fn bigger(&self) -> &SinkMergeNode {
        if self.left_small {
            self.right.as_ref().unwrap()
        } else {
            &self.left
        }
    }
    fn bigger_mut(&mut self) -> &mut SinkMergeNode {
        if self.left_small {
            self.right.as_mut().unwrap()
        } else {
            &mut self.left
        }
    }
    fn back_bigger(&self) -> &SinkMergeNode {
        if self.back_left_big {
            &self.left
        } else {
            self.right.as_ref().unwrap()
        }
    }
    fn back_bigger_mut(&mut self) -> &mut SinkMergeNode {
        if self.back_left_big {
            &mut self.left
        } else {
            self.right.as_mut().unwrap()
        }
    }
}
impl SinkIterator for SinkMergeIter {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        while self.smaller().valid {
            if let Some(k) = self.smaller().key() {
                if self.temp_key.as_slice() != k.as_ref() {
                    if !self.back_temp_key.is_empty() && self.back_temp_key == k.as_ref() {
                        return Ok(false);
                    }
                    self.temp_key = k.to_vec();
                    return Ok(true);
                }
            }

            let result = self.smaller_mut().next()?;
            if self.bigger().valid {
                if result {
                    if self.bigger().key().is_none() && !self.bigger_mut().next()? {
                        continue;
                    }
                    match self.smaller().key().cmp(&self.bigger().key()) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => {
                            self.bigger_mut().next()?;
                        }
                        std::cmp::Ordering::Greater => {
                            self.left_small = !self.left_small;
                        }
                    };
                } else {
                    self.left_small = !self.left_small;
                }
            }
        }
        Ok(false)
    }
}
impl DoubleEndedSinkIterator for SinkMergeIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        while self.back_bigger().valid {
            if let Some(k) = self.back_bigger().key_back() {
                if self.back_temp_key.as_slice() == k.as_ref() {
                    if !self.temp_key.is_empty() && self.temp_key == k.as_ref() {
                        return Ok(false);
                    }
                    self.back_temp_key = k.to_vec();
                    return Ok(true);
                }
            }
            let result = self.back_bigger_mut().next_back()?;
            if self.back_smaller().valid {
                if result {
                    if self.back_smaller().key_back().is_none()
                        && !self.back_smaller_mut().next_back()?
                    {
                        continue;
                    }
                    match self
                        .back_bigger()
                        .key_back()
                        .cmp(&self.back_smaller().key_back())
                    {
                        std::cmp::Ordering::Less => {
                            self.back_left_big = !self.back_left_big;
                        }
                        std::cmp::Ordering::Equal => {
                            self.back_smaller_mut().next_back()?;
                        }
                        std::cmp::Ordering::Greater => {}
                    }
                } else {
                    self.back_left_big = !self.back_left_big;
                }
            }
        }
        Ok(false)
    }
}
impl KvSinkIter<ValueMeta> for SinkMergeIter {
    fn key(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        self.smaller().key()
    }

    fn value(&self) -> Option<ValueMeta> {
        self.smaller().value()
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for SinkMergeIter {
    fn key_back(&self) -> Option<crate::kv::KeyTsBorrow<'_>> {
        self.back_bigger().key_back()
    }

    fn value_back(&self) -> Option<ValueMeta> {
        self.back_bigger().value_back()
    }
}
impl KvSeekIter for SinkMergeIter {
    fn seek(&mut self, k: crate::kv::KeyTsBorrow<'_>) -> anyhow::Result<bool> {
        let l = self.left.seek(k)?;
        let r = match self.right.as_mut() {
            Some(r) => r.seek(k)?,
            None => false,
        };
        if self.bigger().valid {
            if self.smaller().valid {
                self.left_small = !self.left_small
            } else {
                if self.bigger().key().is_none() {
                    self.bigger_mut().next()?;
                }
                if self.bigger().key().is_some() {
                    match self.smaller().key().cmp(&self.bigger().key()) {
                        std::cmp::Ordering::Less => {}
                        std::cmp::Ordering::Equal => {
                            self.bigger_mut().next()?;
                        }
                        std::cmp::Ordering::Greater => {
                            self.left_small = !self.left_small;
                        }
                    };
                }
            }
        }
        if l || r {
            self.temp_key = self.smaller().key().unwrap().to_vec();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
