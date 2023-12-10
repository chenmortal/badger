use crate::{
    iter::{
        DoubleEndedSinkIter, DoubleEndedSinkIterator, KvDoubleEndedSinkIter, KvSeekIter,
        KvSinkIter, SinkIter, SinkIterator,
    },
    kv::{KeyTsBorrow, ValueMeta},
};

use super::{Block, EntryHeader, TableInner, HEADER_SIZE, Table};
pub(crate) struct SinkTableIter {
    inner: Table,
    use_cache: bool,
    block_iter: Option<SinkBlockIter>,
    back_block_iter: Option<SinkBlockIter>,
}
impl Table {
    pub(crate) fn iter(&self, use_cache: bool) -> SinkTableIter {
        SinkTableIter {
            inner: self.clone(),
            use_cache,
            block_iter: None,
            back_block_iter: None,
        }
    }
}
impl SinkIter for SinkTableIter {
    type Item = SinkBlockIter;

    fn item(&self) -> Option<&Self::Item> {
        self.block_iter.as_ref()
    }
}
impl DoubleEndedSinkIter for SinkTableIter {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.back_block_iter.as_ref()
    }
}
impl SinkTableIter {
    fn double_ended_eq(&self) -> bool {
        if let Some(iter) = self.block_iter.as_ref() {
            if let Some(back_iter) = self.back_block_iter.as_ref() {
                if iter.key() == back_iter.key_back() && iter.value() == back_iter.value_back() {
                    return true;
                }
            }
        }
        return false;
    }
}
impl SinkIterator for SinkTableIter {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        if self.double_ended_eq() {
            return Ok(false);
        }
        let new_block_index = match self.block_iter.as_mut() {
            Some(iter) => {
                if iter.next()? {
                    return Ok(!self.double_ended_eq());
                }
                let block_index: usize = iter.inner.block_index.into();
                if block_index == self.inner.block_offsets_len() - 1 {
                    return Ok(false);
                }
                (block_index + 1).into()
            }
            None => {
                if self.inner.block_offsets_len() == 0 {
                    return Ok(false);
                }
                0u32.into()
            }
        };
        let next_block = self.inner.get_block(new_block_index, self.use_cache)?;
        self.block_iter = next_block.iter().into();
        if self.block_iter.as_mut().unwrap().next()? {
            return Ok(!self.double_ended_eq());
        } else {
            return Ok(false);
        };
    }
}
impl DoubleEndedSinkIterator for SinkTableIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        if self.double_ended_eq() {
            return Ok(false);
        }
        let new_block_index = match self.back_block_iter.as_mut() {
            Some(back_iter) => {
                if back_iter.next_back()? {
                    return Ok(!self.double_ended_eq());
                }
                let block_index: usize = back_iter.inner.block_index.into();
                if block_index == 0 {
                    return Ok(false);
                }
                (block_index - 1).into()
            }
            None => {
                if self.inner.block_offsets_len() == 0 {
                    return Ok(false);
                }
                (self.inner.block_offsets_len() - 1).into()
            }
        };
        let block = self.inner.get_block(new_block_index, self.use_cache)?;
        self.back_block_iter = block.iter().into();
        if self.back_block_iter.as_mut().unwrap().next_back()? {
            return Ok(!self.double_ended_eq());
        } else {
            return Ok(false);
        };
    }
}
impl KvSinkIter<ValueMeta> for SinkTableIter {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        if let Some(iter) = self.block_iter.as_ref() {
            return iter.key();
        }
        None
    }

    fn value(&self) -> Option<ValueMeta> {
        if let Some(iter) = self.block_iter.as_ref() {
            return iter.value();
        }
        None
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for SinkTableIter {
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        if let Some(back_iter) = self.back_block_iter.as_ref() {
            return back_iter.key_back();
        }
        None
    }

    fn value_back(&self) -> Option<ValueMeta> {
        if let Some(back_iter) = self.back_block_iter.as_ref() {
            return back_iter.value_back();
        }
        None
    }
}
impl KvSeekIter for SinkTableIter {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> anyhow::Result<bool> {
        let index_buf = self.inner.get_index()?;
        let search = index_buf
            .offsets()
            .binary_search_by(|b| b.key_ts().partial_cmp(&k).unwrap());
        let index = match search {
            Ok(index) => index,
            Err(index) => {
                if index >= index_buf.offsets().len() {
                    return Ok(false);
                }
                index - 1
            }
        };
        let next_block = self.inner.get_block(index.into(), self.use_cache)?;
        self.block_iter = next_block.iter().into();
        match self.block_iter.as_mut() {
            Some(block) => {
                return block.seek(k);
            }
            None => {
                unreachable!()
            }
        }
    }
}
pub(crate) struct SinkBlockIter {
    inner: Block,
    base_key: Vec<u8>,
    key: Vec<u8>,
    header: EntryHeader,
    entry_index: Option<usize>,

    back_key: Vec<u8>,
    back_header: EntryHeader,
    back_entry_index: Option<usize>,
}
impl From<Block> for SinkBlockIter {
    fn from(value: Block) -> Self {
        Self {
            inner: value,
            base_key: Default::default(),
            key: Default::default(),
            header: Default::default(),
            entry_index: None,
            back_key: Default::default(),
            back_header: Default::default(),
            back_entry_index: None,
        }
    }
}

impl SinkIter for SinkBlockIter {
    type Item = usize;

    fn item(&self) -> Option<&Self::Item> {
        self.entry_index.as_ref()
    }
}

impl DoubleEndedSinkIter for SinkBlockIter {
    fn item_back(&self) -> Option<&<Self as SinkIter>::Item> {
        self.back_entry_index.as_ref()
    }
}
impl SinkBlockIter {
    fn set_entry_index(&mut self, entry_index: usize) {
        self.entry_index = entry_index.into();
        let entry_offset = self.inner.entry_offsets[entry_index] as usize;
        let data = &self.inner.data()[entry_offset..];
        let next_header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
        let prev_overlap = self.header.get_overlap();
        let next_overlap = next_header.get_overlap();
        if next_overlap > prev_overlap {
            self.key.truncate(prev_overlap);
            self.key
                .extend_from_slice(&self.base_key[prev_overlap..next_overlap]);
        } else {
            self.key.truncate(next_overlap);
        }
        self.key
            .extend_from_slice(&data[HEADER_SIZE..HEADER_SIZE + next_header.get_diff()]);
        self.header = next_header;
    }
}
//base key 123 1  iter.key=null
//123 100
//123 121  pre_overlap=6 overlap:4 -> iter.key=123 1;  diffkey=21  -> iter.key=123 121 (just create iter, and may not seek to  start , so also pre_overlap==0)
//123 122  pre_overlap=4 overlap:5 -> iter.key=123 12; diffkey=2   -> iter.key=123 122
//123 211  pre_overlap=5 overlap:3 -> iter.key=123  ;  diffkey=211 -> iter.key=123 211
impl SinkIterator for SinkBlockIter {
    fn next(&mut self) -> Result<bool, anyhow::Error> {
        match self.entry_index {
            Some(id) => {
                match self.back_entry_index {
                    Some(back_id) => {
                        if id + 1 == back_id {
                            return Ok(false);
                        }
                    }
                    None => {
                        if id == self.inner.entry_offsets.len() - 1 {
                            return Ok(false);
                        }
                    }
                }
                self.set_entry_index(id + 1);
                return Ok(true);
            }
            None => {
                if self.inner.entry_offsets.len() == 0 {
                    return Ok(false);
                }

                if self.base_key.len() == 0 {
                    let data = self.inner.data();
                    let header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                    self.base_key = data[HEADER_SIZE..HEADER_SIZE + header.get_diff()].to_vec();
                    self.header = header;
                }
                self.key = self.base_key.to_vec();
                self.entry_index = 0.into();
                return Ok(true);
            }
        }
    }
}
impl DoubleEndedSinkIterator for SinkBlockIter {
    fn next_back(&mut self) -> Result<bool, anyhow::Error> {
        match self.back_entry_index {
            Some(back_id) => {
                match self.entry_index {
                    Some(id) => {
                        if back_id - 1 == id {
                            return Ok(false);
                        }
                    }
                    None => {
                        if back_id == 0 {
                            return Ok(false);
                        }
                    }
                }

                self.back_entry_index = Some(back_id - 1);
                let next_back_entry_offset = self.inner.entry_offsets[back_id - 1] as usize;
                let data = &self.inner.data()[next_back_entry_offset..];
                let next_back_header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                let prev_back_overlap = self.back_header.get_overlap();
                let next_back_overlap = next_back_header.get_overlap();

                if next_back_overlap > prev_back_overlap {
                    self.back_key.truncate(prev_back_overlap);
                    self.back_key
                        .extend_from_slice(&self.base_key[prev_back_overlap..next_back_overlap]);
                } else {
                    self.back_key.truncate(next_back_overlap);
                }
                self.back_key.extend_from_slice(
                    &data[HEADER_SIZE..HEADER_SIZE + next_back_header.get_diff()],
                );

                self.back_header = next_back_header;
                return Ok(true);
            }
            None => {
                if self.inner.entry_offsets.len() == 0 {
                    return Ok(false);
                }

                if self.base_key.len() == 0 {
                    let data = self.inner.data();
                    let header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                    self.base_key = data[HEADER_SIZE..HEADER_SIZE + header.get_diff()].to_vec();
                    self.header = header;
                }

                let last_offset = *self.inner.entry_offsets.last().unwrap() as usize;
                let data = &self.inner.data()[last_offset..];
                self.back_header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
                self.back_key = self.base_key[..self.back_header.get_overlap()].to_vec();
                self.back_key.extend_from_slice(
                    &data[HEADER_SIZE..HEADER_SIZE + self.back_header.get_diff()],
                );
                self.back_entry_index = Some(self.inner.entry_offsets.len() - 1);
                return Ok(true);
            }
        }
    }
}

impl KvSinkIter<ValueMeta> for SinkBlockIter {
    fn key(&self) -> Option<KeyTsBorrow<'_>> {
        if self.key.len() == 0 {
            return None;
        }
        return Some(self.key.as_slice().into());
    }

    fn value(&self) -> Option<ValueMeta> {
        if let Some(entry_id) = self.entry_index {
            let next_entry_id = entry_id + 1;
            let end_offset = if next_entry_id == self.inner.entry_offsets.len() {
                self.inner.entries_index_start
            } else {
                self.inner.entry_offsets[next_entry_id] as usize
            };
            let start_offset =
                self.inner.entry_offsets[entry_id] as usize + HEADER_SIZE + self.header.get_diff();
            let value = &self.inner.data()[start_offset..end_offset];
            return ValueMeta::deserialize(value);
        }
        None
    }
}
impl KvDoubleEndedSinkIter<ValueMeta> for SinkBlockIter {
    fn key_back(&self) -> Option<KeyTsBorrow<'_>> {
        if self.back_key.len() == 0 {
            return None;
        }
        return Some(self.back_key.as_slice().into());
    }

    fn value_back(&self) -> Option<ValueMeta> {
        if let Some(back_entry_id) = self.back_entry_index {
            let last_entry_id = back_entry_id + 1;
            let end_offset = if last_entry_id == self.inner.entry_offsets.len() {
                self.inner.entries_index_start
            } else {
                self.inner.entry_offsets[last_entry_id] as usize
            };
            let start_offset = self.inner.entry_offsets[back_entry_id] as usize
                + HEADER_SIZE
                + self.back_header.get_diff();
            let value = &self.inner.data()[start_offset..end_offset];
            return ValueMeta::deserialize(value);
        }
        None
    }
}
impl KvSeekIter for SinkBlockIter {
    fn seek(&mut self, k: KeyTsBorrow<'_>) -> anyhow::Result<bool> {
        if self.entry_index.is_none() {
            if !self.next()? {
                return Ok(false);
            };
        }

        let search = self.inner.entry_offsets.binary_search_by(|offset| {
            let entry_offset = *offset as usize;
            let data = &self.inner.data()[entry_offset..];
            let header = EntryHeader::deserialize(&data[..HEADER_SIZE]);
            if k.len() >= header.get_overlap() {
                if header.get_overlap() > 8 {
                    let split =
                        (header.get_overlap() + header.get_diff() - 8).min(header.get_overlap());
                    match self.base_key[..split].cmp(&k[..split]) {
                        std::cmp::Ordering::Equal => {}
                        ord => return ord,
                    }
                }
            }
            let mut key = vec![0u8; header.get_overlap() + header.get_diff()];
            key[..header.get_overlap()].copy_from_slice(&self.base_key[..header.get_overlap()]);
            key[header.get_overlap()..]
                .copy_from_slice(&data[HEADER_SIZE..HEADER_SIZE + header.get_diff()]);
            KeyTsBorrow::cmp(&key, &k)
        });

        let entry_index = match search {
            Ok(index) => index,
            Err(index) => {
                if index >= self.inner.get_entry_offsets().len() {
                    return Ok(false);
                }
                index
            }
        };
        self.set_entry_index(entry_index);
        return Ok(true);
    }
}
