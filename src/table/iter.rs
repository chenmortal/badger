use super::{
    block::Block,
    builder::{Header, HEADER_SIZE},
    Table,
};
use anyhow::bail;

pub(crate) struct TableIter {
    table: Table,
    block_pos: u32,
    block_iter: Option<BlockIter>,
    is_reversed: bool,
    use_cache: bool,
}
pub(crate) struct BlockIter {
    entry_pos: usize,
    base_key: Vec<u8>,
    key: Vec<u8>,
    val: Vec<u8>,
    entry_offsets: Vec<u32>,
    block: Block,

    table_id: u64,
    block_id: u32,

    prev_overlap: usize,
}

impl TableIter {
    #[inline]
    pub(crate) fn new(table: Table, is_reversed: bool, use_cache: bool) -> Self {
        Self {
            table,
            block_pos: 0,
            block_iter: None,
            is_reversed,
            use_cache,
        }
    }
    #[inline]
    pub(crate) async fn rewind(&mut self) -> anyhow::Result<()> {
        if self.is_reversed {
            self.seek_to_last().await
        } else {
            self.seek_to_first().await
        }
    }

    async fn seek_to_first(&mut self) -> anyhow::Result<()> {
        let num_blocks = self.table.0.get_offsets_len();
        if num_blocks == 0 {
            bail!("Block offsets len()==0,so the num of blocks is 0")
        }
        self.block_pos = 0;
        let block = self.table.get_block(self.block_pos, self.use_cache).await?;
        let mut block_iter = BlockIter::new(self.table.get_id(), self.block_pos, block);
        block_iter.seek_to_first()?;
        self.block_iter = block_iter.into();

        Ok(())
    }
    async fn seek_to_last(&mut self) -> anyhow::Result<()> {
        let num_blocks = self.table.0.get_offsets_len();
        if num_blocks == 0 {
            bail!("Block offsets len()==0,so the num of blocks is 0")
        }
        self.block_pos = num_blocks - 1;
        let block = self.table.get_block(self.block_pos, self.use_cache).await?;
        let mut block_iter = BlockIter::new(self.table.get_id(), self.block_pos, block);
        block_iter.seek_to_last()?;

        self.block_iter = block_iter.into();

        Ok(())
    }
    #[inline]
    pub(crate) fn get_key(&self)->Option<&[u8]>{
        if let Some(s) = &self.block_iter {
            let p:&[u8] = s.key.as_ref();
            p.into()
        }else{
            None
        }
    }
}
impl BlockIter {
    pub(crate) fn new(table_id: u64, block_id: u32, block: Block) -> Self {
        Self {
            // data: block.get_actual_data().to_vec(),
            entry_pos: 0,
            base_key: Default::default(),
            key: Default::default(),
            val: Default::default(),
            entry_offsets: block.get_entry_offsets().clone(),
            block,
            table_id,
            block_id,
            prev_overlap: 0,
            // error: None,
        }
    }

    fn seek_to_first(&mut self) -> anyhow::Result<()> {
        self.set_offset(0)
    }
    fn seek_to_last(&mut self) -> anyhow::Result<()> {
        self.set_offset(self.entry_offsets.len() - 1)
    }

    #[inline]
    fn set_offset(&mut self, entry_pos: usize) -> anyhow::Result<()> {
        if entry_pos >= self.entry_offsets.len() {
            bail!(
                "Invalid entry_pos {} >= entry_offsets.len() {}",
                entry_pos,
                self.entry_offsets.len()
            );
        }
        let actual_data = self.block.get_actual_data();

        self.entry_pos = entry_pos;
        let start_offset = self.entry_offsets[entry_pos] as usize;

        if self.base_key.len() == 0 {
            let base_header = Header::deserialize(&actual_data[..HEADER_SIZE]);
            self.base_key = actual_data[HEADER_SIZE..HEADER_SIZE + base_header.get_diff()].to_vec();
        }

        let end_offset = if entry_pos == self.entry_offsets.len() - 1 {
            actual_data.len()
        } else {
            self.entry_offsets[entry_pos + 1] as usize
        };

        let entry_data = &actual_data[start_offset..end_offset];
        //base key 123 1  iter.key=null
        //123 100
        //123 121  pre_overlap=0 overlap:4 -> iter.key=123 1;  diffkey=21  -> iter.key=123 121 (just create iter, and may not seek to  start , so also pre_overlap==0)
        //123 122  pre_overlap=4 overlap:5 -> iter.key=123 12; diffkey=2   -> iter.key=123 122
        //123 211  pre_overlap=5 overlap:3 -> iter.key=123  ;  diffkey=211 -> iter.key=123 211

        let header = Header::deserialize(&entry_data[..HEADER_SIZE]);
        let header_overlap = header.get_overlap();

        if header_overlap > self.prev_overlap {
            self.key.truncate(self.prev_overlap);
            self.key
                .extend_from_slice(&self.base_key[self.prev_overlap..header_overlap]);
        }
        self.prev_overlap = header_overlap;

        let value_offset = HEADER_SIZE + header.get_diff();

        self.key.truncate(header_overlap);
        self.key
            .extend_from_slice(&entry_data[HEADER_SIZE..value_offset]);

        self.val = entry_data[value_offset..].to_vec();
        Ok(())
    }
}