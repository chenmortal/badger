use std::sync::Arc;

use scopeguard::defer;

use crate::{
    closer::Closer,
    db::DB,
    table::{opt::TableOption, write::TableBuilder},
};

use super::memtable::MemTable;

impl DB {
    pub(crate) async fn flush_memtable(self, closer: Closer) {
        defer!(closer.done());
        let mut recv_memtable = self.recv_memtable.lock().await;
        while let Some(memtable) = recv_memtable.recv().await {}
    }
    async fn handle_memtable_flush(
        &self,
        memtable: Arc<MemTable>,
        drop_prefixed: Vec<&[u8]>,
    ) -> anyhow::Result<()> {
        let table_opt =
            TableOption::new(&self.key_registry, &self.block_cache, &self.index_cache).await;
        let skip_list_iter = memtable.skip_list.iter();
        let mut table_builder =
            TableBuilder::build_l0_table(skip_list_iter, Vec::new(), table_opt)?;
        if table_builder.is_empty() {
            let _ = table_builder.finish().await;
            return Ok(());
        }
        
        Ok(())
    }
}
