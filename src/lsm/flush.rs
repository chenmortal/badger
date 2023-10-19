use std::sync::Arc;

use scopeguard::defer;

use crate::{closer::Closer, db::DB, table::opt::TableOption};

use super::memtable::MemTable;

impl DB {
    pub(crate) async fn flush_memtable(self, closer: Closer) {
        defer!(closer.done());
        let mut recv_memtable = self.recv_memtable.lock().await;
        while let Some(memtable) = recv_memtable.recv().await {}
    }
    async fn handle_memtable_flush(&self, memtable: Arc<MemTable>, drop_prefixed: Vec<&[u8]>) {
        let table =
            TableOption::new(&self.key_registry, &self.block_cache, &self.index_cache).await;
        let skip_list_iter = memtable.skip_list.iter();

    }
}
