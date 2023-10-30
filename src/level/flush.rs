use std::{sync::Arc, time::Duration};

use log::error;
use scopeguard::defer;

use crate::{
    util::closer::Closer,
    db::DB,
    default::SSTABLE_FILE_EXT,
    options::Options,
    table::{opt::TableOption, write::TableBuilder},
    util::dir_join_id_suffix, memtable::MemTable,
};

impl DB {
    #[deny(unused)]
    pub(crate) async fn flush_memtable(self, closer: Closer) {
        defer!(closer.done());
        let mut recv_memtable = self.recv_memtable.lock().await;
        while let Some(memtable) = recv_memtable.recv().await {
            loop {
                if let Err(e) = self
                    .handle_memtable_flush(memtable.clone(), Vec::new())
                    .await
                {
                    error!("flushing memtable to disk:{}, retrying", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                let mut immut_w = self.immut_memtable.write().await;
                if let Some(s) = immut_w.pop_front() {
                    assert_eq!(s.wal().fid(), memtable.wal().fid())
                };
                drop(immut_w);
                break;
            }
        }
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
        let file_id = self.level_controller.get_reserve_file_id();
        let file_path = dir_join_id_suffix(Options::dir(), file_id, SSTABLE_FILE_EXT);
        let table = table_builder.build(file_path).await?;
        // todo!();
        Ok(())
    }
}