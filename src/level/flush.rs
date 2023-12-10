use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use log::{error, info};
use scopeguard::defer;

use crate::{
    db::DB,
    memtable::MemTable,
    pb::badgerpb4::ManifestChange,
    table::{write::TableBuilder, Table},
    util::closer::Closer,
    util::DBFileId,
};

use super::{level_handler::LevelHandler, levels::LevelsControllerInner};

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
        _drop_prefixed: Vec<&[u8]>,
    ) -> anyhow::Result<()> {
        let cipher = self.key_registry.latest_cipher().await?;
        let table_opt = self.opt.table.clone();
        let skip_list_iter = memtable.skip_list.iter();
        let mut table_builder =
            TableBuilder::build_l0_table(skip_list_iter, Vec::new(), table_opt, cipher)?;
        if table_builder.is_empty() {
            let _ = table_builder.finish().await;
            return Ok(());
        }
        let file_id = self.level_controller.get_reserve_file_id();
        let file_path = file_id.join_dir(self.opt.level_controller.dir());
        let table = table_builder
            .build(
                file_path,
                self.index_cache.clone(),
                self.block_cache.clone(),
            )
            .await?;
        self.level_controller.push_level0_table(table).await?;
        Ok(())
    }
}
impl LevelsControllerInner {
    async fn push_level0_table(&self, table: Table) -> anyhow::Result<()> {
        self.manifest().push_changes(vec![ManifestChange::new(
            table.table_id(),
            0,
            table
                .cipher()
                .and_then(|x| x.cipher_key_id().into())
                .unwrap_or_default(),
            table.config().compression(),
        )])?;

        let handler = &(self.levels()[0]);

        let level0_tables_stall = self.level_config().num_level_zero_tables_stall();
        async fn push_level0(
            handler: &LevelHandler,
            table: &Table,
            level0_tables_stall: usize,
        ) -> bool {
            assert!(handler.level() == 0.into());
            let mut handler_w = handler.write().await;
            if handler_w.tables.len() >= level0_tables_stall {
                drop(handler_w);
                return false;
            };
            handler_w.tables.push(table.clone());
            handler_w.total_size += table.size();
            handler_w.total_stale_size = table.stale_data_size();
            drop(handler_w);
            true
        }

        while !push_level0(handler, &table, level0_tables_stall).await {
            let start = Instant::now();
            while handler.get_tables_len().await >= level0_tables_stall {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let dur = start.elapsed();
            if dur.as_secs() > 1 {
                info!("Level 0 was stalled for {}", dur.as_millis());
            }
            self.level_0_stalls_ms()
                .fetch_add(dur.as_millis() as u64, Ordering::Relaxed);
        }
        Ok(())
    }
}
