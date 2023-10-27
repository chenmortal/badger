use std::sync::Arc;

use anyhow::bail;
use tokio::sync::RwLock;

use crate::{db::DB, errors::DBError, kv::KeyTs, memtable::MemTable, util::metrics::add_num_gets};

impl DB {
    pub(crate) async fn get(&self, key_ts: &KeyTs) -> anyhow::Result<()> {
        if self.is_closed() {
            bail!(DBError::DBClosed);
        }
        add_num_gets(1);
        let (mut_mem,immut_mem) = self.get_memtable().await;
        if let Some(mem) = mut_mem {
            let mem_r = mem.read().await;
            let p = mem_r.get(key_ts);
            //  mem_r.skip_list.get(&key_ts.serialize());

            // mem_r.skip_list.
        }
        // todo!();
        // let v = ValueStruct::default();
        // Ok(v)
        Ok(())
    }
    pub(crate) async fn get_memtable(&self) -> (Option<Arc<RwLock<MemTable>>>, Vec<Arc<MemTable>>) {
        let mut_memtable = self.memtable.clone();
        let immut_memtables_r = self.immut_memtable.read().await;
        let immut = immut_memtables_r
            .iter()
            .map(|x| x.clone())
            .rev()
            .collect::<Vec<_>>();
        drop(immut_memtables_r);
        (mut_memtable, immut)
    }
}
