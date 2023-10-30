use std::sync::Arc;

use anyhow::bail;
use tokio::sync::RwLock;

#[cfg(feature = "metrics")]
use crate::util::metrics::{add_num_gets, add_num_gets_with_result, add_num_memtable_gets};
use crate::{
    db::DB,
    errors::DBError,
    kv::{KeyTs, Meta, TxnTs, ValueMeta},
    memtable::MemTable,
    txn,
};
impl DB {
    pub(crate) async fn get(&self, key_ts: &KeyTs) -> anyhow::Result<Option<(TxnTs, ValueMeta)>> {
        if self.is_closed() {
            bail!(DBError::DBClosed);
        }
        #[cfg(feature = "metrics")]
        add_num_gets(1);
        let mut max_txn_ts = TxnTs::default();
        let (mut_mem, immut_mem) = self.get_memtable().await;
        if let Some(mem) = mut_mem {
            let mem_r = mem.read().await;
            let v = mem_r.get(key_ts, true);
            drop(mem_r);
            #[cfg(feature = "metrics")]
            add_num_memtable_gets(1);
            if let Some((txn_ts, value_meta)) = v.as_ref() {
                if !value_meta.meta().is_empty() || !value_meta.value().is_empty() {
                    if *txn_ts == key_ts.txn_ts() {
                        #[cfg(feature = "metrics")]
                        add_num_gets_with_result(1);
                        return Ok(v);
                    }
                    max_txn_ts = max_txn_ts.max(*txn_ts);
                }
            }
        }
        for mem in immut_mem {
            let v = mem.get(key_ts, true);
            #[cfg(feature = "metrics")]
            add_num_memtable_gets(1);
            if let Some((txn_ts, value_meta)) = v.as_ref() {
                if value_meta.meta().is_empty() && value_meta.value().is_empty() {
                    continue;
                }
                if *txn_ts == key_ts.txn_ts() {
                    #[cfg(feature = "metrics")]
                    add_num_gets_with_result(1);
                    return Ok(v);
                }
                max_txn_ts = max_txn_ts.max(*txn_ts);
            }
        }
        // todo!();
        // let v = ValueStruct::default();
        // Ok(v)
        Ok(None)
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
