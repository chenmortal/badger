use std::{
    alloc::System,
    collections::{HashMap, HashSet},
    hash::BuildHasher,
    sync::{atomic::AtomicU32, Arc},
    time::SystemTime,
};

use anyhow::anyhow;
use anyhow::bail;
use libc::EXTPROC;
use tokio::sync::Mutex;

use crate::{
    db::DB, errors::DBError, kv::KeyTs, txn::{HASH, BADGER_PREFIX}, util::now_since_unix, value::BIT_DELETE,
};

use super::{
    entry::Entry,
    item::{self, Item, ItemInner, PRE_FETCH_STATUS},
    TxnTs, TXN_KEY,
};

pub struct Txn {
    read_ts: TxnTs,
    commit_ts: u64,
    size: usize,
    count: usize,
    db: DB,
    conflict_keys: Option<HashSet<u64>>,
    reads: Mutex<Vec<u64>>,
    pending_writes: Option<HashMap<Vec<u8>, Arc<Entry>>>, // Vec<u8> -> String
    duplicate_writes: Vec<Arc<Entry>>,
    num_iters: AtomicU32,
    discarded: bool,
    done_read: bool,
    update: bool,
}
impl Txn {
    pub(super) async fn new(db: DB, mut update: bool, is_managed: bool) -> anyhow::Result<Self> {
        if db.opt.read_only && update {
            update = false;
        }

        let s = Self {
            read_ts: if !is_managed {
                db.oracle.get_latest_read_ts().await?
            } else {
                TxnTs(0)
            },
            commit_ts: 0,
            size: TXN_KEY.len() + 10,
            count: 1,

            conflict_keys: if update && db.opt.detect_conflicts {
                Some(HashSet::new())
            } else {
                None
            },

            reads: Mutex::new(Vec::new()),
            pending_writes: if update { HashMap::new().into() } else { None },
            duplicate_writes: Default::default(),
            num_iters: Default::default(),
            discarded: false,
            done_read: false,
            db,
            update,
        };
        Ok(s)
    }
    pub async fn get(&self, key: &[u8]) -> anyhow::Result<Item> {
        if key.len() == 0 {
            bail!(DBError::EmptyKey);
        } else if self.discarded {
            bail!(DBError::DiscardedTxn);
        }
        self.db.is_banned(key).await?;
        let mut item_inner = ItemInner::default();
        if self.update {
            match self.pending_writes.as_ref().unwrap().get(key) {
                Some(e) => {
                    if e.key() == key {
                        if is_deleted_or_expired(e.meta(), e.expires_at()) {
                            bail!(DBError::KeyNotFound);
                        };
                        item_inner.meta = e.meta();
                        item_inner.val = e.value().to_vec();
                        item_inner.user_meta = e.user_meta();
                        item_inner.key = key.to_vec();
                        item_inner.status = PRE_FETCH_STATUS;
                        item_inner.version = self.read_ts;
                        item_inner.expires_at = e.expires_at();
                        return Ok(item_inner.into());
                    }
                }
                None => {}
            };
            let hash = HASH.hash_one(key);
            let mut reads_m = self.reads.lock().await;
            reads_m.push(hash);
            drop(reads_m);
        }
        let seek = KeyTs::new(key, self.read_ts);

        // todo!()
        let value_struct = self
            .db
            .get_value(&seek)
            .await
            .map_err(|e| anyhow!("DB::Get key: {:?} for {}", &key, e))?;

        if value_struct.value().is_empty() && value_struct.meta() == 0 {
            bail!(DBError::KeyNotFound)
        }
        if is_deleted_or_expired(value_struct.meta(), value_struct.expires_at()) {
            bail!(DBError::KeyNotFound)
        }

        item_inner.key = key.to_vec();
        item_inner.version = value_struct.version();
        item_inner.meta = value_struct.meta();
        item_inner.user_meta = value_struct.user_meta();
        item_inner.vptr = value_struct.value().clone();
        item_inner.expires_at = value_struct.expires_at();
        item_inner.db = self.db.clone().into();
        Ok(item_inner.into())
    }
    pub fn set(&self, key: &[u8], value: &[u8]) {
        self.set_entry(Arc::new(Entry::new(key, value)));
    }
    pub fn delete(&self, key: &[u8]) {
        let mut e = Entry::default();
        e.set_key(key.to_vec());
        e.set_meta(BIT_DELETE);
        self.set_entry(Arc::new(e));
    }
    pub fn set_entry(&self, e: Arc<Entry>) {
        self.modify(e);
    }
    fn modify(&self, e: Arc<Entry>) ->anyhow::Result<()>{
        const MAX_KEY_SIZE:usize=65000;
        if !self.update {
            bail!(DBError::ReadOnlyTxn)
        }
        if self.discarded {
            bail!(DBError::DiscardedTxn)
        }
        if e.key().is_empty(){
            bail!(DBError::EmptyKey)
        }
        if e.key().starts_with(BADGER_PREFIX) {
            bail!(DBError::InvalidKey)
        }
        
        Ok(())
    }
}

#[inline]
fn is_deleted_or_expired(meta: u8, expires_at: u64) -> bool {
    if meta & BIT_DELETE > 0 {
        return true;
    }
    if expires_at == 0 {
        return false;
    }
    expires_at <= now_since_unix().as_secs()
}
