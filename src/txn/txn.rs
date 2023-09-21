use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::anyhow;
use anyhow::bail;
use tokio::sync::{Mutex, Notify};

use crate::{
    db::DB,
    errors::DBError,
    kv::KeyTs,
    txn::{BADGER_PREFIX, HASH},
    util::now_since_unix,
    value::{BIT_DELETE, BIT_FIN_TXN, BIT_TXN},
};

use super::{
    entry::{DecEntry, Entry},
    item::{Item, ItemInner, PRE_FETCH_STATUS},
    TxnTs, TXN_KEY,
};

pub struct Txn {
    pub(super) read_ts: TxnTs,
    pub(super) commit_ts: TxnTs,
    size: usize,
    count: usize,
    db: DB,
    pub(super) conflict_keys: Option<HashSet<u64>>,
    pub(super) read_key_hash: Mutex<Vec<u64>>,
    pending_writes: Option<HashMap<Vec<u8>, DecEntry>>, // Vec<u8> -> String
    duplicate_writes: Vec<DecEntry>,
    // num_iters: AtomicU32,
    discarded: bool,
    pub(super) done_read: AtomicBool,
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
                TxnTs::default()
            },
            commit_ts: TxnTs::default(),
            size: TXN_KEY.len() + 10,
            count: 1, // One extra entry for BitFin.

            conflict_keys: if update && db.opt.detect_conflicts {
                Some(HashSet::new())
            } else {
                None
            },

            read_key_hash: Mutex::new(Vec::new()),
            pending_writes: if update { HashMap::new().into() } else { None },
            duplicate_writes: Default::default(),
            // num_iters: Default::default(),
            discarded: false,
            done_read: AtomicBool::new(false),
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
            let mut reads_m = self.read_key_hash.lock().await;
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
    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.set_entry(Entry::new(key, value)).await
    }
    pub async fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        let mut e = Entry::default();
        e.set_key(key.to_vec());
        e.set_meta(BIT_DELETE);
        self.set_entry(e).await
    }
    pub async fn set_entry(&mut self, e: Entry) -> anyhow::Result<()> {
        self.modify(e.into()).await
    }
    #[inline]
    async fn modify(&mut self, e: DecEntry) -> anyhow::Result<()> {
        let exceeds_size = |prefix: &str, max: usize, key: &[u8]| {
            bail!(
                "{} with size {} exceeded {} limit. {}:\n{:?}",
                prefix,
                key.len(),
                max,
                prefix,
                if key.len() > 1024 { &key[..1024] } else { key }
            )
        };

        let mut check_size = |e: &DecEntry| {
            let count = self.count + 1;
            e.set_value_threshold(self.db.opt.value_threshold);
            let size = self.size + e.estimate_size() + 10;
            if count >= self.db.opt.max_batch_count || size >= self.db.opt.max_batch_size {
                bail!(DBError::TxnTooBig)
            }
            self.count = count;
            self.size = size;
            Ok(())
        };

        const MAX_KEY_SIZE: usize = 65000;
        if !self.update {
            bail!(DBError::ReadOnlyTxn)
        }
        if self.discarded {
            bail!(DBError::DiscardedTxn)
        }
        if e.key().is_empty() {
            bail!(DBError::EmptyKey)
        }
        if e.key().starts_with(BADGER_PREFIX) {
            bail!(DBError::InvalidKey)
        }
        if e.key().len() > MAX_KEY_SIZE {
            exceeds_size("Key", MAX_KEY_SIZE, e.key())?;
        }
        if e.value().len() > self.db.opt.valuelog_file_size {
            exceeds_size("Value", self.db.opt.valuelog_file_size, e.value())?
        }
        self.db.is_banned(&e.key()).await?;

        check_size(&e)?;

        if let Some(c) = self.conflict_keys.as_mut() {
            c.insert(HASH.hash_one(e.key()));
        }

        if let Some(p) = self.pending_writes.as_mut() {
            let new_version = e.version();
            if let Some(old) = p.insert(e.key().to_vec(), e) {
                if old.version() != new_version {
                    self.duplicate_writes.push(old);
                }
            };
        }
        Ok(())
    }
    pub async fn commit(&mut self) -> anyhow::Result<()> {
        match self.pending_writes.as_ref() {
            Some(s) => {
                if s.len() == 0 {
                    return Ok(());
                }
            }
            None => {
                return Ok(());
            }
        };
        self.commit_pre_check()?;
        let (commit_ts, notify) = self.commit_and_send().await?;
        notify.notified().await;
        self.db.oracle.done_commit(commit_ts).await?;
        Ok(())
    }
    fn commit_pre_check(&self) -> anyhow::Result<()> {
        if self.discarded {
            bail!("Trying to commit a discarded txn")
        }
        let mut keep_togther = true;
        if let Some(s) = self.pending_writes.as_ref() {
            for (_, e) in s {
                if e.version() != TxnTs::default() {
                    keep_togther = false;
                    break;
                }
            }
        }
        if keep_togther && self.db.opt.managed_txns && self.commit_ts == TxnTs::default() {
            bail!("CommitTs cannot be zero. Please use commitat instead")
        }
        Ok(())
    }
    async fn commit_and_send(&mut self) -> anyhow::Result<(TxnTs, Arc<Notify>)> {
        let oracle = &self.db.oracle;
        let _guard = oracle.send_write_req.lock().await;
        let commit_ts = oracle.get_latest_commit_ts(self).await?;
        let mut keep_together = true;
        let mut set_version = |e: &mut DecEntry| {
            if e.version() == TxnTs::default() {
                e.set_version(commit_ts)
            } else {
                keep_together = false;
            }
        };

        let mut pending_wirtes_len = 0;
        if let Some(p) = self.pending_writes.as_mut() {
            pending_wirtes_len = p.len();
            p.iter_mut().map(|x| x.1).for_each(&mut set_version);
        }
        let duplicate_writes_len = self.duplicate_writes.len();
        self.duplicate_writes.iter_mut().for_each(set_version);

        //read from pending_writes and duplicate_writes to Vec<>
        let mut entries = Vec::with_capacity(pending_wirtes_len + duplicate_writes_len + 1);
        let mut process_entry = |mut e: DecEntry| {
            if keep_together {
                *e.meta_mut() |= BIT_TXN;
            }
            entries.push(e);
        };

        if let Some(p) = self.pending_writes.as_mut() {
            p.drain()
                .take(pending_wirtes_len)
                .for_each(|(_, e)| process_entry(e));
        }
        self.duplicate_writes
            .drain(0..)
            .take(duplicate_writes_len)
            .for_each(|e| process_entry(e));

        if keep_together {
            debug_assert!(commit_ts != TxnTs::default());
            let mut entry = Entry::new(TXN_KEY, commit_ts.to_u64().to_string().as_bytes());
            entry.set_version(commit_ts);
            entry.set_meta(BIT_FIN_TXN);
            entries.push(entry.into())
        }

        let notify = match self
            .db
            .send_entires_to_write_channel(entries, self.size)
            .await
        {
            Ok(n) => n,
            Err(e) => {
                oracle.done_commit(commit_ts).await?;
                bail!(e)
            }
        };

        Ok((commit_ts, notify))
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

