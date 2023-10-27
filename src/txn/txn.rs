use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicBool, AtomicI32},
};

use anyhow::bail;
use parking_lot::Mutex;
use tokio::sync::oneshot::Receiver;

use crate::{
    db::DB,
    errors::DBError,
    kv::{Meta, TxnTs, Entry},
    options::Options,
    txn::{BADGER_PREFIX, HASH},
    util::now_since_unix,
};

use super::TXN_KEY;

pub struct Txn {
    pub(super) read_ts: TxnTs,
    pub(super) commit_ts: TxnTs,
    size: usize,
    count: usize,
    db: DB,
    conflict_keys: Option<HashSet<u64>>,
    read_key_hash: Mutex<Vec<u64>>,
    pending_writes: Option<HashMap<Vec<u8>, Entry>>, // Vec<u8> -> String
    duplicate_writes: Vec<Entry>,
    num_iters: AtomicI32,
    discarded: bool,
    done_read: AtomicBool,
    update: bool,
}
impl Txn {
    pub(super) async fn new(db: DB, mut update: bool, is_managed: bool) -> anyhow::Result<Self> {
        if Options::read_only() && update {
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

            conflict_keys: if update && Options::detect_conflicts() {
                Some(HashSet::new())
            } else {
                None
            },

            read_key_hash: Mutex::new(Vec::new()),
            pending_writes: if update { HashMap::new().into() } else { None },
            duplicate_writes: Default::default(),
            discarded: false,
            done_read: AtomicBool::new(false),
            db,
            update,
            num_iters: AtomicI32::new(0),
        };
        Ok(s)
    }

    #[inline]
    pub(super) async fn modify(&mut self, mut e: Entry) -> anyhow::Result<()> {
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
        let threshold = Options::value_threshold();
        let mut check_size = |e: &mut Entry| {
            let count = self.count + 1;
            e.try_set_value_threshold(threshold);
            let size = self.size + e.estimate_size(threshold) + 10;
            if count >= Options::max_batch_count() as usize
                || size >= Options::max_batch_size() as usize
            {
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
        if e.value().len() > Options::vlog_file_size() {
            exceeds_size("Value", Options::vlog_file_size() as usize, e.value().as_ref())?
        }
        self.db.is_banned(&e.key()).await?;

        check_size(&mut e)?;

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

    pub(super) fn commit_pre_check(&self) -> anyhow::Result<()> {
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
        if keep_togther && Options::managed_txns() && self.commit_ts == TxnTs::default() {
            bail!("CommitTs cannot be zero. Please use commitat instead")
        }
        Ok(())
    }
    pub(super) async fn commit_and_send(
        &mut self,
    ) -> anyhow::Result<(TxnTs, Receiver<anyhow::Result<()>>)> {
        let oracle = &self.db.oracle;
        let _guard = oracle.send_write_req.lock();
        let commit_ts = oracle.get_latest_commit_ts(self).await?;
        let mut keep_together = true;
        let mut set_version = |e: &mut Entry| {
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
        let mut process_entry = |mut e: Entry| {
            if keep_together {
                e.meta_mut().insert(Meta::TXN)
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
            let mut entry = Entry::new(TXN_KEY.into(), commit_ts.to_u64().to_string().into());
            entry.set_version(commit_ts);
            entry.set_meta(Meta::FIN_TXN);
            entries.push(entry.into())
        }

        let receiver = match self
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

        Ok((commit_ts, receiver))
    }

    pub(super) fn discarded(&self) -> bool {
        self.discarded
    }

    pub(super) fn set_discarded(&mut self, discarded: bool) {
        self.discarded = discarded;
    }

    pub(super) fn db(&self) -> &DB {
        &self.db
    }

    pub(super) fn update(&self) -> bool {
        self.update
    }

    pub(super) fn pending_writes(&self) -> Option<&HashMap<Vec<u8>, Entry>> {
        self.pending_writes.as_ref()
    }

    pub(super) fn num_iters(&self) -> &AtomicI32 {
        &self.num_iters
    }

    pub(super) fn done_read(&self) -> &AtomicBool {
        &self.done_read
    }

    pub(super) fn read_key_hash(&self) -> &Mutex<Vec<u64>> {
        &self.read_key_hash
    }

    pub(super) fn conflict_keys(&self) -> Option<&HashSet<u64>> {
        self.conflict_keys.as_ref()
    }
}

#[inline]
pub(super) fn is_deleted_or_expired(meta: Meta, expires_at: u64) -> bool {
    if meta.contains(Meta::DELETE) {
        return true;
    };

    if expires_at == 0 {
        return false;
    }
    expires_at <= now_since_unix().as_secs()
}
