mod item;
pub(crate) mod oracle;
mod txn;
mod water_mark;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;

use ahash::RandomState;
use anyhow::anyhow;
use anyhow::bail;
use bytes::Bytes;
use rand::{thread_rng, Rng};

use crate::kv::Entry;
use crate::kv::Meta;
use crate::{db::DB, errors::DBError, kv::KeyTs, options::Options};

use self::{
    item::{Item, ItemInner, PRE_FETCH_STATUS},
    txn::{is_deleted_or_expired, Txn},
};
/// Prefix for internal keys used by badger.
const BADGER_PREFIX: &[u8] = b"!badger!";
/// For indicating end of entries in txn.
const TXN_KEY: &[u8] = b"!badger!txn";
/// For storing the banned namespaces.
const BANNED_NAMESPACES_KEY: &[u8] = b"!badger!banned";

lazy_static! {
    pub(crate) static ref HASH: RandomState = ahash::RandomState::with_seed(thread_rng().gen());
}

//Transaction
type TxnFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>>>>;
impl DB {
    pub async fn update<F>(&self, f: F) -> anyhow::Result<()>
    where
        F: Fn(&mut Txn) -> TxnFuture,
    {
        #[inline]
        async fn run<F>(txn: &mut Txn, f: F) -> anyhow::Result<()>
        where
            F: Fn(&mut Txn) -> TxnFuture,
        {
            f(txn).await?;
            txn.commit().await
        }

        if self.is_closed() {
            bail!(DBError::DBClosed);
        }
        if Options::managed_txns() {
            panic!("Update can only be used with managed_txns=false");
        }
        let mut txn = Txn::new(self.clone(), true, false).await?;
        let result = run(&mut txn, f).await;
        txn.discard().await?;
        result
    }
    pub async fn get_update_txn(&self) -> anyhow::Result<Txn> {
        if self.is_closed() {
            bail!(DBError::DBClosed);
        }
        if Options::managed_txns() {
            panic!("Update can only be used with managed_txns=false");
        }
        let txn = Txn::new(self.clone(), true, false).await?;
        Ok(txn)
    }
}
impl Txn {
    pub async fn get<B: Into<Bytes>>(&self, key: B) -> anyhow::Result<Item> {
        let key: Bytes = key.into();
        if key.len() == 0 {
            bail!(DBError::EmptyKey);
        } else if self.discarded() {
            bail!(DBError::DiscardedTxn);
        }
        self.db().is_banned(&key).await?;
        let mut item_inner = ItemInner::default();
        if self.update() {
            match self.pending_writes().as_ref().unwrap().get(key.as_ref()) {
                Some(e) => {
                    if e.key().as_ref() == key {
                        if e.is_deleted() || e.is_expired() {
                            bail!(DBError::KeyNotFound);
                        }
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
            let hash = HASH.hash_one(key.clone());
            let mut reads_m = self.read_key_hash().lock();
            reads_m.push(hash);
            drop(reads_m);
        }
        let seek = KeyTs::new(key.clone(), self.read_ts);

        let value_struct = self
            .db()
            .get_value(&seek)
            .await
            .map_err(|e| anyhow!("DB::Get key: {:?} for {}", &key, e))?;

        if value_struct.value().is_empty() && value_struct.meta().bits() == 0 {
            bail!(DBError::KeyNotFound)
        }
        if is_deleted_or_expired(value_struct.meta(), value_struct.expires_at()) {
            bail!(DBError::KeyNotFound)
        }

        // item_inner.key = key.to_vec();
        // item_inner.version = value_struct.version();
        // item_inner.meta = value_struct.meta();
        // item_inner.user_meta = value_struct.user_meta();
        // item_inner.vptr = value_struct.value().clone();
        // item_inner.expires_at = value_struct.expires_at();
        // item_inner.db = self.db().clone().into();
        Ok(item_inner.into())
    }
    pub async fn set<B: Into<Bytes>>(&mut self, key: B, value: B) -> anyhow::Result<()> {
        self.set_entry(Entry::new(key.into(), value.into())).await
    }
    pub async fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        let mut e = Entry::default();
        e.set_key(key.to_vec());
        e.set_meta(Meta::DELETE);
        self.set_entry(e).await
    }
    pub async fn set_entry(&mut self, e: Entry) -> anyhow::Result<()> {
        self.modify(e.into()).await
    }
    pub async fn commit(&mut self) -> anyhow::Result<()> {
        match self.pending_writes().as_ref() {
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
        let (commit_ts, recv) = self.commit_and_send().await?;
        let result = recv.await;
        self.db().oracle.done_commit(commit_ts).await?;
        result??;
        Ok(())
    }
    pub async fn discard(&mut self) -> anyhow::Result<()> {
        if self.discarded() {
            return Ok(());
        }
        if self.num_iters().load(Ordering::Acquire) > 0 {
            panic!("Unclosed iterator at time of Txn.discard.")
        }
        self.set_discarded(true);
        if !Options::managed_txns() {
            self.db().oracle.done_read(&self).await?;
        }
        Ok(())
    }
}
