pub(crate) mod entry;
mod item;
pub(crate) mod oracle;
mod txn;
mod water_mark;

use ahash::RandomState;
use anyhow::bail;
use rand::{thread_rng, Rng};

use crate::{db::DB, errors::DBError, options::Options};

use self::txn::Txn;
/// Prefix for internal keys used by badger.
const BADGER_PREFIX: &[u8] = b"!badger!";
/// For indicating end of entries in txn.
const TXN_KEY: &[u8] = b"!badger!txn";
/// For storing the banned namespaces.
const BANNED_NAMESPACES_KEY: &[u8] = b"!badger!banned";


lazy_static! {
    pub(crate) static ref HASH: RandomState = ahash::RandomState::with_seed(thread_rng().gen());
}
///this means TransactionTimestamp
// pub type TxnTs=u64;
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnTs(u64);
impl TxnTs {
    #[inline(always)]
    pub(crate) fn sub_one(&self) -> Self {
        Self(self.0 - 1)
    }
    #[inline(always)]
    pub(crate) fn add_one_mut(&mut self) {
        self.0 += 1;
    }
    #[inline(always)]
    pub(crate) fn to_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for TxnTs {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

//Transaction
impl DB {
    // pub(crate) async fn update<F>(&self, mut f: F) -> anyhow::Result<()>
    // where
    //     F: FnMut(&mut Txn),
    // {
    //     if self.is_closed() {
    //         bail!(DBError::DBClosed);
    //     }
    //     // let p = self.clone();
    //     if self.opt.managed_txns {
    //         panic!("Update can only be used with managed_txns=false");
    //     }
    //     let mut txn = Txn::new(self.clone(), true, false).await?;
    //     f(&mut txn);
    //     // let mut v = vec![1,2];;
    //     // v.binary_search_by(f);
    //     Ok(())
    // }
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
