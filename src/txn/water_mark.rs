use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    ops::Deref,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use scopeguard::defer;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
};

use crate::{kv::TxnTs, util::closer::Closer};

use super::{oracle::OracleInner, Txn};

#[derive(Debug, Clone)]
pub(crate) struct WaterMark(Arc<WaterMarkInner>);

impl Deref for WaterMark {
    type Target = WaterMarkInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) struct WaterMarkInner {
    done_until: AtomicU64,
    last_index: AtomicU64,
    sender: Sender<Mark>,
    name: String,
}

#[derive(Debug, Default)]
pub(super) struct Mark {
    txn_ts: TxnTs,
    waiter: Option<Arc<Notify>>,
    indices: Vec<TxnTs>,
    done: bool,
}

impl Mark {
    pub(super) fn new(txn_ts: TxnTs, done: bool) -> Self {
        Self {
            txn_ts,
            waiter: None,
            indices: Vec::new(),
            done,
        }
    }
}

impl WaterMark {
    pub(crate) fn new(name: &str, closer: Closer,done_until:TxnTs) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Mark>(100);
        let water_mark = WaterMark(Arc::new(WaterMarkInner {
            done_until: AtomicU64::new(done_until.to_u64()),
            last_index: AtomicU64::new(0),
            sender,
            name: name.to_owned(),
        }));
        tokio::spawn(water_mark.clone().process(closer, receiver));
        water_mark
    }

    async fn process(self, closer: Closer, mut receiver: Receiver<Mark>) {
        defer!(closer.done());

        let mut waiters = HashMap::<TxnTs, Vec<Arc<Notify>>>::new();
        let mut min_heap = BinaryHeap::<Reverse<TxnTs>>::new();
        let mut pending = HashMap::<TxnTs, isize>::new();

        let mut process_one =
            |txn_ts: TxnTs, done: bool, waiters: &mut HashMap<TxnTs, Vec<Arc<Notify>>>| {
                match pending.get_mut(&txn_ts) {
                    Some(prev) => {
                        *prev += if done { 1 } else { -1 };
                    }
                    None => {
                        min_heap.push(Reverse(txn_ts));
                        pending.insert(txn_ts, if done { 1 } else { -1 });
                    }
                };

                let done_until = self.done_until();
                assert!(
                    done_until <= txn_ts,
                    "Name: {} done_util: {done_until}. txn_ts:{txn_ts}",
                    self.name
                );

                let mut until = done_until;

                while !min_heap.is_empty() {
                    let min = min_heap.peek().unwrap().0;
                    if let Some(done) = pending.get(&min) {
                        if *done < 0 {
                            break;
                        }
                    }
                    min_heap.pop();
                    pending.remove(&min);
                    until = min;
                }

                if until != done_until {
                    // self.done_until cheanged only here and one instance, one process task
                    // so compare_exchange must be ok
                    assert!(self
                        .done_until
                        .compare_exchange(
                            done_until.to_u64(),
                            until.to_u64(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok());
                }
                assert!(done_until <= until);
                if until.to_u64() - done_until.to_u64() <= waiters.len() as u64 {
                    for idx in (done_until.to_u64() + 1)..=until.to_u64() {
                        let txn: TxnTs = idx.into();
                        if let Some(to_notifies) = waiters.get(&txn) {
                            to_notifies.iter().for_each(|x| x.notify_one());
                        };
                        waiters.remove(&txn);
                    }
                } else {
                    let mut need_remove = Vec::with_capacity(waiters.len());
                    for (txn, to_notifies) in waiters.iter() {
                        if *txn <= until {
                            to_notifies.iter().for_each(|x| x.notify_one());
                            need_remove.push(*txn);
                        }
                    }
                    need_remove.iter().for_each(|x| {
                        waiters.remove(x);
                    });
                }
            };

        loop {
            select! {
                _=closer.captured()=>{
                    return ;
                }
                Some(mark)=receiver.recv()=>{
                    match mark.waiter {
                        Some(notify) => {
                            if self.done_until() >= mark.txn_ts {
                                notify.notify_one();
                            } else {
                                match waiters.get_mut(&mark.txn_ts) {
                                    Some(v) => {
                                        v.push(notify);
                                    }
                                    None => {
                                        waiters.insert(mark.txn_ts, vec![notify]);
                                    }
                                };
                            };
                        }
                        None => {
                            if mark.txn_ts > TxnTs::default() {
                                process_one(mark.txn_ts, mark.done, &mut waiters);
                            }
                            for indice in mark.indices {
                                process_one(indice, mark.done, &mut waiters);
                            }
                        }
                    }
                }
            }
        }
    }
}

impl WaterMarkInner {
    pub(crate) async fn begin(&self, txn_ts: TxnTs) -> anyhow::Result<()> {
        self.last_index.store(txn_ts.to_u64(), Ordering::SeqCst);
        self.sender.send(Mark::new(txn_ts, false)).await?;
        Ok(())
    }
    //only for txn_mark
    pub(crate) async fn wait_for_mark(&self, read_ts: TxnTs) -> anyhow::Result<()> {
        //please refer to docs/draw/wait_for_mark.jpg
        if self.done_until() >= read_ts {
            //need to check conflict
            return Ok(());
        }

        //not really commit,need to wait for writing to lsm
        let wait = Arc::new(Notify::new());
        self.sender
            .send(Mark {
                txn_ts: read_ts,
                waiter: wait.clone().into(),
                indices: Vec::new(),
                done: false,
            })
            .await?;

        wait.notified().await;
        Ok(())
    }
    #[inline]
    pub(crate) fn done_until(&self) -> TxnTs {
        self.done_until.load(Ordering::SeqCst).into()
    }
}
impl OracleInner {
    #[inline]
    pub(crate) async fn done_commit(&self, commit_ts: TxnTs) -> anyhow::Result<()> {
        if !self.config().managed_txns {
            self.txn_mark()
                .sender
                .send(Mark::new(commit_ts, true))
                .await?;
        }
        Ok(())
    }
    pub(super) async fn done_read(&self, txn: &Txn) -> anyhow::Result<()> {
        if !txn
            .done_read()
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.read_mark()
                .sender
                .send(Mark::new(txn.read_ts, true))
                .await?;
        };
        Ok(())
    }
}
