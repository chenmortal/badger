use std::{
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Notify, Semaphore,
    },
    task::JoinHandle,
};

use super::TxnTs;
#[derive(Debug)]
pub(crate) struct WaterMark {
    done_until: Arc<AtomicU64>,
    last_index: AtomicU64,
    pub(super) sender: Sender<Mark>,
    // receiver: Receiver<Mark>,
    name: String,
}
#[derive(Debug, Default)]
pub(super) struct Mark {
    txn_ts: TxnTs,
    waiter: Option<Arc<Notify>>,
    indices: Vec<u64>,
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
struct Closer {
    waiting: Vec<JoinHandle<()>>,
}
impl WaterMark {
    pub(crate) fn new(name: &str, close_sem: Arc<Semaphore>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Mark>(100);
        let done_until = Arc::new(AtomicU64::new(0));
        tokio::spawn(Self::process(close_sem, receiver, done_until.clone()));
        WaterMark {
            done_until,
            last_index: AtomicU64::new(0),
            name: name.to_string(),
            sender,
            // receiver,
        }
    }
    pub(crate) async fn begin(&self, txn_ts: TxnTs) -> anyhow::Result<()> {
        self.last_index.store(txn_ts.0, Ordering::SeqCst);
        self.sender.send(Mark::new(txn_ts, false)).await?;
        Ok(())
    }
    pub(crate) async fn wait_for_mark(&self, txn_ts: TxnTs) -> anyhow::Result<()> {
        //please refer to draw/wait_for_mark.jpg
        if self.done_until() >= txn_ts {
            //need to check conflict
            return Ok(());
        }

        //not really commit,need to wait for writing to lsm
        let wait = Arc::new(Notify::new());
        self.sender
            .send(Mark {
                txn_ts,
                waiter: wait.clone().into(),
                indices: Vec::new(),
                done: false,
            })
            .await?;

        wait.notified().await;
        Ok(())
    }
    async fn process(
        close_sem: Arc<Semaphore>,
        mut receiver: Receiver<Mark>,
        done_util: Arc<AtomicU64>,
    ) {
        #[derive(Debug, PartialEq, Eq)]
        struct RevTxn(TxnTs);
        impl PartialOrd for RevTxn {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                other.0.partial_cmp(&self.0)
            }
        }
        impl Ord for RevTxn {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                other.0.cmp(&self.0)
            }
        }
        impl From<TxnTs> for RevTxn {
            fn from(value: TxnTs) -> Self {
                RevTxn(value)
            }
        }
        let min_heap = BinaryHeap::<RevTxn>::new();
        let mut pending = HashMap::<TxnTs, usize>::new();
        let mut waiters = HashMap::<TxnTs, Vec<Arc<Notify>>>::new();

        let process_one = |txn_ts: TxnTs, done: bool| {
            // pending.
        };

        let mut p = |mark: Mark| match mark.waiter {
            Some(notify) => {
                if TxnTs::from(done_util.load(Ordering::SeqCst)) >= mark.txn_ts {
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
            None => if mark.txn_ts > TxnTs::default() {},
        };
        loop {
            select! {
                _=close_sem.acquire()=>{
                    return ;
                }
                Some(mark)=receiver.recv()=>{

                }
            }
        }
    }
    #[inline]
    pub(crate) fn done_until(&self) -> TxnTs {
        self.done_until.load(Ordering::SeqCst).into()
    }
}

#[tokio::test]
async fn test_a() {
    #[derive(Debug, PartialEq, Eq)]
    struct RevTxn(TxnTs);
    impl PartialOrd for RevTxn {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            other.0.partial_cmp(&self.0)
        }
    }
    impl Ord for RevTxn {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.0.cmp(&self.0)
        }
    }
    impl From<TxnTs> for RevTxn {
        fn from(value: TxnTs) -> Self {
            RevTxn(value)
        }
    }
    // let mut a=HashMap::<TxnTs,Vec<u8>>::new();
    let mut p = std::collections::BinaryHeap::<RevTxn>::new();
    p.push(TxnTs(2).into());
    p.push(TxnTs(1).into());
    p.push(TxnTs(3).into());
    dbg!(p.peek());
    dbg!(p.pop());
    dbg!(p.peek());
    // p.peek();
    // p.pop()
    // let (mut sender, mut receiver) = tokio::sync::mpsc::channel::<Mark>(100);
    // let h = tokio::spawn(async move {
    //     select! {
    //         Some(m)=receiver.recv()=>{
    //             dbg!(m);
    //         }
    //     }
    // });
    // sender.send(Mark::new(TxnTs::default(), true)).await;
    // h.await;
}

#[tokio::test]
async fn test_b() {
    use std::sync::Arc;
    use tokio::sync::Barrier;
    // tokio::sync::futures::Notified;
    let mut handles = Vec::with_capacity(10);
    let barrier = Arc::new(Barrier::new(10));
    for _ in 0..10 {
        let c = barrier.clone();
        // The same messages will be printed together.
        // You will NOT see any interleaving.
        handles.push(tokio::spawn(async move {
            println!("before wait");
            let wait_result = c.wait().await;
            println!("after wait");
            wait_result
        }));
    }

    // Will not resolve until all "after wait" messages have been printed
    let mut num_leaders = 0;
    for handle in handles {
        let wait_result = handle.await.unwrap();
        if wait_result.is_leader() {
            num_leaders += 1;
        }
    }

    // Exactly one barrier will resolve as the "leader"
    assert_eq!(num_leaders, 1);
}
