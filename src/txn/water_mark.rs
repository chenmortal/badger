use std::{
    mem::size_of,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use bincode::de;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
    task::JoinHandle,
};

use super::TxnTs;
#[derive(Debug)]
pub(crate) struct WaterMark {
    done_until: AtomicU64,
    last_index: AtomicU64,
    sender: Sender<Mark>,
    receiver: Receiver<Mark>,
    name: String,
}
#[derive(Debug)]
struct Mark {
    txn_ts: TxnTs,
    waiter: Option<Arc<Notify>>,
    indices: Vec<u64>,
    done: bool,
}
struct Closer {
    waiting: Vec<JoinHandle<()>>,
}
impl WaterMark {
    pub(crate) fn new(name: &str) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Mark>(100);
        WaterMark {
            done_until: AtomicU64::new(0),
            last_index: AtomicU64::new(0),
            name: name.to_string(),
            sender,
            receiver,
        }
    }
    pub(crate) async fn begin(&self, txn_ts: TxnTs) -> anyhow::Result<()> {
        self.last_index.store(txn_ts.0, Ordering::SeqCst);
        let mark = Mark {
            txn_ts,
            waiter: None,
            indices: Vec::new(),
            done: false,
        };
        self.sender.send(mark).await?;
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
    fn process(closer: &Closer) {
        // let pending=
    }
    #[inline]
    pub(crate) fn done_until(&self) -> TxnTs {
        self.done_until.load(Ordering::SeqCst).into()
    }
}

#[tokio::test]
async fn test_a() {
    let p = Arc::new(tokio::sync::Notify::new());
    let k = p.clone();
    let o = tokio::spawn(async move {
        p.notified().await;
        println!("hbc");
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    k.notify_one();
    dbg!(size_of::<Mark>());

    o.await;
}
struct Tx(u64);
#[test]
fn test_txn(){
    dbg!(size_of::<u64>());
}
