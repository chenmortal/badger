use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use log::{error, debug};
use tokio::{
    select,
    sync::{mpsc::Receiver, Notify, Semaphore},
};

use crate::{
    db::{DBInner, DB},
    default::KV_WRITES_ENTRIES_CHANNEL_CAPACITY,
    errors::DBError,
    kv::ValuePointer,
    metrics::{add_num_bytes_written_user, add_num_puts, set_pending_writes},
    txn::entry::DecEntry,
};
use anyhow::bail;
pub(crate) struct WriteReq {
    entries: Vec<DecEntry>,
    ptrs: Vec<ValuePointer>,
    notify: Arc<Notify>,
}

impl WriteReq {
    pub(crate) fn new(entries: Vec<DecEntry>, notify: Arc<Notify>) -> Self {
        let len = entries.len();
        Self {
            entries,
            ptrs: Vec::with_capacity(len),
            notify,
        }
    }
}
impl DB {
    #[inline]
    pub(crate) async fn send_entires_to_write_channel(
        &self,
        entries: Vec<DecEntry>,
        entries_size: usize,
    ) -> anyhow::Result<Arc<Notify>> {
        if self.block_writes.load(Ordering::SeqCst) == 1 {
            bail!(DBError::BlockedWrites)
        };
        let entires_len = entries.len();
        add_num_bytes_written_user(entries_size);
        let notify = Arc::new(Notify::new());
        let notify_clone = notify.clone();
        let w_req = WriteReq::new(entries, notify);
        self.send_write_req.send(w_req).await?;
        add_num_puts(entires_len);
        Ok(notify_clone)
    }
    #[inline]
    pub(crate) async fn do_writes(
        &self,
        mut recv_write_req: Receiver<WriteReq>,
        close_sem: Arc<Semaphore>,
    ) {
        let notify_send = Arc::new(Notify::new());
        let notify_recv = notify_send.clone();
        let mut write_reqs = Vec::with_capacity(10);
        async fn write_requests(db: DB, write_reqs: Vec<WriteReq>, notify_send: Arc<Notify>) {
            match db.write_requests(write_reqs).await {
                Ok(_) => {}
                Err(e) => {
                    error!("write Requests: {}", e);
                }
            };
            notify_send.notify_one();
        }
        let req_len = Arc::new(AtomicUsize::new(0));
        set_pending_writes(self.opt.dir.clone(), req_len.clone()).await;
        loop {
            select! {
                Some(write_req)=recv_write_req.recv()=>{
                    write_reqs.push(write_req);
                    req_len.store(write_reqs.len(), Ordering::Relaxed);
                    if write_reqs.len() >= 3*KV_WRITES_ENTRIES_CHANNEL_CAPACITY{
                        notify_recv.notified().await;
                        tokio::spawn(write_requests(self.clone(), write_reqs, notify_send.clone()));
                        write_reqs=Vec::with_capacity(10);
                        req_len.store(write_reqs.len(), Ordering::Relaxed);
                    }
                },
                _=notify_recv.notified()=>{
                    tokio::spawn(write_requests(self.clone(), write_reqs, notify_send.clone()));
                    write_reqs=Vec::with_capacity(10);
                    req_len.store(write_reqs.len(), Ordering::Relaxed);
                }
                _= close_sem.acquire()=>{
                    while let Some(w) = recv_write_req.recv().await {
                        write_reqs.push(w);
                    }
                    notify_recv.notified().await;
                    write_requests(self.clone(), write_reqs, notify_send.clone()).await;
                    return ;
                }
            }
        }
    }
    async fn write_requests(&self, reqs: Vec<WriteReq>) -> anyhow::Result<()> {
        if reqs.len()==0{
            return Ok(());
        }
        debug!("write_requests called. Writing to value log");
        
        Ok(())
    }
}
// #[tokio::t]

#[tokio::test]
async fn test_notify() {
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    // let notify_close = Arc::new(Notify::new());
    // let notify_close_clone = notify.clone();
    let close = Arc::new(AtomicBool::new(false));
    let close_clone = close.clone();
    let (s, mut r) = tokio::sync::mpsc::channel::<String>(2);

    tokio::spawn(async move {
        for i in 0..10 {
            tokio::time::sleep(Duration::from_secs_f32(1.0)).await;
            let notify_c = notify.clone();
            tokio::spawn(async move {
                // notify_clone.notify_one();
                // close.compare_exchange(current, new, success, failure)

                // close_clone.swap(true, Ordering::SeqCst);
                // notify.notify_one();
                notify_c.notify_one();
                // tokio::time::interval(Duration::from_secs(11));
            });
        }
    });

    // tokio::spawn(async move {
    //     let start = tokio::time::Instant::now();

    //     let mut p = tokio::time::interval(Duration::from_secs(1));
    //     let mut count = 0;
    //     loop {
    //         if close.load(Ordering::SeqCst) == true {
    //             // s.send(99.to_string()).await;
    //             break;
    //         }

    //         // tokio::time::sleep(Duration::from_secs_f32(0.5)).await;
    //         // if tokio::time::Instant::now().duration_since(start).as_secs() == 10 {
    //         //     return;
    //         // };

    //         // p.tick().await;
    //         s.send(count.to_string()).await;
    //         // notify.notify_one();
    //         count += 1;
    //         // notify.notify_one();
    //         // println!("task complete")
    //     }
    // });
    // while let Some(s) = r.recv().await {

    // }
    let mut count = 0;

    loop {
        if count == 1 {
            notify_clone.notified().await;
            println!("cc");
        }
        select! {
            Some(l)=r.recv()=>{
                // dbg!(l);
            }
            k=notify_clone.notified()=>{

                dbg!("ok");
                // // dbg!(r.recv().await);
                // while let Some(s) = r.recv().await {
                //     dbg!(s);
                // }
                // // match r.try_recv() {
                // //     Ok(s) => {
                // //         dbg!(s);
                // //     },
                // //     Err(e) => {
                // //         dbg!(e);
                // //     },
                // // }
                // break;
            }
        }
        // match r.try_recv() {
        //     Ok(a) => {
        //         dbg!(a);
        //     }
        //     Err(e) => {
        //         dbg!(e);
        //     }
        // }
        // let s = now_since_unix().as_secs();
        // select! {
        //     a=notify_clone.notified()=>{
        //         dbg!(&count);
        //     }
        // }
        // count += 1;
        // if s%3==0{
        //     notify_clone.notified().await;
        //     println!("task ready a");
        // }
        // notify_clone.notified().await;
        // println!("task ready")
    }
    // n.notified();
}
