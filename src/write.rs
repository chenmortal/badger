use std::{sync::{atomic::Ordering, Arc}, time::{Duration, SystemTime}};

use tokio::{sync::{Notify, Semaphore, mpsc::Receiver}, select};

use crate::{
    db::{DBInner, DB},
    errors::DBError,
    kv::ValuePointer,
    metrics::{add_num_bytes_written_user, add_num_puts},
    txn::entry::DecEntry, util::now_since_unix,
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
    pub(crate) async fn do_writes(&self,mut recv_write_req:Receiver<WriteReq>, sem_clone: Arc<Semaphore>) {
        let notify_send = Arc::new(Notify::new());
        let notify_recv = notify_send.clone();
        // self.recv_write_req.recv();
        // sem_clone.acquire()
        loop {
            select! {
                Some(wr)=recv_write_req.recv()=>{

                },
                _=sem_clone.acquire()=>{

                }
            }
        }

        loop {}
    }
}
#[tokio::test]
async fn test_notify() {
    let notify = Arc::new(Notify::new());
    let notify_clone=notify.clone();
    tokio::spawn(async move {
        
        let mut p= tokio::time::interval(Duration::from_secs(1));
        loop {
           p.tick().await;
           notify.notify_one();
           println!("task complete")
        }
    });
    
    loop {
        let s=now_since_unix().as_secs();
        if s%3==0{
            notify_clone.notified().await;
            println!("task ready a");
        }
        notify_clone.notified().await;
        println!("task ready")
    }
    // n.notified();
}
