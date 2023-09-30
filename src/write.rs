use std::{
    mem::replace,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use log::{debug, error};
use tokio::{
    select,
    sync::{
        mpsc::{error::TrySendError, Receiver},
        oneshot, Notify, Semaphore,
    },
};

use crate::{
    db::{DBInner, DB},
    default::KV_WRITES_ENTRIES_CHANNEL_CAPACITY,
    errors::DBError,
    kv::ValuePointer,
    metrics::{add_num_bytes_written_user, add_num_puts, set_pending_writes},
    options::Options,
    txn::entry::DecEntry,
};
use anyhow::anyhow;
use anyhow::bail;
pub(crate) struct WriteReq {
    entries_vptrs: Vec<(DecEntry, ValuePointer)>,
    send_result: oneshot::Sender<anyhow::Result<()>>,
}

impl WriteReq {
    pub(crate) fn new(
        mut entries: Vec<DecEntry>,
        send_result: oneshot::Sender<anyhow::Result<()>>,
    ) -> Self {
        let p = entries
            .drain(..)
            .map(|x| (x, ValuePointer::default()))
            .collect::<Vec<_>>();
        Self {
            entries_vptrs: p,
            send_result,
        }
    }

    pub(crate) fn entries_vptrs_mut(&mut self) -> &mut Vec<(DecEntry, ValuePointer)> {
        &mut self.entries_vptrs
    }

    pub(crate) fn entries_vptrs(&self) -> &[(DecEntry, ValuePointer)] {
        self.entries_vptrs.as_ref()
    }
}
impl DB {
    #[inline]
    pub(crate) async fn send_entires_to_write_channel(
        &self,
        entries: Vec<DecEntry>,
        entries_size: usize,
    ) -> anyhow::Result<oneshot::Receiver<anyhow::Result<()>>> {
        if self.block_writes.load(Ordering::SeqCst) == 1 {
            bail!(DBError::BlockedWrites)
        };
        let entires_len = entries.len();
        add_num_bytes_written_user(entries_size);
        let (send_result, receiver) = oneshot::channel::<anyhow::Result<()>>();
        let w_req = WriteReq::new(entries, send_result);
        self.send_write_req.send(w_req).await?;
        add_num_puts(entires_len);
        Ok(receiver)
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
        set_pending_writes(Options::dir().clone(), req_len.clone()).await;
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
    async fn write_requests(&self, mut reqs: Vec<WriteReq>) -> anyhow::Result<()> {
        if reqs.len() == 0 {
            return Ok(());
        }
        debug!("write_requests called. Writing to value log");

        fn done_err(e: &anyhow::Error, reqs: Vec<WriteReq>) -> anyhow::Result<()> {
            let e = e.to_string();
            for ele in reqs {
                if let Err(e) = ele.send_result.send(Err(anyhow!(e.clone()))) {
                    return e;
                };
            }
            Ok(())
        }
        if let Err(e) = self.vlog.write(&mut reqs).await {
            done_err(&e, reqs)?;
            bail!(e);
        };

        debug!("Writing to memtable");
        let mut count = 0;
        for req in reqs.iter() {
            if req.entries_vptrs().len() == 0 {
                continue;
            }
            count += req.entries_vptrs().len();
        }
        // for ele in reqs.iter_mut() {
        //     ele.send_result.send(result);
        // }
        // reqs.iter().for_each(|req|req.send_result.send(result.clone()));
        Ok(())
    }
}
#[tokio::test]
async fn test_recv() {
    let p = tokio::sync::RwLock::new(String::from("a"));
    let mut w = p.write().await;

    let k = replace(&mut *w, String::from("b"));
    dbg!(k);
    dbg!(w);
    // replace(dest, src)
    let (sender, receiver) = tokio::sync::mpsc::channel::<String>(1);
    let a = sender.try_send(String::from("a"));
    dbg!(a);
    let b = sender.try_send(String::from("b"));
    match b {
        Ok(_) => {}
        Err(e) => match e {
            TrySendError::Full(f) => {
                dbg!(f);
            }
            TrySendError::Closed(e) => {
                println!("err")
            }
        },
    }
    // once_cell::sync::OnceCell::new();

    // tokio::sync::OnceCell::const_new();
    // dbg!(b);
}
