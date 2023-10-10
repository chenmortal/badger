use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::bail;
use tokio::{
    select,
    sync::{
        mpsc::{error::{SendError, TryRecvError}, Receiver, Sender},
        RwLock, Semaphore,
    },
};

use crate::{pb::badgerpb4::Kv, tire::Trie, write::WriteReq};

struct Subscriber {
    // matcher:
    sender: Sender<Vec<Arc<Kv>>>,
    active: AtomicU64,
}
struct Publisher {
    inner: Arc<PublisherInner>,
}
struct PublisherInner {
    sender: Sender<Vec<WriteReq>>,
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    indexer: RwLock<Trie>,
}
impl PublisherInner {
    fn new() {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<WriteReq>>(1000);
        let subscribers = HashMap::new();
        let s = Self {
            sender,
            subscribers,
            next_id: 0,
            indexer: Trie::default().into(),
        };
    }
}
impl Publisher {
    pub(crate) fn new(close_sem: Arc<Semaphore>) {}
    pub(crate) async fn publish_updates(
        &self,
        reqs: Vec<WriteReq>,
    ) -> Result<(), SendError<Vec<Arc<Kv>>>> {
        let mut batch_updates = HashMap::<u64, Vec<Arc<Kv>>>::new();
        let indexer_r = self.inner.indexer.read().await;
        for req in reqs {
            for (dec_entry, _) in req.entries_vptrs() {
                let key_ts = dec_entry.key_ts().get_bytes();
                let ids = indexer_r.get(&key_ts);
                if ids.len() == 0 {
                    continue;
                }
                let kv: Arc<Kv> = Kv {
                    key: dec_entry.key().to_vec(),
                    value: dec_entry.value().to_vec(),
                    user_meta: vec![],
                    version: dec_entry.version().to_u64(),
                    expires_at: dec_entry.expires_at(),
                    meta: vec![dec_entry.user_meta()],
                    stream_id: 0,
                    stream_done: false,
                }
                .into();

                for id in ids {
                    match batch_updates.get_mut(&id) {
                        Some(kv_list) => kv_list.push(kv.clone()),
                        None => {
                            let kv_list = vec![kv.clone()];
                            batch_updates.insert(id, kv_list);
                        }
                    }
                }
            }
        }
        drop(indexer_r);

        for (id, kvs) in batch_updates.drain() {
            if let Some(s) = self.inner.subscribers.get(&id) {
                if s.active.load(Ordering::SeqCst) == 1 {
                    s.sender.send(kvs).await?;
                }
            }
        }
        Ok(())
    }
    pub(crate) async fn listen_for_updates(
        self,
        close_sem: Arc<Semaphore>,
        mut recv: Receiver<Vec<WriteReq>>,
    ) ->anyhow::Result<()>{
        let mut v=vec![];
        if let Some(mut reqs) = recv.recv().await {
            match recv.try_recv() {
                Ok(s) => {
                    v.push(s);
                    // reqs.extend_one(item)
                }
                Err(e) => match e {
                    TryRecvError::Empty => {},
                    TryRecvError::Disconnected => bail!(e),
                },
            }
            
        }
        loop {
            // recv.recv()
            select! {
             _=close_sem.acquire()=>{
                 return Ok(());
             },
             Some(s)=recv.recv()=>{

             }
            }
        }
    }
}
