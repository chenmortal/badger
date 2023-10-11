use std::{
    collections::HashMap,
    ops::Deref,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::bail;
use tokio::{
    select,
    sync::{
        mpsc::{
            self,
            error::{SendError, TryRecvError},
            Receiver, Sender,
        },
        Mutex, Semaphore,
    },
};

use crate::{
    pb::badgerpb4::{Kv, Match},
    tire::{Trie, TrieError},
    util::Closer,
    write::WriteReq,
};
#[derive(Debug)]
struct Subscriber {
    id: u64,
    matches: Vec<Match>,
    sender: Sender<Vec<Arc<Kv>>>,
    sub_closer: Closer,
    active: AtomicU64,
}
#[derive(Debug, Clone)]
struct Publisher(Arc<Mutex<PublisherInner>>);
impl Deref for Publisher {
    type Target = Arc<Mutex<PublisherInner>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
struct PublisherInner {
    sender: Sender<Vec<WriteReq>>,
    subscribers: HashMap<u64, Arc<Subscriber>>,
    next_id: u64,
    indexer: Trie,
}
impl PublisherInner {
    fn new(sender: Sender<Vec<WriteReq>>) -> Self {
        let subscribers = HashMap::new();
        Self {
            sender,
            subscribers,
            next_id: 0,
            indexer: Trie::default().into(),
        }
    }
}
impl Publisher {
    pub(crate) fn new(close_sem: Arc<Semaphore>) {
        let (sender, recv) = tokio::sync::mpsc::channel::<Vec<WriteReq>>(1000);
        let s = Self(Mutex::new(PublisherInner::new(sender)).into());
        tokio::spawn(s.clone().listen_for_updates(close_sem, recv));
    }

    pub(crate) async fn publish_updates(
        &self,
        reqs_vec: Vec<Vec<WriteReq>>,
    ) -> Result<(), SendError<Vec<Arc<Kv>>>> {
        let mut batch_updates = HashMap::<u64, Vec<Arc<Kv>>>::new();
        let s = self.lock().await;

        for reqs in reqs_vec {
            for req in reqs {
                for (dec_entry, _) in req.entries_vptrs() {
                    let key_ts = dec_entry.key_ts().get_bytes();
                    let ids = s.indexer.get(&key_ts);
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
        }

        for (id, kvs) in batch_updates.drain() {
            if let Some(s) = s.subscribers.get(&id) {
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
    ) -> anyhow::Result<()> {
        loop {
            select! {
             _=close_sem.acquire()=>{
                 return Ok(());
             },
             Some(s)=recv.recv()=>{
                let mut v = vec![s];
                match recv.try_recv() {
                    Ok(s) => {
                        v.push(s);
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {}
                        TryRecvError::Disconnected => bail!(e),
                    },
                }
                self.publish_updates(v).await?;
             }
            }
        }
    }
}
impl Subscriber {
    async fn new(
        publisher: &Publisher,
        sub_closer: Closer,
        matches: Vec<Match>,
    ) -> Result<Arc<Subscriber>, TrieError> {
        let (sender, receiver) = mpsc::channel::<Vec<Arc<Kv>>>(1000);
        let mut publisher = publisher.lock().await;
        let id = publisher.next_id;
        publisher.next_id += 1;
        let sub: Arc<Self> = Self {
            sender,
            active: AtomicU64::new(1),
            id,
            matches,
            sub_closer,
        }
        .into();
        publisher.subscribers.insert(id, sub.clone());

        for m in sub.matches.iter() {
            publisher.indexer.push_match(m, id)?;
        }
        Ok(sub)
    }
}
