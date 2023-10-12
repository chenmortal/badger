use anyhow::bail;
use anyhow::Error;
use bytes::BufMut;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use std::cmp::Ordering;
use std::future::Future;
use std::{
    collections::HashSet,
    fs::read_dir,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::default::SSTABLE_FILE_EXT;
#[derive(Debug, Clone)]
pub(crate) struct Closer {
    semaphore: Arc<Semaphore>,
    wait: u32,
}

impl Closer {
    pub(crate) fn sem_clone(&mut self) -> Arc<Semaphore> {
        self.wait += 1;
        self.semaphore.clone()
    }
    pub(crate) fn new() -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(0)),
            wait: 0,
        }
    }
    pub(crate) fn done_all(&self) {
        self.semaphore.add_permits(self.wait as usize);
    }
    pub(crate) fn done_one(&self) {
        self.semaphore.add_permits(1);
    }
    #[inline]
    pub(crate) async fn wait_all(
        &self,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        let (a, b) = tokio::sync::oneshot::channel::<()>();
        a.send(());
        let p = b.await;
        self.semaphore.acquire_many(self.wait).await
    }
}
pub(crate) struct OneShotClose;
impl OneShotClose {
    pub(crate) fn new() -> (OneShotCloseSend, OneShotCloseRecv) {
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        (OneShotCloseSend(sender), OneShotCloseRecv(receiver))
    }
}

pub(crate) struct OneShotCloseSend(oneshot::Sender<()>);
impl OneShotCloseSend {
    pub(crate) fn send(self) {
        let _ = self.0.send(());
    }
}
pub(crate) struct OneShotCloseRecv(oneshot::Receiver<()>);
impl OneShotCloseRecv {
    pub(crate) async fn recv(self) -> Result<(), oneshot::error::RecvError> {
        self.0.await
    }
}

pub(crate) struct Throttle {
    semaphore: Arc<Semaphore>,
    max_permits: u32,
    receiver: Receiver<Error>,
    sender: Sender<Error>,
}
pub(crate) struct ThrottlePermit {
    semaphore_permit: OwnedSemaphorePermit,
    sender: Sender<Error>,
}
impl ThrottlePermit {
    #[inline]
    pub(crate) async fn done_with_future(
        self,
        future: impl Future<Output = Result<(), anyhow::Error>>,
    ) {
        match future.await {
            Ok(_) => {}
            Err(error) => {
                match self.sender.send(error).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("Throttle done send error mismatch,{}", e);
                    }
                };
            }
        }
    }
    pub(crate) async fn done_with_error(&self, e: Option<Error>) {
        if let Some(error) = e {
            match self.sender.send(error).await {
                Ok(_) => {}
                Err(e) => {
                    panic!("Throttle done send error mismatch,{}", e);
                }
            };
        }
    }
}
impl Throttle {
    pub(crate) fn new(max: u32) -> Self {
        let semaphore = Arc::new(Semaphore::new(max as usize));
        let (sender, receiver) = tokio::sync::mpsc::channel::<Error>(1);
        Self {
            semaphore,
            max_permits: max,
            receiver,
            sender,
        }
    }
    #[inline]
    pub(crate) async fn acquire(&mut self) -> anyhow::Result<ThrottlePermit> {
        loop {
            select! {
                permit =self.semaphore.clone().acquire_owned()=>{
                    let semaphore_permit=permit?;
                    let sender = self.sender.clone();
                    return Ok(ThrottlePermit {
                        semaphore_permit,
                        sender,
                    });
                },
                error=self.receiver.recv()=>{
                    if let Some(e) = error {
                        bail!(e)
                    }
                }

            }
        }
    }
    #[inline]
    pub(crate) async fn finish(&mut self) -> anyhow::Result<()> {
        let permit = self.semaphore.acquire_many(self.max_permits).await?;
        if let Some(e) = self.receiver.recv().await {
            bail!(e);
        }
        Ok(())
    }
}

#[inline(always)]
pub(crate) fn now_since_unix() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

#[inline(always)]
pub(crate) fn secs_to_systime(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

#[inline(always)]
pub(crate) fn parse_file_id(path: &PathBuf, suffix: &str) -> Option<u64> {
    if let Some(name) = path.file_name() {
        if let Some(name) = name.to_str() {
            if name.ends_with(suffix) {
                let name = name.trim_end_matches(suffix);
                if let Ok(id) = name.parse::<u64>() {
                    return Some(id);
                };
            };
        }
    };
    None
}

#[inline]
pub(crate) fn get_sst_id_set(dir: &PathBuf) -> HashSet<u64> {
    let mut id_set = HashSet::new();
    if let Ok(read_dir) = read_dir(dir) {
        for ele in read_dir {
            if let Ok(entry) = ele {
                let path = entry.path();
                if path.is_file() {
                    if let Some(id) = parse_file_id(&path, SSTABLE_FILE_EXT) {
                        id_set.insert(id);
                    };
                }
            }
        }
    };
    return id_set;
}
#[inline(always)]
pub(crate) fn dir_join_id_suffix(dir: &PathBuf, id: u32, suffix: &str) -> PathBuf {
    dir.join(format!("{:06}{}", id, suffix))
}
#[inline(always)]
pub(crate) fn compare_key(a: &[u8], b: &[u8]) -> Ordering {
    match a[..a.len() - 8].cmp(&b[..b.len() - 8]) {
        Ordering::Less => return Ordering::Less,
        Ordering::Equal => {}
        Ordering::Greater => return Ordering::Greater,
    };
    a[a.len() - 8..].cmp(&b[b.len() - 8..])
}
#[inline(always)]
pub(crate) fn parse_key(key: &[u8]) -> Option<&[u8]> {
    if key.len() <= 8 {
        return None;
    }
    key[..key.len() - 8].into()
}
#[inline(always)]
pub(crate) fn key_with_ts(key: Option<&[u8]>, ts: u64) -> Vec<u8> {
    match key {
        Some(s) => {
            let mut out = Vec::with_capacity(s.len() + 8);
            out.put(s);
            out.put_u64(u64::MAX - ts);
            out
        }
        None => {
            let mut out = Vec::with_capacity(8);
            out.put_u64(u64::MAX - ts);
            out
        }
    }
}

