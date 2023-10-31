use anyhow::bail;
use anyhow::Error;
use bytes::BufMut;
pub(crate) mod arena;
pub(crate) mod bloom;
pub(crate) mod cache;
pub(crate) mod closer;
pub(crate) mod lock;
pub(crate) mod log_file;
pub(crate) mod metrics;
pub(crate) mod mmap;
pub(crate) mod publisher;
pub(crate) mod rayon;
pub(crate) mod skip_list;
pub(crate) mod sys;
pub(crate) mod tire;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::future::Future;
use std::path::Path;
use std::{
    collections::HashSet,
    fs::read_dir,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) trait DBFileId: From<u32> + Into<u32> + Debug + Copy {
    const SUFFIX: &'static str;

    fn parse<P: AsRef<Path>>(path: P) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let path_buf = path.as_ref();
        if let Some(name) = path_buf.file_name() {
            if let Some(name) = name.to_str() {
                if name.ends_with(Self::SUFFIX) {
                    let name = name.trim_end_matches(Self::SUFFIX);
                    if let Ok(id) = name.parse::<u32>() {
                        return Ok(id.into());
                    };
                };
            }
        };
        bail!(
            "failed parse PathBuf {:?} , maybe not ends with {}",
            path_buf,
            Self::SUFFIX
        )
    }
    fn join_dir<P: AsRef<Path>>(self, parent_dir: P) -> PathBuf {
        let dir = parent_dir.as_ref();
        let id: u32 = self.into();
        dir.join(format!("{:06}{}", id, Self::SUFFIX))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MemTableId(u32);
impl From<u32> for MemTableId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Into<u32> for MemTableId {
    fn into(self) -> u32 {
        self.0
    }
}
impl DBFileId for MemTableId {
    const SUFFIX: &'static str = ".mem";
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SSTableId(u32);
impl From<u32> for SSTableId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Into<u32> for SSTableId {
    fn into(self) -> u32 {
        self.0
    }
}
impl DBFileId for SSTableId {
    const SUFFIX: &'static str = ".sst";
}
impl SSTableId {
    pub(crate) fn parse_set_from_dir<P: AsRef<Path>>(dir: P) -> HashSet<SSTableId> {
        let mut id_set = HashSet::new();
        let dir = dir.as_ref();
        if let Ok(read_dir) = read_dir(dir) {
            for ele in read_dir {
                if let Ok(entry) = ele {
                    let path = entry.path();
                    if path.is_file() {
                        if let Ok(id) = Self::parse(path) {
                            id_set.insert(id);
                        };
                    }
                }
            }
        };
        return id_set;
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct VlogId(u32);
impl From<u32> for VlogId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
impl Into<u32> for VlogId {
    fn into(self) -> u32 {
        self.0
    }
}
impl DBFileId for VlogId {
    const SUFFIX: &'static str = ".vlog";
}

#[test]
fn test_id() {}

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
    pub(crate) async fn done_with_future<T>(
        self,
        future: impl Future<Output = Result<T, anyhow::Error>>,
    ) -> Option<T> {
        match future.await {
            Ok(t) => t.into(),
            Err(error) => {
                match self.sender.send(error).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("Throttle done send error mismatch,{}", e);
                    }
                };
                None
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
        let _permit = self.semaphore.acquire_many(self.max_permits).await?;
        match self.receiver.try_recv() {
            Ok(e) => {
                bail!(e)
            }
            Err(_) => Ok(()),
        }
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
