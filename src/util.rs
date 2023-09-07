use anyhow::bail;
use anyhow::Error;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use std::collections::HashMap;
use std::{
    collections::HashSet,
    fs::read_dir,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::default::SSTABLE_FILE_EXT;
pub(crate) struct Closer {
    semaphore: Arc<Semaphore>,
    wait: usize,
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
    pub(crate) fn close_all(&self) {
        self.semaphore.add_permits(self.wait);
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

#[inline]
pub(crate) fn now_since_unix() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

#[inline]
pub(crate) fn secs_to_systime(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

#[inline]
pub(crate) fn parse_file_id(path: PathBuf, suffix: &str) -> Option<u64> {
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
                    if let Some(id) = parse_file_id(path, SSTABLE_FILE_EXT) {
                        id_set.insert(id);
                    };
                }
            }
        }
    };
    return id_set;
}
#[inline]
pub(crate) fn dir_join_id_suffix(dir: &PathBuf, id: u32, suffix: &str) -> PathBuf {
    dir.join(format!("{:06}{}", id, suffix))
}
#[test]
fn test_a() {
    let mut k: HashMap<String, u32> = HashMap::new();
    let mut n: HashMap<Arc<String>, Arc<u32>> = HashMap::new();
    let s = "abc".to_string();
    let p = Arc::new(s.clone());
    k.insert(s.clone(), 1);
    n.insert(p, Arc::new(1));
    dbg!(k.get(&s));
    let l = n.get(&s);
}

#[test]
fn test_b() {

    // dbg!(SystemTime::now());
    // dbg!(secs_to_systime(now_since_unix().as_secs()));
}
#[tokio::test]
async fn test_c(){
    let mut tick=tokio::time::interval(Duration::from_secs(3));
    // tokio::time::interval_at(start, period)
    // tokio::time::
    // tokio::time::sleep(Duration::from_secs(1)).await;
    // let start = SystemTime::now();
    // tick.
    // tick.poll_tick(cx);
    // tokio::runtime::
    // tick.reset_immediately()
    let start = tokio::time::Instant::now();
    // let p=tick.tick().await;
    // let k = p.duration_since(start).as_millis();
    for i in 0..4{
        select! {
            time=tick.tick()=>{
                dbg!(time.duration_since(start).as_millis());
                // dbg!(format!("{}-{:?}",i,time));
            }
            // d{
            //     println!("a {}",i);
            // }
        }
        
    }

}