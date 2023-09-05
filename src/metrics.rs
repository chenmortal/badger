use log::debug;
use std::{
    collections::HashMap,
    fs::{metadata, read_dir},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    select,
    sync::{Notify, RwLock, Semaphore},
};

use crate::options::Options;
lazy_static! {
    static ref LSM_SIZE: RwLock<HashMap<PathBuf, u64>> = RwLock::new(HashMap::new());
    static ref VLOG_SIZE: RwLock<HashMap<PathBuf, u64>> = RwLock::new(HashMap::new());
}

struct CalculateSize {
    enabled: bool,
    dir: PathBuf,
    value_dir: PathBuf,
}

#[inline]
pub(crate) async fn set_lsm_size(enabled: bool, k: &PathBuf, v: u64) {
    if !enabled {
        return;
    }
    let mut lsm_size_w = LSM_SIZE.write().await;
    lsm_size_w.insert(k.clone(), v);
    drop(lsm_size_w)
}
#[inline]
pub(crate) async fn set_vlog_size(enabled: bool, k: &PathBuf, v: u64) {
    if !enabled {
        return;
    }
    let mut vlog_size_w = VLOG_SIZE.write().await;
    vlog_size_w.insert(k.clone(), v);
    drop(vlog_size_w)
}

#[inline]
pub(crate) async fn calculate_size(opt: &Arc<Options>) {
    // let opt = &self.opt;
    let (lsm_size, mut vlog_size) = match total_size(&opt.dir) {
        Ok(r) => r,
        Err(e) => {
            debug!("Cannot calculate_size {:?} for {}", opt.dir, e);
            (0, 0)
        }
    };
    set_lsm_size(opt.metrics_enabled, &opt.dir, lsm_size).await;
    if opt.value_dir != opt.dir {
        match total_size(&opt.value_dir) {
            Ok((_, v)) => {
                vlog_size = v;
            }
            Err(e) => {
                debug!("Cannot calculate_size {:?} for {}", opt.value_dir, e);
                vlog_size = 0;
            }
        };
    }
    set_vlog_size(opt.metrics_enabled, &opt.value_dir, vlog_size).await;
}

pub(crate) async fn update_size(opt: Arc<Options>, sem: Arc<Semaphore>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
    loop {
        select! {
            _instant=interval.tick() =>{
                calculate_size(&opt).await;
            },
            _=sem.acquire()=>{
                break;
            }
        }
    }
}
fn total_size(dir: &PathBuf) -> anyhow::Result<(u64, u64)> {
    let mut lsm_size = 0;
    let mut vlog_size = 0;
    let read_dir = read_dir(dir)?;
    for ele in read_dir {
        let entry = ele?;
        let path = entry.path();
        if path.is_dir() {
            match total_size(&path) {
                Ok((sub_lsm, sub_vlog)) => {
                    lsm_size += sub_lsm;
                    vlog_size += sub_vlog;
                }
                Err(e) => {
                    debug!(
                        "Got error while calculating total size of directory: {:?} for {}",
                        path, e
                    );
                }
            }
        } else if path.is_file() {
            let meta_data = metadata(&path)?;
            let size = meta_data.len();
            let path = path.to_string_lossy();

            if path.ends_with(".sst") {
                lsm_size += size;
            } else if path.ends_with(".vlog") {
                vlog_size += size;
            }
        }
    }
    Ok((lsm_size, vlog_size))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn test_a() {
    let notify = Arc::new(Notify::new());
    let p = notify.notified();
    // tokio::sync::broadcast::channel(1);
    let hand = tokio::spawn(test_tick(notify.clone()));
    let handa = tokio::spawn(test_tick_a(notify.clone()));
    tokio::time::sleep(Duration::from_secs(3)).await;
    notify.notify_one();
    notify.notify_one();
    // notify.notify_waiters();
    // hand.abort();
    // dbg!(hand.is_finished());
    dbg!(hand.await);
    dbg!(handa.await);
    // dbg!(p.unwrap_err());
    // dbg!(hand.await.unwrap_err().is_cancelled());
}
async fn test_tick(notify: Arc<Notify>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let start = SystemTime::now();
    // for i in 0..10 {
    // interval.tick().await;
    // println!("{}",i);
    // }
    // let p = notify.notified();;

    loop {
        let n = notify.notified();
        select! {
            instant=interval.tick() =>{
                dbg!(instant);
            },
            notified=n=>{
                dbg!(notified);
                break;
            }
        }
    }
    dbg!(SystemTime::now().duration_since(start));
}
async fn test_tick_a(notify: Arc<Notify>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs_f32(1.5));
    let start = SystemTime::now();
    // for i in 0..10 {
    // interval.tick().await;
    // println!("{}",i);
    // }
    // let p = notify.notified();;
    loop {
        // let n=notify.notified();
        select! {
            instant=interval.tick() =>{
                dbg!(instant);
            },
            notified=notify.notified()=>{
                dbg!(notified);
                break;
            }
        }
    }
    dbg!(SystemTime::now().duration_since(start));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn test_b() {
    let mut closer = Closer::new();
    // let sem = Arc::new(Semaphore::new(0));
    // let notify = Arc::new(Notify::new());
    // let p = notify.notified();
    // tokio::sync::broadcast::channel(1);
    let hand = tokio::spawn(test_tick_b(closer.sem_clone()));
    let handa = tokio::spawn(test_tick_a_b(closer.sem_clone()));
    let handc = tokio::spawn(test_tick_b(closer.sem_clone()));

    tokio::time::sleep(Duration::from_secs(3)).await;
    // notify.notify_one();
    // notify.notify_one();
    // sem.add_permits(2);
    closer.close_all();
    // notify.notify_waiters();
    // hand.abort();
    // dbg!(hand.is_finished());
    dbg!(hand.await);
    dbg!(handa.await);
    // dbg!(p.unwrap_err());
    // dbg!(hand.await.unwrap_err().is_cancelled());
}
async fn test_tick_b(sem: Arc<Semaphore>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_micros(1));
    let start = SystemTime::now();
    // for i in 0..10 {
    // interval.tick().await;
    // println!("{}",i);
    // }
    // let p = notify.notified();;

    loop {
        select! {
            instant=interval.tick() =>{
                // dbg!(instant);
            },
            arq_r=sem.acquire()=>{
                dbg!(arq_r);
                break;
            }
        }
    }
    dbg!(SystemTime::now().duration_since(start));
}
async fn test_tick_a_b(sem: Arc<Semaphore>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs_f32(0.5));
    let start = SystemTime::now();
    // for i in 0..10 {
    // interval.tick().await;
    // println!("{}",i);
    // }
    // let p = notify.notified();;
    loop {
        // let n=notify.notified();
        select! {
            instant=interval.tick() =>{
                // dbg!(instant);
            },
            acq_r=sem.acquire()=>{
                dbg!(acq_r);
                break;
            }
        }
    }
    dbg!(SystemTime::now().duration_since(start));
}
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
