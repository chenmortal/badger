use std::{
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
};

use log::info;
use stretto::Histogram;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Notify, Semaphore,
    },
};

use crate::options::Options;
#[derive(Debug, Clone)]
pub(crate) struct VlogThreshold(Arc<VlogThresholdInner>);
#[derive(Debug)]
pub(crate) struct VlogThresholdInner {
    percentile: f64,
    value_threshold: AtomicUsize,
    vlog_metrics: Histogram,
    close_sem: Arc<Semaphore>,
    sender: Sender<Vec<i64>>,
    clear_notify: Arc<Notify>,
    default_threshold: usize,
    // receiver:Receiver<Vec<usize>>
}
impl VlogThresholdInner {
    pub(crate) fn new(
        opt: &Options,
        close_sem: Arc<Semaphore>,
        sender: Sender<Vec<i64>>,
        clear_notify: Arc<Notify>,
    ) -> Self {
        let max_bd = opt.max_value_threshold;
        let min_bd = opt.value_threshold as f64;
        assert!(max_bd >= min_bd);
        let size = (max_bd - min_bd + 1 as f64).min(1024.0);
        let bd_step = (max_bd - min_bd) / size;
        let mut bounds = Vec::<f64>::with_capacity(size as usize);
        for i in 0..bounds.len() {
            if i == 0 {
                bounds[0] = min_bd;
                continue;
            }
            if i == size as usize - 1 {
                bounds[i] = max_bd;
                continue;
            }
            bounds[i] = bounds[i - 1] + bd_step;
        }
        let histogram_data = Histogram::new(bounds);

        Self {
            percentile: opt.vlog_percentile,
            value_threshold: AtomicUsize::new(opt.value_threshold),
            vlog_metrics: histogram_data,
            close_sem,
            sender,
            clear_notify,
            default_threshold: opt.value_threshold,
        }
    }

    pub(crate) fn sender(&self) -> Sender<Vec<i64>> {
        self.sender.clone()
    }
    pub(crate) fn clear(&self) {
        self.set_value_threshold(self.default_threshold);
        self.clear_notify.notify_one();
    }
    pub(crate) fn value_threshold(&self) -> usize {
        self.value_threshold
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn set_value_threshold(&self, value_threshold: usize) {
        self.value_threshold
            .store(value_threshold, std::sync::atomic::Ordering::SeqCst);
    }
}
impl Deref for VlogThreshold {
    type Target = VlogThresholdInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl VlogThreshold {
    pub(crate) fn new(opt: &Arc<Options>, close_sem: Arc<Semaphore>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<i64>>(1000);
        let clear_notify = Arc::new(Notify::new());
        let clear_notified = clear_notify.clone();
        let vlog_threshold = VlogThreshold(Arc::new(VlogThresholdInner::new(
            opt,
            close_sem,
            sender,
            clear_notify,
        )));
        let vlog_c = vlog_threshold.clone();
        tokio::spawn(vlog_c.listen_for_value_threshold_update(receiver, clear_notified));
        vlog_threshold
    }
    pub(crate) async fn listen_for_value_threshold_update(
        self,
        mut receiver: Receiver<Vec<i64>>,
        clear_notified: Arc<Notify>,
    ) {
        loop {
            select! {
                _=self.close_sem.acquire()=>{
                    return ;
                }
                Some(v)=receiver.recv()=>{
                    for ele in v {
                        self.vlog_metrics.update(ele);
                    }
                    let p = self.vlog_metrics.percentile(self.percentile) as usize;
                    if self.value_threshold() != p{
                        info!("updating value of threshold to: {}",p);
                        self.set_value_threshold(p);
                    }
                }
                _=clear_notified.notified()=>{
                    self.vlog_metrics.clear()
                }
            }
        }
    }
}
