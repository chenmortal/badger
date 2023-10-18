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
        Notify,
    },
};

use crate::{closer::Closer, options::Options};
#[derive(Debug, Clone)]
pub(crate) struct VlogThreshold(Arc<VlogThresholdInner>);
#[derive(Debug)]
pub(crate) struct VlogThresholdInner {
    percentile: f64,
    value_threshold: AtomicUsize,
    vlog_metrics: Histogram,
    closer: Closer,
    sender: Sender<Vec<usize>>,
    clear_notify: Arc<Notify>,
}
impl VlogThresholdInner {
    pub(crate) fn new(
        closer: Closer,
        sender: Sender<Vec<usize>>,
        clear_notify: Arc<Notify>,
    ) -> Self {
        let max_bd = Options::max_value_threshold();
        let min_bd = Options::value_threshold() as f64;
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
            percentile: Options::vlog_percentile(),
            value_threshold: AtomicUsize::new(Options::value_threshold()),
            vlog_metrics: histogram_data,
            closer,
            sender,
            clear_notify,
        }
    }

    pub(crate) fn sender(&self) -> Sender<Vec<usize>> {
        self.sender.clone()
    }
    pub(crate) fn clear(&self) {
        self.set_value_threshold(Options::value_threshold());
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
    pub(crate) fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<usize>>(1000);
        let clear_notify = Arc::new(Notify::new());
        let clear_notified = clear_notify.clone();
        let closer = Closer::new(1);
        let vlog_threshold = VlogThreshold(Arc::new(VlogThresholdInner::new(
            closer,
            sender,
            clear_notify,
        )));
        let vlog_c = vlog_threshold.clone();
        tokio::spawn(vlog_c.listen_for_value_threshold_update(receiver, clear_notified));
        vlog_threshold
    }
    pub(crate) async fn listen_for_value_threshold_update(
        self,
        mut receiver: Receiver<Vec<usize>>,
        clear_notified: Arc<Notify>,
    ) {
        loop {
            select! {
                _=self.closer.captured()=>{
                    return ;
                }
                Some(v)=receiver.recv()=>{
                    for ele in v {
                        self.vlog_metrics.update(ele as i64);
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
