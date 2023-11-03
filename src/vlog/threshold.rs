use std::{
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
};

use anyhow::bail;
use log::info;
use stretto::Histogram;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
};

use crate::{default::MAX_VALUE_THRESHOLD, util::closer::Closer};
#[derive(Debug, Clone)]
pub(crate) struct VlogThreshold(Arc<VlogThresholdInner>);
#[derive(Debug)]
pub(crate) struct VlogThresholdInner {
    // percentile: f64,
    config: VlogThresholdConfig,
    value_threshold: AtomicUsize,
    vlog_metrics: Histogram,
    closer: Closer,
    sender: Sender<Vec<usize>>,
    clear_notify: Arc<Notify>,
}
#[derive(Debug, Clone, Copy)]
pub struct VlogThresholdConfig {
    max_value_threshold: usize,
    vlog_percentile: f64,
    value_threshold: usize,
}

impl VlogThresholdConfig {
    // pub fn set_max_value_threshold(&mut self, max_value_threshold: usize) {
    //     self.max_value_threshold = max_value_threshold;
    // }

    pub fn set_vlog_percentile(&mut self, vlog_percentile: f64) {
        self.vlog_percentile = vlog_percentile;
    }

    pub fn set_value_threshold(&mut self, value_threshold: usize) {
        self.value_threshold = value_threshold;
    }
    #[deny(unused)]
    pub(crate) fn check_threshold_config(
        &mut self,
        max_batch_size: usize,
    ) -> anyhow::Result<()> {
        // self.max_value_threshold = MAX_VALUE_THRESHOLD.min(max_batch_size);
        // assert!(self.max_value_threshold >= self.value_threshold)

        self.max_value_threshold = MAX_VALUE_THRESHOLD.min(max_batch_size);

        if self.vlog_percentile < 0.0 || self.vlog_percentile > 1.0 {
            bail!("vlog_percentile must be within range of 0.0-1.0")
        }
        if self.value_threshold > MAX_VALUE_THRESHOLD {
            bail!(
                "Invalid ValueThreshold, must be less or equal to {}",
                MAX_VALUE_THRESHOLD
            );
        }
        if self.value_threshold > max_batch_size {
            bail!("Valuethreshold {} greater than max batch size of {}. Either reduce Valuethreshold or increase max_table_size",self.value_threshold,max_batch_size);
        }
        Ok(())
    }

    pub fn value_threshold(&self) -> usize {
        self.value_threshold
    }
}
impl Default for VlogThresholdConfig {
    fn default() -> Self {
        Self {
            max_value_threshold: MAX_VALUE_THRESHOLD,
            vlog_percentile: 0.0,
            value_threshold: MAX_VALUE_THRESHOLD,
        }
    }
}
impl VlogThresholdInner {
    pub(crate) fn new(
        config: VlogThresholdConfig,
        closer: Closer,
        sender: Sender<Vec<usize>>,
        clear_notify: Arc<Notify>,
    ) -> Self {
        let max_bd = config.max_value_threshold as f64;
        let min_bd = config.value_threshold as f64;
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
            config,
            value_threshold: AtomicUsize::new(config.value_threshold),
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
        self.set_value_threshold(self.config.value_threshold);
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
    pub(crate) fn new(config: VlogThresholdConfig) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<usize>>(1000);
        let clear_notify = Arc::new(Notify::new());
        let clear_notified = clear_notify.clone();
        let closer = Closer::new(1);
        let vlog_threshold = VlogThreshold(Arc::new(VlogThresholdInner::new(
            config,
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
                    let p = self.vlog_metrics.percentile(self.config.vlog_percentile) as usize;
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
