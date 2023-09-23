use std::sync::atomic::AtomicUsize;

use stretto::Histogram;

use crate::options::Options;
pub(crate) struct VlogThreshold {
    percentile: f64,
    value_threshold: AtomicUsize,
    vlog_metrics: Histogram,
}
impl VlogThreshold {
    pub(crate) fn new(opt: &Options) -> Self {
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
        }
    }
}
