pub(crate) struct HistogramData {
    bounds: Vec<f64>,
    count: i64,
    count_per_bucket: Vec<i64>,
    min: i64,
    max: i64,
    sum: i64,
}
impl HistogramData {
    pub(crate) fn new(bounds: Vec<f64>) -> Self {
        let count_per_bucket = Vec::with_capacity(bounds.len() + 1);
        Self {
            bounds,
            count: 0,
            count_per_bucket,
            min: 0,
            max: i64::MAX,
            sum: 0,
        }
    }
}
