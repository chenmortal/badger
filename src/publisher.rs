use std::{collections::HashMap, sync::atomic::AtomicU64};

use tokio::sync::mpsc::Sender;

use crate::write::WriteReq;

struct Subscriber {
    // matcher:
    active: AtomicU64,
}
struct Publisher {
    sender: Sender<Vec<WriteReq>>,
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
}
impl Publisher {
    fn new() {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<WriteReq>>(1000);
        let subscribers = HashMap::new();
        let s = Self {
            sender,
            subscribers,
            next_id: 0,
        };
    }
}
