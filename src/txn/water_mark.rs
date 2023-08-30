use std::sync::atomic::AtomicU64;

use tokio::task::JoinHandle;
pub(crate) struct WaterMark {
    done_until: AtomicU64,
    last_index: AtomicU64,
    name: String,
}
// impl WaterMark {
    
// }
struct Mark{
    index:u64,
    indices:Vec<u64>,
    done:bool
}
struct Closer{
    waiting:Vec<JoinHandle<()>>
}
impl WaterMark {
    pub(crate) fn new(name:&str)->Self{
        WaterMark{
            done_until: AtomicU64::new(0),
            last_index: AtomicU64::new(0),
            name: name.to_string(),
        }
    }
    fn process(closer:&Closer){
        // let pending=
    }
}