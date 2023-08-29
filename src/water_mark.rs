use std::sync::atomic::AtomicU64;

use tokio::task::JoinHandle;
struct WaterMark {
    done_until: AtomicU64,
    last_index: AtomicU64,
    name: String,
}
struct Mark{
    index:u64,
    indices:Vec<u64>,
    done:bool
}
struct Closer{
    waiting:Vec<JoinHandle<()>>
}
impl WaterMark {
    fn process(closer:&Closer){
        // let pending=
    }
}