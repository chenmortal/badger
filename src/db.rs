use tokio::sync::RwLock;

use crate::options::Options;
pub struct DB{
   lock:RwLock<()>,

}
impl DB {
    pub fn new(opt:Options){
        
    }
}