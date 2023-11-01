
use crate::kv::{KeyTs, TxnTs, ValueMeta};

use super::{levels::LevelsController, level_handler::LevelHandler};

impl LevelsController {
    pub(crate) fn get(
        &self,
        key: &KeyTs,
        max_txn_ts: TxnTs,
        max_value_meta: &ValueMeta,
        start_level: usize,
    ) {
        let version = key.txn_ts();
        for level_handler in &self.levels()[start_level..] {

        }
    }
}
impl LevelHandler {
    pub(crate) fn get(&self,key:&KeyTs){

    }
    pub(crate) async fn get_table_for_key(&self,key:&KeyTs){
        let table_handlers = self.read().await;
        if self.level()==0{

        }else {
            
        }
    }
}
