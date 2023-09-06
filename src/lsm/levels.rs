use std::{sync::{
    atomic::{AtomicI64, AtomicU64},
    Arc,
}, collections::HashSet};

use anyhow::bail;

use crate::{db::DB, manifest::{Manifest, self}, lsm::compaction::LevelCompactStatus};

use super::{compaction::CompactStatus, level_handler::LevelHandler};

pub(crate) struct LevelsController {
    next_file_id: AtomicU64,
    l0_stalls_ms: AtomicI64,
    levels: Vec<Arc<LevelHandler>>,
    db: Arc<DB>,
    compact_status: CompactStatus,
}
impl LevelsController {
    pub(crate) fn new(db: Arc<DB>, manifest: &Manifest) {
        debug_assert!(db.opt.num_level_zero_tables_stall > db.opt.num_level_zero_tables);

        let compact_status = CompactStatus::new(db.opt.max_levels);
        let mut levels_control = Self {
            next_file_id: Default::default(),
            l0_stalls_ms: Default::default(),
            levels: Vec::with_capacity(db.opt.max_levels),
            db:db.clone(),
            compact_status,
        };

        let opt = &levels_control.db.opt;
        for i in 0..opt.max_levels{
            levels_control.levels[i]=Arc::new(LevelHandler::new(db.clone(), i));
            levels_control.compact_status.levels[i]=Arc::new(LevelCompactStatus::default());
        }
    }
}
pub(crate) fn revert_to_manifest(manifest:&Manifest,sst_id_set:HashSet<u64>)->anyhow::Result<()>{
    //check all files in manifest exist;
    for (id,_) in manifest.tables.iter() {
        if !sst_id_set.contains(id) {
            bail!("file does not exist for table {}",id);
        };
    }

    //delete files that shouldn't exist;
    
    Ok(())
}
