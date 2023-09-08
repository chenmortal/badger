use std::{
    alloc::System,
    collections::HashSet,
    fs::{remove_file, OpenOptions},
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use anyhow::bail;
use log::{debug, info};
use tokio::{select, sync::Notify};

use crate::{
    db::DB,
    default::SSTABLE_FILE_EXT,
    lsm::{compaction::LevelCompactStatus, mmap::open_mmap_file},
    manifest::Manifest,
    table::TableOption,
    util::{dir_join_id_suffix, get_sst_id_set, Throttle},
};

use super::{compaction::CompactStatus, level_handler::LevelHandler};

pub(crate) struct LevelsController {
    next_file_id: AtomicU64,
    l0_stalls_ms: AtomicI64,
    levels: Vec<Arc<LevelHandler>>,
    db: Arc<DB>,
    compact_status: CompactStatus,
}
impl LevelsController {
    pub(crate) async fn new(db: Arc<DB>, manifest: &Manifest) -> anyhow::Result<()> {
        debug_assert!(db.opt.num_level_zero_tables_stall > db.opt.num_level_zero_tables);

        let compact_status = CompactStatus::new(db.opt.max_levels);
        let mut levels_control = Self {
            next_file_id: Default::default(),
            l0_stalls_ms: Default::default(),
            levels: Vec::with_capacity(db.opt.max_levels),
            db: db.clone(),
            compact_status,
        };

        let opt = &levels_control.db.opt;
        for i in 0..opt.max_levels {
            levels_control.levels[i] = Arc::new(LevelHandler::new(db.clone(), i));
            levels_control.compact_status.levels[i] = Arc::new(LevelCompactStatus::default());
        }

        revert_to_manifest(&opt.dir, manifest, get_sst_id_set(&opt.dir))?;

        let num_opened = Arc::new(AtomicU32::new(0));
        let mut throttle = Throttle::new(3);

        let send_stop = Arc::new(Notify::new());

        //
        let rev_stop = send_stop.clone();
        let num_opened_clone = num_opened.clone();
        let tables_len = manifest.tables.len();
        tokio::spawn(async move {
            let start = tokio::time::Instant::now();
            let mut tick = tokio::time::interval(Duration::from_secs(3));
            loop {
                select! {
                    i=tick.tick()=>{
                        info!("{} tables out of {} opened in {}",
                        num_opened_clone.load(Ordering::SeqCst),
                        tables_len,
                        i.duration_since(start).as_millis());
                    },
                    _stop=rev_stop.notified()=>{
                        break;
                    }
                };
            }
        });

        // num_opened.fetch_add(1, Ordering::SeqCst);
        let mut max_file_id = 0;
        for (file_id, table_manifest) in manifest.tables.iter() {
            let path = dir_join_id_suffix(&opt.dir, *file_id as u32, SSTABLE_FILE_EXT);
            let permit = match throttle.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    bail!(e);
                }
            };
            max_file_id = max_file_id.max(*file_id);
            let db_clone = db.clone();
            let tm = *table_manifest;
            let p = async move {
                let opt = &db_clone.opt;
                let read_only = opt.read_only;
                let registry_r = db_clone.key_registry.read().await;
                let data_key = match registry_r.get_data_key(tm.keyid).await {
                    Ok(dk) => dk,
                    Err(e) => {
                        bail!(e)
                    }
                };
                drop(registry_r);
                let table_opt = TableOption::new(&db_clone).await;
                let mut fp_open_opt = OpenOptions::new();
                fp_open_opt.read(true).write(!read_only);
                let (mmap_f, is_new) = open_mmap_file(&path, fp_open_opt, read_only, 0)
                    .map_err(|e| anyhow!("Opening file: {:?} for {}", path, e))?;
                

                Ok(())
                // Ok(())
            };
            // tokio::spawn(async move{
            //     // async fn run(){
            //         let registry_r = db_clone.key_registry.read().await;
            //     // // db_clone.key_registry.
            //     match registry_r.get_data_key(tm.keyid).await {
            //         Ok(dk) => {},
            //         Err(e) => {
            //             return e;
            //         },
            //     };
            //     // }

            //     // registry_r.data_keys.get(&tm.keyid);

            //     // permit.done_with_error(None).await;
            // });
        }
        send_stop.notify_one();
        Ok(())
    }
}
pub(crate) fn revert_to_manifest(
    dir: &PathBuf,
    manifest: &Manifest,
    sst_id_set: HashSet<u64>,
) -> anyhow::Result<()> {
    //check all files in manifest exist;
    for (id, _) in manifest.tables.iter() {
        if !sst_id_set.contains(id) {
            bail!("file does not exist for table {}", id);
        };
    }
    //delete files that shouldn't exist;
    for id in sst_id_set {
        match manifest.tables.get(&id) {
            Some(_) => {}
            None => {
                debug!("Table file {} not referenced in Manifest", id);
                let sst_path = dir_join_id_suffix(dir, id as u32, SSTABLE_FILE_EXT);
                remove_file(sst_path)
                    .map_err(|e| anyhow!("While removing table {} for {}", id, e))?;
            }
        }
    }
    Ok(())
}
