use std::{
    collections::{BTreeMap, HashSet},
    fs::{remove_file, OpenOptions},
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::anyhow;
use anyhow::bail;
use log::{debug, error, info};
use rand::Rng;
use tokio::{
    select,
    sync::{Mutex, Notify, Semaphore},
};

use crate::{
    db::{BlockCache, IndexCache},
    default::SSTABLE_FILE_EXT,
    key_registry::KeyRegistry,
    lsm::{compaction::LevelCompactStatus, mmap::open_mmap_file},
    manifest::Manifest,
    options::Options,
    pb::ERR_CHECKSUM_MISMATCH,
    sys::sync_dir,
    table::{Table, TableOption},
    util::{dir_join_id_suffix, get_sst_id_set, Throttle},
};

use super::{compaction::CompactStatus, level_handler::LevelHandler};
#[derive(Debug)]
pub(crate) struct LevelsController {
    next_file_id: AtomicU64,
    l0_stalls_ms: AtomicI64,
    levels: Vec<LevelHandler>,
    compact_status: CompactStatus,
}
struct Targets {
    base_level: usize,
    target_size: Vec<usize>,
    file_size: Vec<usize>,
}
struct CompactionPriority {
    level: u32,
    score: f64,
    adjusted: f64,
    drop_prefixes: Vec<Vec<u8>>,
    targets: Targets,
}
impl LevelsController {
    pub(crate) async fn new(
        opt: Arc<Options>,
        manifest: &Arc<Mutex<Manifest>>,
        key_registry: KeyRegistry,
        block_cache: &Option<BlockCache>,
        index_cache: &Option<IndexCache>,
    ) -> anyhow::Result<LevelsController> {
        debug_assert!(opt.num_level_zero_tables_stall > opt.num_level_zero_tables);

        let compact_status = CompactStatus::new(opt.max_levels);
        let mut levels_control = Self {
            next_file_id: Default::default(),
            l0_stalls_ms: Default::default(),
            levels: Vec::with_capacity(opt.max_levels),
            // db: db.clone(),
            compact_status,
        };

        // let opt = &levels_control.db.opt;
        for i in 0..opt.max_levels {
            levels_control.levels[i] = LevelHandler::new(i);
            levels_control.compact_status.levels[i] = Arc::new(LevelCompactStatus::default());
        }

        let manifest_lock = manifest.lock().await;
        let manifest = &*manifest_lock;
        revert_to_manifest(&opt.dir, manifest, get_sst_id_set(&opt.dir))?;

        let num_opened = Arc::new(AtomicU32::new(0));
        let mut throttle = Throttle::new(3);

        let send_stop = Arc::new(Notify::new());

        let rev_stop = send_stop.clone();
        let num_opened_clone = num_opened.clone();
        let tables_len = manifest.tables.len();
        let start = tokio::time::Instant::now();
        let start_clone = start.clone();
        tokio::spawn(async move {
            // let start = tokio::time::Instant::now();
            let mut tick = tokio::time::interval(Duration::from_secs(3));
            loop {
                select! {
                    i=tick.tick()=>{
                        info!("{} tables out of {} opened in {}",
                        num_opened_clone.load(Ordering::SeqCst),
                        tables_len,
                        i.duration_since(start_clone).as_millis());
                    },
                    _stop=rev_stop.notified()=>{
                        break;
                    }
                };
            }
        });

        let tables = Arc::new(Mutex::new(BTreeMap::<u8, Vec<Table>>::new()));

        let mut max_file_id = 0;
        for (file_id, table_manifest) in manifest.tables.iter() {
            let num_opened_clone = num_opened.clone();
            let tables_clone = tables.clone();
            let path = dir_join_id_suffix(&opt.dir, *file_id as u32, SSTABLE_FILE_EXT);
            let permit = match throttle.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    close_all_tables(&tables).await;
                    bail!(e);
                }
            };
            max_file_id = max_file_id.max(*file_id);

            let tm = *table_manifest;
            let level = table_manifest.level;

            let key_registry_clone = key_registry.clone();
            let block_cache_clone = block_cache.clone();
            let index_cache_clone = index_cache.clone();
            let opt_clone = opt.clone();

            let future = async move {
                let read_only = opt_clone.read_only;
                let registry_r = key_registry_clone.read().await;
                let data_key = match registry_r.get_data_key(tm.keyid).await {
                    Ok(dk) => dk,
                    Err(e) => {
                        bail!(e)
                    }
                };
                drop(registry_r);
                let mut table_opt = TableOption::new(
                    &key_registry_clone,
                    &opt_clone,
                    &block_cache_clone,
                    &index_cache_clone,
                )
                .await;
                table_opt.datakey = data_key;
                table_opt.compression = tm.compression;
                let mut fp_open_opt = OpenOptions::new();
                fp_open_opt.read(true).write(!read_only);
                let (mmap_f, _is_new) = open_mmap_file(&path, fp_open_opt, read_only, 0)
                    .map_err(|e| anyhow!("Opening file: {:?} for {}", path, e))?;
                match Table::open(mmap_f, table_opt).await {
                    Ok(table) => {
                        let mut tables_m = tables_clone.lock().await;
                        match tables_m.get_mut(&level) {
                            Some(s) => {
                                s.push(table);
                            }
                            None => {
                                tables_m.insert(level, vec![table]);
                            }
                        }
                        drop(tables_m);
                    }
                    Err(e) => {
                        if e.to_string().ends_with(ERR_CHECKSUM_MISMATCH) {
                            error!("{}", e);
                            error!("Ignoring table {:?}", path);
                        } else {
                            bail!("Opening table:{:?} for {}", path, e)
                        }
                    }
                };
                Ok(())
            };
            tokio::spawn(async move {
                permit.done_with_future(future).await;
                num_opened_clone.fetch_add(1, Ordering::SeqCst)
            });
        }
        drop(manifest_lock);
        match throttle.finish().await {
            Ok(_) => {}
            Err(e) => {
                close_all_tables(&tables).await;
                bail!(e);
            }
        }
        send_stop.notify_one();

        info!(
            "All {} tables opened in {}",
            num_opened.load(Ordering::SeqCst),
            tokio::time::Instant::now()
                .duration_since(start)
                .as_millis()
        );

        levels_control
            .next_file_id
            .store(max_file_id + 1, Ordering::SeqCst);

        let tables_m = tables.lock().await;

        for (i, tables) in tables_m.iter() {
            levels_control.levels[*i as usize].init_tables(tables).await;
        }
        drop(tables_m);
        match levels_control.validate().await {
            Ok(_) => {}
            Err(e) => {
                let _ = levels_control.cleanup_levels().await;
                bail!("Level validation for {}", e);
            }
        }

        match sync_dir(&opt.dir) {
            Ok(_) => {}
            Err(e) => {
                let _ = levels_control.cleanup_levels().await;
                bail!(e);
            }
        };
        Ok(levels_control)
    }
    async fn validate(&self) -> anyhow::Result<()> {
        for level_handler in self.levels.iter() {
            level_handler
                .validate()
                .await
                .map_err(|e| anyhow!("Levels Controller {}", e))?;
        }
        Ok(())
    }
    async fn cleanup_levels(&self) -> anyhow::Result<()> {
        let mut first_err = None;
        for level_handler in self.levels.iter() {
            match level_handler.sync_mmap().await {
                Ok(_) => {}
                Err(e) => {
                    if first_err.is_none() {
                        first_err = e.into();
                    }
                }
            }
        }
        match first_err {
            Some(e) => {
                bail!(e)
            }
            None => Ok(()),
        }
    }
    #[inline]
    fn last_level(&self) -> &LevelHandler {
        debug_assert!(self.levels.len() > 0);
        self.levels.last().unwrap()
    }
    pub(crate) async fn start_compact(opt: &Arc<Options>) {
        let num = opt.num_compactors;
    }

    pub(crate) async fn run_compact(&self, id: usize, sem: Arc<Semaphore>, opt: &Arc<Options>) {
        let sleep =
            tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(0..1000)));
        select! {
            _=sleep=>{},
            _=sem.acquire()=>{return ;}
        }
        let mut count = 0;
        let mut ticker = tokio::time::interval(Duration::from_millis(50));

        let level = self.last_level().get_level().await;
        let targets = self.level_targets(opt).await;
        // ticker.tick()

        loop {
            select! {
                _=ticker.tick()=>{
                    count+=1;

                }
                _=sem.acquire()=>{return ;}
            }
        }
    }
    async fn do_compact(&self, id: usize, priority: CompactionPriority) {}
    async fn level_targets(&self, opt: &Arc<Options>) -> Targets {
        let levels_len = self.levels.len();
        let mut targets = Targets {
            base_level: 0,
            target_size: vec![0; levels_len],
            file_size: vec![0; levels_len],
        };
        let mut level_size = self.last_level().get_total_size().await;
        for i in (1..levels_len).rev() {
            targets.target_size[i] = level_size.max(opt.base_level_size);
            if targets.base_level == 0 && level_size <= opt.base_level_size {
                targets.base_level = i;
            }
            level_size /= opt.level_size_multiplier;
        }

        let mut table_size = opt.base_table_size;
        for i in 0..levels_len {
            targets.file_size[i] = if i == 0 {
                opt.memtable_size
            } else if i <= targets.base_level {
                table_size
            } else {
                table_size *= opt.table_size_multiplier;
                table_size
            }
        }

        for i in targets.base_level + 1..levels_len - 1 {
            if self.levels[i].get_total_size().await > 0 {
                break;
            }
            targets.base_level = i;
        }

        let base_level = targets.base_level;
        let levels = &self.levels;

        if base_level < levels.len() - 1
            && levels[base_level].get_total_size().await == 0
            && levels[base_level + 1].get_total_size().await < targets.target_size[base_level + 1]
        {
            targets.base_level += 1;
        }
        targets
    }
}
#[inline]
async fn close_all_tables(tables: &Arc<Mutex<BTreeMap<u8, Vec<Table>>>>) {
    let tables_m = tables.lock().await;
    for (_, table_slice) in tables_m.iter() {
        for table in table_slice {
            let _ = table.sync_mmap();
        }
    }
    drop(tables_m);
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
#[test]
fn test_a() {
    // let mut v = Vec::with_capacity(2);;
    // v.insert(1, 1);
    // v[1]=1;
    // dbg!(v);
}
