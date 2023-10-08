use std::{
    collections::{BTreeMap, HashSet},
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
    lsm::{compaction::LevelCompactStatus, mmap::MmapFile},
    manifest::Manifest,
    metrics::{add_num_compaction_tables, sub_num_compaction_tables},
    options::Options,
    pb::ERR_CHECKSUM_MISMATCH,
    sys::sync_dir,
    table::{
        iter::{ConcatIter, TableIter},
        merge::MergeIter,
        Table, TableOption,
    },
    txn::oracle::Oracle,
    util::{
        compare_key, dir_join_id_suffix, get_sst_id_set, key_with_ts, parse_key, Closer, Throttle,
    },
};

use super::{
    compaction::{CompactStatus, KeyRange},
    level_handler::LevelHandler,
};
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
    level: usize,
    score: f64,
    adjusted: f64,
    drop_prefixes: Vec<Vec<u8>>,
    targets: Targets,
}
pub(super) struct CompactDef {
    compactor_id: usize,
    // targets: Targets,
    priority: CompactionPriority,
    pub(super) this_level: LevelHandler,
    pub(super) next_level: LevelHandler,
    pub(super) top: Vec<Table>,
    pub(super) bottom: Vec<Table>,
    pub(super) this_range: KeyRange,
    pub(super) next_range: KeyRange,
    splits: Vec<KeyRange>,
    this_size: usize,
    // drop_prefixes: Vec<Vec<u8>>,
}
impl LevelsController {
    pub(crate) async fn new(
        manifest: &Arc<Mutex<Manifest>>,
        key_registry: KeyRegistry,
        block_cache: &Option<BlockCache>,
        index_cache: &Option<IndexCache>,
    ) -> anyhow::Result<LevelsController> {
        debug_assert!(Options::num_level_zero_tables_stall() > Options::num_level_zero_tables());

        let compact_status = CompactStatus::new(Options::max_levels());
        let mut levels_control = Self {
            next_file_id: Default::default(),
            l0_stalls_ms: Default::default(),
            levels: Vec::with_capacity(Options::max_levels()),
            compact_status,
        };

        let mut compact_status_w = levels_control.compact_status.0.write().await;
        for i in 0..Options::max_levels() {
            levels_control.levels.push(LevelHandler::new(i));
            compact_status_w.levels.push(LevelCompactStatus::default());
        }
        drop(compact_status_w);

        let manifest_lock = manifest.lock().await;
        let manifest = &*manifest_lock;
        revert_to_manifest(Options::dir(), manifest, get_sst_id_set(Options::dir()))?;

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
            let path = dir_join_id_suffix(Options::dir(), *file_id as u32, SSTABLE_FILE_EXT);
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
            // let opt_clone = opt.clone();

            let future = async move {
                let read_only = Options::read_only();
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
                    // &opt_clone,
                    &block_cache_clone,
                    &index_cache_clone,
                )
                .await;
                table_opt.datakey = data_key;
                table_opt.compression = tm.compression;
                let mut fp_open_opt = OpenOptions::new();
                fp_open_opt.read(true).write(!read_only);

                let (mmap_f,_is_new)=MmapFile::open(&path, fp_open_opt,0)?;
                
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

        match sync_dir(Options::dir()) {
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
    pub(crate) async fn start_compact(
        level_controller: Arc<Self>,
        opt: &Arc<Options>,
        closer: &mut Closer,
        sem: Arc<Semaphore>,
        oracle: &Arc<Oracle>,
    ) {
        let num = Options::num_compactors();
        for task_id in 0..num {
            let sem = closer.sem_clone();
            let opt_clone = opt.clone();
            let oracle_clone = oracle.clone();
            let level_controller_clone = level_controller.clone();
            tokio::spawn(async move {
                level_controller_clone
                    .run_compact(task_id, sem, opt_clone, &oracle_clone)
                    .await;
            });
        }
    }

    pub(crate) async fn run_compact(
        &self,
        task_id: usize,
        sem: Arc<Semaphore>,
        opt: Arc<Options>,
        oracle: &Arc<Oracle>,
    ) {
        let sleep =
            tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(0..1000)));
        select! {
            _=sleep=>{},
            _=sem.acquire()=>{return ;}
        }
        let mut count = 0;
        let mut ticker = tokio::time::interval(Duration::from_millis(50));

        // let level = self.last_level().get_level().await;
        // let targets = self.level_targets(&opt).await;
        // ticker.tick()
        // fn run (priotirty:CompactionPriority){

        // }
        // let run= |priority:CompactionPriority|{

        // };
        let priority = CompactionPriority {
            level: self.last_level().get_level(),
            score: 0.0,
            adjusted: 0.0,
            drop_prefixes: Vec::new(),
            targets: self.level_targets().await,
        };
        self.do_compact(task_id, priority, &opt, oracle).await;
        loop {
            select! {
                _=ticker.tick()=>{
                    count+=1;
                    // if Options::lmax_compaction  && task_id==2 && count >=200{

                    // }
                }
                _=sem.acquire()=>{return ;}
            }
        }
    }

    async fn run_compact_def(
        &self,
        task_id: usize,
        level: usize,
        compact_def: &mut CompactDef,
    ) -> anyhow::Result<()> {
        if compact_def.priority.targets.file_size.len() == 0 {
            bail!("Filesizes cannot be zero. Targets are not set");
        }
        let time_start = SystemTime::now();

        // let this_level = compact_def.this_level.clone();
        // let next_level = compact_def.next_level.clone();

        debug_assert!(compact_def.splits.len() == 0);

        if compact_def.this_level.get_level() != compact_def.next_level.get_level() {
            self.add_splits(compact_def).await;
        }

        if compact_def.splits.len() == 0 {
            compact_def.splits.push(KeyRange::default());
        }

        let num_tables = compact_def.top.len() + compact_def.bottom.len();
        add_num_compaction_tables(num_tables);
        let result = self.compact_build_tables(level, compact_def).await;
        sub_num_compaction_tables(num_tables);
        result?;
        Ok(())
    }

    async fn do_compact(
        &self,
        task_id: usize,
        mut priority: CompactionPriority,
        opt: &Arc<Options>,
        oracle: &Arc<Oracle>,
    ) -> anyhow::Result<()> {
        let priority_level = priority.level;
        debug_assert!(priority_level < Options::max_levels());
        if priority.targets.base_level == 0 {
            priority.targets = self.level_targets().await;
        }
        let this_level = self.levels[priority_level].clone();
        let next_level = if priority_level == 0 {
            self.levels[priority.targets.base_level].clone()
        } else {
            this_level.clone()
        };

        let mut compact_def = CompactDef {
            compactor_id: task_id,
            this_level,
            next_level,
            top: Vec::new(),
            bottom: Vec::new(),
            this_range: KeyRange::default(),
            next_range: KeyRange::default(),
            splits: Vec::new(),
            this_size: 0,
            priority,
        };
        if priority_level == 0 {
            if !self.fill_tables_level0(&mut compact_def).await {
                bail!("Unable to fill tables")
            };
        } else {
            if priority_level != Options::max_levels() - 1 {
                compact_def.next_level = self.levels[priority_level + 1].clone();
            }
            if !self.fill_tables(&mut compact_def, opt, oracle).await {
                bail!("Unable to fill tables")
            };
        }
        Ok(())
    }
    async fn level_targets(&self) -> Targets {
        let levels_len = self.levels.len();
        let mut targets = Targets {
            base_level: 0,
            target_size: vec![0; levels_len],
            file_size: vec![0; levels_len],
        };
        let mut level_size = self.last_level().get_total_size().await;
        for i in (1..levels_len).rev() {
            targets.target_size[i] = level_size.max(Options::base_level_size());
            if targets.base_level == 0 && level_size <= Options::base_level_size() {
                targets.base_level = i;
            }
            level_size /= Options::level_size_multiplier();
        }

        let mut table_size = Options::base_table_size();
        for i in 0..levels_len {
            targets.file_size[i] = if i == 0 {
                Options::memtable_size() as usize
            } else if i <= targets.base_level {
                table_size
            } else {
                table_size *= Options::table_size_multiplier();
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
    async fn try_fill_max_level_tables(
        &self,
        compact_def: &mut CompactDef,
        oracle: &Arc<Oracle>,
        opt: &Arc<Options>,
    ) -> Option<bool> {
        let this_r = compact_def.this_level.handler_tables.read().await;
        let next_r = compact_def.next_level.handler_tables.read().await;
        let tables = this_r.tables.clone();
        if tables.len() == 0 {
            return false.into();
        }
        if compact_def.this_level.get_level() != Options::max_levels() - 1 {
            return None;
        }
        let mut sorted_tables = tables.clone();
        if sorted_tables.len() != 0 {
            sorted_tables.sort_unstable_by(|a, b| b.stale_data_size().cmp(&a.stale_data_size()));
        }

        if sorted_tables.len() > 0 && sorted_tables[0].stale_data_size() == 0 {
            return false.into();
        }
        compact_def.bottom.clear();

        let now = SystemTime::now();

        for table in sorted_tables {
            if table.max_version() > oracle.discard_at_or_below().await {
                continue;
            }

            if now.duration_since(table.created_at()).unwrap() < Duration::from_secs(60 * 60) {
                continue;
            }

            if table.stale_data_size() < 10 << 20 {
                continue;
            }

            compact_def.this_size = table.size();
            compact_def.this_range = KeyRange::from_table(&table).await;
            compact_def.next_range = compact_def.this_range.clone();
            let this_level = compact_def.this_level.get_level();
            if self
                .compact_status
                .is_overlaps_with(this_level, &compact_def.this_range)
                .await
            {
                continue;
            };
            let table_size = table.size();
            compact_def.top = vec![table.clone()];
            let need_file_size = compact_def.priority.targets.file_size[this_level];
            if table_size >= need_file_size {
                break;
            }

            // collect_bottom_tables
            let mut total_size = table_size;
            let mut j =
                match tables.binary_search_by(|a| compare_key(a.smallest(), table.smallest())) {
                    Ok(s) => s,
                    Err(s) => s,
                };
            debug_assert!(tables[j].id() == table.id());
            j += 1;
            while j < tables.len() {
                let new_t = &tables[j];
                total_size += new_t.size();
                if total_size >= need_file_size {
                    break;
                }
                compact_def.bottom.push(new_t.clone());
                compact_def
                    .next_range
                    .extend(KeyRange::from_table(new_t).await);
                j += 1;
            }
            //

            if !self.compact_status.compare_and_add(compact_def).await {
                compact_def.bottom.clear();
                compact_def.next_range = KeyRange::default();
                continue;
            };
            return true.into();
        }
        if compact_def.top.len() == 0 {
            return false.into();
        }

        let r = self.compact_status.compare_and_add(compact_def).await;
        drop(this_r);
        drop(next_r);
        return r.into();
    }
    async fn fill_tables(
        &self,
        compact_def: &mut CompactDef,
        opt: &Arc<Options>,
        oracle: &Arc<Oracle>,
    ) -> bool {
        //if compact_def.this_level.level is not last return None;
        if let Some(s) = self
            .try_fill_max_level_tables(compact_def, oracle, opt)
            .await
        {
            return s;
        }

        let this_level_r = compact_def.this_level.handler_tables.read().await;
        let next_level_r = compact_def.next_level.handler_tables.read().await;
        let mut tables = this_level_r.tables.clone();
        tables.sort_unstable_by(|a, b| a.max_version().cmp(&b.max_version()));

        for table in tables {
            compact_def.this_size = table.size();
            compact_def.this_range = KeyRange::from_table(&table).await;

            if self
                .compact_status
                .is_overlaps_with(compact_def.this_level.get_level(), &compact_def.this_range)
                .await
            {
                continue;
            };
            compact_def.top = vec![table.clone()];

            let (left_index, right_index) = compact_def
                .next_level
                .overlapping_tables(&compact_def.this_range)
                .await;
            compact_def.bottom = next_level_r.tables[left_index..right_index].to_vec();

            if compact_def.bottom.len() == 0 {
                compact_def.next_range = compact_def.this_range.clone();
                if !self.compact_status.compare_and_add(&compact_def).await {
                    continue;
                };
                return true;
            }

            compact_def.next_range = KeyRange::from_tables(&compact_def.bottom).await.unwrap(); //bottom.len !=0 so can unwrap()

            if self
                .compact_status
                .is_overlaps_with(compact_def.next_level.get_level(), &compact_def.next_range)
                .await
            {
                continue;
            };

            if !self.compact_status.compare_and_add(compact_def).await {
                continue;
            };
            return true;
        }
        false
    }
    async fn fill_tables_level0(&self, compact_def: &mut CompactDef) -> bool {
        if self.fill_tables_level0_to_levelbase(compact_def).await {
            true
        } else {
            self.fill_tables_level0_to_level0(compact_def).await
        }
    }
    async fn fill_tables_level0_to_levelbase(&self, compact_def: &mut CompactDef) -> bool {
        if compact_def.next_level.get_level() == 0 {
            panic!("Base level can't be zero");
        }

        if compact_def.priority.adjusted > 0.0 && compact_def.priority.adjusted < 1.0 {
            return false;
        }

        let this_level_r = compact_def.this_level.handler_tables.read().await;
        let next_level_r = compact_def.next_level.handler_tables.read().await;

        if this_level_r.tables.len() == 0 {
            return false;
        };
        let mut top = Vec::new();
        if compact_def.priority.drop_prefixes.len() == 0 {
            let mut key_range = KeyRange::default();
            for table in this_level_r.tables.iter() {
                let k = KeyRange::from_table(table).await;
                if key_range.is_overlaps_with(&k) {
                    top.push(table.clone());
                    key_range.extend(k);
                } else {
                    break;
                };
            }
        } else {
            top = this_level_r.tables.clone();
        }

        compact_def.this_range = KeyRange::from_tables(&top).await.unwrap();
        compact_def.top = top;

        let (left_index, right_index) = compact_def
            .next_level
            .overlapping_tables(&compact_def.this_range)
            .await;

        compact_def.bottom = next_level_r.tables[left_index..right_index].to_vec();

        compact_def.next_range = if compact_def.bottom.len() == 0 {
            compact_def.this_range.clone()
        } else {
            KeyRange::from_tables(&compact_def.bottom).await.unwrap() //len!=0 so can unwrap()
        };

        let r = self.compact_status.compare_and_add(compact_def).await;
        drop(this_level_r);
        drop(next_level_r);
        return r;
    }

    async fn fill_tables_level0_to_level0(&self, compact_def: &mut CompactDef) -> bool {
        if compact_def.compactor_id != 0 {
            return false;
        }

        compact_def.next_level = self.levels[0].clone();
        compact_def.next_range = KeyRange::default();
        compact_def.bottom.clear();

        debug_assert!(compact_def.this_level.get_level() == 0);
        debug_assert!(compact_def.next_level.get_level() == 0);

        let targets = &mut compact_def.priority.targets;

        let this_level_handler_r = compact_def.this_level.handler_tables.read().await;
        let mut compact_status_w = self.compact_status.write().await;
        let mut out = Vec::new();
        let now = SystemTime::now();

        for table in this_level_handler_r.tables.iter() {
            if table.size() >= targets.file_size[0] {
                continue;
            }

            if now.duration_since(table.created_at()).unwrap() < Duration::from_secs(10) {
                continue;
            };

            if compact_status_w.tables.contains(&table.id()) {
                continue;
            }
            out.push(table.clone());
        }
        drop(this_level_handler_r);
        if out.len() < 4 {
            return false;
        }

        compact_def.this_range = KeyRange::default_with_inf();
        compact_def.top = out;

        let this_level_compact_status =
            &mut compact_status_w.levels[compact_def.this_level.get_level()];
        this_level_compact_status
            .0
            .ranges
            .push(KeyRange::default_with_inf());

        for table in compact_def.top.iter() {
            compact_status_w.tables.insert(table.id());
        }
        targets.file_size[0] = u32::MAX as usize;
        drop(compact_status_w);
        true
    }
    async fn add_splits(&self, compact_def: &mut CompactDef) {
        compact_def.splits.clear();
        let mut width = (compact_def.bottom.len() as f64 / 5.0).ceil() as usize;
        width = width.max(3);
        let mut skr = compact_def.this_range.clone();
        skr.extend_borrow(&compact_def.next_range);

        for i in 0..compact_def.bottom.len() {
            if i == compact_def.bottom.len() - 1 {
                skr.right.clear();
                compact_def.splits.push(skr.clone());
                return;
            }
            if i % width == width - 1 {
                let biggest = compact_def.bottom[i].0.biggest.read().await;
                skr.right = key_with_ts(parse_key(&biggest), 0);
                compact_def.splits.push(skr.clone());
                skr.left = skr.right.clone();
            }
        }
    }
    async fn compact_build_tables(
        &self,
        level: usize,
        compact_def: &mut CompactDef,
    ) -> anyhow::Result<()> {
        let mut valid = Vec::new();
        't: for table in compact_def.bottom.iter() {
            for prefix in compact_def.priority.drop_prefixes.iter() {
                if table.smallest().starts_with(&prefix) {
                    let biggest = table.0.biggest.read().await;
                    if biggest.starts_with(&prefix) {
                        continue 't;
                    }
                };
            }
            valid.push(table.clone());
        }

        let mut out = Vec::new();
        if level == 0 {
            compact_def
                .top
                .iter()
                .rev()
                .for_each(|t| out.push(t.clone()));
        } else if compact_def.top.len() > 0 {
            out.push(compact_def.top[0].clone());
        };

        let mut throttle = Throttle::new(3);
        for key_range in compact_def.splits.iter() {
            match throttle.acquire().await {
                Ok(permit) => {
                    let out_concat = ConcatIter::new(out.clone(), false, false);
                    let valid_concat = ConcatIter::new(valid.clone(), false, false);
                    let merget_iter = MergeIter::new(vec![out_concat, valid_concat], false);
                    tokio::spawn(async move {
                        permit.done_with_error(None).await;
                    });
                }
                Err(e) => {
                    error!("cannot start subcompaction: {}", e);
                    bail!(e)
                }
            };
        }
        Ok(())
    }
    async fn sub_compact(
        &self,
        merget_iter: MergeIter<TableIter>,
        key_range: KeyRange,
        compact_def: &mut CompactDef,
        oracle: &Arc<Oracle>,
    ) {
        let mut all_tables = Vec::with_capacity(compact_def.top.len() + compact_def.bottom.len());
        all_tables.extend_from_slice(&compact_def.top);
        all_tables.extend_from_slice(&compact_def.bottom);

        let has_overlap = self
            .check_overlap(&all_tables, compact_def.next_level.get_level() + 1)
            .await;

        let discard_ts = oracle.discard_at_or_below().await;
    }
    async fn check_overlap(&self, tables: &Vec<Table>, level: usize) -> bool {
        let key_range = KeyRange::from_tables(&tables).await.unwrap();
        for i in level..self.levels.len() {
            let (left, right) = self.levels[i].overlapping_tables(&key_range).await;
            if right - left > 0 {
                return true;
            }
        }
        return false;
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
    't: for i in 0..4 {
        for j in 0..3 {
            if j == 2 {
                continue 't;
            }
            println!("{}-{}", i, j);
        }
    }
    // let mut v = Vec::with_capacity(2);;
    // v.insert(1, 1);
    // v[1]=1;
    // dbg!(v);
}
