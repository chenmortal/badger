use std::{
    collections::HashSet,
    fmt::Display,
    fs::{remove_file, OpenOptions},
    iter::Step,
    ops::{Add, AddAssign, Deref, Sub},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use aead::generic_array::typenum::Le;
use anyhow::anyhow;
use anyhow::bail;
use log::{debug, error, info};
use tokio::select;
use tokio_util::sync::CancellationToken;

use super::{compaction::CompactStatus, level_handler::LevelHandler};

use crate::{
    default::DEFAULT_DIR,
    key_registry::KeyRegistry,
    kv::{KeyTs, TxnTs},
    level::compaction::LevelCompactStatus,
    manifest::{Manifest, ManifestInfo},
    pb::ChecksumError,
    table::{Table, TableConfig},
    util::Throttle,
    util::{
        cache::{BlockCache, IndexCache},
        mmap::MmapFile,
        DBFileId,
    },
    util::{sys::sync_dir, SSTableId},
};
#[derive(Debug, Clone)]
pub(crate) struct LevelsController(Arc<LevelsControllerInner>);
impl Deref for LevelsController {
    type Target = LevelsControllerInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug)]
pub(crate) struct LevelsControllerInner {
    manifest: Manifest,
    next_file_id: AtomicU32,
    level_0_stalls_ms: AtomicU64,
    levels: Vec<LevelHandler>,
    compact_status: CompactStatus,
    memtable_size: usize,
    max_levels: u8,
    level_config: LevelsControllerConfig,
    table_config: TableConfig,
}
pub(crate) const LEVEL0: Level = Level(0);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub(crate) struct Level(u8);
impl From<u8> for Level {
    fn from(value: u8) -> Self {
        Self(value)
    }
}
impl From<u32> for Level {
    fn from(value: u32) -> Self {
        Self(value as u8)
    }
}
impl From<usize> for Level {
    fn from(value: usize) -> Self {
        Self(value as u8)
    }
}
impl Into<usize> for Level {
    fn into(self) -> usize {
        self.0 as usize
    }
}
impl Into<u32> for Level {
    fn into(self) -> u32 {
        self.0 as u32
    }
}
impl Level {
    pub(crate) fn to_usize(&self) -> usize {
        self.0 as usize
    }
}
impl Add<u8> for Level {
    type Output = Self;

    fn add(self, rhs: u8) -> Self::Output {
        Self(self.0 + rhs)
    }
}
impl Sub<u8> for Level {
    type Output = Self;

    fn sub(self, rhs: u8) -> Self::Output {
        Self(self.0 - rhs)
    }
}
impl AddAssign<u8> for Level {
    fn add_assign(&mut self, rhs: u8) {
        self.0 += rhs
    }
}
impl Step for Level {
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        end.0.checked_sub(start.0).and_then(|x| Some(x as usize))
    }

    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        if count < u8::MAX as usize {
            start
                .0
                .checked_add(count as u8)
                .and_then(|x| Some(Level(x)))
        } else {
            None
        }
    }

    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        if count < u8::MAX as usize {
            start
                .0
                .checked_sub(count as u8)
                .and_then(|x| Some(Level(x)))
        } else {
            None
        }
    }
}
#[test]
fn test_level_step() {
    let start = 0;
    let end = 5;
    let start_l: Level = start.into();
    let end_l: Level = end.into();
    let mut step = start;
    for l in start_l..end_l {
        assert_eq!(l, Level(step));
        step += 1;
    }
    assert_eq!(step, end);
}

impl Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Level {}", self.0))
    }
}

#[derive(Debug, Clone)]
pub struct LevelsControllerConfig {
    dir: PathBuf,
    read_only: bool,
    memtable_size: usize,
    num_level_zero_tables_stall: usize,
    num_level_zero_tables: usize,
    max_level: Level,
    base_level_size: usize,
    level_size_multiplier: usize,
    table_size_multiplier: usize,
    num_compactors: usize,
    compactl0_on_close: bool,
    lmax_compaction: bool,
    num_versions_to_keep: usize,
}
impl Default for LevelsControllerConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from(DEFAULT_DIR),
            read_only: false,
            memtable_size: 64 << 20,
            num_level_zero_tables_stall: 15,
            num_level_zero_tables: 5,
            max_level: Level(6),
            base_level_size: 10 << 20,
            level_size_multiplier: 10,
            table_size_multiplier: 2,
            num_compactors: 4,
            compactl0_on_close: false,
            lmax_compaction: false,
            num_versions_to_keep: 1,
        }
    }
}
impl LevelsControllerConfig {
    #[deny(unused)]
    pub(crate) fn check_levels_controller_config(&mut self) -> anyhow::Result<()> {
        if self.num_compactors == 1 {
            bail!("Cannot have 1 compactor. Need at least 2");
        }
        if self.read_only {
            self.compactl0_on_close = false;
        }
        Ok(())
    }
    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn set_dir(&mut self, dir: PathBuf) {
        self.dir = dir;
    }

    pub fn num_level_zero_tables(&self) -> usize {
        self.num_level_zero_tables
    }

    pub fn num_level_zero_tables_stall(&self) -> usize {
        self.num_level_zero_tables_stall
    }

    pub fn num_compactors(&self) -> usize {
        self.num_compactors
    }

    pub fn lmax_compaction(&self) -> bool {
        self.lmax_compaction
    }

    pub fn base_level_size(&self) -> usize {
        self.base_level_size
    }

    pub fn level_size_multiplier(&self) -> usize {
        self.level_size_multiplier
    }

    pub fn memtable_size(&self) -> usize {
        self.memtable_size
    }

    pub fn table_size_multiplier(&self) -> usize {
        self.table_size_multiplier
    }

    pub fn num_versions_to_keep(&self) -> usize {
        self.num_versions_to_keep
    }
}
impl LevelsControllerConfig {
    pub(crate) async fn build(
        &self,
        table_config: TableConfig,
        manifest: Manifest,
        key_registry: KeyRegistry,
        block_cache: Option<BlockCache>,
        index_cache: IndexCache,
    ) -> anyhow::Result<LevelsController> {
        assert!(self.num_level_zero_tables_stall > self.num_level_zero_tables);

        let compact_status = CompactStatus::default();
        let mut compact_status_w = compact_status.write();
        compact_status_w
            .levels_mut()
            .resize_with(self.max_level.0 as usize, LevelCompactStatus::default);
        drop(compact_status_w);

        let (max_file_id, level_tables) = self
            .open_tables_by_manifest(
                table_config.clone(),
                &manifest,
                key_registry,
                block_cache,
                index_cache,
            )
            .await?;
        let next_file_id = AtomicU32::new(max_file_id + 1);

        let mut levels = Vec::with_capacity(level_tables.len());
        let mut level = LEVEL0;
        for tables in level_tables {
            let handler = LevelHandler::new(level);
            level += 1;
            handler.init_tables(tables).await;
            levels.push(handler);
        }

        let levels_control = LevelsControllerInner {
            next_file_id,
            level_0_stalls_ms: Default::default(),
            levels,
            compact_status,
            memtable_size: self.memtable_size,
            max_levels: self.max_level.0,
            level_config: self.clone(),
            table_config,
            manifest,
        };

        levels_control.validate().await?;
        sync_dir(&self.dir)?;

        Ok(LevelsController(levels_control.into()))
    }
    fn watch_num_opened(num_opened: Arc<AtomicUsize>, tables_len: usize) -> CancellationToken {
        let start = tokio::time::Instant::now();
        let cancell = CancellationToken::new();
        let cancell_clone = cancell.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3));
            loop {
                select! {
                    i=tick.tick()=>{
                        info!("{} tables out of {} opened in {}",
                        num_opened.load(Ordering::SeqCst),
                        tables_len,
                        i.duration_since(start).as_millis());
                    },
                    _stop=cancell_clone.cancelled()=>{
                        info!(
                            "All {} tables opened in {}",
                            num_opened.load(Ordering::SeqCst),
                            tokio::time::Instant::now()
                                .duration_since(start)
                                .as_millis()
                        );
                        break;
                    }
                };
            }
        });
        cancell
    }
    async fn open_tables_by_manifest(
        &self,
        default_table_config: TableConfig,
        manifest: &Manifest,
        key_registry: KeyRegistry,
        block_cache: Option<BlockCache>,
        index_cache: IndexCache,
    ) -> anyhow::Result<(u32, Vec<Vec<Table>>)> {
        let manifest_lock = manifest.lock();
        let manifest = &*manifest_lock;

        revert_to_manifest(
            &self.dir,
            manifest,
            SSTableId::parse_set_from_dir(&self.dir),
        )?;

        let num_opened = Arc::new(AtomicUsize::new(0));
        let tables_len = manifest.tables.len();
        let watch_cancel_token = Self::watch_num_opened(num_opened.clone(), tables_len);

        let mut max_file_id: u32 = 0;
        let mut throttle = Throttle::new(3);
        let mut open_table_tasks = Vec::new();
        open_table_tasks.resize_with(self.max_level.0 as usize, Vec::new);
        for (file_id, table_manifest) in manifest.tables.iter() {
            let num_opened_clone = num_opened.clone();
            let path = file_id.join_dir(&self.dir);
            let permit = throttle.acquire().await?;

            max_file_id = max_file_id.max((*file_id).into());

            let compression = table_manifest.compression;
            let key_id = table_manifest.keyid;
            let key_registry_clone = key_registry.clone();
            let block_cache_clone = block_cache.clone();
            let index_cache_clone = index_cache.clone();
            let read_only = self.read_only;
            let mut table_config = default_table_config.clone();
            let future = async move {
                let cipher = key_registry_clone.get_cipher(key_id).await?;
                table_config.set_compression(compression);
                let mut fp_open_opt = OpenOptions::new();
                fp_open_opt.read(true).write(!read_only);

                let (mmap_f, _is_new) = MmapFile::open(&path, fp_open_opt, 0)?;

                match table_config
                    .open(mmap_f, cipher, index_cache_clone, block_cache_clone)
                    .await
                {
                    Ok(table) => {
                        return Ok(table.into());
                    }
                    Err(e) => {
                        if e.downcast_ref::<ChecksumError>().is_some() {
                            error!("{}", e);
                            error!("Ignoring table {:?}", path);
                        } else {
                            bail!("Opening table:{:?} for {}", path, e)
                        };
                    }
                };
                Ok(None)
            };
            let task = tokio::spawn(async move {
                let table = permit.done_with_future(future).await;
                num_opened_clone.fetch_add(1, Ordering::Relaxed);
                table.and_then(|x| x)
            });
            let task_level = table_manifest.level;
            if task_level < self.max_level {
                open_table_tasks[task_level.to_usize()].push(task);
            } else {
                open_table_tasks.last_mut().unwrap().push(task);
            }
        }

        drop(manifest_lock);

        throttle.finish().await?;
        watch_cancel_token.cancel();

        let mut level_tables = Vec::new();
        for tasks in open_table_tasks {
            let mut tables = Vec::with_capacity(tasks.len());
            for task in tasks {
                if let Some(table) = task.await? {
                    tables.push(table);
                }
            }
            level_tables.push(tables);
        }
        Ok((max_file_id, level_tables))
    }
}
impl LevelsControllerInner {
    async fn validate(&self) -> anyhow::Result<()> {
        for level_handler in self.levels.iter() {
            level_handler
                .validate()
                .await
                .map_err(|e| anyhow!("Levels Controller {}", e))?;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn last_level_handler(&self) -> &LevelHandler {
        debug_assert!(self.levels.len() > 0);
        self.levels.last().unwrap()
    }

    #[inline]
    pub(crate) fn max_level(&self) -> Level {
        self.level_config().max_level
    }

    // pub(crate) async fn start_compact(
    //     level_controller: Arc<Self>,
    //     closer: &mut Closer,
    //     sem: Arc<Semaphore>,
    //     oracle: &Arc<OracleInner>,
    // ) {
    //     let num = level_controller.level_config.num_compactors;
    //     for task_id in 0..num {
    //         let closer_c = closer.clone();
    //         let oracle_clone = oracle.clone();
    //         let level_controller_clone = level_controller.clone();
    //         tokio::spawn(async move {
    //             level_controller_clone
    //                 .run_compact(task_id, closer_c, &oracle_clone)
    //                 .await;
    //         });
    //     }
    // }

    // pub(crate) async fn run_compact(
    //     &self,
    //     task_id: usize,
    //     closer: Closer,
    //     // sem: Arc<Semaphore>,
    //     // opt: Arc<Options>,
    //     oracle: &Arc<OracleInner>,
    // ) {
    //     let sleep =
    //         tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(0..1000)));
    //     select! {
    //         _=sleep=>{},
    //         _=closer.captured()=>{return ;}
    //     }
    //     let mut count = 0;
    //     let mut ticker = tokio::time::interval(Duration::from_millis(50));

    //     // let level = self.last_level().get_level().await;
    //     // let targets = self.level_targets(&opt).await;
    //     // ticker.tick()
    //     // fn run (priotirty:CompactionPriority){

    //     // }
    //     // let run= |priority:CompactionPriority|{

    //     // };
    //     let priority = CompactionPriority {
    //         level: self.last_level_handler().level().into(),
    //         score: 0.0,
    //         adjusted: 0.0,
    //         drop_prefixes: Vec::new(),
    //         targets: self.targets().await,
    //     };
    //     self.do_compact(task_id, priority, oracle).await;
    //     loop {
    //         select! {
    //             _=ticker.tick()=>{
    //                 count+=1;
    //                 // if Options::lmax_compaction  && task_id==2 && count >=200{

    //                 // }
    //             }
    //             _=closer.captured()=>{return ;}
    //         }
    //     }
    // }

    // async fn run_compact_def(
    //     &self,
    //     task_id: usize,
    //     level: usize,
    //     compact_def: &mut CompactDef,
    // ) -> anyhow::Result<()> {
    //     if compact_def.priority.targets.file_size.len() == 0 {
    //         bail!("Filesizes cannot be zero. Targets are not set");
    //     }
    //     let time_start = SystemTime::now();

    //     // let this_level = compact_def.this_level.clone();
    //     // let next_level = compact_def.next_level.clone();

    //     debug_assert!(compact_def.splits.len() == 0);

    //     if compact_def.this_level.level() != compact_def.next_level.level() {
    //         self.add_splits(compact_def).await;
    //     }

    //     if compact_def.splits.len() == 0 {
    //         compact_def.splits.push(KeyRange::default());
    //     }

    //     let num_tables = compact_def.top.len() + compact_def.bottom.len();
    //     #[cfg(feature = "metrics")]
    //     add_num_compaction_tables(num_tables);
    //     let result = self.compact_build_tables(level, compact_def).await;
    //     #[cfg(feature = "metrics")]
    //     sub_num_compaction_tables(num_tables);
    //     result?;
    //     Ok(())
    // }

    // async fn do_compact(
    //     &self,
    //     task_id: usize,
    //     mut priority: CompactionPriority,
    //     oracle: &Arc<OracleInner>,
    // ) -> anyhow::Result<()> {
    //     let priority_level = priority.level;
    //     debug_assert!(priority_level < self.max_levels as usize);
    //     if priority.targets.base_level == 0 {
    //         priority.targets = self.targets().await;
    //     }
    //     let this_level = self.levels[priority_level].clone();
    //     let next_level = if priority_level == 0 {
    //         self.levels[priority.targets.base_level].clone()
    //     } else {
    //         this_level.clone()
    //     };

    //     let mut compact_def = CompactDef {
    //         compactor_id: task_id,
    //         this_level,
    //         next_level,
    //         top: Vec::new(),
    //         bottom: Vec::new(),
    //         this_range: KeyRange::default(),
    //         next_range: KeyRange::default(),
    //         splits: Vec::new(),
    //         this_size: 0,
    //         priority,
    //     };
    //     if priority_level == 0 {
    //         if !self.fill_tables_level0(&mut compact_def).await {
    //             bail!("Unable to fill tables")
    //         };
    //     } else {
    //         if priority_level != self.max_levels as usize - 1 {
    //             compact_def.next_level = self.levels[priority_level + 1].clone();
    //         }
    //         if !self.fill_tables(&mut compact_def, oracle).await {
    //             bail!("Unable to fill tables")
    //         };
    //     }
    //     Ok(())
    // }

    // async fn add_splits(&self, compact_def: &mut CompactDef) {
    //     compact_def.splits.clear();
    //     let mut width = (compact_def.bottom.len() as f64 / 5.0).ceil() as usize;
    //     width = width.max(3);
    //     let mut skr = compact_def.this_range.clone();
    //     skr.extend_borrow(&compact_def.next_range);

    //     for i in 0..compact_def.bottom.len() {
    //         if i == compact_def.bottom.len() - 1 {
    //             skr.right.clear();
    //             compact_def.splits.push(skr.clone());
    //             return;
    //         }
    //         if i % width == width - 1 {
    //             let biggest = compact_def.bottom[i].biggest();
    //             skr.right = key_with_ts(parse_key(&biggest), 0);
    //             compact_def.splits.push(skr.clone());
    //             skr.left = skr.right.clone();
    //         }
    //     }
    // }
    // async fn compact_build_tables(
    //     &self,
    //     level: usize,
    //     compact_def: &mut CompactDef,
    // ) -> anyhow::Result<()> {
    //     let mut valid = Vec::new();
    //     't: for table in compact_def.bottom.iter() {
    //         for prefix in compact_def.priority.drop_prefixes.iter() {
    //             if table.smallest().key().starts_with(&prefix) {
    //                 let biggest = table.biggest();
    //                 if biggest.key().starts_with(&prefix) {
    //                     continue 't;
    //                 }
    //             };
    //         }
    //         valid.push(table.clone());
    //     }

    //     let mut out = Vec::new();
    //     if level == 0 {
    //         compact_def
    //             .top
    //             .iter()
    //             .rev()
    //             .for_each(|t| out.push(t.clone()));
    //     } else if compact_def.top.len() > 0 {
    //         out.push(compact_def.top[0].clone());
    //     };

    //     let mut throttle = Throttle::new(3);
    //     for key_range in compact_def.splits.iter() {
    //         match throttle.acquire().await {
    //             Ok(permit) => {
    //                 let out_concat = ConcatIter::new(out.clone(), false, false);
    //                 let valid_concat = ConcatIter::new(valid.clone(), false, false);
    //                 let merget_iter = MergeIter::new(vec![out_concat, valid_concat], false);
    //                 tokio::spawn(async move {
    //                     permit.done_with_error(None).await;
    //                 });
    //             }
    //             Err(e) => {
    //                 error!("cannot start subcompaction: {}", e);
    //                 bail!(e)
    //             }
    //         };
    //     }
    //     Ok(())
    // }
    // async fn sub_compact(
    //     &self,
    //     merget_iter: MergeIter<TableIter>,
    //     key_range: KeyRange,
    //     compact_def: &mut CompactDef,
    //     oracle: &Arc<OracleInner>,
    // ) {
    //     let mut all_tables = Vec::with_capacity(compact_def.top.len() + compact_def.bottom.len());
    //     all_tables.extend_from_slice(&compact_def.top);
    //     all_tables.extend_from_slice(&compact_def.bottom);

    //     let has_overlap = self
    //         .check_overlap(&all_tables, compact_def.next_level.level() + 1)
    //         .await;

    //     let discard_ts = oracle.discard_at_or_below().await;
    // }
    // async fn check_overlap(&self, tables: &Vec<Table>, level: Level) -> bool {
    //     let key_range = KeyRange::from_tables(&tables).unwrap();
    //     for i in level.into()..self.levels.len() {
    //         let (left, right) = self.levels[i].overlapping_tables(&key_range).await;
    //         if right - left > 0 {
    //             return true;
    //         }
    //     }
    //     return false;
    // }
    pub(crate) fn get_reserve_file_id(&self) -> SSTableId {
        self.next_file_id.fetch_add(1, Ordering::AcqRel).into()
    }

    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub(crate) fn levels(&self) -> &[LevelHandler] {
        self.levels.as_ref()
    }

    pub(crate) fn level_handler(&self, level: Level) -> &LevelHandler {
        &self.levels[level.to_usize()]
    }

    pub(crate) fn level_config(&self) -> &LevelsControllerConfig {
        &self.level_config
    }

    pub(crate) fn level_0_stalls_ms(&self) -> &AtomicU64 {
        &self.level_0_stalls_ms
    }

    pub(crate) fn table_config(&self) -> &TableConfig {
        &self.table_config
    }

    pub(crate) fn compact_status(&self) -> &CompactStatus {
        &self.compact_status
    }
}

pub(crate) fn revert_to_manifest(
    dir: &PathBuf,
    manifest: &ManifestInfo,
    sst_id_set: HashSet<SSTableId>,
) -> anyhow::Result<()> {
    //check all files in manifest exist;
    for (id, _) in manifest.tables.iter() {
        if !sst_id_set.contains(id) {
            bail!("file does not exist for table {:?}", id);
        };
    }
    //delete files that shouldn't exist;
    for id in sst_id_set {
        match manifest.tables.get(&id) {
            Some(_) => {}
            None => {
                debug!("Table file {:?} not referenced in Manifest", id);
                let sst_path = id.join_dir(dir);
                remove_file(sst_path)
                    .map_err(|e| anyhow!("While removing table {:?} for {}", id, e))?;
            }
        }
    }
    Ok(())
}
pub(crate) struct TableInfo {
    table_id: SSTableId,
    level: Level,
    left: KeyTs,
    right: KeyTs,
    key_count: u32,
    on_disk_size: u32,
    stale_data_size: u32,
    index_len: usize,
    bloom_filter_len: usize,
    uncompressed_size: u32,
    max_version: TxnTs,
}

impl TableInfo {
    pub(crate) fn max_version(&self) -> TxnTs {
        self.max_version
    }
}
impl LevelsControllerInner {
    pub(crate) async fn get_table_info(&self) -> anyhow::Result<Vec<TableInfo>> {
        let mut results = Vec::with_capacity(self.levels.len() * 20);
        for level in self.levels.iter() {
            let handler = level.read().await;
            for table in handler.tables.iter() {
                let info = TableInfo {
                    table_id: table.table_id(),
                    level: level.level(),
                    left: table.smallest().clone(),
                    right: table.biggest().clone(),
                    key_count: table.key_count(),
                    on_disk_size: table.on_disk_size(),
                    #[cfg(not(feature = "async_cache"))]
                    stale_data_size: table.get_stale_data_size()?,
                    #[cfg(feature = "async_cache")]
                    stale_data_size: table.get_stale_data_size().await?,
                    index_len: table.index_len(),
                    bloom_filter_len: table.bloom_filter_len(),
                    uncompressed_size: table.uncompressed_size(),
                    max_version: table.max_version(),
                };
                results.push(info);
            }
            drop(handler);
        }
        results.sort_unstable_by(|a, b| {
            match a.level.cmp(&b.level) {
                std::cmp::Ordering::Equal => {}
                ord => return ord,
            }
            a.table_id.cmp(&b.table_id)
        });
        Ok(results)
    }
}
