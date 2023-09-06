use std::{
    collections::{HashMap, HashSet},
    fs::read_dir,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::default::SSTABLE_FILE_EXT;

#[inline]
pub(crate) fn now_since_unix() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

#[inline]
pub(crate) fn secs_to_systime(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(secs))
        .unwrap()
}

#[inline]
pub(crate) fn parse_file_id(path: PathBuf, suffix: &str) -> Option<u64> {
    if let Some(name) = path.file_name() {
        if let Some(name) = name.to_str() {
            if name.ends_with(suffix) {
                let name = name.trim_end_matches(suffix);
                if let Ok(id) = name.parse::<u64>() {
                    return Some(id);
                };
            };
        }
    };
    None
}

#[inline]
pub(crate) fn get_sst_id_set(dir: &PathBuf) -> HashSet<u64> {
    let mut id_set = HashSet::new();
    if let Ok(read_dir) = read_dir(dir) {
        for ele in read_dir {
            if let Ok(entry) = ele {
                let path = entry.path();
                if path.is_file() {
                    if let Some(id) = parse_file_id(path, SSTABLE_FILE_EXT) {
                        id_set.insert(id);
                    };
                }
            }
        }
    };
    return id_set;
}
#[inline]
pub(crate) fn dir_join_id_suffix(dir:&PathBuf,id:u32,suffix: &str)->PathBuf{
    dir.join(format!("{:06}{}", id, suffix))
}
#[test]
fn test_a() {
    let mut k: HashMap<String, u32> = HashMap::new();
    let mut n: HashMap<Arc<String>, Arc<u32>> = HashMap::new();
    let s = "abc".to_string();
    let p = Arc::new(s.clone());
    k.insert(s.clone(), 1);
    n.insert(p, Arc::new(1));
    dbg!(k.get(&s));
    let l = n.get(&s);
}

#[test]
fn test_b() {
    dbg!(SystemTime::now());
    dbg!(secs_to_systime(now_since_unix().as_secs()));
}
