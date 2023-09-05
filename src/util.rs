use std::{time::{Duration, SystemTime}, collections::HashMap, sync::Arc};

pub(crate) fn now_since_unix() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}
pub(crate) fn secs_to_systime(secs:u64)->SystemTime{
    SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(secs)).unwrap()
}
#[test]
fn test_a(){
    let mut k:HashMap<String, u32> = HashMap::new();
    let mut n:HashMap<Arc<String>,Arc<u32>> =HashMap::new();
    let s="abc".to_string();
    let p = Arc::new(s.clone());
    k.insert(s.clone(), 1);
    n.insert(p, Arc::new(1));
    dbg!(k.get(&s));
    let l = n.get(&s);
}

#[test]
fn test_b(){
    dbg!(SystemTime::now());
    dbg!(secs_to_systime(now_since_unix().as_secs()));
}
