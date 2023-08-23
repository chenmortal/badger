use std::mem::size_of;

use crate::skl::Node;

pub(crate) const MAX_VALUE_THRESHOLD: i64 = 1 << 20;
pub(crate) const DEFAULT_DIR:&str="./tmp/badger";
pub(crate) const DEFAULT_VALUE_DIR:&str="./tmp/badger";
pub(crate) const LOCK_FILE:&str="LOCK";
pub(crate) const SKL_MAX_HEIGHT:usize=20;
pub(crate) const SKL_MAX_NODE_SIZE:usize=size_of::<Node>();