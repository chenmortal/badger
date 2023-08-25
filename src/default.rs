use std::mem::size_of;

use crate::skl::skip_list::Node;

// use crate::skl::Node;

pub(crate) const MAX_VALUE_THRESHOLD: i64 = 1 << 20;
pub(crate) const DEFAULT_DIR:&str="./tmp/badger";
pub(crate) const DEFAULT_VALUE_DIR:&str="./tmp/badger";
pub(crate) const LOCK_FILE:&str="LOCK";
pub(crate) const MANIFEST_FILE_NAME:&str="MANIFEST";
pub(crate) const MANIFEST_REWRITE_FILE_NAME:&str="MANIFEST-REWEITE";
pub(crate) const MEM_FILE_EXT:&str=".mem";
pub(crate) const MANIFEST_DELETIONS_REWRITE_THRESHOLD: i32=10000;
pub(crate) const SKL_MAX_HEIGHT:usize=20;
pub(crate) const SKL_MAX_NODE_SIZE:usize=size_of::<Node>();