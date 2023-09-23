#![feature(ptr_internals, strict_provenance_atomic_ptr, ptr_sub_ptr,slice_as_chunks,build_hasher_simple_hash_one,async_fn_in_trait)]
#[macro_use]
extern crate lazy_static;
pub mod db;
pub(crate) mod default;
pub mod errors;
mod lock;
mod lsm;
mod manifest;
pub mod options;
mod pb;
mod skl;
mod table;
mod sys;
pub mod txn;
mod vlog;
mod key_registry;
mod metrics;
mod util;
mod iter;
mod kv;
mod write;

#[allow(dead_code, unused_imports)]
#[path = "./fb/flatbuffer_generated.rs"]
mod fb;