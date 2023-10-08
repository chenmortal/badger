#![feature(ptr_internals, ptr_sub_ptr, slice_as_chunks, async_fn_in_trait)]
#[macro_use]
extern crate lazy_static;
pub mod db;
pub(crate) mod default;
pub mod errors;
mod iter;
mod key_registry;
mod kv;
mod lock;
mod lsm;
mod manifest;
mod metrics;
pub mod options;
mod pb;
mod skl;
mod sys;
mod table;
pub mod txn;
mod util;
mod vlog;
mod write;

#[allow(dead_code, unused_imports)]
#[path = "./fb/flatbuffer_generated.rs"]
mod fb;
