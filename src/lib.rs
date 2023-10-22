#![feature(
    ptr_internals,
    ptr_sub_ptr,
    slice_as_chunks,
    async_fn_in_trait,
    unchecked_math
)]
#[macro_use]
extern crate lazy_static;
mod bloom;
mod closer;
pub mod db;
pub(crate) mod default;
pub mod errors;
#[allow(dead_code, unused_imports)]
#[path = "./fb/flatbuffer_generated.rs"]
mod fb;
mod iter;
mod key_registry;
mod kv;
mod lock;
mod lsm;
mod manifest;
mod metrics;
pub mod options;
mod pb;
mod publisher;
mod rayon;
mod skl;
mod sys;
mod table;
mod tire;
pub mod txn;
mod util;
mod vlog;
mod write;
