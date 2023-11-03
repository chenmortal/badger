#![feature(
    ptr_internals,
    ptr_sub_ptr,
    slice_as_chunks,
    async_fn_in_trait,
    unchecked_math,
    async_closure
)]
#[macro_use]
extern crate lazy_static;
pub mod db;
pub(crate) mod default;
pub mod errors;
#[allow(dead_code, unused_imports)]
#[path = "./fb/flatbuffer_generated.rs"]
mod fb;
mod iter;
mod key_registry;
mod kv;
mod level;
mod manifest;
mod memtable;
pub mod config;
mod pb;
mod read;
mod table;
pub mod txn;
mod util;
mod vlog;
mod write;
