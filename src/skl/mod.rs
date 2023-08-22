use std::{sync::atomic::{AtomicU64, AtomicU32}, mem::size_of};

use crate::default::SKL_MAX_HEIGHT;

pub(crate) struct Node{
    value:AtomicU64,
    key_offset:u32,
    key_size:u16,
    height:u16,
    tower:[AtomicU32;SKL_MAX_HEIGHT]
}

fn size(){
    let p = size_of::<Node>();;
}