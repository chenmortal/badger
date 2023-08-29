use std::{fs::OpenOptions, io::Write, path::PathBuf};

use badger::{db::DB, options::{Options, CompressionType}};
use bytes::{BufMut, BytesMut};
// use badger::errors::FileSysErr;
#[test]
fn test_f() {
    let p = OpenOptions::new().open("tests/aa");
    match p {
        Ok(mut k) => {
            k.write_all(format!("{}", 123).as_bytes());
        }
        Err(e) => {
            dbg!(e);
        }
    }
    // let p = OpenOptions::new().open("./aa").map_err(|source|FileSysErr::FailedOpen { source, file_path: PathBuf::from("./aa") } );;
    // match p {
    //     Ok(_) => {},
    //     Err(e) => {
    //         println!("{}",e);
    //     },
    // }
    // let k = OpenOptions::new().open("path").map_err(FileSysErr::from);;
    // match k {
    //     Ok(_) => {},
    //     Err(e) => {
    //         println!("{}",e);
    //     },
    // }
}
#[test]
fn test_open() {
    let mut opt = Options::default();
    let p = DB::open(&mut opt);
    match p {
        Ok(_) => {}
        Err(e) => {
            println!("{}", e);
        }
    }
}
#[test]
fn test_bytes(){
    let mut buf = bytes::BytesMut::with_capacity(8);;
    buf.put(&b"Bdgr"[..]);
    buf.put_u16(12 as u16);
    buf.put_u16(4 as u16);
    dbg!(buf.len());
    dbg!(buf);
}
#[test]
fn test_b(){
    let mut p = BytesMut::new();
    p.put_u32(8);
    // p.get(index)
//   let len=;
  
}

