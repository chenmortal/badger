use std::{fs::OpenOptions, io::Write, path::PathBuf};

use badger::{db::DB, options::Options};
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
