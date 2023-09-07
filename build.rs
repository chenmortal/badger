use prost_build;
use std::{io::Result, process::Command};
fn main() -> Result<()> {
    prost_build::Config::new()
        .out_dir("src/pb")
        .compile_protos(&["src/pb/pb.proto"], &["src/"])?;
    let status = Command::new("flatc")
        .arg("--rust")
        .args(["-o", "src/fb/"])
        .arg("src/fb/flatbuffer.fbs")
        .status()?;
    debug_assert!(status.success());
    Ok(())
}
