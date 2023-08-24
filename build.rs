use std::io::Result;
use prost_build;
fn main() -> Result<()> {
    prost_build::Config::new().out_dir("src/pb").compile_protos(&["src/pb/pb.proto"], &["src/"])?;
    Ok(())
}
