use log::info;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, fmt, util::SubscriberInitExt};

use crate::options::Options;

pub mod options;
fn main() {
    tracing_subscriber::registry()
    .with(fmt::layer())
    .init();
    info!("hello");
    // Options::default().dir(dir);
    println!("Hello, world!");
}
