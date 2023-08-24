use log::info;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, fmt, util::SubscriberInitExt};

use crate::options::Options;

pub mod options;
pub mod db;
pub(crate) mod default;
mod skl;
pub mod errors;
// mod un_safe;
mod lock;
mod manifest;
mod pb;
mod sys; //unsafe mod
// fn main() {
//     tracing_subscriber::registry()
//     .with(fmt::layer())
//     .init();
//     info!("hello");
//     // Options::default().dir(dir);
//     println!("Hello, world!");
// }
