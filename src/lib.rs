#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log as logrs;
extern crate byteorder;
extern crate fs2;
extern crate regex;
extern crate xxhash2;

mod cask;
mod data;
pub mod errors;
mod log;
mod stats;
mod util;

pub use cask::Cask;
