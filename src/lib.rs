//! `Cask` is a key-value store backed by a log-structured hash table which is inspired by
//! [bitcask](https://github.com/basho/bitcask/).
//!
//! Keys are indexed in a `HashMap` and values are written to an append-only log. To avoid the log
//! from getting filled with stale data (updated/deleted entries) a compaction process runs in the
//! background which rewrites the log by removing dead entries and merging log files.
//!
//! # Examples
//!
//! ```rust,no_run
//! use cask::{CaskOptions, SyncStrategy};
//! use cask::errors::Result;
//!
//! fn example() -> Result<()> {
//!     let cask = CaskOptions::default()
//!         .compaction_check_frequency(1200)
//!         .sync(SyncStrategy::Interval(5000))
//!         .max_file_size(1024 * 1024 * 1024)
//!         .open("cask.db")?;
//!
//!     let key = "hello";
//!     let value = "world";
//!
//!     cask.put(key, value)?;
//!     cask.get(key)?;
//!     cask.delete(key)?;
//!
//!     Ok(())
//! }
//! ```

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log as logrs;
extern crate byteorder;
extern crate fs2;
extern crate regex;
extern crate time;
extern crate twox_hash;

mod cask;
mod data;
pub mod errors;
mod file_pool;
mod log;
mod stats;
mod util;

pub use cask::{Cask, CaskOptions, SyncStrategy};
