# Cask

A fast key-value store written in Rust. The underlying storage system is a log-structured hash table
which is inspired by [bitcask](https://github.com/basho/bitcask/).

[![Build Status](https://travis-ci.org/andrebeat/cask.svg?branch=master)](https://travis-ci.org/andrebeat/cask)
[![Crates.io](https://img.shields.io/crates/v/cask.svg?maxage=2592000)](https://crates.io/crates/cask)
[![License](https://img.shields.io/dub/l/vibe-d.svg)](https://raw.githubusercontent.com/andrebeat/cask/master/LICENSE)

[API Documentation](http://andrebeat.github.io/cask)

* * *

**WARNING**: ⚠️ Please do not trust any valuable data to this yet. ⚠️

## Installation

Use the [crates.io](http://crates.io/) repository, add this to your Cargo.toml along with the rest
of your dependencies:

```toml
[dependencies]
cask = "0.7.0"
```

Then, use `Cask` in your crate:

```rust
extern crate cask;
use cask::{Cask, CaskOptions};
```

## Usage

The basic usage of the library is shown below:

```rust
let cask = CaskOptions::default()
    .compaction_check_frequency(1200)
    .sync(SyncStrategy::Interval(5000))
    .max_file_size(1024 * 1024 * 1024)
    .open("cask.db")?;

let key = "hello";
let value = "world";

cask.put(key, value)?;
cask.get(key)?;
cask.delete(key)?;
```

## TODO

- [X] Basic error handling
- [X] Merge files during compaction
- [X] Configurable compaction triggers and thresholds
- [X] Documentation
- [ ] Tests
- [ ] Benchmark
- [ ] Handle database corruption

## License

cask is licensed under the [MIT](http://opensource.org/licenses/MIT) license. See `LICENSE` for
details.
