extern crate cask;

use cask::xxhash::xxhash32;

fn main() {
    let string = "hello world";
    println!("{:?}", xxhash32(string.as_bytes(), string.len() as u64));
}
