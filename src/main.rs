extern crate cask;

use cask::Cask;

fn main() {
    let mut cask = Cask::open("test.db", true);
    cask.put("hello".as_bytes(), "world".as_bytes());
    println!("{:?}", cask.get("hello".as_bytes()));
    cask.delete("hello".as_bytes());
    println!("{:?}", cask.get("hello".as_bytes()));
}
