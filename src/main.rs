extern crate cask;

use cask::key_dir::KeyDir;

fn main() {
    let name = "test";

    let mut keydir = KeyDir::new(name);

    keydir.set("hello", "world".as_bytes());
    keydir.set("foo", "bar".as_bytes());
    keydir.set("grumpy", "cat".as_bytes());

    keydir = KeyDir::open(name);
    println!("{}", String::from_utf8(keydir.get("hello").unwrap()).unwrap());
    println!("{}", String::from_utf8(keydir.get("foo").unwrap()).unwrap());
    println!("{}", String::from_utf8(keydir.get("grumpy").unwrap()).unwrap());
}
