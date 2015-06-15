extern crate gcc;

fn main() {
    gcc::Config::new().file("lib/xxhash/xxhash.c").flag("-O3").compile("libxxhash.a");
}
