#![feature(test)]

extern crate cask;
extern crate rand;
extern crate test;

use std::fs;

use cask::CaskOptions;
use rand::Rng;
use test::Bencher;

#[bench]
fn get_latency(b: &mut Bencher) {
    let id: String = rand::thread_rng().gen_ascii_chars().take(16).collect();
    let path = format!("bench-{}.db", id);

    let cask = CaskOptions::default()
        .compaction_check_frequency(1)
        .max_file_size(50 * 1024 * 1024)
        .sync(false)
        .open(&path)
        .unwrap();

    let key = vec![1u8; 512];
    let vec = vec![1u8; 4096];

    cask.put(key.clone(), &vec).unwrap();

    b.bytes = vec.len() as u64;
    b.iter(|| cask.get(&key).unwrap());

    fs::remove_dir_all(path).unwrap();
}

#[bench]
fn put_latency(b: &mut Bencher) {
    let id: String = rand::thread_rng().gen_ascii_chars().take(16).collect();
    let path = format!("bench-{}.db", id);

    let cask = CaskOptions::default()
        .compaction_check_frequency(1)
        .max_file_size(50 * 1024 * 1024)
        .sync(false)
        .open(&path)
        .unwrap();

    let key = vec![1u8; 512];
    let vec = vec![1u8; 4096];

    b.bytes = vec.len() as u64;
    b.iter(|| cask.put(key.clone(), &vec).unwrap());

    fs::remove_dir_all(path).unwrap();
}
