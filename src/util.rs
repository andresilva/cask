use std::fs::{File, OpenOptions};
use std::io::{Result, Write};
use std::path::Path;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicUsize, Ordering};

use std::hash::Hasher;

use twox_hash::XxHash32;

pub struct TwoXhash32(XxHash32);

impl TwoXhash32 {
    pub fn new() -> TwoXhash32 {
        TwoXhash32(XxHash32::with_seed(0))
    }

    pub fn update(&mut self, buf: &[u8]) {
        self.0.write(buf);
    }

    pub fn get(&self) -> u32 {
        self.0.finish() as u32
    }
}

impl Write for TwoXhash32 {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn xxhash32(buf: &[u8]) -> u32 {
    let mut hash = XxHash32::with_seed(0);
    hash.write(buf);
    hash.finish() as u32
}

pub fn get_file_handle(path: &Path, write: bool) -> Result<File> {
    if write {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    } else {
        OpenOptions::new().read(true).open(path)
    }
}

pub struct Sequence(AtomicUsize);

impl Sequence {
    pub fn new(id: u32) -> Sequence {
        Sequence(AtomicUsize::new(id as usize))
    }

    pub fn increment(&self) -> u32 {
        self.0.fetch_add(1, Ordering::SeqCst) as u32 + 1
    }
}

pub fn human_readable_byte_count(bytes: usize, si: bool) -> String {
    let unit = if si { 1000 } else { 1024 };
    if bytes < unit {
        return format!("{} B", bytes);
    }
    let exp = ((bytes as f64).ln() / (unit as f64).ln()) as usize;

    let units = if si { "kMGTPE" } else { "KMGTPE" };
    let pre = format!(
        "{}{}",
        units.chars().nth(exp - 1).unwrap(),
        if si { "" } else { "i" }
    );

    format!("{:.1} {}B", bytes / unit.pow(exp as u32), pre)
}
