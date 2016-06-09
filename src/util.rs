use std::fs::{File, OpenOptions};
use std::io::{Result, Write};
use std::path::Path;
use std::result::Result::Ok;

use xxhash2::{hash32, State32};

pub struct XxHash32(State32);

impl XxHash32 {
    pub fn new() -> XxHash32 {
        let mut state = State32::new();
        state.reset(0);
        XxHash32(state)
    }

    pub fn update(&mut self, buf: &[u8]) {
        self.0.update(buf);
    }

    pub fn get(&self) -> u32 {
        self.0.finish()
    }
}

impl Write for XxHash32 {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn xxhash32(buf: &[u8]) -> u32 {
    hash32(buf, 0)
}

pub fn get_file_handle(path: &Path, write: bool) -> File {
    if write {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap()
    } else {
        OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap()
    }
}
