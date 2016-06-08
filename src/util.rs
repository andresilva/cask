use std::result::Result::Ok;
use std::io::{Result, Write};

use crc::{crc32, Hasher32};

pub struct Crc32(crc32::Digest);

impl Crc32 {
    pub fn new() -> Crc32 {
        Crc32(crc32::Digest::new(crc32::IEEE))
    }

    pub fn write(&mut self, buf: &[u8]) {
        self.0.write(buf);
    }

    pub fn sum32(&self) -> u32 {
        self.0.sum32()
    }
}

impl Write for Crc32 {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.write(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn crc32(buf: &[u8]) -> u32 {
    crc32::checksum_ieee(buf)
}
