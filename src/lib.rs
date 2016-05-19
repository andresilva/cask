extern crate byteorder;
extern crate crc;
extern crate time;

use std::io::{Cursor, Write};
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;

#[derive(Debug)]
pub struct Entry<'a> {
    key: &'a[u8],
    value: &'a[u8],
    timestamp: u32,
    deleted: bool,
}

const STATIC_SIZE: usize = 14; // crc(4) + timestamp(4) + key_size(2) + value_size(4)
const TOMBSTONE: u32 = !0;

impl<'a> Entry<'a> {
    pub fn new(key: &'a [u8], value: &'a [u8]) -> Entry<'a> {
        assert!(value.len() < TOMBSTONE as usize);
        Entry {
            key: key,
            value: value,
            timestamp: time::now().to_timespec().sec as u32,
            deleted: false,
        }
    }

    pub fn deleted(&self) -> Entry<'a> {
        Entry {
            key: self.key,
            value: &[],
            timestamp: time::now().to_timespec().sec as u32,
            deleted: true,
        }
    }

    pub fn size(&self) -> usize {
        STATIC_SIZE + self.key.len() + self.value.len()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::with_capacity(self.size()));
        cursor.set_position(4);
        cursor.write_u32::<LittleEndian>(self.timestamp).unwrap();
        cursor.write_u16::<LittleEndian>(self.key.len() as u16).unwrap();

        if self.deleted {
            cursor.write_u32::<LittleEndian>(TOMBSTONE).unwrap();
            cursor.write(&self.key).unwrap();
        } else {
            cursor.write_u32::<LittleEndian>(self.value.len() as u32).unwrap();
            cursor.write(&self.key).unwrap();
            cursor.write(&self.value).unwrap();
        }

        let checksum = crc32::checksum_ieee(&cursor.get_ref()[4..]);
        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum).unwrap();

        cursor.into_inner()
    }

    pub fn from_bytes(bytes: &'a [u8]) -> Entry<'a> {
        let mut cursor = Cursor::new(bytes);

        let checksum = cursor.read_u32::<LittleEndian>().unwrap();
        assert_eq!(crc32::checksum_ieee(&bytes[4..]), checksum);

        let timestamp = cursor.read_u32::<LittleEndian>().unwrap();
        let key_size = cursor.read_u16::<LittleEndian>().unwrap();
        let value_size = cursor.read_u32::<LittleEndian>().unwrap();

        Entry {
            key: &bytes[STATIC_SIZE..STATIC_SIZE + key_size as usize],
            value: &bytes[STATIC_SIZE + key_size as usize..],
            timestamp: timestamp,
            deleted: value_size == TOMBSTONE
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
