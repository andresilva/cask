use std::io::prelude::*;
use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use xxhash::xxhash32;

#[derive(Debug)]
pub struct FileEntry {
    pub timestamp: u32,
    pub key_size: u32,
    pub value_size: u64,
    pub key: String,
    pub value: Vec<u8>
}

const STATIC_SIZE: u8 = 16; // 4 + 4 + 8
const TOMBSTONE: &'static str = "▅▆▇█░█▇▆▅";

impl FileEntry {

    pub fn size(&self) -> u64 {
        STATIC_SIZE as u64 + self.key_size as u64 + self.value_size
    }

    pub fn encode(self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::with_capacity(self.size() as usize));

        cursor.set_position(4);
        cursor.write_u32::<LittleEndian>(self.timestamp).unwrap();
        cursor.write_u32::<LittleEndian>(self.key_size).unwrap();
        cursor.write_u64::<LittleEndian>(self.value_size).unwrap();
        cursor.write(&self.key.into_bytes()).unwrap();
        cursor.write(&self.value).unwrap();

        let checksum =
            xxhash32(&cursor.get_ref()[4..], STATIC_SIZE as u64 + self.key_size as u64 + self.value_size);

        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum).unwrap();

        let buf = cursor.into_inner();

        buf
    }

    pub fn decode(encoded: &[u8]) -> FileEntry {
        let mut cursor = Cursor::new(encoded);

        let checksum = cursor.read_u32::<LittleEndian>().unwrap();

        let timestamp = cursor.read_u32::<LittleEndian>().unwrap();
        let key_size = cursor.read_u32::<LittleEndian>().unwrap();
        let value_size = cursor.read_u64::<LittleEndian>().unwrap();

        let mut key = vec![0; key_size as usize];
        cursor.read(&mut key).unwrap();

        let mut value = vec![0; value_size as usize];
        cursor.read(&mut value).unwrap();

        assert_eq!(
            xxhash32(&encoded[4..], STATIC_SIZE as u64 + key_size as u64 + value_size),
            checksum);

        let fe = FileEntry {
            timestamp: timestamp,
            key_size: key_size,
            value_size: value_size,
            key: String::from_utf8(key).unwrap(),
            value: value
        };

        fe
    }
}
