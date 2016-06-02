extern crate byteorder;
extern crate crc;
extern crate time;

use std::borrow::Cow;
use std::io::{Cursor, Read, Write};
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;

#[derive(Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    key: Cow<'a, [u8]>,
    value: Cow<'a, [u8]>,
    timestamp: u32,
    deleted: bool,
}

const STATIC_SIZE: usize = 14; // crc(4) + timestamp(4) + key_size(2) + value_size(4)
const TOMBSTONE: u32 = !0;

impl<'a> Entry<'a> {
    pub fn new<K, V>(key: K, value: V) -> Entry<'a>
        where Cow<'a, [u8]>: From<K>,
              Cow<'a, [u8]>: From<V>
    {
        let v = Cow::from(value);
        assert!(v.len() < TOMBSTONE as usize);

        Entry {
            key: Cow::from(key),
            value: v,
            timestamp: time::now().to_timespec().sec as u32,
            deleted: false,
        }
    }

    pub fn deleted(&self) -> Entry<'a> {
        Entry {
            key: self.key.clone(),
            value: Cow::Borrowed(&[]),
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
            cursor.write_all(&self.key).unwrap();
        } else {
            cursor.write_u32::<LittleEndian>(self.value.len() as u32).unwrap();
            cursor.write_all(&self.key).unwrap();
            cursor.write_all(&self.value).unwrap();
        }

        let checksum = crc32::checksum_ieee(&cursor.get_ref()[4..]);
        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum).unwrap();

        cursor.into_inner()
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) {
        let mut cursor = Cursor::new(Vec::with_capacity(STATIC_SIZE));
        cursor.set_position(4);
        cursor.write_u32::<LittleEndian>(self.timestamp).unwrap();
        cursor.write_u16::<LittleEndian>(self.key.len() as u16).unwrap();

        if self.deleted {
            cursor.write_u32::<LittleEndian>(TOMBSTONE).unwrap();
        } else {
            cursor.write_u32::<LittleEndian>(self.value.len() as u32).unwrap();
        }

        let checksum = {
            // unfortunately I had to inline the checksum code since it only accepts slices as
            // arguments (and I wanted to keep the iterator to avoid needless copying)
            let mut v: u32 = !0;
            let t = &crc32::IEEE_TABLE;
            for i in cursor.get_ref()[4..].iter().chain(self.key.iter().chain(self.value.iter())) {
                v = t[((v as u8) ^ i) as usize] ^ (v >> 8)
            }
            !v
        };

        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum).unwrap();

        writer.write_all(&cursor.into_inner()).unwrap();
        writer.write_all(&self.key).unwrap();

        if !self.deleted {
            writer.write_all(&self.value).unwrap();
        }
    }

    pub fn from_bytes(bytes: &'a [u8]) -> Entry<'a> {
        let mut cursor = Cursor::new(bytes);

        let checksum = cursor.read_u32::<LittleEndian>().unwrap();
        assert_eq!(crc32::checksum_ieee(&bytes[4..]), checksum);

        let timestamp = cursor.read_u32::<LittleEndian>().unwrap();
        let key_size = cursor.read_u16::<LittleEndian>().unwrap();
        let value_size = cursor.read_u32::<LittleEndian>().unwrap();

        Entry {
            key: Cow::from(&bytes[STATIC_SIZE..STATIC_SIZE + key_size as usize]),
            value: Cow::from(&bytes[STATIC_SIZE + key_size as usize..]),
            timestamp: timestamp,
            deleted: value_size == TOMBSTONE,
        }
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Entry<'a> {
        let mut header = vec![0u8; STATIC_SIZE as usize];
        reader.read(&mut header).unwrap();

        let mut cursor = Cursor::new(header);
        let checksum = cursor.read_u32::<LittleEndian>().unwrap();
        let timestamp = cursor.read_u32::<LittleEndian>().unwrap();
        let key_size = cursor.read_u16::<LittleEndian>().unwrap();
        let value_size = cursor.read_u32::<LittleEndian>().unwrap();

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key).unwrap();

        let deleted = value_size == TOMBSTONE;

        let value = if deleted {
            let empty: &[u8] = &[];
            Cow::from(empty)
        } else {
            let mut value = vec![0u8; value_size as usize];
            reader.read_exact(&mut value).unwrap();
            Cow::from(value)
        };

        let crc = {
            // unfortunately I had to inline the checksum code since it only accepts slices as
            // arguments (and I wanted to keep the iterator to avoid needless copying)
            let mut v: u32 = !0;
            let t = &crc32::IEEE_TABLE;
            for i in cursor.get_ref()[4..].iter().chain(key.iter().chain(value.iter())) {
                v = t[((v as u8) ^ i) as usize] ^ (v >> 8)
            }
            !v
        };

        assert_eq!(crc, checksum);

        Entry {
            key: Cow::from(key),
            value: Cow::from(value),
            timestamp: timestamp,
            deleted: deleted,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use Entry;

    #[test]
    fn test_serialization() {
        let key: &[u8] = &[0, 0, 0];
        let value: &[u8] = &[0, 0, 0];
        let entry = Entry::new(key, value);

        assert_eq!(entry.to_bytes().len(), 20);

        assert_eq!(entry, Entry::from_bytes(&entry.to_bytes()));
        assert_eq!(entry, Entry::from_read(&mut Cursor::new(entry.to_bytes())));
        let mut v = Vec::new();
        entry.write_bytes(&mut v);
        assert_eq!(entry, Entry::from_bytes(&v));

        assert_eq!(entry.deleted(),
                   Entry::from_bytes(&entry.deleted().to_bytes()));
        assert_eq!(entry.deleted(),
                   Entry::from_read(&mut Cursor::new(entry.deleted().to_bytes())));
        v.clear();
        entry.deleted().write_bytes(&mut v);
        assert_eq!(entry.deleted(), Entry::from_bytes(&v));

        let empty_entry = Entry::new(key, vec![]);
        assert_eq!(empty_entry, Entry::from_bytes(&empty_entry.to_bytes()));
        assert_eq!(empty_entry,
                   Entry::from_read(&mut Cursor::new(&empty_entry.to_bytes())));
        v.clear();
        empty_entry.write_bytes(&mut v);
        assert_eq!(empty_entry, Entry::from_bytes(&v));

        assert_eq!(empty_entry.deleted(),
                   Entry::from_bytes(&empty_entry.deleted().to_bytes()));
        assert_eq!(empty_entry.deleted(),
                   Entry::from_read(&mut Cursor::new(&empty_entry.deleted().to_bytes())));
        v.clear();
        empty_entry.deleted().write_bytes(&mut v);
        assert_eq!(empty_entry.deleted(), Entry::from_bytes(&v));

        assert!(Entry::from_bytes(&empty_entry.deleted().to_bytes()).deleted);
        assert!(Entry::from_read(&mut Cursor::new(&empty_entry.deleted().to_bytes())).deleted);
        assert!(Entry::from_bytes(&v).deleted);
        assert!(empty_entry.deleted().deleted);
    }

    #[test]
    fn test_deleted() {
        let key: &[u8] = &[0, 0, 0];
        let value: &[u8] = &[0, 0, 0];
        let entry = Entry::new(key, value);

        assert!(entry.deleted().deleted);
        assert_eq!(entry.deleted().value.len(), 0);
    }
}
