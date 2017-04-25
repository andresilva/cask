use std::borrow::Cow;
use std::io::prelude::*;
use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use util::{XxHash32, xxhash32};

const ENTRY_STATIC_SIZE: usize = 18; // checksum(4) + sequence(8) + key_size(2) + value_size(4)
const ENTRY_TOMBSTONE: u32 = !0;

pub type SequenceNumber = u64;

#[derive(Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    pub key: Cow<'a, [u8]>,
    pub value: Cow<'a, [u8]>,
    pub sequence: SequenceNumber,
    pub deleted: bool,
}

impl<'a> Entry<'a> {
    pub fn new<K, V>(sequence: SequenceNumber, key: K, value: V) -> Entry<'a>
        where Cow<'a, [u8]>: From<K>,
              Cow<'a, [u8]>: From<V>
    {
        let v = Cow::from(value);
        assert!(v.len() < ENTRY_TOMBSTONE as usize);

        Entry {
            key: Cow::from(key),
            value: v,
            sequence: sequence,
            deleted: false,
        }
    }

    pub fn deleted<K>(sequence: SequenceNumber, key: K) -> Entry<'a>
        where Cow<'a, [u8]>: From<K>
    {
        Entry {
            key: Cow::from(key),
            value: Cow::Borrowed(&[]),
            sequence: sequence,
            deleted: true,
        }
    }

    pub fn size(&self) -> u64 {
        ENTRY_STATIC_SIZE as u64 + self.key.len() as u64 + self.value.len() as u64
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::with_capacity(self.size() as usize));
        cursor.set_position(4);
        cursor.write_u64::<LittleEndian>(self.sequence).unwrap();
        cursor
            .write_u16::<LittleEndian>(self.key.len() as u16)
            .unwrap();

        if self.deleted {
            cursor.write_u32::<LittleEndian>(ENTRY_TOMBSTONE).unwrap();
            cursor.write_all(&self.key).unwrap();
        } else {
            cursor
                .write_u32::<LittleEndian>(self.value.len() as u32)
                .unwrap();
            cursor.write_all(&self.key).unwrap();
            cursor.write_all(&self.value).unwrap();
        }

        let checksum = xxhash32(&cursor.get_ref()[4..]);
        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum).unwrap();

        cursor.into_inner()
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) {
        let mut cursor = Cursor::new(Vec::with_capacity(ENTRY_STATIC_SIZE));
        cursor.set_position(4);
        cursor.write_u64::<LittleEndian>(self.sequence).unwrap();
        cursor
            .write_u16::<LittleEndian>(self.key.len() as u16)
            .unwrap();

        if self.deleted {
            cursor.write_u32::<LittleEndian>(ENTRY_TOMBSTONE).unwrap();
        } else {
            cursor
                .write_u32::<LittleEndian>(self.value.len() as u32)
                .unwrap();
        }

        let checksum = {
            let mut hasher = XxHash32::new();
            hasher.update(&cursor.get_ref()[4..]);
            hasher.update(&self.key);
            hasher.update(&self.value);
            hasher.get()
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
        assert_eq!(xxhash32(&bytes[4..]), checksum);

        let sequence = cursor.read_u64::<LittleEndian>().unwrap();
        let key_size = cursor.read_u16::<LittleEndian>().unwrap();
        let value_size = cursor.read_u32::<LittleEndian>().unwrap();

        let deleted = value_size == ENTRY_TOMBSTONE;

        let value = if deleted {
            let empty: &[u8] = &[];
            Cow::from(empty)
        } else {
            Cow::from(&bytes[ENTRY_STATIC_SIZE + key_size as usize..])
        };

        Entry {
            key: Cow::from(&bytes[ENTRY_STATIC_SIZE..ENTRY_STATIC_SIZE + key_size as usize]),
            value: value,
            sequence: sequence,
            deleted: value_size == ENTRY_TOMBSTONE,
        }
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Entry<'a> {
        let mut header = vec![0u8; ENTRY_STATIC_SIZE as usize];
        reader.read(&mut header).unwrap();

        let mut cursor = Cursor::new(header);
        let checksum = cursor.read_u32::<LittleEndian>().unwrap();
        let sequence = cursor.read_u64::<LittleEndian>().unwrap();
        let key_size = cursor.read_u16::<LittleEndian>().unwrap();
        let value_size = cursor.read_u32::<LittleEndian>().unwrap();

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key).unwrap();

        let deleted = value_size == ENTRY_TOMBSTONE;

        let value = if deleted {
            let empty: &[u8] = &[];
            Cow::from(empty)
        } else {
            let mut value = vec![0u8; value_size as usize];
            reader.read_exact(&mut value).unwrap();
            Cow::from(value)
        };

        let hash = {
            let mut hasher = XxHash32::new();
            hasher.update(&cursor.get_ref()[4..]);
            hasher.update(&key);
            hasher.update(&value);
            hasher.get()
        };

        assert_eq!(hash, checksum);

        Entry {
            key: Cow::from(key),
            value: value,
            sequence: sequence,
            deleted: deleted,
        }
    }
}

pub struct Hint<'a> {
    pub key: Cow<'a, [u8]>,
    pub entry_pos: u64,
    pub value_size: u32,
    pub sequence: SequenceNumber,
    pub deleted: bool,
}

impl<'a> Hint<'a> {
    pub fn new(e: &'a Entry, entry_pos: u64) -> Hint<'a> {
        Hint {
            key: Cow::from(&*e.key),
            entry_pos: entry_pos,
            value_size: e.value.len() as u32,
            sequence: e.sequence,
            deleted: e.deleted,
        }
    }

    pub fn from(e: Entry<'a>, entry_pos: u64) -> Hint<'a> {
        Hint {
            key: e.key,
            entry_pos: entry_pos,
            value_size: e.value.len() as u32,
            sequence: e.sequence,
            deleted: e.deleted,
        }
    }

    pub fn entry_size(&self) -> u64 {
        ENTRY_STATIC_SIZE as u64 + self.key.len() as u64 + self.value_size as u64
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) {
        writer.write_u64::<LittleEndian>(self.sequence).unwrap();
        writer
            .write_u16::<LittleEndian>(self.key.len() as u16)
            .unwrap();

        if self.deleted {
            writer.write_u32::<LittleEndian>(ENTRY_TOMBSTONE).unwrap();
        } else {
            writer.write_u32::<LittleEndian>(self.value_size).unwrap();
        }

        writer.write_u64::<LittleEndian>(self.entry_pos).unwrap();
        writer.write(&self.key).unwrap();
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Hint<'a> {
        let sequence = reader.read_u64::<LittleEndian>().unwrap();
        let key_size = reader.read_u16::<LittleEndian>().unwrap();
        let value_size = reader.read_u32::<LittleEndian>().unwrap();
        let entry_pos = reader.read_u64::<LittleEndian>().unwrap();

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key).unwrap();

        let deleted = value_size == ENTRY_TOMBSTONE;

        Hint {
            key: Cow::from(key),
            entry_pos: entry_pos,
            value_size: if deleted { 0 } else { value_size },
            sequence: sequence,
            deleted: value_size == ENTRY_TOMBSTONE,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use data::Entry;

    #[test]
    fn test_serialization() {
        let sequence = 0;
        let key: &[u8] = &[0, 0, 0];
        let value: &[u8] = &[0, 0, 0];
        let entry = Entry::new(sequence, key, value);
        let deleted_entry = Entry::deleted(sequence, key);

        assert_eq!(entry.to_bytes().len(), 24);

        assert_eq!(entry, Entry::from_bytes(&entry.to_bytes()));
        assert_eq!(entry, Entry::from_read(&mut Cursor::new(entry.to_bytes())));
        let mut v = Vec::new();
        entry.write_bytes(&mut v);
        assert_eq!(entry, Entry::from_bytes(&v));

        assert_eq!(deleted_entry, Entry::from_bytes(&deleted_entry.to_bytes()));
        assert_eq!(deleted_entry,
                   Entry::from_read(&mut Cursor::new(deleted_entry.to_bytes())));
        v.clear();
        deleted_entry.write_bytes(&mut v);
        assert_eq!(deleted_entry, Entry::from_bytes(&v));
    }

    #[test]
    fn test_deleted() {
        let sequence = 0;
        let key: &[u8] = &[0, 0, 0];

        assert!(Entry::deleted(sequence, key).deleted);
        assert_eq!(Entry::deleted(sequence, key).value.len(), 0);
    }
}
