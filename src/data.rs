use std::borrow::Cow;
use std::io::prelude::*;
use std::io::Cursor;
use std::result::Result::{Err, Ok};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use errors::{Error, Result};
use util::{XxHash32, xxhash32};

const ENTRY_STATIC_SIZE: usize = 18; // checksum(4) + sequence(8) + key_size(2) + value_size(4)
const ENTRY_TOMBSTONE: u32 = !0;
pub const MAX_VALUE_SIZE: u32 = !0 - 1;
pub const MAX_KEY_SIZE: u16 = !0;

pub type SequenceNumber = u64;

#[derive(Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    pub key: Cow<'a, [u8]>,
    pub value: Cow<'a, [u8]>,
    pub sequence: SequenceNumber,
    pub deleted: bool,
}

impl<'a> Entry<'a> {
    pub fn new<K, V>(sequence: SequenceNumber, key: K, value: V) -> Result<Entry<'a>>
        where Cow<'a, [u8]>: From<K>,
              Cow<'a, [u8]>: From<V>
    {
        let v = Cow::from(value);
        let k = Cow::from(key);

        if k.len() > MAX_KEY_SIZE as usize {
            return Err(Error::InvalidKeySize(k.len()));
        }

        if v.len() > MAX_VALUE_SIZE as usize {
            return Err(Error::InvalidValueSize(v.len()));
        }

        Ok(Entry {
               key: k,
               value: v,
               sequence: sequence,
               deleted: false,
           })
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

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut cursor = Cursor::new(Vec::with_capacity(self.size() as usize));
        cursor.set_position(4);
        cursor.write_u64::<LittleEndian>(self.sequence)?;
        cursor.write_u16::<LittleEndian>(self.key.len() as u16)?;

        if self.deleted {
            cursor.write_u32::<LittleEndian>(ENTRY_TOMBSTONE)?;
            cursor.write_all(&self.key)?;
        } else {
            cursor.write_u32::<LittleEndian>(self.value.len() as u32)?;
            cursor.write_all(&self.key)?;
            cursor.write_all(&self.value)?;
        }

        let checksum = xxhash32(&cursor.get_ref()[4..]);
        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum)?;

        Ok(cursor.into_inner())
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut cursor = Cursor::new(Vec::with_capacity(ENTRY_STATIC_SIZE));
        cursor.set_position(4);
        cursor.write_u64::<LittleEndian>(self.sequence)?;
        cursor.write_u16::<LittleEndian>(self.key.len() as u16)?;

        if self.deleted {
            cursor.write_u32::<LittleEndian>(ENTRY_TOMBSTONE)?;
        } else {
            cursor.write_u32::<LittleEndian>(self.value.len() as u32)?;
        }

        let checksum = {
            let mut hasher = XxHash32::new();
            hasher.update(&cursor.get_ref()[4..]);
            hasher.update(&self.key);
            hasher.update(&self.value);
            hasher.get()
        };

        cursor.set_position(0);
        cursor.write_u32::<LittleEndian>(checksum)?;

        writer.write_all(&cursor.into_inner())?;
        writer.write_all(&self.key)?;

        if !self.deleted {
            writer.write_all(&self.value)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Entry<'a>> {
        let mut cursor = Cursor::new(bytes);

        let checksum = cursor.read_u32::<LittleEndian>()?;
        assert_eq!(xxhash32(&bytes[4..]), checksum);

        let sequence = cursor.read_u64::<LittleEndian>()?;
        let key_size = cursor.read_u16::<LittleEndian>()?;
        let value_size = cursor.read_u32::<LittleEndian>()?;

        let deleted = value_size == ENTRY_TOMBSTONE;

        let value = if deleted {
            let empty: &[u8] = &[];
            Cow::from(empty)
        } else {
            Cow::from(&bytes[ENTRY_STATIC_SIZE + key_size as usize..])
        };

        Ok(Entry {
               key: Cow::from(&bytes[ENTRY_STATIC_SIZE..ENTRY_STATIC_SIZE + key_size as usize]),
               value: value,
               sequence: sequence,
               deleted: value_size == ENTRY_TOMBSTONE,
           })
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Result<Entry<'a>> {
        let mut header = vec![0u8; ENTRY_STATIC_SIZE as usize];
        reader.read_exact(&mut header)?;

        let mut cursor = Cursor::new(header);
        let checksum = cursor.read_u32::<LittleEndian>()?;
        let sequence = cursor.read_u64::<LittleEndian>()?;
        let key_size = cursor.read_u16::<LittleEndian>()?;
        let value_size = cursor.read_u32::<LittleEndian>()?;

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key)?;

        let deleted = value_size == ENTRY_TOMBSTONE;

        let value = if deleted {
            let empty: &[u8] = &[];
            Cow::from(empty)
        } else {
            let mut value = vec![0u8; value_size as usize];
            reader.read_exact(&mut value)?;
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

        Ok(Entry {
               key: Cow::from(key),
               value: value,
               sequence: sequence,
               deleted: deleted,
           })
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

    pub fn write_bytes<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u64::<LittleEndian>(self.sequence)?;
        writer.write_u16::<LittleEndian>(self.key.len() as u16)?;

        if self.deleted {
            writer.write_u32::<LittleEndian>(ENTRY_TOMBSTONE)?;
        } else {
            writer.write_u32::<LittleEndian>(self.value_size)?;
        }

        writer.write_u64::<LittleEndian>(self.entry_pos)?;
        writer.write_all(&self.key)?;

        Ok(())
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Result<Hint<'a>> {
        let sequence = reader.read_u64::<LittleEndian>()?;
        let key_size = reader.read_u16::<LittleEndian>()?;
        let value_size = reader.read_u32::<LittleEndian>()?;
        let entry_pos = reader.read_u64::<LittleEndian>()?;

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key)?;

        let deleted = value_size == ENTRY_TOMBSTONE;

        Ok(Hint {
               key: Cow::from(key),
               entry_pos: entry_pos,
               value_size: if deleted { 0 } else { value_size },
               sequence: sequence,
               deleted: value_size == ENTRY_TOMBSTONE,
           })
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
        let entry = Entry::new(sequence, key, value).unwrap();
        let deleted_entry = Entry::deleted(sequence, key);

        assert_eq!(entry.to_bytes().unwrap().len(), 24);

        assert_eq!(entry,
                   Entry::from_bytes(&entry.to_bytes().unwrap()).unwrap());
        assert_eq!(entry,
                   Entry::from_read(&mut Cursor::new(entry.to_bytes().unwrap())).unwrap());
        let mut v = Vec::new();
        entry.write_bytes(&mut v).unwrap();
        assert_eq!(entry, Entry::from_bytes(&v).unwrap());

        assert_eq!(deleted_entry,
                   Entry::from_bytes(&deleted_entry.to_bytes().unwrap()).unwrap());
        assert_eq!(deleted_entry,
                   Entry::from_read(&mut Cursor::new(deleted_entry.to_bytes().unwrap())).unwrap());
        v.clear();
        deleted_entry.write_bytes(&mut v).unwrap();
        assert_eq!(deleted_entry, Entry::from_bytes(&v).unwrap());
    }

    #[test]
    fn test_deleted() {
        let sequence = 0;
        let key: &[u8] = &[0, 0, 0];

        assert!(Entry::deleted(sequence, key).deleted);
        assert_eq!(Entry::deleted(sequence, key).value.len(), 0);
    }
}
