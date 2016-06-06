extern crate byteorder;
extern crate crc;
extern crate fs2;
extern crate time;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use fs2::FileExt;

const ENTRY_STATIC_SIZE: usize = 14; // crc(4) + timestamp(4) + key_size(2) + value_size(4)
const ENTRY_TOMBSTONE: u32 = !0;

const DATA_FILE_EXTENSION: &'static str = "cask.data";
const LOCK_FILE_NAME: &'static str = "cask.lock";

#[derive(Debug, Eq, PartialEq)]
pub struct Entry<'a> {
    key: Cow<'a, [u8]>,
    value: Cow<'a, [u8]>,
    timestamp: u32,
    deleted: bool,
}

impl<'a> Entry<'a> {
    pub fn new<K, V>(key: K, value: V) -> Entry<'a>
        where Cow<'a, [u8]>: From<K>,
              Cow<'a, [u8]>: From<V>
    {
        let v = Cow::from(value);
        assert!(v.len() < ENTRY_TOMBSTONE as usize);

        Entry {
            key: Cow::from(key),
            value: v,
            timestamp: time::now().to_timespec().sec as u32,
            deleted: false,
        }
    }

    pub fn deleted<K>(key: K) -> Entry<'a>
        where Cow<'a, [u8]>: From<K>
    {
        Entry {
            key: Cow::from(key),
            value: Cow::Borrowed(&[]),
            timestamp: time::now().to_timespec().sec as u32,
            deleted: true,
        }
    }

    pub fn size(&self) -> u64 {
        ENTRY_STATIC_SIZE as u64 + self.key.len() as u64 + self.value.len() as u64
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::with_capacity(self.size() as usize));
        cursor.set_position(4);
        cursor.write_u32::<LittleEndian>(self.timestamp).unwrap();
        cursor.write_u16::<LittleEndian>(self.key.len() as u16).unwrap();

        if self.deleted {
            cursor.write_u32::<LittleEndian>(ENTRY_TOMBSTONE).unwrap();
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
        let mut cursor = Cursor::new(Vec::with_capacity(ENTRY_STATIC_SIZE));
        cursor.set_position(4);
        cursor.write_u32::<LittleEndian>(self.timestamp).unwrap();
        cursor.write_u16::<LittleEndian>(self.key.len() as u16).unwrap();

        if self.deleted {
            cursor.write_u32::<LittleEndian>(ENTRY_TOMBSTONE).unwrap();
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
            key: Cow::from(&bytes[ENTRY_STATIC_SIZE..ENTRY_STATIC_SIZE + key_size as usize]),
            value: Cow::from(&bytes[ENTRY_STATIC_SIZE + key_size as usize..]),
            timestamp: timestamp,
            deleted: value_size == ENTRY_TOMBSTONE,
        }
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Entry<'a> {
        let mut header = vec![0u8; ENTRY_STATIC_SIZE as usize];
        reader.read(&mut header).unwrap();

        let mut cursor = Cursor::new(header);
        let checksum = cursor.read_u32::<LittleEndian>().unwrap();
        let timestamp = cursor.read_u32::<LittleEndian>().unwrap();
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

pub struct KeyEntry {
    file_id: usize,
    entry_pos: u64,
    entry_size: u64,
    timestamp: u32,
}

pub type KeyDir = HashMap<Vec<u8>, KeyEntry>;

pub struct Cask {
    path: PathBuf,
    key_dir: KeyDir,
    lock_file: File,
    current_file_id: usize,
    active_file: File,
    sync: bool,
}

fn get_data_file_path(path: &Path, file_id: usize) -> PathBuf {
    path.join(file_id.to_string()).with_extension(DATA_FILE_EXTENSION)
}

fn get_file_handle(path: &Path, write: bool) -> File {
    if write {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .unwrap()
    } else {
        OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap()
    }
}

impl Cask {
    pub fn open(path: &str, sync: bool) -> Cask {
        let path = PathBuf::from(path);

        if path.exists() {
            assert!(path.is_dir());
        } else {
            fs::create_dir(&path).unwrap();
        }

        let lock_file = File::create(path.join(LOCK_FILE_NAME)).unwrap();

        lock_file.try_lock_exclusive().unwrap();

        let mut key_dir = KeyDir::new();

        let current_file_id = 0;

        let mut file = get_file_handle(&get_data_file_path(&path, current_file_id), true);
        let file_size = file.metadata().unwrap().len();

        let mut file_pos = 0;
        while file_pos < file_size {
            let entry = Entry::from_read(&mut file);

            if entry.deleted {
                key_dir.remove(&entry.key.into_owned());
            } else {
                let key_entry = KeyEntry {
                    file_id: current_file_id,
                    entry_pos: file_pos,
                    entry_size: entry.size(),
                    timestamp: entry.timestamp,
                };
                key_dir.insert(entry.key.into_owned(), key_entry);
            }

            file_pos = file.seek(SeekFrom::Current(0)).unwrap();
        }

        Cask {
            path: path,
            key_dir: key_dir,
            lock_file: lock_file,
            current_file_id: current_file_id,
            active_file: file,
            sync: sync,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.key_dir.get(key).and_then(|key_entry| {
            let mut file = get_file_handle(&get_data_file_path(&self.path, key_entry.file_id),
                                           false);

            file.seek(SeekFrom::Start(key_entry.entry_pos)).unwrap();

            let mut entry = vec![0u8; key_entry.entry_size as usize];
            file.read_exact(&mut entry).unwrap();

            let entry = Entry::from_bytes(&entry);

            if entry.deleted {
                None
            } else {
                Some(entry.value.into_owned())
            }
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: &[u8]) {
        let key_entry = {
            let entry = Entry::new(&*key, value);
            let active_file_pos = self.active_file.seek(SeekFrom::Current(0)).unwrap();

            entry.write_bytes(&mut self.active_file);

            KeyEntry {
                file_id: self.current_file_id,
                entry_pos: active_file_pos,
                entry_size: entry.size(),
                timestamp: entry.timestamp,
            }
        };

        self.key_dir.insert(key, key_entry);

        if self.sync {
            self.active_file.sync_data().unwrap();
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        if self.key_dir.remove(key).is_some() {
            let entry = Entry::deleted(key);
            entry.write_bytes(&mut self.active_file);

            if self.sync {
                self.active_file.sync_data().unwrap();
            }
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
        let deleted_entry = Entry::deleted(key);

        assert_eq!(entry.to_bytes().len(), 20);

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
        let key: &[u8] = &[0, 0, 0];

        assert!(Entry::deleted(key).deleted);
        assert_eq!(Entry::deleted(key).value.len(), 0);
    }
}
