#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate crc;
extern crate fs2;
extern crate regex;
extern crate time;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::FileExt;
use regex::Regex;

mod util;
use util::{crc32, Crc32};

const ENTRY_STATIC_SIZE: usize = 14; // crc(4) + timestamp(4) + key_size(2) + value_size(4)
const ENTRY_TOMBSTONE: u32 = !0;

const HINT_STATIC_SIZE: usize = 14; // timestamp(4) + key_size(2) + entry_size(4) + entry_pos(4)

const DATA_FILE_EXTENSION: &'static str = "cask.data";
const HINT_FILE_EXTENSION: &'static str = "cask.hint";
const LOCK_FILE_NAME: &'static str = "cask.lock";

const DEFAULT_SIZE_THRESHOLD: usize = 100 * 1024 * 1024;

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

        let checksum = crc32(&cursor.get_ref()[4..]);
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
            let mut digest = Crc32::new();
            digest.write(&cursor.get_ref()[4..]);
            digest.write(&self.key);
            digest.write(&self.value);
            digest.sum32()
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
        assert_eq!(crc32(&bytes[4..]), checksum);

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
            let mut digest = Crc32::new();
            digest.write(&cursor.get_ref()[4..]);
            digest.write(&key);
            digest.write(&value);
            digest.sum32()
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

pub struct Hint<'a> {
    key: Cow<'a, [u8]>,
    entry_pos: u64,
    value_size: u32,
    timestamp: u32,
    deleted: bool,
}

impl<'a> Hint<'a> {
    pub fn new(e: &'a Entry, entry_pos: u64) -> Hint<'a> {
        Hint {
            key: Cow::from(&*e.key),
            entry_pos: entry_pos,
            value_size: e.value.len() as u32,
            timestamp: e.timestamp,
            deleted: e.deleted,
        }
    }

    pub fn size(&self) -> u64 {
        HINT_STATIC_SIZE as u64 + self.key.len() as u64
    }

    fn entry_size(&self) -> u64 {
        ENTRY_STATIC_SIZE as u64 + self.key.len() as u64 + self.value_size as u64
    }

    pub fn write_bytes<W: Write>(&self, writer: &mut W) {
        writer.write_u32::<LittleEndian>(self.timestamp).unwrap();
        writer.write_u16::<LittleEndian>(self.key.len() as u16).unwrap();
        writer.write_u32::<LittleEndian>(self.value_size).unwrap();
        writer.write_u64::<LittleEndian>(self.entry_pos).unwrap();
        writer.write(&self.key).unwrap();
    }

    pub fn from_read<R: Read>(reader: &mut R) -> Hint<'a> {
        let timestamp = reader.read_u32::<LittleEndian>().unwrap();
        let key_size = reader.read_u16::<LittleEndian>().unwrap();
        let value_size = reader.read_u32::<LittleEndian>().unwrap();
        let entry_pos = reader.read_u64::<LittleEndian>().unwrap();

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key).unwrap();

        Hint {
            key: Cow::from(key),
            entry_pos: entry_pos,
            value_size: value_size,
            timestamp: timestamp,
            deleted: value_size == ENTRY_TOMBSTONE,
        }
    }
}

#[derive(Debug)]
pub struct KeyEntry {
    file_id: u32,
    entry_pos: u64,
    entry_size: u64,
    timestamp: u32,
}

pub type KeyDir = HashMap<Vec<u8>, KeyEntry>;

pub struct CaskInner {
    path: PathBuf,
    lock_file: File,
    key_dir: KeyDir,
    current_file_id: u32,
    active_data_file: File,
    active_hint_file: File,
    active_hint_file_digest: Crc32,
}

impl CaskInner {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.key_dir.get(key).and_then(|key_entry| {
            let mut data_file = get_file_handle(&get_data_file_path(&self.path, key_entry.file_id),
                                                false);

            data_file.seek(SeekFrom::Start(key_entry.entry_pos)).unwrap();

            let mut entry = vec![0u8; key_entry.entry_size as usize];
            data_file.read_exact(&mut entry).unwrap();

            let entry = Entry::from_bytes(&entry);

            if entry.deleted {
                None
            } else {
                Some(entry.value.into_owned())
            }
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: &[u8], sync: bool, size_threshold: usize) {
        let key_entry = {
            let entry = Entry::new(&*key, value);
            let mut active_data_file_pos =
                self.active_data_file.seek(SeekFrom::Current(0)).unwrap();

            if active_data_file_pos + entry.size() > size_threshold as u64 {
                if sync {
                    self.active_data_file.sync_data().unwrap();
                }

                self.current_file_id = time::now().to_timespec().sec as u32;

                self.active_data_file =
                    get_file_handle(&get_data_file_path(&self.path, self.current_file_id), true);
                self.active_hint_file =
                    get_file_handle(&get_hint_file_path(&self.path, self.current_file_id), true);
                self.active_hint_file_digest = Crc32::new();

                active_data_file_pos = 0
            }

            let hint = Hint::new(&entry, active_data_file_pos);

            entry.write_bytes(&mut self.active_data_file);
            hint.write_bytes(&mut self.active_hint_file);
            hint.write_bytes(&mut self.active_hint_file_digest);

            KeyEntry {
                file_id: self.current_file_id,
                entry_pos: active_data_file_pos,
                entry_size: entry.size(),
                timestamp: entry.timestamp,
            }
        };

        self.key_dir.insert(key, key_entry);

        if sync {
            self.active_data_file.sync_data().unwrap();
        }
    }

    pub fn delete(&mut self, key: &[u8], sync: bool) {
        if self.key_dir.remove(key).is_some() {
            let active_data_file_pos = self.active_data_file.seek(SeekFrom::Current(0)).unwrap();
            let entry = Entry::deleted(key);
            let hint = Hint::new(&entry, active_data_file_pos);

            entry.write_bytes(&mut self.active_data_file);
            hint.write_bytes(&mut self.active_hint_file);
            hint.write_bytes(&mut self.active_hint_file_digest);

            if sync {
                self.active_data_file.sync_data().unwrap();
            }
        }
    }
}

impl Drop for CaskInner {
    fn drop(&mut self) {
        self.active_hint_file
            .write_u32::<LittleEndian>(self.active_hint_file_digest.sum32())
            .unwrap();

        self.lock_file.unlock().unwrap();
    }
}

#[derive(Clone)]
pub struct Cask {
    sync: bool,
    size_threshold: usize,
    inner: Arc<RwLock<CaskInner>>,
}

fn get_file_handle(path: &Path, write: bool) -> File {
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

fn get_data_file_path(path: &Path, file_id: u32) -> PathBuf {
    path.join(file_id.to_string()).with_extension(DATA_FILE_EXTENSION)
}

fn find_data_files(path: &Path) -> Vec<u32> {
    let files = fs::read_dir(path).unwrap();

    lazy_static! {
            static ref RE: Regex =
                Regex::new(&format!("(\\d+).{}$", DATA_FILE_EXTENSION)).unwrap();
        }

    let mut files: Vec<u32> = files.flat_map(|f| {
            let file = f.unwrap();
            let file_metadata = file.metadata().unwrap();

            if file_metadata.is_file() {
                let file_name = file.file_name();
                let captures = RE.captures(file_name.to_str().unwrap());
                captures.and_then(|c| c.at(1).and_then(|n| n.parse::<u32>().ok()))
            } else {
                None
            }
        })
        .collect();

    files.sort();

    files
}

fn get_hint_file_path(path: &Path, file_id: u32) -> PathBuf {
    path.join(file_id.to_string()).with_extension(HINT_FILE_EXTENSION)
}

fn is_valid_hint_file(path: &Path) -> bool {
    path.is_file() &&
    {
        let mut hint_file = get_file_handle(path, false);

        // FIXME: avoid reading the whole hint file into memory;
        let mut buf = Vec::new();
        hint_file.read_to_end(&mut buf).unwrap();

        buf.len() >= 4 &&
        {
            let crc = crc32(&buf[..buf.len() - 4]);

            let mut cursor = Cursor::new(&buf[buf.len() - 4..]);
            let checksum = cursor.read_u32::<LittleEndian>().unwrap();

            let valid = crc == checksum;

            if !valid {
                warn!("Found corrupt hint file: {:?}. Recreating", &path);
            }

            valid
        }
    }
}

impl Cask {
    pub fn open(path: &str, sync: bool) -> Cask {
        let path = PathBuf::from(path);

        info!("Opening database: {:?}", &path);

        if path.exists() {
            assert!(path.is_dir());
        } else {
            fs::create_dir(&path).unwrap();
        }

        let lock_file = File::create(path.join(LOCK_FILE_NAME)).unwrap();

        lock_file.try_lock_exclusive().unwrap();

        let mut key_dir = KeyDir::new();

        let data_files = find_data_files(&path);

        for file_id in &data_files {
            let hint_file_path = get_hint_file_path(&path, *file_id);

            if is_valid_hint_file(&hint_file_path) {
                info!("Loading hint file: {:?}", hint_file_path);
                let mut hint_file = get_file_handle(&hint_file_path, false);
                let hint_file_size = hint_file.metadata().unwrap().len();

                let mut hint_file_pos = 0;
                while hint_file_pos < hint_file_size - 4 {
                    let hint = Hint::from_read(&mut hint_file);

                    if hint.deleted {
                        key_dir.remove(&hint.key.into_owned());
                    } else {
                        let key_entry = KeyEntry {
                            file_id: *file_id,
                            entry_pos: hint.entry_pos,
                            entry_size: hint.entry_size(),
                            timestamp: hint.timestamp,
                        };
                        key_dir.insert(hint.key.into_owned(), key_entry);
                    }

                    hint_file_pos = hint_file.seek(SeekFrom::Current(0)).unwrap();
                }
            } else {
                let data_file_path = get_data_file_path(&path, *file_id);
                info!("Loading data file: {:?}", data_file_path);

                let mut data_file = get_file_handle(&data_file_path, false);
                let mut hint_file = get_file_handle(&hint_file_path, true);
                let mut hint_file_digest = Crc32::new();
                let data_file_size = data_file.metadata().unwrap().len();

                let mut data_file_pos = 0;
                while data_file_pos < data_file_size {
                    let entry = Entry::from_read(&mut data_file);

                    {
                        let hint = Hint::new(&entry, data_file_pos);
                        hint.write_bytes(&mut hint_file);
                        hint.write_bytes(&mut hint_file_digest);
                    }

                    if entry.deleted {
                        key_dir.remove(&entry.key.into_owned());
                    } else {
                        let key_entry = KeyEntry {
                            file_id: *file_id,
                            entry_pos: data_file_pos,
                            entry_size: entry.size(),
                            timestamp: entry.timestamp,
                        };
                        key_dir.insert(entry.key.into_owned(), key_entry);
                    }


                    data_file_pos = data_file.seek(SeekFrom::Current(0)).unwrap();
                }

                hint_file.write_u32::<LittleEndian>(hint_file_digest.sum32()).unwrap();
            }
        }

        let current_file_id = time::now().to_timespec().sec as u32;
        let active_data_file = get_file_handle(&get_data_file_path(&path, current_file_id), true);
        let active_hint_file = get_file_handle(&get_hint_file_path(&path, current_file_id), true);
        let active_hint_file_digest = Crc32::new();

        info!("Opened database: {:?}", &path);

        let inner = CaskInner {
            path: path,
            lock_file: lock_file,
            key_dir: key_dir,
            current_file_id: current_file_id,
            active_data_file: active_data_file,
            active_hint_file: active_hint_file,
            active_hint_file_digest: active_hint_file_digest,
        };

        Cask {
            sync: sync,
            size_threshold: DEFAULT_SIZE_THRESHOLD,
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.read().unwrap().get(key)
    }

    pub fn put(&self, key: Vec<u8>, value: &[u8]) {
        self.inner.write().unwrap().put(key, value, self.sync, self.size_threshold)
    }

    pub fn delete(&self, key: &[u8]) {
        self.inner.write().unwrap().delete(key, self.sync)
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
