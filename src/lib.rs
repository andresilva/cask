#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate crc;
extern crate fs2;
extern crate regex;
extern crate time;

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

mod data;
mod util;

use data::{Entry, Hint};
use util::{crc32, Crc32};

const DATA_FILE_EXTENSION: &'static str = "cask.data";
const HINT_FILE_EXTENSION: &'static str = "cask.hint";
const LOCK_FILE_NAME: &'static str = "cask.lock";

const DEFAULT_SIZE_THRESHOLD: usize = 100 * 1024 * 1024;

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
