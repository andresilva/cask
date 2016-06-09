use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::vec::Vec;

use fs2::FileExt;
use time;

use data::{Entry, Hint};
use log::Log;
use util::Crc32;

#[derive(Debug)]
pub struct KeyEntry {
    file_id: u32,
    entry_pos: u64,
    entry_size: u64,
    timestamp: u32,
}

pub type KeyDir = HashMap<Vec<u8>, KeyEntry>;

pub struct CaskInner {
    key_dir: KeyDir,
    log: Log,
}

impl CaskInner {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.key_dir.get(key).and_then(|key_entry| {
            let entry = self.log.read_entry(key_entry.file_id,
                                            key_entry.entry_pos,
                                            key_entry.entry_size,
                                            key_entry.timestamp);
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

            let (file_id, file_pos) = self.log.write_entry(&entry);

            KeyEntry {
                file_id: file_id,
                entry_pos: file_pos,
                entry_size: entry.size(),
                timestamp: entry.timestamp,
            }
        };

        self.key_dir.insert(key, key_entry);
    }

    pub fn delete(&mut self, key: &[u8]) {
        if self.key_dir.remove(key).is_some() {
            let entry = Entry::deleted(key);
            let _ = self.log.write_entry(&entry);
        }
    }
}

#[derive(Clone)]
pub struct Cask {
    inner: Arc<RwLock<CaskInner>>,
}

impl Cask {
    pub fn open(path: &str, sync: bool) -> Cask {
        info!("Opening database: {:?}", &path);
        let mut log = Log::open(path, sync);
        let mut key_dir = KeyDir::new();

        let files = log.find_files();

        for file_id in files {
            let mut f = |hint: Hint| {
                if hint.deleted {
                    key_dir.remove(&hint.key.into_owned());
                } else {
                    let key_entry = KeyEntry {
                        file_id: file_id,
                        entry_pos: hint.entry_pos,
                        entry_size: hint.entry_size(),
                        timestamp: hint.timestamp,
                    };
                    key_dir.insert(hint.key.into_owned(), key_entry);
                }
            };

            match log.hints(file_id) {
                Some(hints) => {
                    for hint in hints {
                        f(hint);
                    }
                }
                None => {
                    for hint in log.recreate_hints(file_id) {
                        f(hint);
                    }
                }
            }
        }

        info!("Opened database: {:?}", &path);

        Cask {
            inner: Arc::new(RwLock::new(CaskInner {
                log: log,
                key_dir: key_dir,
            })),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.read().unwrap().get(key)
    }

    pub fn put(&self, key: Vec<u8>, value: &[u8]) {
        self.inner.write().unwrap().put(key, value)
    }

    pub fn delete(&self, key: &[u8]) {
        self.inner.write().unwrap().delete(key)
    }
}
