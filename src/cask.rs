use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::vec::Vec;

use data::{Entry, Hint};
use log::Log;

#[derive(Debug)]
pub struct IndexEntry {
    file_id: u32,
    entry_pos: u64,
    entry_size: u64,
    timestamp: u32,
}

pub type Index = HashMap<Vec<u8>, IndexEntry>;

pub struct CaskInner {
    index: Index,
    log: Log,
}

impl CaskInner {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.index.get(key).and_then(|index_entry| {
            let entry = self.log.read_entry(index_entry.file_id, index_entry.entry_pos);
            if entry.deleted {
                warn!("Index pointed to dead entry: Entry {{ key: {:?}, timestamp: {} }}",
                      entry.key,
                      entry.timestamp);
                None
            } else {
                Some(entry.value.into_owned())
            }
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: &[u8]) {
        let index_entry = {
            let entry = Entry::new(&*key, value);

            let (file_id, file_pos) = self.log.write_entry(&entry);

            IndexEntry {
                file_id: file_id,
                entry_pos: file_pos,
                entry_size: entry.size(),
                timestamp: entry.timestamp,
            }
        };

        self.index.insert(key, index_entry);
    }

    pub fn delete(&mut self, key: &[u8]) {
        if self.index.remove(key).is_some() {
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
        let mut index = Index::new();

        for file_id in log.files() {
            let mut f = |hint: Hint| {
                if hint.deleted {
                    index.remove(&hint.key.into_owned());
                } else {
                    let index_entry = IndexEntry {
                        file_id: file_id,
                        entry_pos: hint.entry_pos,
                        entry_size: hint.entry_size(),
                        timestamp: hint.timestamp,
                    };
                    index.insert(hint.key.into_owned(), index_entry);
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
                index: index,
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
