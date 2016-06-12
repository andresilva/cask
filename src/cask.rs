use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use std::sync::{Arc, RwLock};
use std::vec::Vec;

use data::{Entry, Hint, SequenceNumber};
use log::{Log, LogWriter};

#[derive(Debug)]
struct IndexEntry {
    file_id: u32,
    entry_pos: u64,
    entry_size: u64,
    sequence: SequenceNumber,
}

type Index = HashMap<Vec<u8>, IndexEntry>;

struct CaskInner {
    current_sequence: SequenceNumber,
    index: Index,
    log: Log,
}

impl CaskInner {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.index.get(key).and_then(|index_entry| {
            let entry = self.log.read_entry(index_entry.file_id, index_entry.entry_pos);
            if entry.deleted {
                warn!("Index pointed to dead entry: Entry {{ key: {:?}, sequence: {} }}",
                      entry.key,
                      entry.sequence);
                None
            } else {
                Some(entry.value.into_owned())
            }
        })
    }

    fn put(&mut self, key: Vec<u8>, value: &[u8]) {
        let index_entry = {
            let entry = Entry::new(self.current_sequence, &*key, value);

            let (file_id, file_pos) = self.log.append_entry(&entry);

            self.current_sequence += 1;

            IndexEntry {
                file_id: file_id,
                entry_pos: file_pos,
                entry_size: entry.size(),
                sequence: entry.sequence,
            }
        };

        self.index.insert(key, index_entry);
    }

    fn delete(&mut self, key: &[u8]) {
        if self.index.remove(key).is_some() {
            let entry = Entry::deleted(self.current_sequence, key);
            let _ = self.log.append_entry(&entry);
            self.current_sequence += 1;
        }
    }

    fn compact(&self, file_id: u32) {
        let new_file_id = self.log.new_file_id();

        info!("Compacting data file: {} into: {}", file_id, new_file_id);

        if let Some(hints) = self.log.hints(file_id) {
            let mut log_writer = LogWriter::new(&self.log.path, new_file_id, false);
            let mut deletes = HashMap::new();

            {
                let inserts = hints.filter(|hint| {
                    let index_entry = self.index.get(&*hint.key);

                    if hint.deleted {
                        if index_entry.is_none() {
                            match deletes.entry(hint.key.to_vec()) {
                                HashMapEntry::Occupied(mut o) => {
                                    if *o.get() < hint.sequence {
                                        o.insert(hint.sequence);
                                    }
                                }
                                HashMapEntry::Vacant(e) => {
                                    e.insert(hint.sequence);
                                }
                            }
                        }

                        false

                    } else {
                        index_entry.is_some() && index_entry.unwrap().sequence == hint.sequence
                    }
                });

                for hint in inserts {
                    log_writer.write(&self.log.read_entry(file_id, hint.entry_pos));
                }
            }

            for (key, sequence) in deletes {
                log_writer.write(&Entry::deleted(sequence, key));
            }
        };
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

        let mut sequence = 0;

        for file_id in log.files() {
            let mut f = |hint: Hint| {
                if hint.sequence > sequence {
                    sequence = hint.sequence;
                }

                if hint.deleted {
                    index.remove(&hint.key.into_owned());
                } else {
                    let index_entry = IndexEntry {
                        file_id: file_id,
                        entry_pos: hint.entry_pos,
                        entry_size: hint.entry_size(),
                        sequence: hint.sequence,
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
            };
        }

        info!("Opened database: {:?}", &path);
        info!("Current sequence number: {:?}", sequence);

        Cask {
            inner: Arc::new(RwLock::new(CaskInner {
                current_sequence: sequence + 1,
                log: log,
                index: index,
            })),
        }
    }

    pub fn compact(&self, file_id: u32) {
        self.inner.read().unwrap().compact(file_id);
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
