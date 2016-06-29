use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use std::vec::Vec;

use data::{Entry, Hint, SequenceNumber};
use log::{Log, LogWriter};
use stats::Stats;

const COMPACTION_CHECK_FREQUENCY: u64 = 60;
const FRAGMENTATION_THRESHOLD: f64 = 0.6;

#[derive(Debug)]
pub struct IndexEntry {
    pub file_id: u32,
    entry_pos: u64,
    pub entry_size: u64,
    sequence: SequenceNumber,
}

struct Index {
    map: HashMap<Vec<u8>, IndexEntry>,
    stats: Stats,
}

impl Index {
    fn new() -> Index {
        Index {
            map: HashMap::new(),
            stats: Stats::new(),
        }
    }

    fn get(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.map.get(key)
    }

    fn insert(&mut self, key: Vec<u8>, index_entry: IndexEntry) -> Option<IndexEntry> {
        self.stats.add_entry(&index_entry);
        self.map.insert(key, index_entry).map(|entry| {
            self.stats.remove_entry(&entry);
            entry
        })
    }

    fn remove(&mut self, key: &[u8]) -> Option<IndexEntry> {
        self.map.remove(key).map(|entry| {
            self.stats.remove_entry(&entry);
            entry
        })
    }

    fn update(&mut self, hint: Hint, file_id: u32) {
        let index_entry = IndexEntry {
            file_id: file_id,
            entry_pos: hint.entry_pos,
            entry_size: hint.entry_size(),
            sequence: hint.sequence,
        };

        match self.map.entry(hint.key.to_vec()) {
            HashMapEntry::Occupied(mut o) => {
                if o.get().sequence <= hint.sequence {
                    self.stats.remove_entry(o.get());
                    if hint.deleted {
                        o.remove();
                    } else {
                        self.stats.add_entry(&index_entry);
                        o.insert(index_entry);
                    }
                } else {
                    self.stats.add_entry(&index_entry);
                    self.stats.remove_entry(&index_entry);
                }
            }
            HashMapEntry::Vacant(e) => {
                if !hint.deleted {
                    self.stats.add_entry(&index_entry);
                    e.insert(index_entry);
                }
            }
        }
    }
}

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
                warn!("Index pointed to dead entry: Entry {{ key: {:?}, sequence: {} }} at file: \
                       {}",
                      entry.key,
                      entry.sequence,
                      index_entry.file_id);
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
}

#[derive(Clone)]
pub struct Cask {
    path: PathBuf,
    dropped: Arc<AtomicBool>,
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

                index.update(hint, file_id);
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

        let cask = Cask {
            path: log.path.clone(),
            dropped: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(RwLock::new(CaskInner {
                current_sequence: sequence + 1,
                log: log,
                index: index,
            })),
        };

        let caskt = cask.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::new(COMPACTION_CHECK_FREQUENCY, 0));

                info!("Compaction thread wake up");

                if caskt.dropped.load(Ordering::SeqCst) {
                    info!("Cask has been dropped, background compaction thread is exiting");
                    break;
                }

                caskt.compact();
            }
        });

        cask
    }

    fn compact_file_aux(&self, file_id: u32) -> Option<u32> {
        if file_id == self.inner.read().unwrap().log.active_file_id {
            return None;
        }

        let hints = {
            self.inner.read().unwrap().log.hints(file_id)
        };

        hints.map(|hints| {
            let new_file_id = {
                self.inner.read().unwrap().log.new_file_id()
            };

            info!("Compacting data file: {} into: {}", file_id, new_file_id);

            let mut log_writer = LogWriter::new(&self.path, new_file_id, false);
            let mut deletes = HashMap::new();

            {
                let inserts = hints.filter(|hint| {
                    let inner = self.inner.read().unwrap();
                    let index_entry = inner.index.get(&*hint.key);

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
                    let log = &self.inner.read().unwrap().log;
                    log_writer.write(&log.read_entry(file_id, hint.entry_pos));
                }
            }

            for (key, sequence) in deletes {
                log_writer.write(&Entry::deleted(sequence, key));
            }

            new_file_id
        })
    }

    pub fn compact_file(&self, file_id: u32) {
        let new_file_id = self.compact_file_aux(file_id);

        if let Some(new_file_id) = new_file_id {
            let hints = {
                self.inner.read().unwrap().log.hints(new_file_id)
            };

            if let Some(hints) = hints {
                for hint in hints {
                    self.inner.write().unwrap().index.update(hint, new_file_id);
                }

                self.inner.write().unwrap().log.swap_file(file_id, new_file_id);

                info!("Finished compacting data file: {} into: {}",
                      file_id,
                      new_file_id);
            };
        }
    }

    pub fn compact(&self) {
        let iter = {
            self.inner
                .read()
                .unwrap()
                .index
                .stats
                .fragmentation()
        };

        for &(file_id, fragmentation) in iter.iter().filter(|e| e.1 >= FRAGMENTATION_THRESHOLD) {

            info!("File {} has fragmentation factor of {}%, adding for compaction",
                  file_id,
                  fragmentation * 100.0);

            self.compact_file(file_id);
        }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Vec<u8>> {
        self.inner.read().unwrap().get(key.as_ref())
    }

    pub fn put<K: Into<Vec<u8>>, V: AsRef<[u8]>>(&self, key: K, value: V) {
        self.inner.write().unwrap().put(key.into(), value.as_ref())
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: K) {
        self.inner.write().unwrap().delete(key.as_ref())
    }
}

impl Drop for Cask {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
