use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use std::path::PathBuf;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use std::vec::Vec;

use data::{Entry, Hint, SequenceNumber};
use errors::Result;
use log::{Log, LogWrite};
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
        self.map
            .insert(key, index_entry)
            .map(|entry| {
                self.stats.remove_entry(&entry);
                entry
            })
    }

    fn remove(&mut self, key: &[u8]) -> Option<IndexEntry> {
        self.map
            .remove(key)
            .map(|entry| {
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
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let value = match self.index.get(key) {
            Some(index_entry) => {
                let entry = self.log
                    .read_entry(index_entry.file_id, index_entry.entry_pos)?;
                if entry.deleted {
                    warn!("Index pointed to dead entry: Entry {{ key: {:?}, sequence: {} }} at \
                           file: {}",
                          entry.key,
                          entry.sequence,
                          index_entry.file_id);
                    None
                } else {
                    Some(entry.value.into_owned())
                }
            }
            _ => None,
        };

        Ok(value)
    }

    fn put(&mut self, key: Vec<u8>, value: &[u8]) -> Result<()> {
        let index_entry = {
            let entry = Entry::new(self.current_sequence, &*key, value)?;

            let (file_id, file_pos) = self.log.append_entry(&entry)?;

            self.current_sequence += 1;

            IndexEntry {
                file_id: file_id,
                entry_pos: file_pos,
                entry_size: entry.size(),
                sequence: entry.sequence,
            }
        };

        self.index.insert(key, index_entry);

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.index.remove(key).is_some() {
            let entry = Entry::deleted(self.current_sequence, key);
            self.log.append_entry(&entry)?;
            self.current_sequence += 1;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Cask {
    path: PathBuf,
    dropped: Arc<AtomicBool>,
    inner: Arc<RwLock<CaskInner>>,
}

impl Cask {
    pub fn open(path: &str, sync: bool) -> Result<Cask> {
        info!("Opening database: {:?}", &path);
        let mut log = Log::open(path, sync)?;
        let mut index = Index::new();

        let mut sequence = 0;

        for file_id in log.files() {
            let mut f = |hint: Hint| {
                if hint.sequence > sequence {
                    sequence = hint.sequence;
                }

                index.update(hint, file_id);
            };

            match log.hints(file_id)? {
                Some(hints) => {
                    for hint in hints {
                        f(hint?);
                    }
                }
                None => {
                    for hint in log.recreate_hints(file_id)? {
                        f(hint?);
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
        thread::spawn(move || loop {
                          thread::sleep(Duration::new(COMPACTION_CHECK_FREQUENCY, 0));

                          info!("Compaction thread wake up");

                          if caskt.dropped.load(Ordering::SeqCst) {
                              info!("Cask has been dropped, background compaction \
                                     thread is exiting");
                              break;
                          }

                          if let Err(err) = caskt.compact() {
                              warn!("Error during compaction: {}", err);
                          }
                      });

        Ok(cask)
    }

    fn compact_file_aux(&self, file_id: u32) -> Result<Option<u32>> {
        let active_file_id = {
            self.inner.read().unwrap().log.active_file_id
        };

        if active_file_id.is_some() && active_file_id.unwrap() == file_id {
            return Ok(None);
        }

        let hints = {
            self.inner.read().unwrap().log.hints(file_id)?
        };

        Ok(match hints {
               Some(hints) => {
                   let mut log_writer = {
                       // FIXME: turn into error
                       self.inner.read().unwrap().log.writer()
                   };

                   info!("Compacting data file: {}", file_id);

                   let mut files = Vec::new();
                   let mut deletes = HashMap::new();

                   {
                       let mut inserts = Vec::new();

                       for hint in hints {
                           let hint = hint?;
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
                           } else if index_entry.is_some() &&
                                     index_entry.unwrap().sequence == hint.sequence {
                               inserts.push(hint)
                           }
                       }

                       for hint in inserts {
                           // FIXME: turn into error
                           let log = &self.inner.read().unwrap().log;
                           let log_write = log_writer
                               .write(&log.read_entry(file_id, hint.entry_pos)?)?;

                           if let LogWrite::NewFile(file_id) = log_write {
                               files.push(file_id);
                           }
                       }
                   }

                   for (key, sequence) in deletes {
                       log_writer.write(&Entry::deleted(sequence, key))?;
                   }

                   info!("Compacted data file: {} into: {:?}", file_id, files);

                   if files.is_empty() {
                       None
                   } else {
                       // FIXME
                       Some(files[0])
                   }
               }
               _ => None,
           })
    }

    pub fn compact_file(&self, file_id: u32) -> Result<()> {
        // FIXME: deal with compaction into nothing (i.e. 100% garbage)
        let new_file_id = self.compact_file_aux(file_id)?;

        if let Some(new_file_id) = new_file_id {
            let hints = {
                self.inner.read().unwrap().log.hints(new_file_id)?
            };

            if let Some(hints) = hints {
                for hint in hints {
                    let hint = hint?;
                    self.inner.write().unwrap().index.update(hint, new_file_id);
                }

                self.inner
                    .write()
                    .unwrap()
                    .log
                    .swap_file(file_id, new_file_id)?;

                info!("Finished compacting data file: {} into: {}",
                      file_id,
                      new_file_id);
            };
        }

        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        let iter = {
            self.inner.read().unwrap().index.stats.fragmentation()
        };

        for &(file_id, fragmentation) in iter.iter().filter(|e| e.1 >= FRAGMENTATION_THRESHOLD) {

            info!("File {} has fragmentation factor of {}%, adding for compaction",
                  file_id,
                  fragmentation * 100.0);

            self.compact_file(file_id)?;
        }

        Ok(())
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.inner.read().unwrap().get(key.as_ref())
    }

    pub fn put<K: Into<Vec<u8>>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.inner.write().unwrap().put(key.into(), value.as_ref())
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.inner.write().unwrap().delete(key.as_ref())
    }
}

impl Drop for Cask {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
