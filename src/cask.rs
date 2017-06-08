use std::collections::{BTreeSet, HashMap};
use std::collections::hash_map::Entry as HashMapEntry;
use std::default::Default;
use std::path::PathBuf;
use std::result::Result::Ok;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::vec::Vec;

use time;

use data::{Entry, Hint, SequenceNumber};
use errors::Result;
use log::{Log, LogWrite};
use stats::Stats;
use util::human_readable_byte_count;

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
    options: CaskOptions,
    dropped: Arc<AtomicBool>,
    inner: Arc<RwLock<CaskInner>>,
    compaction: Arc<Mutex<()>>,
}

#[derive(Clone)]
pub struct CaskOptions {
    sync: SyncStrategy,
    max_file_size: usize,
    file_pool_size: usize,
    compaction: bool,
    compaction_check_frequency: u64,
    compaction_window: (usize, usize),
    fragmentation_trigger: f64,
    dead_bytes_trigger: u64,
    fragmentation_threshold: f64,
    dead_bytes_threshold: u64,
    small_file_threshold: u64,
}

#[derive(Clone, PartialEq)]
pub enum SyncStrategy {
    Never,
    Always,
    Interval(usize),
}

impl Default for CaskOptions {
    fn default() -> CaskOptions {
        CaskOptions {
            sync: SyncStrategy::Interval(1000),
            max_file_size: 2 * 1024 * 1024 * 1024,
            file_pool_size: 2048,
            compaction: true,
            compaction_check_frequency: 3600,
            compaction_window: (0, 23),
            fragmentation_trigger: 0.6,
            dead_bytes_trigger: 512 * 1024 * 1024,
            fragmentation_threshold: 0.4,
            dead_bytes_threshold: 128 * 1024 * 1024,
            small_file_threshold: 10 * 1024 * 1024,
        }
    }
}

#[allow(dead_code)]
impl CaskOptions {
    pub fn new() -> CaskOptions {
        CaskOptions::default()
    }

    pub fn sync(&mut self, sync: SyncStrategy) -> &mut CaskOptions {
        self.sync = sync;
        self
    }

    pub fn max_file_size(&mut self, max_file_size: usize) -> &mut CaskOptions {
        self.max_file_size = max_file_size;
        self
    }

    pub fn file_pool_size(&mut self, file_pool_size: usize) -> &mut CaskOptions {
        self.file_pool_size = file_pool_size;
        self
    }

    pub fn compaction(&mut self, compaction: bool) -> &mut CaskOptions {
        self.compaction = compaction;
        self
    }

    pub fn compaction_check_frequency(&mut self,
                                      compaction_check_frequency: u64)
                                      -> &mut CaskOptions {
        self.compaction_check_frequency = compaction_check_frequency;
        self
    }

    pub fn compaction_window(&mut self, start: usize, end: usize) -> &mut CaskOptions {
        self.compaction_window = (start, end);
        self
    }

    pub fn fragmentation_trigger(&mut self, fragmentation_trigger: f64) -> &mut CaskOptions {
        self.fragmentation_trigger = fragmentation_trigger;
        self
    }

    pub fn dead_bytes_trigger(&mut self, dead_bytes_trigger: u64) -> &mut CaskOptions {
        self.dead_bytes_trigger = dead_bytes_trigger;
        self
    }

    pub fn fragmentation_threshold(&mut self, fragmentation_threshold: f64) -> &mut CaskOptions {
        self.fragmentation_threshold = fragmentation_threshold;
        self
    }

    pub fn dead_bytes_threshold(&mut self, dead_bytes_threshold: u64) -> &mut CaskOptions {
        self.dead_bytes_threshold = dead_bytes_threshold;
        self
    }

    pub fn small_file_threshold(&mut self, small_file_threshold: u64) -> &mut CaskOptions {
        self.small_file_threshold = small_file_threshold;
        self
    }

    pub fn open(&self, path: &str) -> Result<Cask> {
        Cask::open(path, self.clone())
    }
}

impl Cask {
    pub fn open(path: &str, options: CaskOptions) -> Result<Cask> {
        info!("Opening database: {:?}", &path);
        let mut log = Log::open(path,
                                options.sync == SyncStrategy::Always,
                                options.max_file_size,
                                options.file_pool_size)?;
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
            options: options,
            dropped: Arc::new(AtomicBool::new(false)),
            inner: Arc::new(RwLock::new(CaskInner {
                                            current_sequence: sequence + 1,
                                            log: log,
                                            index: index,
                                        })),
            compaction: Arc::new(Mutex::new(())),
        };

        if let SyncStrategy::Interval(millis) = cask.options.sync {
            let cask = cask.clone();

            thread::spawn(move || {
                let duration = Duration::from_millis(millis as u64);
                loop {
                    if cask.dropped.load(Ordering::SeqCst) {
                        info!("Cask has been dropped, background file sync \
                               thread is exiting");
                        break;
                    }

                    debug!("Background file sync");
                    cask.inner.read().unwrap().log.sync().unwrap();

                    thread::sleep(duration);
                }
            });
        };

        if cask.options.compaction {
            let cask = cask.clone();

            thread::spawn(move || {
                let duration = Duration::from_secs(cask.options.compaction_check_frequency);
                loop {
                    if cask.dropped.load(Ordering::SeqCst) {
                        info!("Cask has been dropped, background compaction \
                               thread is exiting");
                        break;
                    }

                    info!("Compaction thread wake up");

                    let current_hour = time::now().tm_hour as usize;
                    let (window_start, window_end) = cask.options.compaction_window;

                    let in_window = if window_start <= window_end {
                        current_hour >= window_start && current_hour <= window_end
                    } else {
                        current_hour >= window_end || current_hour <= window_end
                    };

                    if !in_window {
                        info!("Compaction outside defined window {:?}",
                              cask.options.compaction_window);
                        continue;
                    } else if let Err(err) = cask.compact() {
                        warn!("Error during compaction: {}", err);
                    }

                    thread::sleep(duration);
                }
            });
        }

        Ok(cask)
    }

    fn compact_files_aux(&self, files: &[u32]) -> Result<(Vec<u32>, Vec<u32>)> {
        let active_file_id = {
            self.inner.read().unwrap().log.active_file_id
        };

        let compacted_files_hints = files
            .iter()
            .flat_map(|&file_id| {
                if active_file_id.is_some() && active_file_id.unwrap() == file_id {
                    None
                } else {
                    self.inner
                        .read()
                        .unwrap()
                        .log
                        .hints(file_id)
                        .ok() // FIXME: log the error?
                        .and_then(|hints| hints.map(|h| (file_id, h)))
                }
            });

        let mut compacted_files = Vec::new();
        let mut new_files = Vec::new();
        let mut deletes = HashMap::new();

        let mut log_writer = {
            // FIXME: turn into error
            self.inner.read().unwrap().log.writer()
        };

        for (file_id, hints) in compacted_files_hints {
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
                } else if index_entry.is_some() && index_entry.unwrap().sequence == hint.sequence {
                    inserts.push(hint)
                }
            }

            for hint in inserts {
                // FIXME: turn into error
                let log = &self.inner.read().unwrap().log;
                let log_write = log_writer.write(&log.read_entry(file_id, hint.entry_pos)?)?;

                if let LogWrite::NewFile(file_id) = log_write {
                    new_files.push(file_id);
                }
            }

            compacted_files.push(file_id);
        }

        for (key, sequence) in deletes {
            log_writer.write(&Entry::deleted(sequence, key))?;
        }

        Ok((compacted_files, new_files))
    }

    fn compact_files(&self, files: &[u32]) -> Result<()> {
        info!("Compacting data files: {:?}", files);

        let (ref compacted_files, ref new_files) = self.compact_files_aux(files)?;

        for &file_id in new_files {
            let hints = {
                self.inner.read().unwrap().log.hints(file_id)?
            };

            if let Some(hints) = hints {
                for hint in hints {
                    let hint = hint?;
                    self.inner.write().unwrap().index.update(hint, file_id);
                }
            };
        }

        self.inner
            .write()
            .unwrap()
            .index
            .stats
            .remove_files(compacted_files);

        self.inner
            .write()
            .unwrap()
            .log
            .swap_files(compacted_files, new_files)?;

        // FIXME: print files not compacted
        info!("Finished compacting data files: {:?} into: {:?}",
              compacted_files,
              new_files);

        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        #[allow(unused_variables)]
        let lock = self.compaction.lock().unwrap();

        let active_file_id = {
            self.inner.read().unwrap().log.active_file_id
        };

        let file_stats = {
            self.inner.read().unwrap().index.stats.file_stats()
        };

        let mut files = BTreeSet::new();
        let mut triggered = false;

        for (file_id, fragmentation, dead_bytes) in file_stats {
            if active_file_id.is_some() && file_id == active_file_id.unwrap() {
                continue;
            }

            if !triggered {
                if fragmentation >= self.options.fragmentation_trigger {
                    info!("File {} has fragmentation factor of {:.1}%, triggered compaction",
                          file_id,
                          fragmentation * 100.0);
                    triggered = true;
                    files.insert(file_id);
                } else if dead_bytes >= self.options.dead_bytes_trigger &&
                          !files.contains(&file_id) {
                    info!("File {} has {} of dead data, triggered compaction",
                          file_id,
                          human_readable_byte_count(dead_bytes as usize, true));
                    triggered = true;
                    files.insert(file_id);
                }
            }

            if fragmentation >= self.options.fragmentation_threshold && !files.contains(&file_id) {
                info!("File {} has fragmentation factor of {:.1}%, adding for compaction",
                      file_id,
                      fragmentation * 100.0);
                files.insert(file_id);
            } else if dead_bytes >= self.options.dead_bytes_threshold && !files.contains(&file_id) {
                info!("File {} has {} of dead data, adding for compaction",
                      file_id,
                      human_readable_byte_count(dead_bytes as usize, true));
                files.insert(file_id);
            }

            if !files.contains(&file_id) {
                let file_size = {
                    self.inner.read().unwrap().log.file_size(file_id).ok()
                };

                if let Some(file_size) = file_size {
                    if file_size <= self.options.small_file_threshold {
                        info!("File {} has total size of {}, adding for compaction",
                              file_id,
                              human_readable_byte_count(file_size as usize, true));
                        files.insert(file_id);
                    }
                };
            }
        }

        if triggered {
            let files: Vec<_> = files.into_iter().collect();
            self.compact_files(&files)?;
        } else if !files.is_empty() {
            info!("Compaction of files {:?} aborted due to missing trigger",
                  &files);
        } else {
            info!("No files eligible for compaction")
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
        #[allow(unused_variables)]
        let lock = self.compaction.lock().unwrap();
    }
}
