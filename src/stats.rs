use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;

use cask::IndexEntry;

#[derive(Debug)]
struct StatsEntry {
    entries: u64,
    dead_entries: u64,
    total_bytes: u64,
    dead_bytes: u64,
}

#[derive(Debug)]
pub struct Stats {
    map: HashMap<u32, StatsEntry>,
}

impl Stats {
    pub fn new() -> Stats {
        Stats { map: HashMap::new() }
    }

    pub fn add_entry(&mut self, entry: &IndexEntry) {
        match self.map.entry(entry.file_id) {
            HashMapEntry::Occupied(mut o) => {
                o.get_mut().entries += 1;
                o.get_mut().total_bytes += entry.entry_size;
            }
            HashMapEntry::Vacant(e) => {
                e.insert(StatsEntry {
                    entries: 1,
                    dead_entries: 0,
                    total_bytes: entry.entry_size,
                    dead_bytes: 0,
                });
            }
        }
    }

    pub fn remove_entry(&mut self, entry: &IndexEntry) {
        match self.map.entry(entry.file_id) {
            HashMapEntry::Occupied(mut o) => {
                o.get_mut().dead_entries += 1;
                o.get_mut().dead_bytes += entry.entry_size;
            }
            HashMapEntry::Vacant(_) => {
                warn!("Tried to reclaim non-existant entry {:?}", entry);
            }
        }
    }

    pub fn dead_bytes(&self) -> Vec<(u32, u64)> {
        let mut vec: Vec<_> = self.map.iter().map(|e| (*e.0, e.1.dead_bytes)).collect();
        vec.sort_by_key(|e| e.1);
        vec
    }

    pub fn fragmentation(&self) -> Vec<(u32, f64)> {
        let mut vec: Vec<_> =
            self.map.iter().map(|e| (*e.0, e.1.dead_entries as f64 / e.1.entries as f64)).collect();
        vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
        vec
    }
}
