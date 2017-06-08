use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::Entry;
use std::fs::File;

pub struct FilePool {
    queue: VecDeque<u32>,
    files: HashMap<u32, Vec<File>>,
    capacity: usize,
    size: usize,
}

impl FilePool {
    pub fn new(capacity: usize) -> FilePool {
        FilePool {
            queue: VecDeque::new(),
            files: HashMap::new(),
            capacity: capacity,
            size: 0,
        }
    }

    pub fn get(&mut self, file_id: u32) -> Option<File> {
        let mut remove = false;

        let f = self.files
            .get_mut(&file_id)
            .and_then(|v| {
                let f = v.pop();
                if v.is_empty() {
                    remove = true;
                }
                f
            });

        if f.is_some() {
            if remove {
                self.files.remove(&file_id);
            }

            if let Some(index) = self.queue.iter().position(|&f| f == file_id) {
                self.queue.remove(index);
            }

            self.size -= 1;
        }

        f
    }

    pub fn put(&mut self, file_id: u32, file: File) {
        self.queue.push_back(file_id);

        match self.files.entry(file_id) {
            Entry::Occupied(mut o) => {
                o.get_mut().push(file);
            }
            Entry::Vacant(e) => {
                e.insert(vec![file]);
            }
        }

        self.size += 1;

        if self.size > self.capacity {
            self.remove_lru();
        }
    }

    fn remove_lru(&mut self) {
        if let Some(file_id) = self.queue.pop_front() {
            let mut remove = false;

            if let Some(files) = self.files.get_mut(&file_id) {
                files.pop();

                if files.is_empty() {
                    remove = true;
                }

                self.size -= 1;
            }

            if remove {
                self.files.remove(&file_id);
            }
        }
    }
}
