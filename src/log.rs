use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom, Take};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::slice::Iter;
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::FileExt;
use time;
use regex::Regex;

use data::{Entry, Hint};
use util::{crc32, Crc32, get_file_handle};

const DATA_FILE_EXTENSION: &'static str = "cask.data";
const HINT_FILE_EXTENSION: &'static str = "cask.hint";
const LOCK_FILE_NAME: &'static str = "cask.lock";

const DEFAULT_SIZE_THRESHOLD: usize = 100 * 1024 * 1024;

pub struct Log {
    path: PathBuf,
    sync: bool,
    size_threshold: usize,
    lock_file: File,
    current_file_id: u32,
    active_data_file: File,
    active_hint_file: File,
    active_hint_file_digest: Crc32,
}

pub struct Entries<'a> {
    data_file: Take<File>,
    data_file_pos: u64,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for Entries<'a> {
    type Item = (u64, Entry<'a>);

    fn next(&mut self) -> Option<(u64, Entry<'a>)> {
        if self.data_file.limit() == 0 {
            None
        } else {
            let entry = Entry::from_read(&mut self.data_file);
            let entry_pos = self.data_file_pos;

            self.data_file_pos += entry.size();

            Some((entry_pos, entry))
        }
    }
}

pub struct Hints<'a> {
    hint_file: Take<File>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for Hints<'a> {
    type Item = Hint<'a>;

    fn next(&mut self) -> Option<Hint<'a>> {
        if self.hint_file.limit() == 0 {
            None
        } else {
            Some(Hint::from_read(&mut self.hint_file))
        }
    }
}

pub struct RecreateHints<'a> {
    hint_file: File,
    hint_file_digest: Crc32,
    entries: Entries<'a>,
}

impl<'a> Iterator for RecreateHints<'a> {
    type Item = Hint<'a>;

    fn next(&mut self) -> Option<Hint<'a>> {
        self.entries.next().map(|e| {
            let (entry_pos, entry) = e;
            let hint = Hint::from(entry, entry_pos);
            hint.write_bytes(&mut self.hint_file);
            hint.write_bytes(&mut self.hint_file_digest);
            hint
        })
    }
}

impl<'a> Drop for RecreateHints<'a> {
    fn drop(&mut self) {
        while self.next().is_some() {}
        self.hint_file
            .write_u32::<LittleEndian>(self.hint_file_digest.sum32())
            .unwrap();
    }
}

impl Log {
    pub fn open(path: &str, sync: bool) -> Log {
        let path = PathBuf::from(path);

        if path.exists() {
            assert!(path.is_dir());
        } else {
            fs::create_dir(&path).unwrap();
        }

        let lock_file = File::create(path.join(LOCK_FILE_NAME)).unwrap();
        lock_file.try_lock_exclusive().unwrap();

        let current_file_id = time::now().to_timespec().sec as u32;
        let active_data_file = get_file_handle(&get_data_file_path(&path, current_file_id), true);
        let active_hint_file = get_file_handle(&get_hint_file_path(&path, current_file_id), true);
        let active_hint_file_digest = Crc32::new();

        Log {
            path: path,
            sync: sync,
            size_threshold: DEFAULT_SIZE_THRESHOLD,
            lock_file: lock_file,
            current_file_id: current_file_id,
            active_data_file: active_data_file,
            active_hint_file: active_hint_file,
            active_hint_file_digest: active_hint_file_digest,
        }
    }

    pub fn find_files(&self) -> Vec<u32> {
        let files = fs::read_dir(&self.path).unwrap();

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
                        .and_then(|id| if id != self.current_file_id {
                            Some(id)
                        } else {
                            None
                        })
                } else {
                    None
                }
            })
            .collect();

        files.sort();

        files
    }

    pub fn entries<'a>(&self, file_id: u32) -> Entries<'a> {
        let data_file_path = get_data_file_path(&self.path, file_id);
        info!("Loading data file: {:?}", data_file_path);
        let mut data_file = get_file_handle(&data_file_path, false);
        let data_file_size = data_file.metadata().unwrap().len();

        Entries {
            data_file: data_file.take(data_file_size),
            data_file_pos: 0,
            phantom: PhantomData,
        }
    }

    pub fn hints<'a>(&self, file_id: u32) -> Option<Hints<'a>> {
        let hint_file_path = get_hint_file_path(&self.path, file_id);
        if is_valid_hint_file(&hint_file_path) {
            info!("Loading hint file: {:?}", hint_file_path);
            let mut hint_file = get_file_handle(&hint_file_path, false);
            let hint_file_size = hint_file.metadata().unwrap().len();

            Some(Hints {
                hint_file: hint_file.take(hint_file_size - 4),
                phantom: PhantomData,
            })
        } else {
            None
        }
    }

    pub fn recreate_hints<'a>(&self, file_id: u32) -> RecreateHints<'a> {
        let hint_file_path = get_hint_file_path(&self.path, file_id);
        info!("Re-creating hint file: {:?}", hint_file_path);
        let mut hint_file = get_file_handle(&hint_file_path, true);
        let entries = self.entries(file_id);

        RecreateHints {
            hint_file: hint_file,
            hint_file_digest: Crc32::new(),
            entries: entries,
        }
    }

    pub fn read_entry<'a>(&self,
                          file_id: u32,
                          entry_pos: u64,
                          entry_size: u64,
                          timestamp: u32)
                          -> Entry<'a> {
        let mut data_file = get_file_handle(&get_data_file_path(&self.path, file_id), false);
        data_file.seek(SeekFrom::Start(entry_pos)).unwrap();
        Entry::from_read(&mut data_file)
    }

    pub fn write_entry<'a>(&mut self, entry: &Entry<'a>) -> (u32, u64) {
        let mut active_data_file_pos = self.active_data_file.seek(SeekFrom::Current(0)).unwrap();

        if active_data_file_pos + entry.size() > self.size_threshold as u64 {
            if self.sync {
                self.active_data_file.sync_data().unwrap();
            }

            self.current_file_id = time::now().to_timespec().sec as u32;

            self.active_data_file =
                get_file_handle(&get_data_file_path(&self.path, self.current_file_id), true);

            // FIXME: hint file checksum is not being appended
            self.active_hint_file =
                get_file_handle(&get_hint_file_path(&self.path, self.current_file_id), true);
            self.active_hint_file_digest = Crc32::new();

            active_data_file_pos = 0
        }

        let hint = Hint::new(&entry, active_data_file_pos);

        entry.write_bytes(&mut self.active_data_file);
        hint.write_bytes(&mut self.active_hint_file);
        hint.write_bytes(&mut self.active_hint_file_digest);

        if self.sync {
            self.active_data_file.sync_data().unwrap();
        }

        (self.current_file_id, active_data_file_pos)
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        self.active_hint_file
            .write_u32::<LittleEndian>(self.active_hint_file_digest.sum32())
            .unwrap();

        self.lock_file.unlock().unwrap();
    }
}

fn get_data_file_path(path: &Path, file_id: u32) -> PathBuf {
    path.join(file_id.to_string()).with_extension(DATA_FILE_EXTENSION)
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
                warn!("Found corrupt hint file: {:?}", &path);
            }

            valid
        }
    }
}
