use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom, Take};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::result::Result::Ok;
use std::sync::Arc;
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fs2::FileExt;
use regex::Regex;

use data::{Entry, Hint};
use errors::{Error, Result};
use util::{Sequence, XxHash32, get_file_handle, xxhash32};

const DATA_FILE_EXTENSION: &'static str = "cask.data";
const HINT_FILE_EXTENSION: &'static str = "cask.hint";
const LOCK_FILE_NAME: &'static str = "cask.lock";

const DEFAULT_SIZE_THRESHOLD: usize = 2 * 1024 * 1024 * 1024;

pub struct Log {
    pub path: PathBuf,
    sync: bool,
    size_threshold: usize,
    lock_file: File,
    files: Vec<u32>,
    file_id_seq: Arc<Sequence>,
    log_writer: LogWriter,
    pub active_file_id: Option<u32>,
}

impl Log {
    pub fn open(path: &str, sync: bool) -> Result<Log> {
        let path = PathBuf::from(path);

        if path.exists() {
            assert!(path.is_dir());
        } else {
            fs::create_dir(&path)?;
        }

        let lock_file = File::create(path.join(LOCK_FILE_NAME))?;
        lock_file.try_lock_exclusive()?;

        let files = find_data_files(&path)?;

        let current_file_id = if files.is_empty() {
            0
        } else {
            files[files.len() - 1]
        };

        let size_threshold = DEFAULT_SIZE_THRESHOLD;

        let file_id_seq = Arc::new(Sequence::new(current_file_id));

        info!("Current file id: {}", current_file_id);

        let log_writer = LogWriter::new(&path, sync, size_threshold, file_id_seq.clone());

        Ok(Log {
               path: path,
               sync: sync,
               size_threshold: size_threshold,
               lock_file: lock_file,
               files: files,
               file_id_seq: file_id_seq,
               log_writer: log_writer,
               active_file_id: None,
           })
    }

    pub fn file_size(&self, file_id: u32) -> Result<u64> {
        let data_file = get_file_handle(&get_data_file_path(&self.path, file_id), false)?;
        Ok(data_file.metadata()?.len())
    }

    pub fn files(&self) -> Vec<u32> {
        self.files.clone()
    }

    pub fn entries<'a>(&self, file_id: u32) -> Result<Entries<'a>> {
        let data_file_path = get_data_file_path(&self.path, file_id);
        info!("Loading data file: {:?}", data_file_path);
        let data_file = get_file_handle(&data_file_path, false)?;
        let data_file_size = data_file.metadata()?.len();

        Ok(Entries {
               data_file: data_file.take(data_file_size),
               data_file_pos: 0,
               phantom: PhantomData,
           })
    }

    pub fn hints<'a>(&self, file_id: u32) -> Result<Option<Hints<'a>>> {
        let hint_file_path = get_hint_file_path(&self.path, file_id);
        Ok(if is_valid_hint_file(&hint_file_path)? {
               info!("Loading hint file: {:?}", hint_file_path);
               let hint_file = get_file_handle(&hint_file_path, false)?;
               let hint_file_size = hint_file.metadata()?.len();

               Some(Hints {
                        hint_file: hint_file.take(hint_file_size - 4),
                        phantom: PhantomData,
                    })
           } else {
               None
           })
    }

    pub fn recreate_hints<'a>(&mut self, file_id: u32) -> Result<RecreateHints<'a>> {
        let hint_file_path = get_hint_file_path(&self.path, file_id);
        warn!("Re-creating hint file: {:?}", hint_file_path);

        let hint_writer = HintWriter::new(&self.path, file_id)?;
        let entries = self.entries(file_id)?;

        Ok(RecreateHints {
               hint_writer: hint_writer,
               entries: entries,
           })
    }

    pub fn read_entry<'a>(&self, file_id: u32, entry_pos: u64) -> Result<Entry<'a>> {
        let mut data_file = get_file_handle(&get_data_file_path(&self.path, file_id), false)?;
        data_file.seek(SeekFrom::Start(entry_pos))?;
        Entry::from_read(&mut data_file)
    }

    pub fn append_entry<'a>(&mut self, entry: &Entry<'a>) -> Result<(u32, u64)> {
        Ok(match self.log_writer.write(entry)? {
               LogWrite::NewFile(file_id) => {
                   if let Some(active_file_id) = self.active_file_id {
                       self.add_file(active_file_id);
                   }
                   self.active_file_id = Some(file_id);
                   info!("New active data file {:?}",
                         self.log_writer.entry_writer()?.data_file_path);
                   (file_id, 0)
               }
               LogWrite::Ok(entry_pos) => (self.active_file_id.unwrap(), entry_pos),
           })
    }

    pub fn writer(&self) -> LogWriter {
        LogWriter::new(&self.path,
                       self.sync,
                       self.size_threshold,
                       self.file_id_seq.clone())
    }

    pub fn swap_files(&mut self, old_files: &[u32], new_files: &[u32]) -> Result<()> {
        for &file_id in old_files {
            let idx = self.files
                .binary_search(&file_id)
                .map_err(|_| Error::InvalidFileId(file_id))?;

            self.files.remove(idx);

            let data_file_path = get_data_file_path(&self.path, file_id);
            let hint_file_path = get_hint_file_path(&self.path, file_id);

            fs::remove_file(data_file_path)?;
            let _ = fs::remove_file(hint_file_path);
        }

        self.files.extend(new_files);
        self.files.sort();

        Ok(())
    }

    fn add_file(&mut self, file_id: u32) {
        self.files.push(file_id);
        self.files.sort();
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

pub struct LogWriter {
    path: PathBuf,
    sync: bool,
    size_threshold: usize,
    file_id_seq: Arc<Sequence>,
    entry_writer: Option<EntryWriter>,
}

pub enum LogWrite {
    Ok(u64),
    NewFile(u32),
}

impl LogWriter {
    pub fn new(path: &Path,
               sync: bool,
               size_threshold: usize,
               file_id_seq: Arc<Sequence>)
               -> LogWriter {

        LogWriter {
            path: path.to_path_buf(),
            sync: sync,
            size_threshold: size_threshold,
            file_id_seq: file_id_seq,
            entry_writer: None,
        }
    }

    fn entry_writer(&mut self) -> Result<&EntryWriter> {
        if self.entry_writer.is_none() {
            self.new_entry_writer()?;
        }
        Ok(self.entry_writer.as_ref().unwrap())
    }

    fn new_entry_writer(&mut self) -> Result<u32> {
        let file_id = self.file_id_seq.increment();

        if self.entry_writer.is_some() {
            info!("Closed data file {:?}",
                  self.entry_writer.as_ref().unwrap().data_file_path);
        }

        self.entry_writer = Some(EntryWriter::new(&self.path, self.sync, file_id)?);
        Ok(file_id)
    }

    pub fn write(&mut self, entry: &Entry) -> Result<LogWrite> {
        Ok(if self.entry_writer.is_none() || // FIXME: clean up
              self.entry_writer.as_ref().unwrap().data_file_pos + entry.size() >
              self.size_threshold as u64 {

               if self.entry_writer.is_some() {
                   info!("Data file {:?} reached file limit",
                         self.entry_writer.as_ref().unwrap().data_file_path);
               }

               let file_id = self.new_entry_writer()?;
               let entry_pos = self.entry_writer.as_mut().unwrap().write(entry)?;

               assert_eq!(entry_pos, 0);

               LogWrite::NewFile(file_id)
           } else {
               let entry_pos = self.entry_writer.as_mut().unwrap().write(entry)?;
               LogWrite::Ok(entry_pos)
           })
    }
}

pub struct EntryWriter {
    sync: bool,
    data_file_path: PathBuf,
    data_file: File,
    data_file_pos: u64,
    hint_writer: HintWriter,
}

impl EntryWriter {
    pub fn new(path: &Path, sync: bool, file_id: u32) -> Result<EntryWriter> {
        let data_file_path = get_data_file_path(path, file_id);
        let data_file = get_file_handle(&data_file_path, true)?;

        info!("Created new data file {:?}", data_file_path);

        let hint_writer = HintWriter::new(path, file_id)?;

        Ok(EntryWriter {
               sync: sync,
               data_file_path: data_file_path,
               data_file: data_file,
               data_file_pos: 0,
               hint_writer: hint_writer,
           })
    }

    pub fn write<'a>(&mut self, entry: &Entry<'a>) -> Result<u64> {
        let entry_pos = self.data_file_pos;

        let hint = Hint::new(entry, entry_pos);
        entry.write_bytes(&mut self.data_file)?;

        self.hint_writer.write(&hint)?;

        if self.sync {
            self.data_file.sync_data()?;
        }

        self.data_file_pos += entry.size();

        Ok(entry_pos)
    }
}

impl Drop for EntryWriter {
    fn drop(&mut self) {
        if self.sync {
            let _ = self.data_file.sync_data();
        }
    }
}

struct HintWriter {
    hint_file: File,
    hint_file_hasher: XxHash32,
}

impl HintWriter {
    pub fn new(path: &Path, file_id: u32) -> Result<HintWriter> {
        let hint_file = get_file_handle(&get_hint_file_path(path, file_id), true)?;

        Ok(HintWriter {
               hint_file: hint_file,
               hint_file_hasher: XxHash32::new(),
           })
    }

    pub fn write<'a>(&mut self, hint: &Hint<'a>) -> Result<()> {
        hint.write_bytes(&mut self.hint_file)?;
        hint.write_bytes(&mut self.hint_file_hasher)?;
        Ok(())
    }
}

impl Drop for HintWriter {
    fn drop(&mut self) {
        let _ = self.hint_file
            .write_u32::<LittleEndian>(self.hint_file_hasher.get());
    }
}

pub struct Entries<'a> {
    data_file: Take<File>,
    data_file_pos: u64,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for Entries<'a> {
    type Item = (u64, Result<Entry<'a>>);

    // TODO: candidate for corruption handling
    fn next(&mut self) -> Option<(u64, Result<Entry<'a>>)> {
        let limit = self.data_file.limit();
        if limit == 0 {
            None
        } else {
            let entry = Entry::from_read(&mut self.data_file);
            let entry_pos = self.data_file_pos;

            let read = limit - self.data_file.limit();

            self.data_file_pos += read;

            let entry = match entry {
                Ok(entry) => {
                    assert_eq!(entry.size(), read);
                    Ok(entry)
                }
                e => e,
            };

            Some((entry_pos, entry))
        }
    }
}

pub struct Hints<'a> {
    hint_file: Take<File>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Iterator for Hints<'a> {
    type Item = Result<Hint<'a>>;

    fn next(&mut self) -> Option<Result<Hint<'a>>> {
        if self.hint_file.limit() == 0 {
            None
        } else {
            Some(Hint::from_read(&mut self.hint_file))
        }
    }
}

pub struct RecreateHints<'a> {
    hint_writer: HintWriter,
    entries: Entries<'a>,
}

impl<'a> Iterator for RecreateHints<'a> {
    type Item = Result<Hint<'a>>;

    fn next(&mut self) -> Option<Result<Hint<'a>>> {
        self.entries
            .next()
            .map(|e| {
                let (entry_pos, entry) = e;
                let hint = Hint::from(entry?, entry_pos);
                self.hint_writer.write(&hint)?;
                Ok(hint)
            })
    }
}

impl<'a> Drop for RecreateHints<'a> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

fn get_data_file_path(path: &Path, file_id: u32) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id).with_extension(DATA_FILE_EXTENSION)
}

fn get_hint_file_path(path: &Path, file_id: u32) -> PathBuf {
    let file_id = format!("{:010}", file_id);
    path.join(file_id).with_extension(HINT_FILE_EXTENSION)
}

fn find_data_files(path: &Path) -> Result<Vec<u32>> {
    let files = fs::read_dir(path)?;

    lazy_static! {
        static ref RE: Regex =
            Regex::new(&format!("(\\d+).{}$", DATA_FILE_EXTENSION)).unwrap();
    }

    let mut data_files = Vec::new();

    for file in files {
        let file = file?;
        if file.metadata()?.is_file() {
            let file_name = file.file_name();
            let captures = RE.captures(file_name.to_str().unwrap());
            if let Some(n) =
                captures.and_then(|c| c.get(1).and_then(|n| n.as_str().parse::<u32>().ok())) {
                data_files.push(n)
            }
        }
    }

    data_files.sort();

    Ok(data_files)
}

fn is_valid_hint_file(path: &Path) -> Result<bool> {
    Ok(path.is_file() &&
       {
           let mut hint_file = get_file_handle(path, false)?;

           // FIXME: avoid reading the whole hint file into memory;
           let mut buf = Vec::new();
           hint_file.read_to_end(&mut buf)?;

           buf.len() >= 4 &&
           {
               let hash = xxhash32(&buf[..buf.len() - 4]);

               let mut cursor = Cursor::new(&buf[buf.len() - 4..]);
               let checksum = cursor.read_u32::<LittleEndian>()?;

               let valid = hash == checksum;

               if !valid {
                   warn!("Found corrupt hint file: {:?}", &path);
               }

               valid
           }
       })
}
