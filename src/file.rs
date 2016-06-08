use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::vec::Vec;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use time;
use regex::Regex;

use data::{Entry, Hint};
use util::{crc32, Crc32};

const DATA_FILE_EXTENSION: &'static str = "cask.data";
const HINT_FILE_EXTENSION: &'static str = "cask.hint";

const DEFAULT_SIZE_THRESHOLD: usize = 100 * 1024 * 1024;

pub fn get_file_handle(path: &Path, write: bool) -> File {
    if write {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap()
    } else {
        OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap()
    }
}

pub fn get_data_file_path(path: &Path, file_id: u32) -> PathBuf {
    path.join(file_id.to_string()).with_extension(DATA_FILE_EXTENSION)
}

pub fn get_hint_file_path(path: &Path, file_id: u32) -> PathBuf {
    path.join(file_id.to_string()).with_extension(HINT_FILE_EXTENSION)
}

pub fn find_data_files(path: &Path) -> Vec<u32> {
    let files = fs::read_dir(path).unwrap();

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
            } else {
                None
            }
        })
        .collect();

    files.sort();

    files
}

pub fn is_valid_hint_file(path: &Path) -> bool {
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
                warn!("Found corrupt hint file: {:?}. Recreating", &path);
            }

            valid
        }
    }
}
