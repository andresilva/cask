use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

pub fn get_file_path(name: &str, file_id: u16) -> PathBuf {
    PathBuf::from(format!("{}-{}.cask", name, file_id))
}

pub fn get_file_handle(path: &Path, write: bool) -> File {
    let display = path.display();

    if write {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path) {
                Err(e) => panic!("couldn't open for writing {}: {}", display, Error::description(&e)),
                Ok(file) => file
            }
    } else {
        match OpenOptions::new()
            .read(true)
            .open(path) {
                Err(e) => panic!("couldn't open for reading {}: {}", display, Error::description(&e)),
                Ok(file) => file
            }
    }
}
