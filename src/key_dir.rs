use byteorder;
use std;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use byteorder::Error::UnexpectedEOF;

use file_entry::FileEntry;
use file_util::{get_file_path, get_file_handle};

#[derive(Debug)]
pub struct KeyEntry {
    file_id: u16,
    value_size: u64,
    value_pos: u64,
    timestamp: u32
}

pub struct KeyDir<'a> {
    name: &'a str,
    index: HashMap<String, KeyEntry>,
    current_file_id: u16,
    current_file: File
}

impl<'a> KeyDir<'a> {

    pub fn new(name: &'a str) -> KeyDir<'a> {
        KeyDir { name: name, index: HashMap::new(), current_file_id: 0, current_file: get_file_handle(get_file_path(name, 0).as_path(), true) }
    }

    pub fn open(name: &'a str) -> KeyDir<'a> {
        let mut keydir = KeyDir::new(name);

        {
            let ref mut file = keydir.current_file;
            loop {
                let mut buf = vec![];
                let current_file_pos = file.seek(SeekFrom::Current(0)).unwrap();

                fn read_header(f: &mut File, b: &mut Vec<u8>) -> byteorder::Result<(u32, u64)> {
                    let mut buf = b;
                    let checksum = try!(f.read_u32::<LittleEndian>());
                    buf.write_u32::<LittleEndian>(checksum);

                    let timestamp = try!(f.read_u32::<LittleEndian>());
                    buf.write_u32::<LittleEndian>(timestamp);

                    let key_size = try!(f.read_u32::<LittleEndian>());
                    buf.write_u32::<LittleEndian>(key_size);

                    let value_size = try!(f.read_u64::<LittleEndian>());
                    buf.write_u64::<LittleEndian>(value_size);

                    Ok((key_size, value_size))
                }

                fn read_payload(f: &mut File, key_size: u32, value_size: u64, b: &mut Vec<u8>) -> std::io::Result<usize> {
                    let mut buf = b;

                    let mut key = vec![0; key_size as usize];
                    try!(f.read(&mut key));

                    let mut value = vec![0; value_size as usize];
                    try!(f.read(&mut value));

                    buf.write(&mut key);
                    buf.write(&mut value);

                    Ok(1)
                }

                let (key_size, value_size) = match read_header(file, &mut buf) {
                    Err(UnexpectedEOF) => break,
                    Err(e) => panic!("couldn't read: {}", Error::description(&e)),
                    Ok(e) => e
                };

                match read_payload(file, key_size, value_size, &mut buf) {
                    Err(e) => panic!("couldn't read: {}", Error::description(&e)),
                    Ok(0) => break,
                    _ => ()
                }

                let fe = FileEntry::decode(&buf);
                let size = fe.size() + 4; // FIXME: account for checksum

                match keydir.index.entry(fe.key) {
                    Entry::Occupied(o) => {
                        let e = o.into_mut();
                        e.file_id = keydir.current_file_id;
                        e.value_pos = current_file_pos;
                        e.value_size = size;
                        e.timestamp = 1;
                    },
                    Entry::Vacant(e) => {
                        e.insert(
                            KeyEntry {
                                file_id: keydir.current_file_id,
                                value_pos: current_file_pos,
                                value_size: size,
                                timestamp: 1
                            });
                    }
                }
            }
        }

        keydir
    }

    pub fn get(&self, k: &str) -> Option<Vec<u8>> {
        self.index.get(k).map(|e| {
            let path_buf = get_file_path(self.name, e.file_id);
            let path = path_buf.as_path();
            let mut file = get_file_handle(path, false);

            match file.seek(SeekFrom::Start(e.value_pos)) {
                Err(err) => panic!("couldn't seek {} to {}: {}", path.display(), e.value_pos, Error::description(&err)),
                Ok(p) => assert_eq!(p, e.value_pos)
            }

            let mut buf = vec![0; e.value_size as usize];

            // Vec::with_capacity(e.value_size as usize);
            // unsafe { vec.set_len(e.value_size as usize); }

            match file.read(&mut buf) {
                Err(err) => panic!("couldn't read {} bytes from {}: {}", e.value_size, path.display(), Error::description(&err)),
                Ok(n) => assert_eq!(n, e.value_size as usize)
            }

            let fe: FileEntry = FileEntry::decode(&buf);

            assert_eq!(
                k,
                fe.key);

            fe.value
        })
    }

    pub fn set(&mut self, k: &'a str, v: &[u8]) {
        // acquire write lock

        let fe =
            FileEntry { timestamp: 0, key_size: k.len() as u32, value_size: v.len() as u64, key: k.to_string(), value: v.to_vec() };

        let serialized = fe.encode();

        let current_file_pos = self.current_file.seek(SeekFrom::Current(0)).unwrap();

        match self.current_file.write_all(&serialized) {
            Err(err) => {
                let path_buf = get_file_path(self.name, self.current_file_id);
                panic!("couldn't write {} bytes to {}: {}", v.len(), path_buf.as_path().display(), Error::description(&err))
            },
            _ => ()
        }

        match self.index.entry(k.to_string()) {
            Entry::Occupied(o) => {
                let e = o.into_mut();
                e.file_id = self.current_file_id;
                e.value_pos = current_file_pos;
                e.value_size = serialized.len() as u64;
                e.timestamp = 1;
            },
            Entry::Vacant(e) => {
                e.insert(
                    KeyEntry {
                        file_id: self.current_file_id,
                        value_pos: current_file_pos,
                        value_size: serialized.len() as u64,
                        timestamp: 1
                    });
            }
        }

        // flush?
        self.current_file.sync_data();
    }
}
