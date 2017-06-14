use std::convert::From;
use std::error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use data::{MAX_KEY_SIZE, MAX_VALUE_SIZE};

/// Basic type to represent all possible errors that can occur when interacting with a `Cask`.
#[derive(Debug)]
pub enum Error {
    /// IO error.
    Io(io::Error),
    /// Tried to perform an operation on an invalid `file_id`.
    InvalidFileId(u32),
    /// Invalid key size, i.e. larger than the maximum key size.
    InvalidKeySize(usize),
    /// Invalid value size, i.e. larger than the maximum value size.
    InvalidValueSize(usize),
    /// Invalid checksum found, potential data corruption.
    InvalidChecksum { expected: u32, found: u32 },
    /// Invalid path provided.
    InvalidPath(String),
}

/// Value returned from potentially-error operations.
pub type Result<T> = result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::InvalidFileId(file_id) => write!(f, "Invalid file id: {}", file_id),
            Error::InvalidKeySize(size) => {
                write!(f,
                       "Invalid key size, max: {}, found: {}",
                       MAX_KEY_SIZE,
                       size)
            }
            Error::InvalidValueSize(size) => {
                write!(f,
                       "Invalid value size, max: {}, found: {}",
                       MAX_VALUE_SIZE,
                       size)
            }
            Error::InvalidChecksum { expected, found } => {
                write!(f,
                       "Invalid checksum, expected: {}, found: {}",
                       expected,
                       found)
            }
            Error::InvalidPath(ref path) => write!(f, "Invalid path provided: {}", path),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}


impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::InvalidFileId(..) => "Invalid file id",
            Error::InvalidChecksum { .. } => "Invalid checksum",
            Error::InvalidKeySize(..) => "Invalid key size",
            Error::InvalidValueSize(..) => "Invalid value size",
            Error::InvalidPath(..) => "Invalid path",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Io(ref err) => Some(err),
            _ => None,
        }
    }
}
