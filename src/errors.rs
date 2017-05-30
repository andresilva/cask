use std::convert::From;
use std::error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use data::{MAX_KEY_SIZE, MAX_VALUE_SIZE};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidKeySize(usize),
    InvalidValueSize(usize),
    InvalidChecksum { expected: u32, found: u32 },
}

pub type Result<T> = result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
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
            Error::InvalidChecksum { .. } => "Invalid checksum",
            Error::InvalidKeySize(..) => "Invalid key size",
            Error::InvalidValueSize(..) => "Invalid value size",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Io(ref err) => Some(err),
            _ => None,
        }
    }
}
