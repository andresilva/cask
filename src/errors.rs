use std::convert::From;
use std::error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidChecksum { expected: u32, found: u32 },
}

pub type Result<T> = result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
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
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Io(ref err) => Some(err),
            _ => None,
        }
    }
}
