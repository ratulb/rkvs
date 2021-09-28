use std::fmt;
use std::io;
use std::result;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub struct KvsError(String);

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        let msg = &(err.to_string())[..];
        eprintln!("Error {}", msg);
        KvsError(err.to_string())
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        eprintln!("serde error!");
        KvsError(err.to_string())
    }
}
impl From<String> for KvsError {
    fn from(msg: String) -> Self {
        KvsError(msg)
    }
}
impl From<&str> for KvsError {
    fn from(msg: &str) -> Self {
        KvsError(msg.to_string())
    }
}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError(err.to_string())
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError(err.to_string())
    }
}

pub type Result<T> = result::Result<T, KvsError>;
