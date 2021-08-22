use std::io;
use std::result;

#[derive(Debug)]
pub struct KvsError;

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        let msg =  &(err.to_string())[..];
        eprintln!("Error {}" , msg);
        KvsError
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(_: serde_json::Error) -> Self {
        eprintln!("serde error!");
        KvsError
    }
}

pub type Result<T> = result::Result<T, KvsError>;
