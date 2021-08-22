use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

pub struct KvStore {
    path: PathBuf,
    readers: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    index: BTreeMap<String, CommandPos>,
    uncompacted: u64,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;

        for gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, *gen))?)?;
            uncompacted += load(*gen, &mut reader, &mut index)?;
            readers.insert(*gen, reader);
        }
        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            current_gen,
            index,
            uncompacted,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .index
                .insert(key, (self.current_gen, pos..self.writer.pos).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        let compacation_gen = self.current_gen +1;
        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;
        let mut compaction_writer = self.new_log_file(compacation_gen)?;
        let mut new_pos = 0;
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self.readers
                .get_mut(&cmd_pos.gen).expect("Could not find reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }
            let mut entry_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compacation_gen, new_pos..new_pos+len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;
        let stale_gens: Vec<_> = self.readers.keys()
            .filter(|&&gen| gen < compacation_gen)
            .cloned().collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.uncompacted = 0;

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Failed to get reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { key: _, value } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                eprintln!("Command serilalization error");
                Err(KvsError)
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove {
                key: key.to_owned(),
            };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            let old_cmd = self.index.remove(&key).expect("Key not found");
            self.uncompacted += old_cmd.len;
            Ok(())
        } else {
            Err(KvsError)
        }
    }
    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>>{
        new_log_file(&self.path, gen, &mut self.readers)
    }
}

#[inline(always)]
fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> CommandPos {
        CommandPos {
            gen: gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos += self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

#[inline(always)]
fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&path)?;
    let writer = BufWriterWithPos::new(file)?;
    let file = File::open(&path)?;
    let reader = BufReaderWithPos::new(file)?;
    readers.insert(gen, reader);
    Ok(writer)
}

#[inline(always)]
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        //.filter(|de| de.is_ok())
        //.flat_map(|res| res.ok())
        .filter_map(|rs| rs.ok())
        .map(|de| de.path())
        .filter(|pb| pb.is_file() && pb.extension() == Some("log".as_ref()))
        .flat_map(|pb| {
            pb.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

#[inline(always)]
fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

#[test]
fn test_open() {
    let store = KvStore::open("./logs/");
    assert_eq!(store.ok().is_some(), true);
}
#[test]
fn test_set() {
    let mut store = KvStore::open("./logs/").unwrap();
    let result = store.set("key1".to_string(), "value1".to_string());
    assert_eq!(result.ok(), Some(()));
}

#[test]
fn test_set_get() {
    let mut store = KvStore::open("./logs/").unwrap();
    let result = store.set("key1".to_string(), "value1".to_string());
    assert_eq!(result.ok(), Some(()));
    let get = store.get("key1".to_string()).ok().unwrap().unwrap();
    assert_eq!(get, String::from("value1"));
}
