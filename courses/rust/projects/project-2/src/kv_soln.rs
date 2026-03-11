use serde_json::Deserializer;

use crate::{KvStore, Result};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::PathBuf,
};

pub struct KVStore {
    path: PathBuf,
    gen: u64,
    uncompaction_bytes: u64,
    index: BTreeMap<String, CommandPosition>,
    readers: HashMap<u64, BufReaderWithPosition<File>>,
    writer: BufWriterWithPosition<File>,
}

impl KVStore {
    fn compact(&mut self) -> Result<()> {
        let compact_gen = self.gen + 1;
        self.gen += 2; //incr +2 to accomodate the compact_gen as gen+1

        let mut compact_writer = self.new_log_file(compact_gen)?;
        let mut writer_pos: u64 = 0;

        let cmd_pos = self.index.values_mut();
        for cmd in cmd_pos {
            let reader = self.readers.get_mut(&cmd.gen).expect("reader not found");
            if cmd.pos != reader.pos {
                reader.seek(SeekFrom::Start(cmd.pos))?;
            }
            //get values and copy to compact gen
            let mut new_reader = reader.take(cmd.len);
            let len = io::copy(&mut new_reader, &mut compact_writer)?;
            *cmd = (compact_gen, writer_pos..writer_pos + len).into();
            writer_pos += len;
        }
        compact_writer.flush()?;

        //remove old readers gen < compact_gen
        let stale_readers: Vec<_> = self
            .readers
            .keys()
            .filter(|&k| *k < compact_gen)
            .cloned()
            .collect();
        for reader in stale_readers {
            self.readers.remove(&reader);
        }

        self.uncompaction_bytes = 0;

        Ok(())
    }

    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPosition<File>> {
        new_log_file(&self.path, &mut self.readers, gen)
    }
}

fn sorted_gen_list(path: &PathBuf) -> Result<Vec<u64>> {
    let mut gens = fs::read_dir(path)?;
    let mut gen_list = Vec::new();
    for gen in gens.by_ref() {
        let gen = gen?;
        if gen.file_type()?.is_file(){
            let filename = gen.file_name();
            // let file_ext = gen.path().extension();
            if let Some(ext) = gen.path().extension() {
                if ext == "log" {
                    // process log file
                    if let Some(file) = filename.to_str() {
                        let gen = file.trim_end_matches(".log").parse::<u64>()?;
                        gen_list.push(gen);

                    }

                }
            }
        }
    }
    gen_list.sort();
    Ok(gen_list)
}
//load index from a given file
fn load(
    gen: u64,
    reader: &mut BufReaderWithPosition<File>,
    index: &mut BTreeMap<String, CommandPosition>,
) -> Result<u64> {
    let mut uncompacted_bytes = 0u64;
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        let cmd = cmd?;
        match cmd{
            Command::Set{key,.. } => {
                let cmd = index.insert(key, (gen, pos..new_pos).into());
                if let Some(old_cmd) = cmd{
                    uncompacted_bytes += old_cmd.len;
                }
            }
            Command::Remove{key} => {
                if let Some(old_cmd) = index.remove(&key){
                    uncompacted_bytes += old_cmd.len;
                }
                //add the remove command for compaction as well
                uncompacted_bytes += new_pos - pos;
            }
        }
        pos = new_pos;
        }
    Ok(uncompacted_bytes)
}

fn new_log_file(
    path: &PathBuf,
    readers: &mut HashMap<u64, BufReaderWithPosition<File>>,
    gen: u64,
) -> Result<BufWriterWithPosition<File>> {
    let path = log_path(path, gen);
    let writer = BufWriterWithPosition::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufReaderWithPosition::new(File::open(&path)?)?);
    Ok(writer)
}

fn log_path(dir: &PathBuf, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

#[derive(serde::Deserialize, serde::Serialize)]
enum Command {
    Set{ key: String, value: String},
    Remove{ key: String}
}

struct CommandPosition {
    gen: u64,
    pos: u64,
    len: u64,
}

impl CommandPosition {
    fn new(gen: u64, pos: u64, len: u64) -> Self {
        Self { gen, pos, len }
    }
}

impl From<(u64, Range<u64>)> for CommandPosition {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        Self::new(gen, range.start, range.end - range.start)
    }
}

struct BufReaderWithPosition<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPosition<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            reader: BufReader::new(inner),
            pos: pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPosition<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPosition<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPosition<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPosition<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPosition {
            writer: BufWriter::new(inner),
            pos: pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPosition<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPosition<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
