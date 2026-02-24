use std::{collections::{BTreeMap, HashMap}, fs::File, io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, ops::Range, path::PathBuf};
use crate::{KvStore, Result};

pub struct KVStore {
    path: PathBuf,
    gen: u64,
    compaction_bytes: u64,
    index: BTreeMap<String, CommandPosition>,
    readers: HashMap<u64, BufReaderWithPosition<File>>,
    writer: BufWriterWithPosition<File>
}

struct CommandPosition {
    gen: u64,
    pos: u64,
    len: u64
}

impl CommandPosition {
    fn new(gen:u64, pos:u64, len:u64) -> Self {
        Self { gen, pos, len }
    }
}

impl From<(u64, Range<u64>)> for CommandPosition {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        Self::new(gen, range.start, range.end - range.start)
    }
}

struct BufReaderWithPosition<R:Read + Seek>{
    reader: BufReader<R>,
    pos: u64
}

impl<R:Read + Seek> BufReaderWithPosition<R>{
    pub fn new(self,mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            reader: BufReader::new(inner),
            pos:pos
        })
    }
}

impl<R:Read+Seek> BufReaderWithPosition<R>{
    pub fn read(&mut self,buf:&mut[u8]) -> io::Result<usize> {
        let pos = self.reader.read(buf)?;
        self.pos += pos as u64;
        Ok(pos)
    }
    
}

impl<R:Read+Seek> Seek for BufReaderWithPosition<R>{
     fn seek(&mut self,pos: SeekFrom) ->  io::Result<u64>{
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPosition<W:Write + Seek>{
    writer: BufWriter<W>,
    pos: u64
}