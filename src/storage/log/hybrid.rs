use super::{Range, Scan, Store};
use crate::error::{Error, Result};

use bincode;
use bincode::config::FixintEncoding;
use std::cmp::{max, min};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Display;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek as _, SeekFrom, Write};
use std::ops::Bound;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use tokio_util::sync::PollSemaphore;

/// 一个混合日志存储，将已提交的条目存储在追加写文件中，未提交的条目存储在内存中，元数据存储在单独的文件中（应为磁盘上的键值存储）。
///
/// 日志文件包含顺序的二进制日志条目，使用大端字节序的u32进行长度前缀编码。
/// 只有在条目被提交且永久化后，才将其刷新到磁盘上，因此文件是追加写入的。
///
/// 在内存中维护了一个条目位置和大小的索引。这个索引在启动时通过扫描文件重建，
/// 因为将索引保存在单独的文件中需要额外的fsync操作，这是昂贵的。
/// 由于数据集预计较小，在启动时扫描文件的成本是相对较低的。

pub struct Hybrid {
    /// 追加写的日志文件。通过互斥锁进行保护，以实现内部可变性（例如读取定位）。
    file: Mutex<File>,
    /// 日志文件中条目位置和大小的索引。
    index: BTreeMap<u64, (u64, u32)>,
    /// 未提交的日志条目。
    uncommitted: VecDeque<Vec<u8>>,
    /// 元数据缓存。在更改时刷新到磁盘。
    metadata: HashMap<Vec<u8>, Vec<u8>>,
    /// 用于存储元数据的文件。
    /// FIXME 应为一个磁盘上的B树键值存储。
    metadata_file: File,
    /// 如果为true，则对写入进行fsync操作。
    sync: bool,
}

impl Display for Hybrid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "hybrid")
    }
}

impl Hybrid {
    /// Creates or opens a new hybrid log, with files in the given directory.
    pub fn new(dir: &Path, sync: bool) -> Result<Self> {
        create_dir_all(dir).unwrap();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-log"))
            .unwrap();

        let metadata_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-metadata"))
            .unwrap();

        Ok(Self {
            file: Mutex::new(file),
            index: Self::build_index(&file)?,
            uncommitted: VecDeque::new(),
            metadata: Self::load_metadata(&metadata_file)?,
            metadata_file: metadata_file,
            sync: sync,
        })
    }

    /// Builds the index by scanning the log file.
    fn build_index(file: &File) -> Result<BTreeMap<u64, (u64, u32)>> {
        let filesize = file.metadata().unwrap().len();
        let mut bufreader = BufReader::new(file);
        let mut index = BTreeMap::new();
        let mut sizebuf = [0; 4];
        let mut pos = 0;
        let mut i = 1;
        while pos < filesize {
            bufreader.read_exact(&mut sizebuf).unwrap();
            pos += 4;
            let size = u32::from_be_bytes(sizebuf);
            index.insert(i, (pos, size));
            let mut buf = vec![0; size as usize];
            bufreader.read_exact(&mut buf).unwrap();
            pos += size as u64;
            i += 1;
        }
        Ok(index)
    }

    /// Loads metadata from a file.
    fn load_metadata(file: &File) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        match bincode::deserialize_from(file) {
            Ok(metadata) => Ok(metadata),
            Err(err) => {
                if let bincode::ErrorKind::Io(err) = &*err {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(HashMap::new());
                    }
                }
                // Err(err.into())
                // overwrite
                Err(Error::Internal(format!("{}", err)))
            }
        }
    }
}

impl Store for Hybrid {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.uncommitted.push_back(entry);
        Ok(self.len())
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        if index > self.len() {
            return Err(Error::Internal(format!(
                "Cannot commit non-existant index {}",
                index
            )));
        }

        if index < self.index.len() as u64 {
            return Err(Error::Internal(format!(
                "Cannot commit below current committed index {}",
                self.index.len() as u64
            )));
        }
        if index == self.index.len() as u64 {
            return Ok(());
        }

        let mut file = self.file.lock().unwrap();
        // 获取当前pos
        let mut pos = file.seek(SeekFrom::End(0)).unwrap();
        let mut bufwriter = BufWriter::new(&mut *file);
        for i in (self.index.len() as u64 + 1)..=index {
            let entry = self
                .uncommitted
                .pop_front()
                .ok_or_else(|| Error::Internal("Unexpected end of uncommitted entries".into()))
                .unwrap();

            // 写入长度
            bufwriter
                .write_all(&(entry.len() as u32).to_be_bytes())
                .unwrap();
            pos += 4;
            self.index.insert(i, (pos, entry.len() as u32));
            bufwriter.write_all(&entry).unwrap();
            pos += entry.len() as u64;
        }

        bufwriter.flush().unwrap();
        drop(bufwriter);

        if self.sync {
            file.sync_data().unwrap();
        }
        Ok(())
    }

    fn committed(&self) -> u64 {
        self.index.len() as u64
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        match index {
            0 => Ok(None),
            i if i <= self.index.len() as u64 => {
                let (pos, size) = self.index.get(&i).copied().ok_or_else(|| {
                    Error::Internal(format!("Indexed position not found for entry {}", i))
                })?;
                let mut entry = vec![0; size as usize];
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(pos)).unwrap();

            }
        }
    }

    fn len(&self) -> u64 {
        todo!()
    }

    fn scan(&self, range: Range) -> Scan {
        todo!()
    }

    fn size(&self) -> u64 {
        todo!()
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        todo!()
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        todo!()
    }
}
