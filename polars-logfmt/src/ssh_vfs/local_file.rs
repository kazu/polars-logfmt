//! ローカルファイル用のSeekableVfsFile実装
use crate::{SeekableVfsFile, VfsFileStat};
use std::fs::File;
use std::io::{Read, Result, Seek, SeekFrom};

pub struct LocalSeekableFile {
    file: File,
    size: u64,
    path: String,
}

impl LocalSeekableFile {
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            file,
            size,
            path: path.to_string(),
        })
    }
}

impl SeekableVfsFile for LocalSeekableFile {
    fn seek(&mut self, pos: u64) -> Result<u64> {
        self.file.seek(SeekFrom::Start(pos))
    }
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.file.read(buf)
    }
    fn size(&mut self) -> Result<u64> {
        Ok(self.size)
    }
    fn stat(&mut self) -> Result<VfsFileStat> {
        Ok(VfsFileStat {
            size: self.size,
            is_seekable: true,
            mtime: self.file.metadata()?.modified().ok(),
        })
    }
    fn clone_handle(&self) -> Result<Box<dyn SeekableVfsFile + Send>> {
        let new_file = LocalSeekableFile::open(&self.path)?;
        Ok(Box::new(new_file))
    }
}

impl std::io::Read for LocalSeekableFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}
