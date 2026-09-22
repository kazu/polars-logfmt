//! SFTP経由plainファイル用のSeekableVfsFile実装
use crate::{SeekableVfsFile, VfsFileStat};
use ssh2::{File as SftpFile, Sftp};
use std::io::{Read, Result, Seek, SeekFrom};
use std::sync::Arc;

pub struct SshSeekablePlainFile {
    pub path: String,
    pub offset: u64,
    pub size: Option<u64>,
    pub sftp: Arc<Sftp>,
    pub file: Option<SftpFile>,
}

impl SshSeekablePlainFile {
    pub fn open(path: &str, sftp: Arc<Sftp>) -> Result<Self> {
        let mut sftp_file = sftp.open(path)?;
        let size = sftp_file.stat()?.size;
        Ok(Self {
            path: path.to_string(),
            offset: 0,
            size,
            sftp,
            file: Some(sftp_file),
        })
    }
}

impl SeekableVfsFile for SshSeekablePlainFile {
    fn seek(&mut self, pos: u64) -> Result<u64> {
        if let Some(file) = &mut self.file {
            file.seek(SeekFrom::Start(pos))?;
            self.offset = pos;
            Ok(self.offset)
        } else {
            Err(std::io::Error::other("file not initialized"))
        }
    }
    fn clone_handle(&self) -> Result<Box<dyn SeekableVfsFile + Send>> {
        // Arc<Sftp>のポインタ値を記録（Debug未実装でもアドレスで判別）
        tracing::debug!(
            sftp_ptr = Arc::as_ptr(&self.sftp) as usize,
            path = %self.path,
            "clone_handle: self"
        );
        let new_file = SshSeekablePlainFile::open(&self.path, self.sftp.clone())?;
        tracing::debug!(
            sftp_ptr = Arc::as_ptr(&new_file.sftp) as usize,
            path = %new_file.path,
            "clone_handle: new_file"
        );
        Ok(Box::new(new_file))
    }
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(file) = &mut self.file {
            let n = file.read(buf)?;
            self.offset += n as u64;
            Ok(n)
        } else {
            Err(std::io::Error::other("file not initialized"))
        }
    }
    fn size(&mut self) -> Result<u64> {
        self.size
            .ok_or_else(|| std::io::Error::other("size unknown"))
    }
    fn stat(&mut self) -> Result<VfsFileStat> {
        Ok(VfsFileStat {
            size: self.size.unwrap_or(0),
            is_seekable: true,
            mtime: None,
        })
    }
}

impl Read for SshSeekablePlainFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        SeekableVfsFile::read(self, buf)
    }
}
