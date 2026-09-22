//! SFTP経由seekable zstファイル用のSeekableVfsFile実装（設計枠組み）
// russh-sftp/zstd-seekableクレート導入時に本実装

use crate::{SeekableVfsFile, VfsFileStat};
use ssh2::{File as SftpFile, Sftp};
use std::io::{Read, Result, Seek, SeekFrom};
use std::sync::Arc;
use zeekstd::Decoder;

pub struct SshSeekableZstFile {
    pub path: String,
    pub offset: u64,
    pub size: Option<u64>,
    pub sftp: Arc<Sftp>,
    pub decoder: Option<Decoder<'static, SftpFile>>, // zstd-seekableラッパー
}

impl SshSeekableZstFile {
    #[allow(unused)]
    pub fn open(path: &str, sftp: Arc<Sftp>) -> Result<Self> {
        // SFTP経由でファイルを開き、zeekstd::Decoderでラップ
        let sftp_file = sftp
            .open(path)
            .map_err(|e| std::io::Error::other(format!("sftp open: {}", e)))?;
        // Decoder<'static, SftpFile>を作る（SftpFile: Read+Seek）
        let mut decoder =
            Decoder::new(sftp_file).map_err(|e| std::io::Error::other(e.to_string()))?;
        // 論理サイズ取得
        let seek_table = decoder.seek_table();
        let size = seek_table
            .frame_end_decomp(seek_table.num_frames().saturating_sub(1))
            .unwrap_or(0);
        // Decoderは一度moveしたらsftp_fileを取り出せないので、BoxでラップするかOptionで持つ
        Ok(Self {
            path: path.to_string(),
            offset: 0,
            size: Some(size),
            sftp,
            decoder: Some(decoder),
        })
    }
}

impl SeekableVfsFile for SshSeekableZstFile {
    fn seek(&mut self, pos: u64) -> Result<u64> {
        // SFTP+zstd-seekableでseek
        if let Some(decoder) = &mut self.decoder {
            decoder.seek(SeekFrom::Start(pos))?;
            self.offset = pos;
            Ok(self.offset)
        } else {
            Err(std::io::Error::other("decoder not initialized"))
        }
    }
    fn clone_handle(&self) -> Result<Box<dyn SeekableVfsFile + Send>> {
        // sftpとpathを使って新しいハンドルをopenして返す
        let new_file = SshSeekableZstFile::open(&self.path, self.sftp.clone())?;
        Ok(Box::new(new_file))
    }
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(decoder) = &mut self.decoder {
            let mut total = 0usize;
            while total < buf.len() {
                let n = decoder.read(&mut buf[total..])?;
                if n == 0 {
                    break;
                }
                total += n;
            }
            self.offset += total as u64;
            Ok(total)
        } else {
            Err(std::io::Error::other("decoder not initialized"))
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
    fn seek_table_decomp_frames(&mut self) -> Option<Vec<(u64, u64)>> {
        if let Some(decoder) = &mut self.decoder {
            let seek_table = decoder.seek_table();
            let n_u32 = seek_table.num_frames();
            if n_u32 == 0 {
                return None;
            }
            let n = n_u32 as usize;
            let mut out: Vec<(u64, u64)> = Vec::with_capacity(n);
            let mut prev_end: u64 = 0;
            for i in 0..n_u32 {
                let end = match seek_table.frame_end_decomp(i) {
                    Ok(v) => v,
                    Err(_) => return None,
                };
                let start = prev_end;
                let len = end.saturating_sub(start);
                out.push((start, len));
                prev_end = end;
            }
            Some(out)
        } else {
            None
        }
    }
}

impl Read for SshSeekableZstFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // SeekableVfsFile::readを呼ぶ
        SeekableVfsFile::read(self, buf)
    }
}
