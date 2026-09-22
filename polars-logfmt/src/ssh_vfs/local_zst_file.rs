//! ローカルseekable zstファイル用のSeekableVfsFile実装
//! ローカルseekable zstファイル用のSeekableVfsFile実装（設計枠組み）
// zstd-seekableクレート導入時に本実装

use crate::{SeekableVfsFile, VfsFileStat};
use std::fs::File;
use std::io::{Read, Result};
use zeekstd::Decoder;

pub struct LocalSeekableZstdFile<'a> {
    pub inner: Decoder<'a, File>,
    pos: u64,
    size: u64,
    path: String,
}

impl<'a> LocalSeekableZstdFile<'a> {
    pub fn get_path(&self) -> &str {
        &self.path
    }
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path)?;
        let decoder =
            Decoder::new(file).map_err(|e: zeekstd::Error| std::io::Error::other(e.to_string()))?;
        // SeekTableのAPIで論理サイズ取得
        let seek_table = decoder.seek_table();
        let size = seek_table
            .frame_end_decomp(seek_table.num_frames().saturating_sub(1))
            .unwrap_or(0);
        Ok(Self {
            inner: decoder,
            pos: 0,
            size,
            path: path.to_string(),
        })
    }
}

impl<'a> SeekableVfsFile for LocalSeekableZstdFile<'a> {
    fn seek(&mut self, pos: u64) -> Result<u64> {
        use std::io::Seek;
        self.inner.seek(std::io::SeekFrom::Start(pos))?;
        self.pos = pos;
        Ok(self.pos)
    }
    fn clone_handle(&self) -> Result<Box<dyn SeekableVfsFile + Send>> {
        let new_file = LocalSeekableZstdFile::open(&self.path)?;
        Ok(Box::new(new_file))
    }
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let decoder = &mut self.inner;
        let mut total = 0usize;
        while total < buf.len() {
            let n = decoder.read(&mut buf[total..])?;
            if n == 0 {
                break;
            }
            total += n;
        }
        self.pos += total as u64;
        Ok(total)
    }
    fn size(&mut self) -> Result<u64> {
        Ok(self.size)
    }
    fn stat(&mut self) -> Result<VfsFileStat> {
        Ok(VfsFileStat {
            size: self.size,
            is_seekable: true,
            mtime: None,
        })
    }
    fn seek_table_decomp_frames(&mut self) -> Option<Vec<(u64, u64)>> {
        let seek_table = self.inner.seek_table();
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
    }
}

impl<'a> Read for LocalSeekableZstdFile<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        SeekableVfsFile::read(self, buf)
    }
}
