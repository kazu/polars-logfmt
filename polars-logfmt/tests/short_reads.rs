//! The parallel scan reads a whole frame from a file whose reads come back
//! short, as reads over sftp do.

use std::io::{Cursor, Read};

use polars_logfmt::lazy::LazyLogFmtReaderBuilder;
use polars_logfmt::{SeekableVfsFile, VfsFileStat};

/// Bytes read at most `STEP` at a time.
struct ShortReads {
    data: Vec<u8>,
    pos: usize,
}

const STEP: usize = 7;

impl Read for ShortReads {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = buf.len().min(STEP).min(self.data.len() - self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl SeekableVfsFile for ShortReads {
    fn seek(&mut self, pos: u64) -> std::io::Result<u64> {
        self.pos = (pos as usize).min(self.data.len());
        Ok(self.pos as u64)
    }

    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Read::read(self, buf)
    }

    fn size(&mut self) -> std::io::Result<u64> {
        Ok(self.data.len() as u64)
    }

    fn stat(&mut self) -> std::io::Result<VfsFileStat> {
        Ok(VfsFileStat {
            size: self.data.len() as u64,
            is_seekable: true,
            mtime: None,
        })
    }

    fn clone_handle(&self) -> std::io::Result<Box<dyn SeekableVfsFile + Send>> {
        Ok(Box::new(ShortReads {
            data: self.data.clone(),
            pos: 0,
        }))
    }
}

#[test]
fn aligned_scan_reads_every_line() {
    let text: String = (0..50)
        .map(|i| format!("level=info n={i} msg=m{i}\n"))
        .collect();
    let file = ShortReads {
        data: text.clone().into_bytes(),
        pos: 0,
    };
    let collect = |builder: LazyLogFmtReaderBuilder| {
        builder
            .aligned_cols_cnt(true)
            .build()
            .expect("build")
            .scan()
            .expect("scan")
            .collect()
            .expect("collect")
    };
    let short = collect(LazyLogFmtReaderBuilder::new().from_seekable_vfs_file(Box::new(file)));
    let whole = collect(LazyLogFmtReaderBuilder::new().from_cursor(Cursor::new(text.into_bytes())));
    assert_eq!(short.height(), 50);
    assert!(
        short.equals_missing(&whole),
        "short reads:\n{short}\nwhole:\n{whole}"
    );
}
