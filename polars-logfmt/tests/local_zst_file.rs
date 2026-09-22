// LocalSeekableZstdFile unit tests

mod common;

use common::{as_str, write_data_zst as write_zst};
use polars_logfmt::SeekableVfsFile;
use polars_logfmt::ssh_vfs::local_zst_file::LocalSeekableZstdFile;

#[test]
fn test_open_and_size() {
    let dir = tempfile::tempdir().unwrap();
    let zst = write_zst(&dir);
    let file = LocalSeekableZstdFile::open(as_str(&zst));
    assert!(file.is_ok(), "open should succeed");
    let mut file = file.unwrap();
    let size = file.size().unwrap();
    assert!(size > 0, "size should be positive");
}

#[test]
fn test_read_head() {
    let dir = tempfile::tempdir().unwrap();
    let zst = write_zst(&dir);
    let mut file = LocalSeekableZstdFile::open(as_str(&zst)).unwrap();
    let mut buf = [0u8; 16];
    let n = SeekableVfsFile::read(&mut file, &mut buf).unwrap();
    assert!(n > 0, "read should return data");
}

#[test]
fn test_seek_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let zst = write_zst(&dir);
    let mut file = LocalSeekableZstdFile::open(as_str(&zst)).unwrap();
    let size = file.size().unwrap();
    let seek_pos = size / 2;
    let pos = SeekableVfsFile::seek(&mut file, seek_pos).unwrap();
    assert_eq!(pos, seek_pos);
    let mut buf = [0u8; 8];
    let n = SeekableVfsFile::read(&mut file, &mut buf).unwrap();
    assert!(n > 0, "read after seek should return data");
}

#[test]
fn test_stat() {
    let dir = tempfile::tempdir().unwrap();
    let zst = write_zst(&dir);
    let mut file = LocalSeekableZstdFile::open(as_str(&zst)).unwrap();
    let stat = file.stat().unwrap();
    assert!(stat.is_seekable);
    assert!(stat.size > 0);
}
