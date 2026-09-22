//! Fixture helpers shared by the integration tests: every test writes its
//! input into a `tempfile::TempDir` so nothing depends on the machine.
#![allow(dead_code)]

use std::fs::File;
use std::path::{Path, PathBuf};

/// Three text lines used by the vfs tests.
pub const DATA: &[u8] = b"testdata12345678\nsecondline\nthirdline";

/// Writes `data` to `<dir>/<name>` and returns the path.
pub fn write_plain(dir: &tempfile::TempDir, name: &str, data: &[u8]) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, data).expect("write plain fixture");
    path
}

/// Compresses `plain` into `<dir>/<name>` as seekable zstd, one frame per
/// 64 KiB, records separated by `\n`, and returns the path.
pub fn write_zst(dir: &tempfile::TempDir, plain: &Path, name: &str) -> PathBuf {
    let zst = dir.path().join(name);
    let mut input = File::open(plain).expect("open plain fixture");
    let mut output = File::create(&zst).expect("create zst fixture");
    seekzstdsep::compress_to_seekable_zst(&mut input, &mut output, 65536, true, b"\n", None)
        .expect("compress fixture");
    zst
}

/// `DATA` as `<dir>/plain.txt`.
pub fn write_data_plain(dir: &tempfile::TempDir) -> PathBuf {
    write_plain(dir, "plain.txt", DATA)
}

/// `DATA` as `<dir>/plain.seek.zst`.
pub fn write_data_zst(dir: &tempfile::TempDir) -> PathBuf {
    let plain = write_data_plain(dir);
    write_zst(dir, &plain, "plain.seek.zst")
}

pub fn as_str(path: &Path) -> &str {
    path.to_str().expect("utf-8 path")
}
