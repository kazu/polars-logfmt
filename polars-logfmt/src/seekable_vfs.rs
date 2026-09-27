//! 共通トレイト・型定義
use std::io::Result;

pub struct VfsFileStat {
    pub size: u64,
    pub is_seekable: bool,
    pub mtime: Option<std::time::SystemTime>,
}

pub trait SeekableVfsFile: Send + Sync + std::io::Read {
    fn seek(&mut self, pos: u64) -> Result<u64>;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn size(&mut self) -> Result<u64>;
    fn stat(&mut self) -> Result<VfsFileStat>;
    /// 独立したハンドル（状態を持つ新インスタンス）を生成するAPI
    fn clone_handle(&self) -> Result<Box<dyn SeekableVfsFile + Send>>;
    /// If the underlying implementation can expose zstd SeekTable information,
    /// return a vector of (decomp_start, decomp_len) for each frame.
    /// Default implementation returns `None` which indicates that SeekTable
    /// information is not available and callers should fall back to coarse splitting.
    fn seek_table_decomp_frames(&mut self) -> Option<Vec<(u64, u64)>> {
        None
    }
}

/// Text read a unit at a time by number, each unit whole lines: the frames of
/// a compressed file as whatever decompresses them hands them on. The units in
/// the order of their numbers are the text, and any of them can be read from
/// several threads at once.
pub trait UnitSource: Send + Sync {
    /// Appends unit `index` to `dst` and returns `true`, or returns `false`
    /// when there is no such unit. A unit may be empty.
    fn read_unit(&self, index: usize, dst: &mut Vec<u8>) -> Result<bool>;

    /// How many units there are, or `None` when that is only known by reading
    /// them. Without it the units are read in order on one thread.
    fn count_units(&self) -> Option<usize>;
}

pub trait SeekableVfs: Send + Sync {
    fn open(&self, path: &str) -> Result<Box<dyn SeekableVfsFile>>;
    fn stat(&self, path: &str) -> Result<VfsFileStat>;
}
