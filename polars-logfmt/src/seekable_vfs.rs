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

pub trait SeekableVfs: Send + Sync {
    fn open(&self, path: &str) -> Result<Box<dyn SeekableVfsFile>>;
    fn stat(&self, path: &str) -> Result<VfsFileStat>;
}
