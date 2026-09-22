pub mod local_file;
pub mod local_zst_file;
pub mod ssh_plain_file;
pub mod ssh_zst_file;
use ssh2::Session;
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpStream;
// use tokio::runtime::Runtime; // 必要に応じて
impl SshSeekableZstdFile {
    /// 非同期でread（tokio/async用）
    pub async fn read_async(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                let n = f.read(buf)?;
                self.offset += n as u64;
                Ok(n)
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Err(std::io::Error::other("zstd-seekable未実装"))
        }
    }

    /// 同期でread（内部でblock_on）
    pub fn read_sync(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                let n = f.read(buf)?;
                self.offset += n as u64;
                Ok(n)
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Err(std::io::Error::other("zstd-seekable未実装"))
        }
    }

    /// 同期でread（内部でblock_on）
    /// 非同期でseek（tokio/async用）
    pub async fn seek_async(&mut self, pos: u64) -> std::io::Result<u64> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                f.seek(SeekFrom::Start(pos))?;
                self.offset = pos;
                Ok(self.offset)
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Err(std::io::Error::other("zstd-seekable未実装"))
        }
    }

    /// 同期でseek（内部でblock_on）
    pub fn seek_sync(&mut self, pos: u64) -> std::io::Result<u64> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                f.seek(SeekFrom::Start(pos))?;
                self.offset = pos;
                Ok(self.offset)
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Err(std::io::Error::other("zstd-seekable未実装"))
        }
    }

    /// 非同期でstat取得
    pub async fn stat_async(&mut self) -> std::io::Result<VfsFileStat> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                let stat = f.stat()?;
                Ok(VfsFileStat {
                    size: stat.size.unwrap_or(0),
                    is_seekable: true,
                    mtime: stat
                        .mtime
                        .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs(t)),
                })
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Err(std::io::Error::other("zstd-seekable未実装"))
        }
    }

    /// 同期でstat取得
    pub fn stat_sync(&mut self) -> std::io::Result<VfsFileStat> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                let stat = f.stat()?;
                Ok(VfsFileStat {
                    size: stat.size.unwrap_or(0),
                    is_seekable: true,
                    mtime: stat
                        .mtime
                        .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs(t)),
                })
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Err(std::io::Error::other("zstd-seekable未実装"))
        }
    }

    /// SFTP経由で新しいハンドルを生成
    pub fn open(path: &str, sftp: std::sync::Arc<ssh2::Sftp>, is_zst: bool) -> Result<Self> {
        let (sftp_file, size) = if !is_zst {
            tracing::debug!(path = %path, "sftp.open");
            let mut f = sftp.open(path).map_err(|e| {
                std::io::Error::other(format!("sftp open: {} (path: {:?})", e, path))
            })?;
            let size = f.stat().ok().and_then(|stat| stat.size);
            (Some(f), size)
        } else {
            // is_zst==trueでここが呼ばれるのは設計上誤り。panicで検出。
            panic!(
                "SshSeekableZstdFile::open called with is_zst=true; this is a bug. Use SshSeekableZstFile for zst files."
            );
        };
        Ok(Self {
            path: path.to_string(),
            offset: 0,
            size,
            is_zst,
            sftp: Some(sftp),
            sftp_file,
        })
    }
}
/*
! russh-sftpベースのSeekableVfsFile/SshSeekableZstdVfs実装（スケルトン）
--- seek/offset/stat対応インターフェース設計方針 ---
・seek: SFTPのファイルハンドルでoffset指定のreadを実装
・read: SFTPのread_at/seek+readでバッファ取得
・size/stat: SFTPのstat/lstatで取得
※zstdフレームのseekable対応は上位でラップ予定

実装例は今後russh-sftpクレートのAPIに合わせて具体化する
*/

use crate::SeekableVfsFile;
use crate::VfsFileStat;
use std::io::Result;

/// russh-sftp経由でseekable zstdファイルを扱う仮想ファイル
/// SFTP経由でファイルを扱う（seekable zst/非圧縮両対応）
pub struct SshSeekableZstdFile {
    pub path: String,
    pub offset: u64,                              // 現在のオフセット
    pub size: Option<u64>,                        // statで取得したサイズ
    pub is_zst: bool,                             // seekable zst判定
    pub sftp: Option<std::sync::Arc<ssh2::Sftp>>, // SFTPインスタンス保持（SSHのみ）
    sftp_file: Option<ssh2::File>,                // SFTP経由plainファイル用
}

impl SeekableVfsFile for SshSeekableZstdFile {
    fn seek(&mut self, pos: u64) -> Result<u64> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                f.seek(SeekFrom::Start(pos))?;
                self.offset = pos;
                Ok(self.offset)
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            self.offset = pos;
            Ok(self.offset)
        }
    }
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                let n = f.read(buf)?;
                self.offset += n as u64;
                Ok(n)
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Ok(0)
        }
    }
    fn size(&mut self) -> Result<u64> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                let stat = f.stat()?;
                Ok(stat.size.unwrap_or(0))
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            self.size
                .ok_or_else(|| std::io::Error::other("size unknown"))
        }
    }
    fn stat(&mut self) -> Result<VfsFileStat> {
        if !self.is_zst {
            if let Some(f) = &mut self.sftp_file {
                let stat = f.stat()?;
                Ok(VfsFileStat {
                    size: stat.size.unwrap_or(0),
                    is_seekable: true,
                    mtime: stat
                        .mtime
                        .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs(t)),
                })
            } else {
                Err(std::io::Error::other("sftp_file not open"))
            }
        } else {
            Ok(VfsFileStat {
                size: self.size.unwrap_or(0),
                is_seekable: true,
                mtime: None,
            })
        }
    }
    fn clone_handle(&self) -> Result<Box<dyn SeekableVfsFile + Send>> {
        match &self.sftp {
            Some(sftp_arc) => {
                let new_file =
                    SshSeekableZstdFile::open(&self.path, sftp_arc.clone(), self.is_zst)?;
                // デバッグ用: sftpの有無とis_zstを出力
                debug_assert_eq!(
                    self.sftp.is_none(),
                    new_file.sftp.is_none(),
                    "sftp is_none mismatch: self.is_zst={}, self.sftp.is_none()={}, new_file.sftp.is_none()={}",
                    self.is_zst,
                    self.sftp.is_none(),
                    new_file.sftp.is_none()
                );
                Ok(Box::new(new_file))
            }
            None => Err(std::io::Error::other("sftp required for clone_handle")),
        }
    }
}
impl std::io::Read for SshSeekableZstdFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        SeekableVfsFile::read(self, buf)
    }
}
/// russh-sftp経由でファイルを開く仮想ファイルシステム
pub struct SshSeekableZstdVfs {
    // TODO: russh-sftpの接続情報等
}
impl SshSeekableZstdVfs {
    #[allow(clippy::new_without_default)]
    pub fn new(/* 接続情報 */) -> Self {
        Self {
            // ...
        }
    }
}
impl crate::SeekableVfs for SshSeekableZstdVfs {
    fn open(&self, path: &str) -> Result<Box<dyn SeekableVfsFile>> {
        if path.starts_with("ssh://") {
            // SFTP経由
            // ssh://user@host/path の形式をパース
            let url = url::Url::parse(path)
                .map_err(|e| std::io::Error::other(format!("invalid ssh url: {}", e)))?;
            let path_on_remote = url.path();
            let is_zst = path_on_remote.ends_with(".zst") || path_on_remote.ends_with(".seek.zst");
            let user = url.username();
            let host = url
                .host_str()
                .ok_or_else(|| std::io::Error::other("no host in ssh url"))?;
            let port = url.port().unwrap_or(22);
            // TCP接続
            let tcp = TcpStream::connect(format!("{}:{}", host, port))
                .map_err(|e| std::io::Error::other(format!("tcp connect failed: {}", e)))?;
            let mut sess =
                Session::new().map_err(|e| std::io::Error::other(format!("ssh session: {}", e)))?;
            sess.set_tcp_stream(tcp);
            sess.handshake()
                .map_err(|e| std::io::Error::other(format!("ssh handshake: {}", e)))?;
            // TODO: 鍵認証やパスワード認証を追加
            sess.userauth_agent(user)
                .map_err(|e| std::io::Error::other(format!("ssh userauth: {}", e)))?;
            if !sess.authenticated() {
                return Err(std::io::Error::other(
                    "ssh authentication failed".to_string(),
                ));
            }
            let sftp = sess
                .sftp()
                .map_err(|e| std::io::Error::other(format!("sftp: {}", e)))?;
            let sftp_arc = std::sync::Arc::new(sftp);
            if is_zst {
                // ssh seekable zstファイル（zeekstd経由）
                match crate::ssh_vfs::ssh_zst_file::SshSeekableZstFile::open(
                    path_on_remote,
                    sftp_arc.clone(),
                ) {
                    Ok(f) => Ok(Box::new(f)),
                    Err(e) => Err(e),
                }
            } else {
                // ssh未圧縮ファイルはSshSeekablePlainFileで返す
                match crate::ssh_vfs::ssh_plain_file::SshSeekablePlainFile::open(
                    path_on_remote,
                    sftp_arc.clone(),
                ) {
                    Ok(f) => Ok(Box::new(f)),
                    Err(e) => Err(e),
                }
            }
        } else if path.ends_with(".zst") || path.ends_with(".seek.zst") {
            // ローカルseekable zstファイル
            match local_zst_file::LocalSeekableZstdFile::open(path) {
                Ok(f) => Ok(Box::new(f)),
                Err(e) => Err(e),
            }
        } else {
            // ローカル未圧縮ファイル
            match local_file::LocalSeekableFile::open(path) {
                Ok(f) => Ok(Box::new(f)),
                Err(e) => Err(e),
            }
        }
    }
    fn stat(&self, path: &str) -> Result<VfsFileStat> {
        if path.starts_with("ssh://") {
            // SFTP経由
            // ssh://user@host/path の形式をパース
            let url = url::Url::parse(path)
                .map_err(|e| std::io::Error::other(format!("invalid ssh url: {}", e)))?;
            let path_on_remote = url.path();
            let is_zst = path_on_remote.ends_with(".zst") || path_on_remote.ends_with(".seek.zst");
            let user = url.username();
            let host = url
                .host_str()
                .ok_or_else(|| std::io::Error::other("no host in ssh url"))?;
            let port = url.port().unwrap_or(22);
            // TCP接続
            let tcp = std::net::TcpStream::connect(format!("{}:{}", host, port))
                .map_err(|e| std::io::Error::other(format!("tcp connect failed: {}", e)))?;
            let mut sess = ssh2::Session::new()
                .map_err(|e| std::io::Error::other(format!("ssh session: {}", e)))?;
            sess.set_tcp_stream(tcp);
            sess.handshake()
                .map_err(|e| std::io::Error::other(format!("ssh handshake: {}", e)))?;
            // TODO: 鍵認証やパスワード認証を追加
            sess.userauth_agent(user)
                .map_err(|e| std::io::Error::other(format!("ssh userauth: {}", e)))?;
            let sftp = sess
                .sftp()
                .map_err(|e| std::io::Error::other(format!("sftp: {}", e)))?;
            let sftp_arc = std::sync::Arc::new(sftp);
            if is_zst {
                // ssh seekable zstファイル（zeekstd経由）
                let mut file = crate::ssh_vfs::ssh_zst_file::SshSeekableZstFile::open(
                    path_on_remote,
                    sftp_arc.clone(),
                )?;
                file.stat()
            } else {
                // ssh未圧縮ファイル
                // SFTP statでサイズ取得
                let sftp_file = sftp_arc
                    .open(path_on_remote)
                    .map_err(|e| std::io::Error::other(format!("sftp open: {}", e)))?;
                let mut file = SshSeekableZstdFile {
                    path: path.to_string(),
                    offset: 0,
                    size: None, // statで取得する
                    is_zst: false,
                    sftp: Some(sftp_arc.clone()),
                    sftp_file: Some(sftp_file),
                };
                file.stat()
            }
        } else if path.ends_with(".zst") || path.ends_with(".seek.zst") {
            // ローカルseekable zstファイル
            let mut file = SshSeekableZstdFile {
                path: path.to_string(),
                offset: 0,
                size: None,
                is_zst: true,
                sftp: None,
                sftp_file: None,
            };
            file.stat()
        } else {
            // ローカル未圧縮ファイル
            let mut file = local_file::LocalSeekableFile::open(path)?;
            file.stat()
        }
    }
}
