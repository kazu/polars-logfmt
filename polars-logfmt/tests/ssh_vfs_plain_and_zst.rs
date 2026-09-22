mod common;

use common::{DATA, as_str, write_data_plain as write_plain, write_data_zst as write_zst};
use polars_logfmt::SeekableVfs;
use polars_logfmt::ssh_vfs::SshSeekableZstdVfs;
use std::io::Write;

/// The ssh tests are `#[ignore]` and run only with `--ignored` when this
/// variable names a reachable host: `POLARS_LOGFMT_TEST_SSH=ssh://user@host[:port]/dir`.
/// The directory must exist and be writable through sftp.
const SSH_ENV: &str = "POLARS_LOGFMT_TEST_SSH";

fn ssh_base() -> String {
    std::env::var(SSH_ENV).unwrap_or_else(|_| panic!("set {SSH_ENV}=ssh://user@host/dir"))
}

use polars_logfmt::SeekableVfsFile;
use polars_logfmt::ssh_vfs::ssh_plain_file::SshSeekablePlainFile;
use ssh2::Session;
use std::net::TcpStream;
use std::sync::Arc;

/// 正常系: ローカルzstファイルのopen/size
#[test]
fn test_open_and_size_zst() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_zst(&dir);
    assert!(path.exists(), "zstファイルが存在しない");
    let mut file =
        polars_logfmt::ssh_vfs::local_zst_file::LocalSeekableZstdFile::open(as_str(&path))
            .expect("open zst");
    assert!(file.size().is_ok(), "size should be ok");
}

/// 正常系: ローカルzstファイルのstat
#[test]
fn test_stat_check_zst() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_zst(&dir);
    let mut file =
        polars_logfmt::ssh_vfs::local_zst_file::LocalSeekableZstdFile::open(as_str(&path))
            .expect("open zst");
    let stat = file.stat().unwrap();
    assert!(stat.is_seekable, "should be seekable");
    assert!(stat.size > 0, "size should be positive");
}

/// 正常系: ローカルzstファイルのseek/read
#[test]
fn test_seek_and_read_zst() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_zst(&dir);
    let mut file =
        polars_logfmt::ssh_vfs::local_zst_file::LocalSeekableZstdFile::open(as_str(&path))
            .expect("open zst");
    let size = file.size().unwrap();

    let seek_pos = size / 2;
    let pos = file.seek(seek_pos).unwrap();
    assert_eq!(pos, seek_pos, "seek should go to correct pos");
    let mut buf = [0u8; 8];
    let n = SeekableVfsFile::read(&mut file, &mut buf).unwrap();
    assert!(n > 0, "read after seek should return data");
}

/// 正常系: ローカルzstファイルの内容検証
#[test]
fn test_content_check_zst() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_zst(&dir);
    assert!(path.exists(), "zstファイルが存在しない");
    let mut file =
        polars_logfmt::ssh_vfs::local_zst_file::LocalSeekableZstdFile::open(as_str(&path))
            .expect("open zst");
    let mut actual = Vec::new();
    file.seek(0).unwrap();
    std::io::Read::read_to_end(&mut file, &mut actual).expect("read zst file");

    assert_eq!(
        actual, DATA,
        "decompressed zst content should match plain.txt"
    );
}

/// 正常系: SSH経由plainファイルのopen/size/read
#[test]
#[ignore = "needs POLARS_LOGFMT_TEST_SSH"]
fn test_open_and_size_plain_ssh() {
    // ssh_base() からホスト・ポート・ユーザー名・パスを抽出
    // 例: ssh://user@host:22/パス
    let ssh_base = ssh_base();
    let url = url::Url::parse(&ssh_base).expect("parse ssh base");
    let host = url.host_str().expect("host");
    let port = url.port().unwrap_or(22);
    let user = url.username();
    // パスは /plain.txt を追加
    let remote_path = format!("{}/plain.txt", url.path().trim_end_matches('/'));
    let addr = format!("{}:{}", host, port);
    let tcp = TcpStream::connect(&addr).expect("connect ssh");
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();
    if let Err(e) = sess.userauth_agent(user) {
        // agent認証失敗時はpubkey認証も試行（環境依存）
        let home = std::env::var("HOME").unwrap_or("/root".to_string());
        let privkey = format!("{}/.ssh/id_rsa", home);
        let pubkey = format!("{}/.ssh/id_rsa.pub", home);
        sess.userauth_pubkey_file(
            user,
            Some(std::path::Path::new(&pubkey)),
            std::path::Path::new(&privkey),
            None,
        )
        .expect(&format!("SSH認証失敗: agent={:?}, pubkey={:?}", e, privkey));
    }
    let sftp = Arc::new(sess.sftp().unwrap());
    // リモートにテストファイル作成
    let data = b"testdata12345678";
    let mut remote_file = sftp
        .create(std::path::Path::new(&remote_path))
        .expect("create remote file");
    remote_file.write_all(data).expect("write remote file");
    // VFS経由でopen
    let mut file = SshSeekablePlainFile::open(&remote_path, sftp.clone()).expect("open plain ssh");
    assert!(file.size().is_ok(), "size should be ok");
    let mut buf = [0u8; 8];
    let n = file.read(&mut buf).expect("read remote file");
    assert_eq!(&buf[..n], &data[..n], "read content matches");
}

/// 正常系: ローカルplainファイルのstat
#[test]
fn test_stat_check_plain() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_plain(&dir);
    let mut file = polars_logfmt::ssh_vfs::local_file::LocalSeekableFile::open(as_str(&path))
        .expect("open plain");
    let stat = file.stat().unwrap();
    assert!(stat.is_seekable, "should be seekable");
    assert!(stat.size > 0, "size should be positive");
}

/// 正常系: ローカルplainファイルのseek/read
#[test]
fn test_seek_and_read_plain() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_plain(&dir);
    let mut file = polars_logfmt::ssh_vfs::local_file::LocalSeekableFile::open(as_str(&path))
        .expect("open plain");
    let size = file.size().unwrap();
    let seek_pos = size / 2;
    let pos = file.seek(seek_pos).unwrap();
    assert_eq!(pos, seek_pos, "seek should go to correct pos");
    let mut buf = [0u8; 8];
    let n = SeekableVfsFile::read(&mut file, &mut buf).unwrap_or(0);
    assert!(n > 0, "read after seek should return data");
}

/// 正常系: ローカルplainファイルの内容検証
#[test]
fn test_content_check_plain() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_plain(&dir);
    let created_bytes = std::fs::read(&path).expect("read created plain.txt");
    assert_eq!(
        created_bytes,
        DATA,
        "{} should contain correct test data after creation",
        path.display()
    );
    let mut file = polars_logfmt::ssh_vfs::local_file::LocalSeekableFile::open(as_str(&path))
        .expect("open plain");
    let mut actual = Vec::new();
    file.seek(0).unwrap();
    std::io::Read::read_to_end(&mut file, &mut actual).expect("read plain file");
    assert_eq!(actual, DATA, "plain file content should match");
}

/// 異常系: 存在しないファイルのopenはエラーとなる
#[test]
#[ignore = "needs POLARS_LOGFMT_TEST_SSH"]
fn test_open_nonexistent_file() {
    let vfs = SshSeekableZstdVfs::new();
    let binding = std::path::Path::new(&ssh_base()).join("nonexistent_file.zst");
    let ssh_path = binding.to_str().unwrap();
    let result = vfs.open(ssh_path);
    assert!(
        result.is_err(),
        "存在しないファイルの open はエラーになるべき"
    );
}

/// 正常系: plainファイルのclone_handle/マルチスレッドread
#[test]
fn test_multithreaded_clone_handle_plain() {
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let local_path = write_plain(&dir);
    let vfs = polars_logfmt::ssh_vfs::local_file::LocalSeekableFile::open(as_str(&local_path))
        .expect("open plain file");
    let handle1 = vfs.clone_handle().expect("clone_handle 1");
    let handle2 = vfs.clone_handle().expect("clone_handle 2");
    let mut h1 = handle1;
    let mut h2 = handle2;
    let t1 = thread::spawn(move || {
        h1.seek(0).unwrap();
        let mut buf = [0u8; 8];
        std::io::Read::read(&mut h1, &mut buf).unwrap();
        buf
    });
    let t2 = thread::spawn(move || {
        h2.seek(8).unwrap();
        let mut buf = [0u8; 8];
        std::io::Read::read(&mut h2, &mut buf).unwrap();
        buf
    });
    let b1 = t1.join().unwrap();
    let b2 = t2.join().unwrap();
    assert_ne!(b1, b2, "各ハンドルで独立した位置から読める");
}

/// 正常系: ローカルzstファイルのclone_handle/マルチスレッドread
#[test]
fn test_multithreaded_clone_handle_local_zst() {
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let local_zst_path = write_zst(&dir);
    assert!(local_zst_path.exists(), "zstファイルが存在しない");
    let vfs = polars_logfmt::ssh_vfs::local_zst_file::LocalSeekableZstdFile::open(as_str(
        &local_zst_path,
    ))
    .expect("open local zst file");
    let handle1 = vfs.clone_handle().expect("clone_handle 1");
    let handle2 = vfs.clone_handle().expect("clone_handle 2");
    let mut h1 = handle1;
    let mut h2 = handle2;
    let t1 = thread::spawn(move || {
        h1.seek(0).unwrap();
        let mut buf = [0u8; 8];
        std::io::Read::read(&mut h1, &mut buf).unwrap();
        buf
    });
    let t2 = thread::spawn(move || {
        h2.seek(8).unwrap();
        let mut buf = [0u8; 8];
        std::io::Read::read(&mut h2, &mut buf).unwrap();
        buf
    });
    let b1 = t1.join().unwrap();
    let b2 = t2.join().unwrap();
    assert_ne!(b1, b2, "各ハンドルで独立した位置から読める (zst)");
}

/// 正常系: SSH経由zstファイルのclone_handle/マルチスレッドread
#[test]
#[ignore = "needs POLARS_LOGFMT_TEST_SSH"]
fn test_multithreaded_clone_handle_ssh_zst() {
    use std::thread;
    // plain.txt → test.seek.zst を必ず生成
    let dir = tempfile::tempdir().unwrap();
    let local_zst_path = write_zst(&dir);

    // SSH上に test.seek.zst を生成
    use ssh2::Session;
    use std::net::TcpStream;
    use std::sync::Arc;
    let ssh_base = ssh_base();
    let url = url::Url::parse(&ssh_base).expect("parse ssh base");
    let host = url.host_str().expect("host");
    let port = url.port().unwrap_or(22);
    dbg!("Connecting to SSH", host, port);
    let user = url.username();
    // remote_pathをssh://...形式で組み立てる
    let remote_path_vfs = format!(
        "ssh://{}@{}{}{}",
        user,
        host,
        url.path().trim_end_matches('/'),
        "/test.seek.zst"
    );
    let remote_path_sftp = format!("{}/test.seek.zst", url.path().trim_end_matches('/'));
    let addr = format!("{}:{}", host, port);
    let tcp = TcpStream::connect(&addr).expect("connect ssh");
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();
    if let Err(e) = sess.userauth_agent(user) {
        let home = std::env::var("HOME").unwrap_or("/root".to_string());
        let privkey = format!("{}/.ssh/id_rsa", home);
        let pubkey = format!("{}/.ssh/id_rsa.pub", home);
        dbg!("userauth_agent error", &e);
        dbg!("Attempting userauth_pubkey_file", &privkey, &pubkey);
        sess.userauth_pubkey_file(
            user,
            Some(std::path::Path::new(&pubkey)),
            std::path::Path::new(&privkey),
            None,
        )
        .expect(&format!("SSH認証失敗: agent={:?}, pubkey={:?}", e, privkey));
    }
    let sftp = Arc::new(sess.sftp().unwrap());
    // ローカルの zst ファイルを読み込み
    let local_zst_bytes = std::fs::read(&local_zst_path).expect("read local zst");
    // リモートに zst ファイル作成
    let mut remote_file = sftp
        .create(std::path::Path::new(&remote_path_sftp))
        .expect("create remote zst file");
    remote_file
        .write_all(&local_zst_bytes)
        .expect("write remote zst file");
    // VFS経由でopen
    let vfs = polars_logfmt::ssh_vfs::SshSeekableZstdVfs::new();
    println!("remote_path_vfs: {}", &remote_path_vfs);
    let file = vfs.open(&remote_path_vfs).expect("open ssh zst file");
    let handle1: Result<Box<dyn SeekableVfsFile + Send>, std::io::Error> = file.clone_handle();
    println!("handle1 is_ok: {}", handle1.is_ok());
    let handle2 = file.clone_handle();
    println!("handle2 is_ok: {}", handle2.is_ok());
    assert!(handle1.is_ok(), "clone_handle 1 should succeed");
    assert!(handle2.is_ok(), "clone_handle 2 should succeed");
    // 一時的にBox<SshSeekableZstdFile>へダウンキャストしてsftp有無を確認
    let mut h1: Box<dyn SeekableVfsFile + Send> = handle1.unwrap();
    let mut h2: Box<dyn SeekableVfsFile + Send> = handle2.unwrap();
    // let h1_any = h1.as_ref();
    // let h2_any = h2.as_ref();
    // if let Some(s) = h1_any.downcast_ref::<SshSeekableZstdFile>() {
    //     println!("h1 sftp.is_some(): {}", s.sftp.is_some());
    // } else {
    //     println!(
    //         "h1 is not SshSeekableZstdFile, type: {}",
    //         std::any::type_name_of_val(h1_any)
    //     );
    // }
    // if let Some(s) = h2_any.downcast_ref::<SshSeekableZstdFile>() {
    //     println!("h2 sftp.is_some(): {}", s.sftp.is_some());
    // } else {
    //     println!(
    //         "h2 is not SshSeekableZstdFile, type: {}",
    //         std::any::type_name_of_val(h2_any)
    //     );
    // }

    let t1 = thread::spawn(move || {
        let seek_res = h1.seek(0);
        println!("h1 seek(0) result: {:?}", seek_res);
        let mut buf = [0u8; 8];
        let read_res = std::io::Read::read(&mut h1, &mut buf);
        println!("h1 read result: {:?}, buf: {:?}", read_res, &buf);
        (seek_res, read_res, buf)
    });
    let t2 = thread::spawn(move || {
        let seek_res = h2.seek(8);
        println!("h2 seek(8) result: {:?}", seek_res);
        let mut buf = [0u8; 8];
        let read_res = std::io::Read::read(&mut h2, &mut buf);
        println!("h2 read result: {:?}, buf: {:?}", read_res, &buf);
        (seek_res, read_res, buf)
    });
    let (seek1, read1, b1) = t1.join().unwrap();
    let (seek2, read2, b2) = t2.join().unwrap();
    println!(
        "t1 result: seek={:?}, read={:?}, buf={:?}",
        seek1, read1, b1
    );
    println!(
        "t2 result: seek={:?}, read={:?}, buf={:?}",
        seek2, read2, b2
    );
    assert!(read1.is_ok(), "h1 read should succeed");
    assert!(read2.is_ok(), "h2 read should succeed");
    assert_ne!(b1, b2, "各ハンドルで独立した位置から読める (ssh zst)");
}

/// 正常系: SSH経由plainファイルのclone_handle/マルチスレッドread
#[test]
#[ignore = "needs POLARS_LOGFMT_TEST_SSH"]
fn test_multithreaded_clone_handle_ssh_plain() {
    use ssh2::Session;
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::thread;
    let dir = tempfile::tempdir().unwrap();
    let local_path = write_plain(&dir);

    // SSH上に plain.txt を生成
    let ssh_base = ssh_base();
    let url = url::Url::parse(&ssh_base).expect("parse ssh base");
    let host = url.host_str().expect("host");
    let port = url.port().unwrap_or(22);
    let user = url.username();
    let remote_path_vfs = format!(
        "ssh://{}@{}{}{}",
        user,
        host,
        url.path().trim_end_matches('/'),
        "/plain.txt"
    );
    let remote_path_sftp = format!("{}/plain.txt", url.path().trim_end_matches('/'));
    let addr = format!("{}:{}", host, port);
    let tcp = TcpStream::connect(&addr).expect("connect ssh");
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();
    if let Err(e) = sess.userauth_agent(user) {
        let home = std::env::var("HOME").unwrap_or("/root".to_string());
        let privkey = format!("{}/.ssh/id_rsa", home);
        let pubkey = format!("{}/.ssh/id_rsa.pub", home);
        sess.userauth_pubkey_file(
            user,
            Some(std::path::Path::new(&pubkey)),
            std::path::Path::new(&privkey),
            None,
        )
        .expect(&format!("SSH認証失敗: agent={:?}, pubkey={:?}", e, privkey));
    }
    let sftp = Arc::new(sess.sftp().unwrap());
    // ローカルの plain.txt ファイルを読み込み
    let local_bytes = std::fs::read(&local_path).expect("read local plain.txt");
    // リモートに plain.txt ファイル作成
    let mut remote_file = sftp
        .create(std::path::Path::new(&remote_path_sftp))
        .expect("create remote plain.txt file");
    remote_file
        .write_all(&local_bytes)
        .expect("write remote plain.txt file");
    // VFS経由でopen
    let vfs = polars_logfmt::ssh_vfs::SshSeekableZstdVfs::new();
    println!("remote_path_vfs: {}", &remote_path_vfs);
    let file = vfs.open(&remote_path_vfs).expect("open ssh plain file");
    let handle1: Result<Box<dyn SeekableVfsFile + Send>, std::io::Error> = file.clone_handle();
    println!("handle1 is_ok: {}", handle1.is_ok());
    let handle2 = file.clone_handle();
    println!("handle2 is_ok: {}", handle2.is_ok());
    assert!(handle1.is_ok(), "clone_handle 1 should succeed");
    assert!(handle2.is_ok(), "clone_handle 2 should succeed");
    // 両ハンドルで独立して読めるか
    let mut h1 = handle1.unwrap();
    let mut h2 = handle2.unwrap();
    let t1 = thread::spawn(move || {
        h1.seek(0).unwrap();
        let mut buf = [0u8; 8];
        std::io::Read::read(&mut h1, &mut buf).unwrap();
        buf
    });
    let t2 = thread::spawn(move || {
        h2.seek(8).unwrap();
        let mut buf = [0u8; 8];
        std::io::Read::read(&mut h2, &mut buf).unwrap();
        buf
    });
    let b1 = t1.join().unwrap();
    let b2 = t2.join().unwrap();
    assert_ne!(b1, b2, "各ハンドルで独立した位置から読める (ssh plain)");
}

/// 正常系: LazyLogFmtReaderのclone_handle/マルチスレッドread
#[test]
fn test_multithreaded_logfmtsource_clone_handle() {
    use std::io::{BufRead, BufReader};
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let local_path = write_plain(&dir);
    let file = polars_logfmt::ssh_vfs::local_file::LocalSeekableFile::open(as_str(&local_path))
        .expect("open plain file");
    let reader = polars_logfmt::lazy::LazyLogFmtReader::from_seekable_vfs_file(Box::new(file));
    let reader1 = reader.clone_with_fresh_handle().expect("fresh handle");
    let reader2 = reader.clone_with_fresh_handle().expect("fresh handle");
    let t1 = thread::spawn(move || {
        let r = reader1;
        let mut buf = String::new();
        let mut state = r.reader_state.lock().unwrap();
        if state.reader.is_none() {
            // clone_handle APIで新しいハンドルを取得
            if let polars_logfmt::lazy::LogFmtSource::Seekable(arc_mutex) = &r.source {
                let file_opt = arc_mutex.lock().unwrap();
                if let Some(file) = file_opt.as_ref() {
                    let cloned_file = file.clone_handle().expect("clone_handle failed");
                    let buf_reader: Box<dyn BufRead + Send> = Box::new(BufReader::new(cloned_file));
                    state.reader = Some(buf_reader);
                }
            }
        }
        if let Some(reader) = &mut state.reader {
            reader.read_line(&mut buf).unwrap();
        }
        buf
    });
    let t2 = thread::spawn(move || {
        let r = reader2;
        let mut buf = String::new();
        let mut state = r.reader_state.lock().unwrap();
        if state.reader.is_none() {
            if let polars_logfmt::lazy::LogFmtSource::Seekable(arc_mutex) = &r.source {
                let file_opt = arc_mutex.lock().unwrap();
                if let Some(file) = file_opt.as_ref() {
                    let mut cloned_file = file.clone_handle().expect("clone_handle failed");
                    cloned_file.seek(18).unwrap(); // 1行目の長さ分進める（仮）
                    let buf_reader: Box<dyn BufRead + Send> = Box::new(BufReader::new(cloned_file));
                    state.reader = Some(buf_reader);
                }
            }
        }
        if let Some(reader) = &mut state.reader {
            reader.read_line(&mut buf).unwrap();
        }
        buf
    });
    let b1 = t1.join().unwrap();
    let b2 = t2.join().unwrap();
    assert_ne!(
        b1, b2,
        "各ハンドルで独立した位置から読める (LogFmtSource/LazyLogFmtReader)"
    );
}
