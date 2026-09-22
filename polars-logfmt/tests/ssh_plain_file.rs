// SshSeekablePlainFileの基本動作テスト

use ssh2::Sftp;

#[test]
fn test_clone_handle_plain_file() {
    // テスト用のSftpとパスを用意（モックやテスト環境で差し替え）
    // let sftp = Arc::new(setup_test_sftp());
    // let path = "/tmp/test_plain.txt";
    // let file = SshSeekablePlainFile::open(path, sftp.clone()).expect("open plain file");
    // let cloned = file.clone_handle().expect("clone_handle plain file");
    // // clone後も独立してread/seekできることを確認
    // // ...（必要に応じてread/seekのテストを追加）
}

// テスト用のSftp生成（実際はテスト用SSHサーバやモックを使う）
#[allow(dead_code)]
fn setup_test_sftp() -> Sftp {
    unimplemented!("テスト用Sftpを用意してください")
}
