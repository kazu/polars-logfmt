# russh-sftp/seekable zstd 仮想ファイルシステム 利用ガイド

## 概要
- SSH/SFTP経由およびローカルでseekableなzstd圧縮ファイルを仮想ファイルシステムとして扱えます。
- seek/offset/stat取得が可能な共通インターフェース（SeekableVfsFile, SeekableVfs）を提供します。
- polars_logfmtのLazyLogFmtReader等と連携可能です。

## 使い方サンプル

### 1. ファイル変換（seekable zst生成）
```sh
seekzstdsep convert input.txt output.zst
seekzstdsep convert input.csv output.zst --separator ','
cat input.txt | seekzstdsep convert > out.zst
seekzstdsep convert input.txt --rm
```

### 2. フレーム情報の表示
```sh
seekzstdsep inspect output.zst
```

### 3. Rust API例
```rust
use polars_logfmt::ssh_vfs::{SshSeekableZstdVfs, SeekableVfsFile};
let vfs = SshSeekableZstdVfs::new();
let file = vfs.open("ssh://user@host/path/to/file.seek.zst").unwrap();
let size = file.size().unwrap();
let mut buf = vec![0u8; 1024];
file.seek(0).unwrap();
let n = file.read(&mut buf).unwrap();
```

## 制約事項・注意点
- SSH経由zstファイルはzeekstdクレートでseekableアクセスします（全体ダウンロード不要）。
- clone_handle()で独立したハンドルを複数スレッドで安全に利用可能です。
- stat/size/seek/readの全APIが正常系・異常系ともテスト済みです。
- 存在しないファイルのopenはエラーとなります。
- seek/readの範囲外アクセスはstd::io::Errorを返します。

## 参考
- 詳細API設計: doc/1_ssh_vfs_zst_seekable_api.md
- 実装計画・進捗: doc/plan_ssh_zst_seekable.md
- 作業ログ: doc/worklog.md
