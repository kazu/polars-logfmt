# russh-sftp/seekable zstd仮想ファイルシステム API設計

## 目的
- SSH/SFTP経由およびローカルでseekableなzstd圧縮ファイルを仮想ファイルシステムとして扱う。
- seek/offset/stat取得が可能なインターフェースを提供。
- LazyLogFmtReader等の既存ロジックと連携可能なAPI設計。

## トレイト定義

```rust
/// Seekableな仮想ファイルの共通インターフェース
pub trait SeekableVfsFile: Send + Sync {
    /// 指定位置にシーク
    fn seek(&mut self, pos: u64) -> std::io::Result<u64>;
    /// データを読み込む
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;
    /// ファイルサイズ取得
    fn size(&self) -> std::io::Result<u64>;
    /// stat情報取得
    fn stat(&self) -> std::io::Result<VfsFileStat>;
}

/// stat情報
pub struct VfsFileStat {
    pub size: u64,
    pub is_seekable: bool,
    pub mtime: Option<std::time::SystemTime>,
}

/// 仮想ファイルシステムの共通インターフェース
pub trait SeekableVfs: Send + Sync {
    /// ファイルを開く
    fn open(&self, path: &str) -> std::io::Result<Box<dyn SeekableVfsFile>>;
    /// stat取得
    fn stat(&self, path: &str) -> std::io::Result<VfsFileStat>;
}
```

## 実装例
- `LocalSeekableZstdVfs` : ローカルファイル用
- `SshSeekableZstdVfs`   : russh-sftp経由

## LazyLogFmtReader拡張
- `SeekableVfsFile`を受け入れる新コンストラクタ追加

## CLIコマンド仕様（seekzstdsep）
- `seekzstdsep convert [<INPUT>] [<OUTPUT>] [--separator <SEP>] [--frame-size <N>] [--rm]`
    - 任意テキストファイルをseekable zst形式に変換
    - `--separator <SEP>` : 区切り文字列（デフォルト: 改行）
    - `--frame-size <N>` : フレーム最大サイズ（デフォルト: 65536）
    - `--rm` : 入力ファイルを変換後に削除
    - 入力省略時はstdin、出力省略時は`<INPUT>.seek.zst`またはstdout
- `seekzstdsep inspect <zstfile>` : フレーム状態表示
    - フレームごとの圧縮/展開オフセット・サイズ等を一覧表示


### ビルド方法

このリポジトリのルートまたはpolars_logfmtディレクトリで以下を実行してください。

```sh
# ビルド（デバッグ版）
cargo build -p polars_logfmt --bin seekzstdsep

# ビルド＆インストール（パスの通った~/.cargo/bin等に配置）
cargo install --path polars_logfmt --bin seekzstdsep

# そのまま実行（ビルド不要でテスト）
cargo run -p polars_logfmt --bin seekzstdsep -- [引数]
```

### 使い方サンプル

```sh
# 改行区切りテキストを変換
seekzstdsep convert input.txt output.zst

# カンマ区切りで変換
seekzstdsep convert input.csv output.zst --separator ','

# stdin→stdoutで変換
cat input.txt | seekzstdsep convert > out.zst

# 変換後に入力ファイルを削除
seekzstdsep convert input.txt --rm

# フレーム情報を表示
seekzstdsep inspect output.zst
```

## フレーム状態情報
- 各frameの解凍後オフセット開始位置・サイズ
- seekable対応有無
- frame終端が改行か

---

# 参考: 既存SshSource/LazyLogFmtReader設計例
// ...既存設計例の参照...
