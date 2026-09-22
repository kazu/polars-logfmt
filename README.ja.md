# polars-logfmt

[English](README.md)

logfmt 形式のログを Polars の LazyFrame として読む。入力はローカルファイルでも
SSH 越しのリモートファイルでもよく、プレーンテキストと seekable zstd
([seekzstdsep](https://github.com/kazu/seekzstdsep) 形式)の両方を扱う。

## 構成

- `polars-logfmt/` — ライブラリ本体と CLI `polars_logfmt`。
  `LazyLogFmtReader` / `scan_logfmt` が公開 API。
- `logfmt-scan/` — `nu_plugin_polars_dyn` 向けの scan source。
  `nu-polars-dyn-build` でプラグインに組み込む。

## install

```sh
cargo install --path polars-logfmt
```

## 使い方

ライブラリとしては `scan_logfmt` に source と `LogfmtScanOpts` を渡して LazyFrame を得る。
source はローカルパスか `ssh://user@host/path` で、`.zst` で終われば seekable zstd として読む。

```rust
use polars_logfmt::{LogfmtScanOpts, scan_logfmt};

let opts = LogfmtScanOpts { line_filter: Some("level=error".into()), ..Default::default() };
let lf = scan_logfmt("ssh://user@host/var/log/app.log.seek.zst", &opts)?;
let df = lf.collect()?;
```

CLI `polars_logfmt` は現状ベンチマーク用で、`--source` のログから
`msg="finish to process/write "` を含む行を読み、同じ場所に `.parquet` で書き出す。

```sh
polars_logfmt --source /var/log/app.log
```

nushell から使うには `logfmt-scan` を `nu_plugin_polars_dyn` に組み込む。手順は
`logfmt-scan/src/lib.rs` の crate doc にある。

## 開発

```sh
cargo test --workspace
```

`polars-logfmt/tests/` の一部は環境のファイルに依存する。先頭の `LOCAL_BASE` /
`SSH_BASE` を自分の環境に合わせて書き換えてから実行する。

## License

MIT
