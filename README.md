# polars-logfmt

[日本語](README.ja.md)

Read logfmt logs into Polars LazyFrames. The input can be a local file or a
remote file over SSH, either plain text or seekable zstd
([seekzstdsep](https://github.com/kazu/seekzstdsep) format).

## Layout

- `polars-logfmt/` — the library and the `polars_logfmt` CLI.
  `LazyLogFmtReader` / `scan_logfmt` are the public API.
- `logfmt-scan/` — the scan source for `nu_plugin_polars_dyn`,
  compiled into the plugin with `nu-polars-dyn-build`.

## Install

```sh
cargo install --path polars-logfmt
```

## Usage

As a library, hand a source and `LogfmtScanOpts` to `scan_logfmt` to get a LazyFrame.
The source is a local path or `ssh://user@host/path`; a `.zst` suffix means seekable zstd.

```rust
use polars_logfmt::{LogfmtScanOpts, scan_logfmt};

let opts = LogfmtScanOpts { line_filter: Some("level=error".into()), ..Default::default() };
let lf = scan_logfmt("ssh://user@host/var/log/app.log.seek.zst", &opts)?;
let df = lf.collect()?;
```

The `polars_logfmt` CLI is a benchmark driver for now: it reads the lines containing
`msg="finish to process/write "` from the log given by `--source` and writes them
next to it as `.parquet`.

```sh
polars_logfmt --source /var/log/app.log
```

To use it from nushell, build `logfmt-scan` into `nu_plugin_polars_dyn`; the steps
are in the crate doc of `logfmt-scan/src/lib.rs`.

## Development

```sh
cargo test --workspace
```

Some tests under `polars-logfmt/tests/` depend on files in your environment.
Edit `LOCAL_BASE` / `SSH_BASE` at the top of those files before running them.

## License

MIT
