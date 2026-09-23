//! The `logfmt` scan source for `nu_plugin_polars_dyn`: bytes of logfmt lines into a frame.
//!
//! Build a plugin with it compiled in, beside the sources that open and decompress:
//!
//! ```nu
//! nu-polars-dyn-build seekzstdsep_scan ssh_scan logfmt_scan --path seekzstdsep_scan=../nu_plugin_polars_dyn/seekzstdsep-scan --path ssh_scan=../nu_plugin_polars_dyn/ssh-scan --path logfmt_scan=./logfmt-scan
//! ```
//!
//! It ends a chain: the suffix `.logfmt` puts it last, and whatever came before — `file`, `ssh`,
//! `seek-zst` — hands it the bytes. So `app.logfmt`, `app.logfmt.seek.zst` and
//! `ssh://host/var/log/app.logfmt.seek.zst` all reach the same `scan`, which never learns where
//! the bytes came from.
//!
//! `--opts` under `logfmt` is `polars_logfmt::LogfmtScanOpts` as JSON, so every field may be left
//! out and an unknown key is an error. `cmd` — a command to run over ssh — is refused: opening is
//! the plugin's business, and an `ssh://` source is read over sftp by the `ssh` scan source.
//!
//! ```nu
//! polars_dyn open app.logfmt --opts {logfmt: {line_filter: "level=error", batch_size: 1000}}
//! polars_dyn open ssh://user@host/var/log/app.logfmt.seek.zst --opts {ssh: {port: 2222}}
//! ```

use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

use nu_plugin_polars::scan::{ReadAt, ReadAtCursor, ScanSource, parse_opts};
use polars::prelude::{LazyFrame, PolarsError, PolarsResult, polars_bail};
use polars_logfmt::lazy::{DEFAULT_INFER_SCHEMA_LENGTH, LazyLogFmtReaderBuilder};
use polars_logfmt::{LogfmtScanOpts, SeekableVfsFile, VfsFileStat};
use serde_json::Value;

/// The entry point `nu-polars-dyn-build` calls from the `main.rs` it generates.
pub fn scan_sources() -> &'static [&'static dyn ScanSource] {
    &[&Logfmt]
}

struct Logfmt;

impl ScanSource for Logfmt {
    fn name(&self) -> &'static str {
        "logfmt"
    }

    fn suffixes(&self) -> &'static [&'static str] {
        &[".logfmt"]
    }

    fn scan(&self, source: Arc<dyn ReadAt>, opts: &[u8]) -> PolarsResult<LazyFrame> {
        let opts: LogfmtScanOpts = serde_json::from_value(Value::Object(parse_opts(opts)?))
            .map_err(|e| PolarsError::ComputeError(format!("opts: {e}").into()))?;
        if opts.cmd.is_some() {
            polars_bail!(
                ComputeError:
                "`cmd` is not taken here: the plugin opens the source, and an ssh:// one is read over sftp"
            )
        }
        let mut builder = LazyLogFmtReaderBuilder::new()
            .batch_size(opts.batch_size)
            .n_threads(opts.n_threads)
            .aligned_cols_cnt(opts.aligned_cols_cnt)
            .infer_schema_length(
                opts.infer_schema_length
                    .unwrap_or(DEFAULT_INFER_SCHEMA_LENGTH),
            )
            .schema(opts.schema.clone())
            .from_seekable_vfs_file(Box::new(ReadAtFile::new(source)));
        if let Some(needle) = opts.line_filter.as_deref() {
            let finder = memchr::memmem::Finder::new(needle.as_bytes()).into_owned();
            builder = builder.line_filter(move |line: &str| finder.find(line.as_bytes()).is_some());
        }
        builder
            .build()
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
            .scan()
    }
}

/// A [`SeekableVfsFile`] over the bytes the plugin handed in: a cursor with a position of its own,
/// and a fresh cursor over the same bytes for every `clone_handle`.
struct ReadAtFile {
    source: Arc<dyn ReadAt>,
    cursor: ReadAtCursor,
}

impl ReadAtFile {
    fn new(source: Arc<dyn ReadAt>) -> Self {
        Self {
            cursor: ReadAtCursor::new(source.clone()),
            source,
        }
    }
}

impl Read for ReadAtFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl SeekableVfsFile for ReadAtFile {
    fn seek(&mut self, pos: u64) -> io::Result<u64> {
        self.cursor.seek(SeekFrom::Start(pos))
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }

    fn size(&mut self) -> io::Result<u64> {
        self.source.len()
    }

    fn stat(&mut self) -> io::Result<VfsFileStat> {
        Ok(VfsFileStat {
            size: self.source.len()?,
            is_seekable: true,
            mtime: None,
        })
    }

    fn clone_handle(&self) -> io::Result<Box<dyn SeekableVfsFile + Send>> {
        Ok(Box::new(ReadAtFile::new(self.source.clone())))
    }
}
