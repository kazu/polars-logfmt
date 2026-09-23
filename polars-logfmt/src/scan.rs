//! Single entry point for embedding: open a logfmt source as a [`LazyFrame`].
//!
//! The caller passes the source string untouched and an options struct that
//! can be deserialized from JSON, so a host (for example a nushell plugin)
//! needs no knowledge of the reader builder.

use crate::lazy::LazyLogFmtReaderBuilder;
use crate::logfmt::Schema;
use crate::ssh::SshSource;
use polars::prelude::{LazyFrame, PolarsError, PolarsResult};
use serde::Deserialize;

/// Options for [`scan_logfmt`]. Every field is optional; a JSON object may
/// name any subset of them. An unknown field is a deserialization error.
///
/// ```
/// use polars_logfmt::LogfmtScanOpts;
///
/// let opts: LogfmtScanOpts =
///     serde_json::from_str(r#"{"line_filter": "level=error", "batch_size": 1000}"#)?;
/// assert_eq!(opts.line_filter.as_deref(), Some("level=error"));
/// assert_eq!(opts.batch_size, Some(1000));
/// assert!(!opts.aligned_cols_cnt);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogfmtScanOpts {
    /// Keep only lines containing this substring (byte match, no regex).
    pub line_filter: Option<String>,
    /// Column types to force. Keys are logfmt keys, values are
    /// `"string"`, `"integer"`, `"float"`, `"boolean"`, `"datetime"`,
    /// `"duration"` or `"auto"`. Columns not listed are inferred.
    pub schema: Option<Schema>,
    /// Rows per batch when the scan is pulled in batches.
    pub batch_size: Option<usize>,
    /// Worker threads for the parallel frame scan, at most 8. Defaults to the
    /// polars pool size. The parallel scan only yields rows when
    /// `aligned_cols_cnt` is set; otherwise the file is read single-threaded.
    pub n_threads: Option<usize>,
    /// Use the key set of the first accepted line as the column list and read
    /// the file frame-parallel. A key missing from a later line is null there;
    /// a key the first line did not have is dropped.
    pub aligned_cols_cnt: bool,
    /// Remote command for an `ssh://` source, for example `cat /var/log/app.log`.
    /// When set, the file is streamed through an ssh channel running this
    /// command. When unset, an `ssh://` source is opened over SFTP with
    /// ssh-agent authentication. Setting it for a local path is an error.
    pub cmd: Option<String>,
}

/// Open `source` as a lazy logfmt scan.
///
/// `source` is a local path or an `ssh://user@host/path` URL. A path ending in
/// `.zst` is read as a seekable zstd file. Nothing is read until the returned
/// [`LazyFrame`] is collected, except the first line when no schema is given.
///
/// ```
/// use polars_logfmt::{LogfmtScanOpts, scan_logfmt};
/// # let dir = tempfile::tempdir()?;
/// # let path = dir.path().join("app.logfmt");
/// # std::fs::write(&path, "level=info msg=start n=1\nlevel=error msg=boom n=2\n")?;
/// # let path = path.to_str().expect("utf-8 path");
///
/// let df = scan_logfmt(path, &LogfmtScanOpts::default())?.collect()?;
/// assert_eq!(df.height(), 2);
///
/// let opts = LogfmtScanOpts {
///     line_filter: Some("level=error".into()),
///     ..Default::default()
/// };
/// let df = scan_logfmt(path, &opts)?.collect()?;
/// assert_eq!(df.height(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn scan_logfmt(source: &str, opts: &LogfmtScanOpts) -> PolarsResult<LazyFrame> {
    let mut builder = LazyLogFmtReaderBuilder::new()
        .batch_size(opts.batch_size)
        .n_threads(opts.n_threads)
        .aligned_cols_cnt(opts.aligned_cols_cnt)
        .schema(opts.schema.clone());

    if let Some(needle) = opts.line_filter.as_deref() {
        let finder = memchr::memmem::Finder::new(needle.as_bytes()).into_owned();
        builder = builder.line_filter(move |line: &str| finder.find(line.as_bytes()).is_some());
    }

    builder = match (source.starts_with("ssh://"), opts.cmd.as_ref()) {
        (true, Some(cmd)) => builder
            .from_ssh_source(SshSource::try_new(source).map_err(to_compute_err)?)
            .cmd(Some(cmd.clone())),
        (false, Some(_)) => {
            polars::error::polars_bail!(ComputeError: "cmd is only valid for an ssh:// source: {source}")
        }
        _ => builder.source(Some(source.to_string())),
    };

    builder.build().map_err(to_compute_err)?.scan()
}

fn to_compute_err(e: anyhow::Error) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}
