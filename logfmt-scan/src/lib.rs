//! The `logfmt` scan source for `nu_plugin_polars_dyn`.
//!
//! Build a plugin with it compiled in:
//!
//! ```nu
//! nu-polars-dyn-build logfmt_scan --path logfmt_scan=../polars-logfmt/logfmt-scan
//! ```
//!
//! `--opts` is `polars_logfmt::LogfmtScanOpts` as JSON, so every field may be left out and an
//! unknown key is an error:
//!
//! ```nu
//! polars_dyn open app.logfmt --opts {line_filter: "level=error", batch_size: 1000}
//! polars_dyn open ssh://user@host/var/log/app.logfmt --opts {cmd: "cat /var/log/app.logfmt"}
//! ```

use nu_plugin_polars::scan::{ScanSource, parse_opts};
use polars::prelude::{LazyFrame, PolarsError, PolarsResult};
use polars_logfmt::{LogfmtScanOpts, scan_logfmt};
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
        &[".logfmt", ".logfmt.seek.zst"]
    }

    fn scan(&self, source: &str, opts: &[u8]) -> PolarsResult<LazyFrame> {
        let opts: LogfmtScanOpts = serde_json::from_value(Value::Object(parse_opts(opts)?))
            .map_err(|e| PolarsError::ComputeError(format!("opts: {e}").into()))?;
        scan_logfmt(source, &opts)
    }
}
