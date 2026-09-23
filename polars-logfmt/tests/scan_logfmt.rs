// Integration tests for the `scan_logfmt` entry point.
mod common;

use common::as_str;
use polars::prelude::{DataType, col, len, lit};
use polars_logfmt::logfmt::SchemaField;
use polars_logfmt::{LogfmtScanOpts, scan_logfmt};
use std::path::PathBuf;

const DATA: &str = "level=info msg=start n=1 ts=2026-01-22T12:00:00+09:00\n\
level=error msg=boom n=2 ts=2026-01-22T12:00:01+09:00\n\
level=info msg=done n=3 ts=2026-01-22T12:00:02+09:00\n";

fn write_plain(dir: &tempfile::TempDir) -> PathBuf {
    common::write_plain(dir, "app.logfmt", DATA.as_bytes())
}

fn write_zst(dir: &tempfile::TempDir) -> PathBuf {
    let plain = write_plain(dir);
    common::write_zst(dir, &plain, "app.logfmt.zst")
}

#[test]
fn plain_file_default_opts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_plain(&dir);
    let df = scan_logfmt(as_str(&path), &LogfmtScanOpts::default())
        .expect("scan")
        .collect()
        .expect("collect");
    assert_eq!(df.height(), 3);
    assert_eq!(df.column("n").expect("n").dtype(), &DataType::Int64);
}

#[test]
fn zst_file_default_opts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_zst(&dir);
    let df = scan_logfmt(as_str(&path), &LogfmtScanOpts::default())
        .expect("scan")
        .collect()
        .expect("collect");
    assert_eq!(df.height(), 3);
}

#[test]
fn line_filter_is_substring_match() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_plain(&dir);
    let opts = LogfmtScanOpts {
        line_filter: Some("level=error".into()),
        ..Default::default()
    };
    let df = scan_logfmt(as_str(&path), &opts)
        .expect("scan")
        .collect()
        .expect("collect");
    assert_eq!(df.height(), 1);
    let msg = df.column("msg").expect("msg").str().expect("str");
    assert_eq!(msg.get(0), Some("boom"));
}

#[test]
fn opts_from_json_with_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_plain(&dir);
    let opts: LogfmtScanOpts =
        serde_json::from_str(r#"{"schema": {"n": "string", "ts": "datetime"}, "n_threads": 1}"#)
            .expect("deserialize opts");
    assert_eq!(
        opts.schema.as_ref().and_then(|s| s.get("ts")),
        Some(&SchemaField::DateTime)
    );
    let df = scan_logfmt(as_str(&path), &opts)
        .expect("scan")
        .collect()
        .expect("collect");
    assert_eq!(df.column("n").expect("n").dtype(), &DataType::String);
    assert!(matches!(
        df.column("ts").expect("ts").dtype(),
        DataType::Datetime(_, _)
    ));
}

#[test]
fn filter_and_select_with_default_opts() {
    let dir = tempfile::tempdir().expect("tempdir");
    for path in [write_plain(&dir), write_zst(&dir)] {
        let lf = scan_logfmt(as_str(&path), &LogfmtScanOpts::default()).expect("scan");
        let df = lf
            .clone()
            .filter(col("n").gt(lit(1)))
            .collect()
            .expect("filter collect");
        assert_eq!(df.height(), 2, "{}", path.display());

        let df = lf
            .clone()
            .select([col("msg")])
            .collect()
            .expect("select collect");
        assert_eq!(df.get_column_names(), ["msg"], "{}", path.display());

        let df = lf.select([len()]).collect().expect("len collect");
        assert_eq!(
            df.column("len").expect("len").u32().expect("u32").get(0),
            Some(3),
            "{}",
            path.display()
        );
    }
}

#[test]
fn limit_is_honoured() {
    let dir = tempfile::tempdir().expect("tempdir");
    for path in [write_plain(&dir), write_zst(&dir)] {
        let df = scan_logfmt(as_str(&path), &LogfmtScanOpts::default())
            .expect("scan")
            .limit(2)
            .collect()
            .expect("collect");
        assert_eq!(df.height(), 2, "{}", path.display());
    }
}

/// 20 000 lines, `level=error` on every even `n`, larger than one seekable frame.
fn write_big(dir: &tempfile::TempDir) -> PathBuf {
    let path = dir.path().join("big.logfmt");
    let mut data = String::with_capacity(20_000 * 40);
    for n in 1..=20_000 {
        let level = if n % 2 == 0 { "error" } else { "info" };
        data.push_str(&format!("level={level} msg=m n={n}\n"));
    }
    std::fs::write(&path, data).expect("write big fixture");
    path
}

#[test]
fn limit_then_filter_takes_the_first_n_raw_rows() {
    // `limit(3).filter(..)` pushes both into the scan: the slice applies to the
    // raw rows, the predicate to those three. Only n=2 is an error row there.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_big(&dir);
    let df = scan_logfmt(as_str(&path), &LogfmtScanOpts::default())
        .expect("scan")
        .limit(3)
        .filter(col("level").eq(lit("error")))
        .collect()
        .expect("collect");
    let n: Vec<Option<i64>> = df
        .column("n")
        .expect("n")
        .i64()
        .expect("i64")
        .iter()
        .collect();
    assert_eq!(n, [Some(2)]);

    // The other order keeps the slice above the scan: first three matches.
    let df = scan_logfmt(as_str(&path), &LogfmtScanOpts::default())
        .expect("scan")
        .filter(col("level").eq(lit("error")))
        .limit(3)
        .collect()
        .expect("collect");
    let n: Vec<Option<i64>> = df
        .column("n")
        .expect("n")
        .i64()
        .expect("i64")
        .iter()
        .collect();
    assert_eq!(n, [Some(2), Some(4), Some(6)]);
}

/// `write_big` compressed into several 64 KiB seekable frames.
fn write_big_zst(dir: &tempfile::TempDir) -> PathBuf {
    let plain = write_big(dir);
    common::write_zst(dir, &plain, "big.logfmt.zst")
}

fn aligned() -> LogfmtScanOpts {
    LogfmtScanOpts {
        aligned_cols_cnt: true,
        ..Default::default()
    }
}

fn n_values(df: &polars::prelude::DataFrame) -> Vec<Option<i64>> {
    df.column("n")
        .expect("n")
        .i64()
        .expect("i64")
        .iter()
        .collect()
}

#[test]
fn aligned_batch_size_does_not_cap_collect() {
    let dir = tempfile::tempdir().expect("tempdir");
    for path in [write_big(&dir), write_big_zst(&dir)] {
        let opts = LogfmtScanOpts {
            batch_size: Some(300),
            ..aligned()
        };
        let df = scan_logfmt(as_str(&path), &opts)
            .expect("scan")
            .collect()
            .expect("collect");
        assert_eq!(df.height(), 20_000, "{}", path.display());
    }
}

#[test]
fn aligned_limit_returns_n_distinct_rows() {
    // The parallel scan promises `n` raw rows of the file, not its first `n`:
    // the workers claim rows from a shared pool and all stop once it is empty.
    let dir = tempfile::tempdir().expect("tempdir");
    for path in [write_big(&dir), write_big_zst(&dir)] {
        let df = scan_logfmt(as_str(&path), &aligned())
            .expect("scan")
            .limit(2)
            .collect()
            .expect("collect");
        let n = n_values(&df);
        assert_eq!(n.len(), 2, "{}", path.display());
        assert_ne!(n[0], n[1], "{}", path.display());
        for v in n {
            let v = v.expect("n is never null");
            assert!((1..=20_000).contains(&v), "{}: n={v}", path.display());
        }
    }
}

#[test]
fn aligned_limit_then_filter_filters_the_n_raw_rows() {
    // `limit(3).filter(..)` pushes both into the scan: three raw rows are
    // claimed, then the predicate runs on them, so at most three error rows.
    let dir = tempfile::tempdir().expect("tempdir");
    for path in [write_big(&dir), write_big_zst(&dir)] {
        let df = scan_logfmt(as_str(&path), &aligned())
            .expect("scan")
            .limit(3)
            .filter(col("level").eq(lit("error")))
            .collect()
            .expect("collect");
        let n = n_values(&df);
        assert!(n.len() <= 3, "{}: {n:?}", path.display());
        assert!(
            n.iter().all(|v| v.expect("n is never null") % 2 == 0),
            "{}: {n:?}",
            path.display()
        );

        let df = scan_logfmt(as_str(&path), &aligned())
            .expect("scan")
            .filter(col("level").eq(lit("error")))
            .limit(3)
            .collect()
            .expect("collect");
        assert_eq!(
            n_values(&df),
            [Some(2), Some(4), Some(6)],
            "{}",
            path.display()
        );
    }
}

#[test]
fn aligned_ragged_row_gets_null() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = common::write_plain(
        &dir,
        "ragged.logfmt",
        b"level=info msg=start n=1\nlevel=error msg=boom\nlevel=info msg=done n=3\n",
    );
    let df = scan_logfmt(as_str(&path), &aligned())
        .expect("scan")
        .collect()
        .expect("collect");
    assert_eq!(n_values(&df), [Some(1), None, Some(3)]);
}

#[test]
fn aligned_datetime_column_is_datetime() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_zst(&dir);
    let opts: LogfmtScanOpts =
        serde_json::from_str(r#"{"aligned_cols_cnt": true, "schema": {"ts": "datetime"}}"#)
            .expect("deserialize opts");
    let df = scan_logfmt(as_str(&path), &opts)
        .expect("scan")
        .collect()
        .expect("collect");
    assert_eq!(
        df.column("ts").expect("ts").dtype(),
        &DataType::Datetime(polars::prelude::TimeUnit::Microseconds, None)
    );
}

/// `n` is Integer on the first line, Float on the third; `level` is String
/// throughout; `extra` first appears on the third line.
const MIXED: &str = "level=info n=1\nlevel=error n=2\nlevel=info n=1.5 extra=x\n";

fn write_mixed(dir: &tempfile::TempDir) -> PathBuf {
    common::write_plain(dir, "mixed.logfmt", MIXED.as_bytes())
}

#[test]
fn default_infer_schema_length_uses_the_first_line_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_mixed(&dir);
    let mut lf = scan_logfmt(as_str(&path), &LogfmtScanOpts::default()).expect("scan");
    let schema = lf.collect_schema().expect("schema");
    assert_eq!(schema.get("n"), Some(&DataType::Int64));
    assert!(schema.get("extra").is_none());
}

#[test]
fn infer_schema_length_widens_types_and_adds_late_keys() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_mixed(&dir);
    let opts: LogfmtScanOpts =
        serde_json::from_str(r#"{"infer_schema_length": 3}"#).expect("deserialize opts");
    let mut lf = scan_logfmt(as_str(&path), &opts).expect("scan");
    let schema = lf.collect_schema().expect("schema");
    assert_eq!(schema.get("n"), Some(&DataType::Float64));
    assert_eq!(schema.get("level"), Some(&DataType::String));
    assert_eq!(schema.get("extra"), Some(&DataType::String));
    let df = lf.collect().expect("collect");
    assert_eq!(df.height(), 3);
    let n = df.column("n").expect("n").f64().expect("f64");
    assert_eq!(n.get(2), Some(1.5));
}

#[test]
fn infer_schema_length_counts_lines_after_the_line_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_mixed(&dir);
    // Two accepted lines: `n=1` and `n=1.5`. The rejected `level=error` line
    // does not count, so 2 reaches the Float line.
    let opts = LogfmtScanOpts {
        line_filter: Some("level=info".into()),
        infer_schema_length: Some(2),
        ..Default::default()
    };
    let mut lf = scan_logfmt(as_str(&path), &opts).expect("scan");
    let schema = lf.collect_schema().expect("schema");
    assert_eq!(schema.get("n"), Some(&DataType::Float64));
}

#[test]
fn polars_infer_schema_length_wins_over_the_reader_value() {
    use polars::prelude::AnonymousScan;
    use polars_logfmt::lazy::LazyLogFmtReaderBuilder;
    let reader = LazyLogFmtReaderBuilder::new()
        .from_cursor(std::io::Cursor::new(MIXED.as_bytes().to_vec()))
        .infer_schema_length(1)
        .build()
        .expect("build");
    let schema = AnonymousScan::schema(&reader, Some(3)).expect("schema");
    assert_eq!(schema.get("n"), Some(&DataType::Float64));
    let schema = AnonymousScan::schema(&reader, None).expect("schema");
    assert_eq!(schema.get("n"), Some(&DataType::Int64));
}

/// The probe stream left in `reader_state` feeds the first collect and only
/// that one; the next collect reads the source again.
#[test]
fn first_collect_continues_from_the_stashed_probe_stream() {
    use polars_logfmt::lazy::LazyLogFmtReaderBuilder;
    let source = std::io::Cursor::new(b"n=1\nn=2\n".to_vec());
    let stashed: polars_logfmt::lazy::LineReader =
        Box::new(std::io::Cursor::new(b"n=10\nn=20\nn=30\n".to_vec()));
    let reader = LazyLogFmtReaderBuilder::new()
        .from_cursor(source)
        .schema(Some([("n".to_string(), SchemaField::Integer)].into()))
        .build()
        .expect("build");
    reader.reader_state.lock().expect("lock").reader = Some(stashed);
    let lf = reader.scan_logfmt().expect("scan");
    let first = lf.clone().collect().expect("first collect");
    assert_eq!(n_values(&first), [Some(10), Some(20), Some(30)]);
    let second = lf.collect().expect("second collect");
    assert_eq!(n_values(&second), [Some(1), Some(2)]);
}

/// Runs only with `--ignored` and `POLARS_LOGFMT_TEST_SSH=ssh://user@host[:port]/dir`.
/// The command prints `MIXED`, so no remote file is needed. All three lines
/// were consumed by the probe and must still come out of the scan.
#[test]
#[ignore = "needs POLARS_LOGFMT_TEST_SSH"]
fn ssh_cmd_scan_continues_on_the_probe_connection() {
    let base = std::env::var("POLARS_LOGFMT_TEST_SSH")
        .expect("set POLARS_LOGFMT_TEST_SSH=ssh://user@host/dir");
    let opts = LogfmtScanOpts {
        cmd: Some(format!("printf '{}'", MIXED.replace('\n', "\\n"))),
        infer_schema_length: Some(3),
        ..Default::default()
    };
    let df = scan_logfmt(&base, &opts)
        .expect("scan")
        .collect()
        .expect("collect");
    assert_eq!(df.height(), 3);
    let n = df.column("n").expect("n").f64().expect("f64");
    assert_eq!(n.get(2), Some(1.5));
}

#[test]
fn infer_schema_length_zero_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_mixed(&dir);
    let opts = LogfmtScanOpts {
        infer_schema_length: Some(0),
        ..Default::default()
    };
    let err = scan_logfmt(as_str(&path), &opts)
        .err()
        .expect("scan should fail");
    assert!(err.to_string().contains("no logfmt line"), "{err}");
}

#[test]
fn unknown_json_field_is_an_error() {
    let err = serde_json::from_str::<LogfmtScanOpts>(r#"{"line_fliter": "n=1"}"#)
        .expect_err("typo must be rejected");
    assert!(err.to_string().contains("line_fliter"), "{err}");
}

#[test]
fn cmd_with_local_path_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_plain(&dir);
    let opts = LogfmtScanOpts {
        cmd: Some("cat /x".into()),
        ..Default::default()
    };
    let err = scan_logfmt(as_str(&path), &opts)
        .err()
        .expect("cmd on a local path must fail");
    assert!(err.to_string().contains("ssh://"), "{err}");
}

#[test]
fn empty_file_without_schema_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.logfmt");
    std::fs::write(&path, "").expect("write empty fixture");
    let err = scan_logfmt(as_str(&path), &LogfmtScanOpts::default())
        .err()
        .expect("scan should fail");
    assert!(err.to_string().contains("no logfmt line"), "{err}");
}

fn full_schema() -> LogfmtScanOpts {
    LogfmtScanOpts {
        schema: Some(
            [
                ("level".to_string(), SchemaField::String),
                ("n".to_string(), SchemaField::Integer),
            ]
            .into(),
        ),
        ..Default::default()
    }
}

#[test]
fn empty_file_with_a_full_schema_reads_nothing_and_collects_no_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.logfmt");
    std::fs::write(&path, "").expect("write empty fixture");
    let df = scan_logfmt(as_str(&path), &full_schema())
        .expect("scan must not probe")
        .collect()
        .expect("collect");
    assert_eq!(df.height(), 0);
}

#[test]
fn ssh_cmd_with_a_full_schema_connects_only_at_collect() {
    let opts = LogfmtScanOpts {
        cmd: Some("cat /var/log/app.logfmt".into()),
        ..full_schema()
    };
    let lf = scan_logfmt("ssh://nobody@127.0.0.1:1/var/log/app.logfmt", &opts)
        .expect("scan must not connect");
    let err = lf.collect().expect_err("collect should fail");
    let msg = err.to_string();
    assert!(msg.contains("connect") || msg.contains("refused"), "{msg}");
}

#[test]
fn missing_file_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("missing.logfmt");
    let err = scan_logfmt(as_str(&path), &LogfmtScanOpts::default())
        .err()
        .expect("scan should fail");
    assert!(err.to_string().contains("missing.logfmt"), "{err}");
}

#[test]
fn ssh_url_to_closed_port_is_an_error_not_a_panic() {
    // Port 1 is closed, so the connection attempt fails; the failure must come
    // back as a PolarsError rather than a panic.
    let err = scan_logfmt(
        "ssh://nobody@127.0.0.1:1/var/log/app.logfmt",
        &LogfmtScanOpts::default(),
    )
    .err()
    .expect("scan should fail");
    assert!(err.to_string().contains("connect"), "{err}");
}

#[test]
fn ssh_url_with_cmd_reaches_the_connection_attempt() {
    // Schema inference for the ssh command route reads the first line over the
    // channel, so the error must be the connection failure, not a missing schema.
    let opts = LogfmtScanOpts {
        cmd: Some("cat /var/log/app.logfmt".into()),
        ..Default::default()
    };
    let err = scan_logfmt("ssh://nobody@127.0.0.1:1/var/log/app.logfmt", &opts)
        .err()
        .expect("scan should fail");
    let msg = err.to_string();
    assert!(!msg.contains("schema"), "{msg}");
    assert!(msg.contains("connect") || msg.contains("refused"), "{msg}");
}
