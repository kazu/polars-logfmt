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
