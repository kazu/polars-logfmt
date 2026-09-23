//! Benches for the single-threaded scan and the `aligned_cols_cnt` parallel
//! scan: the per-line column builders and how the workers stop once a
//! pushed-down `limit` is satisfied.
use criterion::{Criterion, criterion_group, criterion_main};
use polars::prelude::{col, lit};
use polars_logfmt::{LogfmtScanOpts, scan_logfmt};
use std::fs::File;
use std::path::PathBuf;

const LINES: usize = 200_000;

/// `level=error` on even `n`, compressed into 64 KiB seekable frames as
/// `<dir>/<name>.zst`. With `ragged`, every 10th line has no `n`.
fn write_fixture(dir: &tempfile::TempDir, name: &str, ragged: bool) -> PathBuf {
    let plain = dir.path().join(name);
    let mut data = String::with_capacity(LINES * 40);
    for n in 1..=LINES {
        let level = if n % 2 == 0 { "error" } else { "info" };
        if ragged && n % 10 == 0 {
            data.push_str(&format!("level={level} msg=m\n"));
        } else {
            data.push_str(&format!("level={level} msg=m n={n}\n"));
        }
    }
    std::fs::write(&plain, data).expect("write fixture");
    let zst = dir.path().join(format!("{name}.zst"));
    let mut input = File::open(&plain).expect("open fixture");
    let mut output = File::create(&zst).expect("create zst fixture");
    seekzstdsep::compress_to_seekable_zst(&mut input, &mut output, 65536, true, b"\n", None)
        .expect("compress fixture");
    zst
}

fn aligned() -> LogfmtScanOpts {
    LogfmtScanOpts {
        aligned_cols_cnt: true,
        ..Default::default()
    }
}

fn bench_aligned(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let zst = write_fixture(&dir, "big.logfmt", false);
    let ragged = write_fixture(&dir, "ragged.logfmt", true);
    let path = zst.to_str().expect("utf-8 path");
    let ragged_path = ragged.to_str().expect("utf-8 path");

    // the single-threaded path: schema probe, then one reader over the file
    c.bench_function("single_collect_zst", |b| {
        b.iter(|| {
            scan_logfmt(path, &LogfmtScanOpts::default())
                .expect("scan")
                .collect()
                .expect("collect")
        })
    });
    // the null fill sits in the per-line loop even when nothing is missing
    c.bench_function("aligned_collect_zst", |b| {
        b.iter(|| {
            scan_logfmt(path, &aligned())
                .expect("scan")
                .collect()
                .expect("collect")
        })
    });
    // the null fill actually runs on every 10th line
    c.bench_function("aligned_collect_ragged_zst", |b| {
        b.iter(|| {
            scan_logfmt(ragged_path, &aligned())
                .expect("scan")
                .collect()
                .expect("collect")
        })
    });
    // how fast the workers stop once the slice is satisfied
    c.bench_function("aligned_limit_1000_zst", |b| {
        b.iter(|| {
            scan_logfmt(path, &aligned())
                .expect("scan")
                .limit(1000)
                .collect()
                .expect("collect")
        })
    });
    // slice, then predicate
    c.bench_function("aligned_limit_1000_filter_zst", |b| {
        b.iter(|| {
            scan_logfmt(path, &aligned())
                .expect("scan")
                .limit(1000)
                .filter(col("level").eq(lit("error")))
                .collect()
                .expect("collect")
        })
    });
}

criterion_group!(benches, bench_aligned);
criterion_main!(benches);
