//! Text read by unit (`from_units`) gives the frame the same text gives read
//! from a seekable file, under each option that changes how it is read.

mod common;

use std::sync::Arc;

use polars::prelude::*;
use polars_logfmt::UnitSource;
use polars_logfmt::lazy::LazyLogFmtReaderBuilder;
use polars_logfmt::ssh_vfs::local_file::LocalSeekableFile;

/// Units of whole lines. The first line of every unit has every key, so the
/// parallel scan agrees on the columns whether it cuts by unit or by bytes; a
/// later line misses keys, adds one, and changes the type of `n`.
const UNITS: &[&str] = &[
    "level=info n=1 msg=start\nlevel=warn n=2\n",
    "",
    "level=info n=3 msg=a\nlevel=error n=x msg=b extra=1\n\n",
    "level=info n=5 msg=c\nlevel=warn n=6 msg=d\nlevel=info msg=e\n",
];

struct Units {
    units: Vec<Vec<u8>>,
    known: bool,
}

impl UnitSource for Units {
    fn read_unit(&self, index: usize, dst: &mut Vec<u8>) -> std::io::Result<bool> {
        match self.units.get(index) {
            Some(unit) => {
                dst.extend_from_slice(unit);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn count_units(&self) -> Option<usize> {
        self.known.then_some(self.units.len())
    }
}

type Options = fn(LazyLogFmtReaderBuilder) -> LazyLogFmtReaderBuilder;

/// The frame, or the error it fails with as text.
fn collect(
    builder: LazyLogFmtReaderBuilder,
    query: fn(LazyFrame) -> LazyFrame,
) -> Result<DataFrame, String> {
    let lf = builder.build().expect("build").scan().expect("scan");
    query(lf).collect().map_err(|e| e.to_string())
}

fn assert_same(options: Options, query: fn(LazyFrame) -> LazyFrame) {
    assert_same_for(options, query, &[true, false]);
}

/// With `known` false the units are read in order, as a cursor is, which
/// `aligned_cols_cnt` reads differently from a file.
fn assert_same_for(options: Options, query: fn(LazyFrame) -> LazyFrame, known: &[bool]) {
    let dir = tempfile::tempdir().expect("tempdir");
    let text = UNITS.concat();
    let path = common::write_plain(&dir, "plain.logfmt", text.as_bytes());
    let file = LocalSeekableFile::open(common::as_str(&path)).expect("open");
    let plain = collect(
        options(LazyLogFmtReaderBuilder::new()).from_seekable_vfs_file(Box::new(file)),
        query,
    );
    for &known in known {
        let units = Arc::new(Units {
            units: UNITS.iter().map(|u| u.as_bytes().to_vec()).collect(),
            known,
        });
        let by_unit = collect(
            options(LazyLogFmtReaderBuilder::new()).from_units(units),
            query,
        );
        let same = match (&plain, &by_unit) {
            (Ok(a), Ok(b)) => a.equals_missing(b),
            (a, b) => a == b,
        };
        assert!(
            same,
            "count known: {known}\nplain:\n{plain:?}\nby unit:\n{by_unit:?}"
        );
    }
}

fn all(lf: LazyFrame) -> LazyFrame {
    lf
}

fn head(lf: LazyFrame) -> LazyFrame {
    lf.limit(3)
}

fn filtered(lf: LazyFrame) -> LazyFrame {
    lf.filter(col("level").eq(lit("info"))).select([col("msg")])
}

#[test]
fn default_options() {
    assert_same(|b| b, all);
    assert_same(|b| b, head);
    assert_same(|b| b, filtered);
}

#[test]
fn aligned_columns() {
    assert_same_for(|b| b.aligned_cols_cnt(true), all, &[true]);
    assert_same_for(|b| b.aligned_cols_cnt(true), filtered, &[true]);
}

#[test]
fn line_filter_and_longer_inference() {
    assert_same(|b| b.line_filter(|l: &str| !l.contains("warn")), all);
    assert_same(|b| b.infer_schema_length(10), all);
    assert_same(|b| b.infer_schema_length(10), head);
}

#[test]
fn batches() {
    assert_same(|b| b.batch_size(Some(2)), all);
}
