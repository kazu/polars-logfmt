mod common;

#[cfg(test)]
mod frames_from_seekable_tests {
    use polars_logfmt::SeekableVfsFile;
    use polars_logfmt::lazy::lazy_logfmt_reader::LazyLogFmtReader;
    use std::fs::{File, remove_file};
    use std::io::Write;

    // テスト用のSeekableVfsFileのモック
    use polars_logfmt::seekable_vfs::VfsFileStat;
    use std::io::Result as IoResult;
    struct MockSeekableFile {
        data: Vec<u8>,
        pos: u64,
        frames: Option<Vec<(u64, u64)>>,
    }
    impl MockSeekableFile {
        fn new(data: Vec<u8>, frames: Option<Vec<(u64, u64)>>) -> Self {
            Self {
                data,
                pos: 0,
                frames,
            }
        }
    }
    impl std::io::Read for MockSeekableFile {
        fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
            let end = std::cmp::min(self.pos as usize + buf.len(), self.data.len());
            let n = end.saturating_sub(self.pos as usize);
            buf[..n].copy_from_slice(&self.data[self.pos as usize..self.pos as usize + n]);
            self.pos += n as u64;
            Ok(n)
        }
    }
    impl SeekableVfsFile for MockSeekableFile {
        fn size(&mut self) -> IoResult<u64> {
            Ok(self.data.len() as u64)
        }
        fn stat(&mut self) -> IoResult<VfsFileStat> {
            Ok(VfsFileStat {
                size: self.data.len() as u64,
                is_seekable: true,
                mtime: None,
            })
        }
        fn seek(&mut self, pos: u64) -> std::io::Result<u64> {
            self.pos = pos;
            Ok(pos)
        }
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let end = std::cmp::min(self.pos as usize + buf.len(), self.data.len());
            let n = end.saturating_sub(self.pos as usize);
            buf[..n].copy_from_slice(&self.data[self.pos as usize..self.pos as usize + n]);
            self.pos += n as u64;
            Ok(n)
        }
        fn clone_handle(&self) -> std::io::Result<Box<dyn SeekableVfsFile + Send>> {
            Ok(Box::new(MockSeekableFile::new(
                self.data.clone(),
                self.frames.clone(),
            )))
        }
        fn seek_table_decomp_frames(&mut self) -> Option<Vec<(u64, u64)>> {
            self.frames.clone()
        }
    }

    macro_rules! param_test_frames_from_seekable {
        ($($name:ident: { data: $data:expr, frames: $frames:expr, expected: $expected:expr }),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    let mut file = MockSeekableFile::new($data.to_vec(), $frames);
                    let frames = LazyLogFmtReader::frames_from_seekable(&mut file).unwrap();
                    let got: Vec<(u64, u64)> = frames.iter().map(|f| (f.start, f.len)).collect();
                    assert_eq!(got, $expected);
                }
            )*
        }
    }

    param_test_frames_from_seekable! {
        local_plain_newline: {
            data: b"line1\nline2\nline3\nline4\n",
            frames: None,
            expected: vec![(0, 24)] // 1フレームのみ（MAX_FRAME_SIZE依存、調整可）
        },
        local_plain_no_newline: {
            data: b"line1\nline2\nline3\nline4",
            frames: None,
            expected: vec![(0, 23)]
        },
        local_zst_seektable: {
            data: b"abc\ndef\nghi\n",
            frames: Some(vec![(0, 4), (4, 4), (8, 4)]),
            expected: vec![(0, 4), (4, 4), (8, 4)]
        },
        ssh_plain_newline: {
            data: b"a\nb\nc\nd\n",
            frames: None,
            expected: vec![(0, 8)]
        },
        ssh_plain_no_newline: {
            data: b"a\nb\nc\nd",
            frames: None,
            expected: vec![(0, 7)]
        },
        ssh_zst_seektable: {
            data: b"x\ny\nz\n",
            frames: Some(vec![(0, 2), (2, 2), (4, 2)]),
            expected: vec![(0, 2), (2, 2), (4, 2)]
        }
    }

    // setup/teardown例（ファイル作成・削除）
    #[allow(dead_code)]
    pub fn setup_test_file(path: &str, data: &[u8]) {
        let mut f = File::create(path).unwrap();
        f.write_all(data).unwrap();
    }
    #[allow(dead_code)]
    pub fn teardown_test_file(path: &str) {
        if std::path::Path::new(path).exists() {
            remove_file(path).unwrap();
        }
    }
}
fn setup_plain_txt() -> String {
    // テスト用ダミー: plain.txtのパスを返す
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    base.join("tests/plain.txt").to_str().unwrap().to_string()
}
// LazyLogFmtReader/LogFmtSource関連テスト

#[test]
fn test_multithreaded_logfmtsource_clone_handle() {
    use std::io::{BufRead, BufReader};
    use std::thread;
    let local_path = setup_plain_txt();
    let file = polars_logfmt::ssh_vfs::local_file::LocalSeekableFile::open(&local_path)
        .expect("open plain file");
    let reader = polars_logfmt::lazy::LazyLogFmtReader::from_seekable_vfs_file(Box::new(file));
    let reader1 = reader.clone_with_fresh_handle().expect("fresh handle");
    let reader2 = reader.clone_with_fresh_handle().expect("fresh handle");
    let t1 = thread::spawn(move || {
        let r = reader1;
        let mut buf = String::new();
        let mut state = r.reader_state.lock().unwrap();
        if state.reader.is_none()
            && let polars_logfmt::lazy::LogFmtSource::Seekable(arc_mutex) = &r.source
        {
            let file_opt = arc_mutex.lock().unwrap();
            if let Some(file) = file_opt.as_ref() {
                let cloned_file = file.clone_handle().expect("clone_handle failed");
                let buf_reader: Box<dyn BufRead + Send> = Box::new(BufReader::new(cloned_file));
                state.reader = Some(buf_reader);
            }
        }
        if let Some(reader) = &mut state.reader {
            reader.read_line(&mut buf).unwrap();
        }
        buf
    });
    let t2 = thread::spawn(move || {
        let r = reader2;
        let mut buf = String::new();
        let mut state = r.reader_state.lock().unwrap();
        if state.reader.is_none() {
            if let polars_logfmt::lazy::LogFmtSource::Seekable(arc_mutex) = &r.source {
                let file_opt = arc_mutex.lock().unwrap();
                if let Some(file) = file_opt.as_ref() {
                    let mut cloned_file = file.clone_handle().expect("clone_handle failed");
                    cloned_file.seek(18).unwrap(); // 1行目の長さ分進める（仮）
                    let buf_reader: Box<dyn BufRead + Send> = Box::new(BufReader::new(cloned_file));
                    state.reader = Some(buf_reader);
                }
            }
        }
        if let Some(reader) = &mut state.reader {
            reader.read_line(&mut buf).unwrap();
        }
        buf
    });
    let b1 = t1.join().unwrap();
    let b2 = t2.join().unwrap();
    assert_ne!(b1, b2);
}

macro_rules! test_next_batch_n_rows {
    ($($name:ident: $n_rows:expr => $expected:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() -> anyhow::Result<()> {
                use polars_logfmt::lazy::LazyLogFmtReader;
                use polars::prelude::*;
                use std::io::Cursor;
                use std::sync::Arc;
                let data = b"a=1 b=2\na=3 b=4\na=5 b=6\n";
                let reader = LazyLogFmtReader::from_cursor(Cursor::new(data.to_vec()));
                let args = AnonymousScanArgs {
                    n_rows: Some($n_rows),
                    output_schema: None,
                    predicate: None,
                    schema: Arc::new(Schema::with_capacity(0)),
                    with_columns: None,
                };
                let df = reader.next_batch(args)?.expect("should return df");
                assert_eq!(df.height(), $expected);
                Ok(())
            }
        )*
    }
}

test_next_batch_n_rows! {
    batch_1: 1 => 1,
    batch_2: 2 => 2,
    batch_3: 3 => 3,
}

macro_rules! test_next_batch_predicate {
    ($($name:ident: $predicate:expr => $expected:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() -> anyhow::Result<()> {
                use polars_logfmt::lazy::LazyLogFmtReader;
                use polars::prelude::*;
                use std::io::Cursor;
                use std::sync::Arc;
                let data = b"a=1 b=2\na=3 b=4\na=5 b=6\n";
                let reader = LazyLogFmtReader::from_cursor(Cursor::new(data.to_vec()));
                let args = AnonymousScanArgs {
                    n_rows: None,
                    output_schema: None,
                    predicate: $predicate,
                    schema: Arc::new(Schema::with_capacity(0)),
                    with_columns: None,
                };
                let df = reader.next_batch(args)?.expect("should return df");
                assert_eq!(df.height(), $expected);
                Ok(())
            }
        )*
    }
}

test_next_batch_predicate! {
    predicate_none: None => 3,
    predicate_a_gt_2: Some(col("a").gt(lit(2))) => 2,
    predicate_b_eq_4: Some(col("b").eq(lit(4))) => 1,
}

macro_rules! test_next_batch_with_columns {
    ($($name:ident: $with_columns:expr => $expected_cols:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() -> anyhow::Result<()> {
                use polars_logfmt::lazy::LazyLogFmtReader;
                use polars::prelude::*;
                use std::io::Cursor;
                use std::sync::Arc;
                let data = b"a=1 b=2\na=3 b=4\na=5 b=6\n";
                let reader = LazyLogFmtReader::from_cursor(Cursor::new(data.to_vec()));
                let args = AnonymousScanArgs {
                    n_rows: None,
                    output_schema: None,
                    predicate: None,
                    schema: Arc::new(Schema::with_capacity(0)),
                    with_columns: $with_columns,
                };
                let df = reader.next_batch(args)?.expect("should return df");
                let cols: Vec<_> = df.columns().iter().map(|s| s.name()).collect();
                assert_eq!(cols, $expected_cols);
                Ok(())
            }
        )*
    }
}

test_next_batch_with_columns! {
    with_columns_a: Some(Arc::from([PlSmallStr::from("a")])) => vec!["a"],
    with_columns_b: Some(Arc::from([PlSmallStr::from("b")])) => vec!["b"],
    with_columns_ab: Some(Arc::from([PlSmallStr::from("a"), PlSmallStr::from("b")])) => vec!["a", "b"],
    with_columns_none: None => vec!["a", "b"],
}

// Parity test: compare next_batch output from Seekable (parallel path) and Cursor (sequential path)
#[test]
fn test_parallel_vs_sequential_parity() -> anyhow::Result<()> {
    use polars::prelude::*;
    use polars_logfmt::lazy::LazyLogFmtReader;
    use std::io::Cursor;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    let data = b"a=1 b=2\na=3 b=4\na=5 b=6\n";
    // create temp file for seekable path
    let mut tmp = NamedTempFile::new()?;
    tmp.write_all(data)?;
    let path = tmp.path().to_str().unwrap().to_string();

    // build seekable reader (SeekableVfsFile based)
    let file = polars_logfmt::ssh_vfs::local_file::LocalSeekableFile::open(&path)?;
    let seek_reader = LazyLogFmtReader::from_seekable_vfs_file(Box::new(file));

    // build cursor reader (sequential)
    let cursor_reader = LazyLogFmtReader::from_cursor(Cursor::new(data.to_vec()));

    let args1 = AnonymousScanArgs {
        n_rows: None,
        output_schema: None,
        predicate: None,
        schema: Arc::new(Schema::with_capacity(0)),
        with_columns: None,
    };
    let args2 = AnonymousScanArgs {
        n_rows: None,
        output_schema: None,
        predicate: None,
        schema: Arc::new(Schema::with_capacity(0)),
        with_columns: None,
    };

    let df_seek = seek_reader.next_batch(args1)?.expect("seek df");
    let df_cursor = cursor_reader.next_batch(args2)?.expect("cursor df");

    // Basic parity: same shape and identical cell values
    assert_eq!(df_seek.height(), df_cursor.height());
    assert_eq!(df_seek.columns().len(), df_cursor.columns().len());
    for col in df_seek.columns().iter().map(|s| s.name()) {
        let s_seek = df_seek.column(col).unwrap();
        let s_cur = df_cursor.column(col).unwrap();
        for r in 0..df_seek.height() {
            let v1 = s_seek.get(r).unwrap();
            let v2 = s_cur.get(r).unwrap();
            assert_eq!(v1, v2, "mismatch at col {} row {}", col, r);
        }
    }
    Ok(())
}

macro_rules! next_batch_schema {
    ($($name:ident: $schema:expr => $expected_types:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() -> anyhow::Result<()> {
                use polars_logfmt::lazy::LazyLogFmtReader;
                use polars::prelude::*;
                use std::io::Cursor;
                use std::sync::Arc;
                let data = b"a=1 b=2\na=3 b=4\na=5 b=6\n";
                let reader = LazyLogFmtReader::from_cursor(Cursor::new(data.to_vec()));
                let args = AnonymousScanArgs {
                    n_rows: None,
                    output_schema: None,
                    predicate: None,
                    schema: Arc::new($schema),
                    with_columns: None,
                };
                let df = reader.next_batch(args)?.expect("should return df");
                let types: Vec<_> = df.columns().iter().map(|s| s.dtype().clone()).collect();
                assert_eq!(types, $expected_types);
                Ok(())
            }
        )*
    }
}

next_batch_schema! {
    schema_int: Schema::from_iter(vec![Field::new("a".into(), DataType::Int64), Field::new("b".into(), DataType::Int64)]) => vec![DataType::Int64, DataType::Int64],
    schema_str: Schema::from_iter(vec![Field::new("a".into(), DataType::String), Field::new("b".into(), DataType::String)]) => vec![DataType::Int64, DataType::Int64],
}

#[allow(dead_code)]
fn setup_logger(loglevel: &str) {
    // Bridge `log` to `tracing` so legacy `log::` calls are captured
    let _ = tracing_log::LogTracer::init();
    // Env filter (RUST_LOG) or default to debug
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(loglevel));

    // Build a registry and attach the tracing-logfmt layer for logfmt output.
    use tracing::dispatcher::{self, Dispatch};
    use tracing_subscriber::layer::SubscriberExt;

    let subscriber = tracing_subscriber::Registry::default().with(filter).with(
        tracing_logfmt::builder()
            .with_timestamp(true)
            .with_level(true)
            .with_target(true)
            .with_location(true)
            .layer(),
    );

    // Set as global default dispatcher
    dispatcher::set_global_default(Dispatch::new(subscriber))
        .expect("Failed to set global tracing dispatcher");
}

use chrono::DateTime;
use polars::prelude::*;
use std::path::PathBuf;

/// Four `ProcessFinish` rows: three carry the filtered `msg`, one of those is a
/// `health_check` that the `cond` closure drops, so the expected height is 2.
const PROCESS_FINISH_DATA: &str = "\
time=2026-01-13T03:25:00+09:00 level=info msg=\"finish to process/write \" old=/data/in/a.cur new=/data/out/a.done writer_number=1 handler_number=1 old_file_bytes=10 bytes=20\n\
time=2026-01-13T03:25:01+09:00 level=info msg=\"finish to process/write \" old=/data/in/health_check.cur new=/data/out/health_check.done writer_number=1 handler_number=2 old_file_bytes=1 bytes=2\n\
time=2026-01-13T03:25:02+09:00 level=info msg=\"start to process \" old=/data/in/b.cur new=/data/out/b.done writer_number=2 handler_number=3 old_file_bytes=30 bytes=40\n\
time=2026-01-13T03:25:03+09:00 level=info msg=\"finish to process/write \" old=/data/in/b.cur new=/data/out/b.done writer_number=2 handler_number=3 old_file_bytes=30 bytes=40\n";

fn write_process_finish_plain(dir: &tempfile::TempDir) -> PathBuf {
    common::write_plain(dir, "app.log", PROCESS_FINISH_DATA.as_bytes())
}

fn write_process_finish_zst(dir: &tempfile::TempDir) -> PathBuf {
    let plain = write_process_finish_plain(dir);
    common::write_zst(dir, &plain, "app.log.seek.zst")
}

struct TestParam {
    src: fn(&tempfile::TempDir) -> PathBuf,
    filter: &'static str,
    cond: Box<dyn Fn(LazyFrame) -> LazyFrame + Send + Sync + 'static>,
    wanted: Box<dyn Fn(polars::frame::DataFrame) -> bool + Send + Sync + 'static>,
}

// DtaaFrame から変換したデータを格納する
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct FinishStat {
    time: DateTime<chrono::Utc>,
    level: String,
    msg: String,
    status: String,
    tid: i64,
    number: i64,
    size: i64,
    name: String,
    cnt: i64,
    lock_wait: i64,
    progress: i64,
    wait_for_start: i64,
    filename: String,
    r#type: String,
    elapsed: i64,
    error: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ProcessFinish {
    time: DateTime<chrono::Utc>,
    level: String,
    msg: String,
    old: String,
    new: String,
    writer_number: i64,
    handler_number: i64,
    old_file_bytes: i64,
    bytes: i64,
}

macro_rules! df_to_struct_convert {
    ($opt:expr, str) => {
        $opt.map(|s| s.to_string()).unwrap_or_default()
    };
    ($opt:expr, i64) => {
        $opt.unwrap_or_default()
    };

    ($opt:expr, datetime) => {
        $opt.map(|micros| {
            use chrono::{DateTime, Utc};
            let secs = micros / 1_000_000;
            let rem_micros = (micros % 1_000_000).abs() as u32;
            let nanos = rem_micros * 1000;
            DateTime::<Utc>::from_timestamp(secs, nanos).expect("invalid timestamp")
        })
        .unwrap_or_else(|| {
            DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("invalid timestamp")
        })
    };

    ($opt:expr, $t:ident) => {
        $opt.unwrap_or_default()
    };
}

macro_rules! df_to_struct_field_iter {
    ($df:expr, $field:ident, datetime) => {
        let mut $field = {
            let s = $df.column(stringify!($field))?;
            let casted = s.cast(&DataType::Int64)?;
            let ca = casted.i64()?;
            let v: Vec<Option<i64>> = ca.into_no_null_iter().map(Some).collect::<Vec<_>>();
            // handle nulls as well
            let v = if v.len() == ca.len() {
                v
            } else {
                ca.iter().collect()
            };
            v.into_iter()
        };
    };
    ($df:expr, $field:ident, $typ:tt) => {
        let mut $field = $df.column(stringify!($field))?.$typ()?.iter();
    };
}

macro_rules! df_to_struct {
    ($df:expr, $struct_name:ident, { $($field:tt: $type:tt),* $(,)? }) => {
        {
            let n_rows = $df.height();
            let mut out: Vec<$struct_name> = Vec::with_capacity(n_rows);

            // 各カラムのイテレータを動的に取得（`datetime` は内部的に i64 として扱う）
            $(
                df_to_struct_field_iter!($df, $field, $type);
            )*

            for _ in 0..n_rows {
                out.push($struct_name {
                    $(
                        // METNION: lod code, $field: $field.next().flatten().unwrap_or_default(),
                        $field: df_to_struct_convert!($field.next().flatten(), $type),
                    )*
                });
            }
            Ok::<Vec<$struct_name>, polars::prelude::PolarsError>(out)
        }
    };
}

#[allow(dead_code)]
fn df_to_finish_stat(df: polars::frame::DataFrame) -> anyhow::Result<Vec<FinishStat>> {
    let finish_stats: Vec<FinishStat> = df_to_struct!(df, FinishStat, {
        time: datetime,
        level: str,
        msg: str,
        status: str,
        tid: i64,
        number: i64,
        size: i64,
        name: str,
        cnt: i64,
        lock_wait: i64,
        progress: i64,
        wait_for_start: i64,
        filename: str,
        r#type: str,
        elapsed: i64,
        error: str,
    })?;
    Ok(finish_stats)
}

fn df_to_process_finish(df: polars::frame::DataFrame) -> anyhow::Result<Vec<ProcessFinish>> {
    let process_finishes: Vec<ProcessFinish> = df_to_struct!(df, ProcessFinish, {
        time: datetime,
        level: str,
        msg: str,
        old: str,
        new: str,
        writer_number: i64,
        handler_number: i64,
        old_file_bytes: i64,
        bytes: i64,
    })?;
    Ok(process_finishes)
}

macro_rules! log_fmt_lazy {
    ($($name:ident: $param:expr),* $(,)?) => {
        $(

            #[test]
            #[allow(unused_must_use)]
            fn $name() -> anyhow::Result<()> {
                use polars::prelude::*;
                use polars_logfmt::lazy::lazy_logfmt_reader::*;


                let t: TestParam = $param;
                let str = t.filter.to_string();
                let dir = tempfile::tempdir()?;
                let src = (t.src)(&dir).to_str().expect("utf-8 path").to_string();
                let cond = t.cond;
                let f = LazyLogFmtReaderBuilder::new()
                    .source(Some(src.clone()))
                    .aligned_cols_cnt(true)
                    .line_filter(move |line: &str| line.contains(&str))
                    .build()?
                    .scan()?;

                let ldf = cond(f)
                        // .filter(col("old").str().contains_literal(lit("health_check")).not())
                        // .filter(col("elapsed").str().contains_literal(lit("1.625670501s")))
                        .sort(["time"], Default::default())
                        .with_columns([col("time").alias("left_time")])
                        .with_streaming(true);

                println!("{}", ldf.explain(true)?);

                let df_with_r = ldf.with_streaming(true).collect();
                assert!(df_with_r.is_ok());

                let df = df_with_r.unwrap();
                dbg!(df.clone());

                let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
                dbg!("columns:", &columns);

                println!("DBG: src={} df.height={}", src, df.height());


                if t.filter.clone().to_string().contains("msg=\"finish to process/write \"") {

                    let p_finished_result = df_to_process_finish(df.clone());
                    if p_finished_result.is_err() {
                        dbg!(p_finished_result.as_ref());
                    }
                    assert!(p_finished_result.is_ok(), "df_to_process_finish failed");
                    let p_finished = p_finished_result.unwrap();
                    dbg!("finish_stats first ", &p_finished[0]);

                    let len = p_finished.len();

                    dbg!("finish_stats last ", &p_finished[len-1]);
                }
                let _a = df.tail(Some(1)).get_row(0);
                println!("df:{}", df);
                // 44560

                let ok = (t.wanted)(df);
                assert!(ok, "predicate failed for src={}", src);

                Ok(())
            }
        )*
    };
}

log_fmt_lazy! {
    local_plain: TestParam {
        src: write_process_finish_plain,
        filter: "msg=\"finish to process/write \"",
        cond: Box::new(|lf: LazyFrame| lf.filter(col("old").str().contains_literal(lit("health_check")).not())),
        wanted: Box::new(|df: polars::frame::DataFrame| df.height() == 2),
    },
    local_zst: TestParam {
        src: write_process_finish_zst,
        cond: Box::new(|lf: LazyFrame| lf.filter(col("old").str().contains_literal(lit("health_check")).not())),
        filter: "msg=\"finish to process/write \"",
        wanted: Box::new(|df: polars::frame::DataFrame| df.height() == 2),
    }
}
