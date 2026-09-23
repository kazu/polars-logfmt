pub mod lazy_logfmt_reader;
mod logfmt_reader_state;
use crate::ssh::SshStream;
pub use lazy_logfmt_reader::DEFAULT_INFER_SCHEMA_LENGTH;
pub use lazy_logfmt_reader::LazyFrameFn;
pub use lazy_logfmt_reader::LazyLogFmtReader;
pub use lazy_logfmt_reader::LazyLogFmtReaderBuilder;
pub use logfmt_reader_state::LogFmtReaderState;

use crate::logfmt::{Row, Schema, SchemaField};

use crate::ssh::SshSource;
use anyhow::Result;
use chrono::DateTime;
// use polars::prelude::TimeUnit; // 未使用
use polars::prelude::DataFrame;
// use zstd::stream::Decoder; // only used in ssh_reader

// use std::collections::BTreeSet; // 未使用
use std::io::{BufRead, Cursor, Read};
use std::sync::Arc;

/// The stream a scan reads lines from.
pub type LineReader = Box<dyn BufRead + Send>;
type SshReaderFn = fn(SshStream, bool) -> anyhow::Result<(SshStream, LineReader)>;
/// Infer the schema from the first `infer_schema_length` lines of `source`
/// that pass `line_filter`.
///
/// For an ssh command source the second value is the probe stream, rewound to
/// its first byte, so the scan can continue on the same connection instead of
/// opening a second one. Cursor and seekable sources reopen by handle, so
/// nothing is returned for them.
fn infer_schema_from_source(
    source: &LogFmtSource,
    cmd: &Option<String>,
    line_filter: Option<LineFilterFn>,
    infer_schema_length: usize,
    ssh_connect: fn(&SshSource, Option<&str>, Option<&str>, &str) -> anyhow::Result<SshStream>,
    ssh_reader: SshReaderFn,
) -> anyhow::Result<(Option<Schema>, Option<LineReader>)> {
    let mut reader: LineReader = match source {
        LogFmtSource::Cursor(cursor) => {
            let mut c = cursor.clone();
            c.set_position(0);
            Box::new(std::io::BufReader::new(c))
        }
        LogFmtSource::Ssh(ssh_source) => {
            let real_cmd = cmd
                .clone()
                .unwrap_or_else(|| format!("cat {}", ssh_source.path));
            let stream = ssh_connect(ssh_source, None, None, &real_cmd)?;
            let (_sess, reader) = ssh_reader(stream, ssh_source.path.ends_with(".zst"))?;
            reader
        }
        LogFmtSource::Seekable(file_arc) => {
            let file_opt = lock_seekable(file_arc);
            let file = file_opt
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("SeekableVfsFile missing"))?;
            let cloned_file = file
                .clone_handle()
                .map_err(|e| anyhow::anyhow!("SeekableVfsFile clone failed: {e}"))?;
            Box::new(std::io::BufReader::new(cloned_file))
        }
    };
    let mut consumed = Vec::new();
    let schema = infer_schema_from_reader(
        &mut *reader,
        line_filter.as_ref(),
        infer_schema_length,
        &mut consumed,
    )?;
    let probe = match source {
        LogFmtSource::Ssh(_) => Some(Box::new(Cursor::new(consumed).chain(reader)) as LineReader),
        LogFmtSource::Cursor(_) | LogFmtSource::Seekable(_) => None,
    };
    Ok((schema, probe))
}

/// Infer the schema from the first `n_rows` non-empty lines of `reader` that
/// pass `line_filter`, merging the types with [`merge_schema_field`].
/// `Ok(None)` when no such line exists (also when `n_rows` is 0). Every byte
/// taken from `reader` is appended to `consumed`.
pub(crate) fn infer_schema_from_reader(
    reader: &mut dyn BufRead,
    line_filter: Option<&LineFilterFn>,
    n_rows: usize,
    consumed: &mut Vec<u8>,
) -> anyhow::Result<Option<Schema>> {
    let mut schema: Option<Schema> = None;
    let mut rows = 0usize;
    let mut line = String::new();
    while rows < n_rows && reader.read_line(&mut line)? > 0 {
        consumed.extend_from_slice(line.as_bytes());
        if line.trim().is_empty() {
            line.clear();
            continue;
        }
        if line_filter.map(|filter| !filter(&line)).unwrap_or(false) {
            line.clear();
            continue;
        }
        let row = crate::logfmt::parse_logfmt_line(&line);
        let row_schema = infer_schema_from_row(&row);
        schema = Some(match schema {
            None => row_schema,
            Some(mut merged) => {
                for (k, v) in row_schema {
                    merged
                        .entry(k)
                        .and_modify(|seen| *seen = merge_schema_field(*seen, v))
                        .or_insert(v);
                }
                merged
            }
        });
        rows += 1;
        line.clear();
    }
    Ok(schema)
}

/// The type a column takes when two probed rows disagree: the same type
/// stays, Integer and Float widen to Float, anything else falls back to
/// String.
fn merge_schema_field(a: SchemaField, b: SchemaField) -> SchemaField {
    match (a, b) {
        _ if a == b => a,
        (SchemaField::Integer, SchemaField::Float) | (SchemaField::Float, SchemaField::Integer) => {
            SchemaField::Float
        }
        _ => SchemaField::String,
    }
}

// logfmt::SchemaField → polars::Schema 変換関数
fn logfmt_schema_to_polars_schema(
    map: &std::collections::BTreeMap<String, crate::logfmt::SchemaField>,
) -> polars::prelude::Schema {
    use polars::prelude::{DataType, Field, Schema, TimeUnit};
    let mut fields = Vec::new();
    for (k, v) in map.iter() {
        let dtype = match v {
            crate::logfmt::SchemaField::String => DataType::String,
            crate::logfmt::SchemaField::Integer => DataType::Int64,
            crate::logfmt::SchemaField::Float => DataType::Float64,
            crate::logfmt::SchemaField::Boolean => DataType::Boolean,
            crate::logfmt::SchemaField::DateTime => {
                DataType::Datetime(TimeUnit::Microseconds, None)
            }
            crate::logfmt::SchemaField::Duration => DataType::Int64,
            crate::logfmt::SchemaField::Auto => DataType::String,
        };
        fields.push(Field::new(k.into(), dtype));
    }
    Schema::from_iter(fields)
}
// SSHストリームからBufReadを返すユーティリティ
fn ssh_reader(mut stream: SshStream, is_zst: bool) -> anyhow::Result<(SshStream, LineReader)> {
    use std::io::BufReader;
    let channel = stream
        .channel
        .take()
        .ok_or_else(|| anyhow::anyhow!("No channel in SshStream"))?;
    if is_zst {
        let decoder = zstd::stream::Decoder::new(channel)?;
        let new_stream = SshStream {
            _sess: stream._sess,
            channel: None,
        };
        Ok((new_stream, Box::new(BufReader::new(decoder))))
    } else {
        let new_stream = SshStream {
            _sess: stream._sess,
            channel: None,
        };
        Ok((new_stream, Box::new(BufReader::new(channel))))
    }
}
/// Rowから型推論でSchemaを生成するユーティリティ
fn infer_schema_from_row(row: &Row) -> Schema {
    use crate::logfmt::ParsedValue;
    use crate::logfmt::SchemaField;
    let mut schema = Schema::new();
    for (k, v) in row.iter() {
        let field = match v {
            ParsedValue::String(_) => SchemaField::String,
            ParsedValue::Integer(_) => SchemaField::Integer,
            ParsedValue::Float(_) => SchemaField::Float,
            ParsedValue::Boolean(_) => SchemaField::Boolean,
            ParsedValue::DateTime(_) => SchemaField::DateTime,
            ParsedValue::Duration(_) => SchemaField::Duration,
        };
        schema.insert(k.clone(), field);
    }
    schema
}

impl LogFmtSource {
    /// clone_handle: SeekableVfsFileの独立ハンドルを生成（Seekableのみ）
    pub fn clone_handle(&self) -> Option<LogFmtSource> {
        match self {
            LogFmtSource::Seekable(arc_mutex) => {
                let file_opt = lock_seekable(arc_mutex);
                if let Some(ref file) = *file_opt {
                    match file.clone_handle() {
                        Ok(new_file) => Some(LogFmtSource::Seekable(std::sync::Arc::new(
                            std::sync::Mutex::new(Some(new_file)),
                        ))),
                        Err(_) => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    pub fn line_filter(&self) -> Option<&str> {
        None // 仮実装: 必要に応じて拡張
    }
}

pub type RowFilter = Arc<dyn Fn(&Row) -> bool + Send + Sync>;
pub type LineFilter = Box<dyn Fn(&str) -> bool + Send + Sync>;
pub type LineFilterFn = Arc<dyn Fn(&str) -> bool + Send + Sync + 'static>;

pub type SeekableSource =
    std::sync::Arc<std::sync::Mutex<Option<Box<dyn crate::SeekableVfsFile + Send>>>>;

pub enum LogFmtSource {
    Ssh(SshSource),
    Cursor(Cursor<Vec<u8>>),
    Seekable(SeekableSource),
}

/// Lock the shared seekable handle. A poisoned lock is recovered instead of
/// panicking: the guarded value is a file handle that stays usable.
pub(crate) fn lock_seekable(
    source: &SeekableSource,
) -> std::sync::MutexGuard<'_, Option<Box<dyn crate::SeekableVfsFile + Send>>> {
    source
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// use std::sync::Mutex; // 未使用

// ...existing code...

// use polars::prelude::SchemaRef; // 未使用
// use polars::prelude::{AnonymousScan, AnonymousScanArgs, PolarsResult}; // 未使用
// use std::any::Any; // 未使用

// check to covert trait
#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}

impl Clone for LogFmtSource {
    fn clone(&self) -> Self {
        match self {
            LogFmtSource::Ssh(s) => LogFmtSource::Ssh(s.clone()),
            LogFmtSource::Cursor(c) => LogFmtSource::Cursor(c.clone()),
            LogFmtSource::Seekable(arc_mutex) => LogFmtSource::Seekable(arc_mutex.clone()),
        }
    }
}

// ...existing code...

// 未使用: DataFrameのカラム整列関数（dead_code警告回避のためコメントアウト）
/*
fn align_dataframes(left: &mut DataFrame, right: &mut DataFrame) -> Result<()> {
    let left_names: Vec<String> = left
        .get_column_names_owned()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let right_names: Vec<String> = right
        .get_column_names_owned()
        .iter()
        .map(|name| name.to_string())
        .collect();

    let right_set: BTreeSet<String> = right_names.iter().cloned().collect();
    let left_set: BTreeSet<String> = left_names.iter().cloned().collect();

    for name in &left_names {
        if !right_set.contains(name) {
            let dtype = left.column(name)?.dtype().clone();
            let series = Series::full_null(name.as_str().into(), right.height(), &dtype);
            right.with_column(series)?;
        }
    }

    for name in &right_names {
        if !left_set.contains(name) {
            let dtype = right.column(name)?.dtype().clone();
            let series = Series::full_null(name.as_str().into(), left.height(), &dtype);
            left.with_column(series)?;
        }
    }

    let left_names_now: Vec<String> = left
        .get_column_names_owned()
        .iter()
        .map(|name| name.to_string())
        .collect();

    for name in &left_names_now {
        let left_dtype = left.column(name)?.dtype().clone();
        let right_dtype = right.column(name)?.dtype().clone();
        if right_dtype != left_dtype {
            let casted = right.column(name)?.cast(&left_dtype)?;
            right.with_column(casted)?;
        }
    }

    let reordered = right.select(
        left_names_now
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<_>>(),
    )?;
    *right = reordered;
    Ok(())
}
*/

fn rows_to_dataframe_filled(rows: &[Row], schema: Option<&Schema>) -> Result<DataFrame> {
    use crate::logfmt::ParsedValue;

    let mut keys = std::collections::BTreeSet::new();
    for row in rows {
        keys.extend(row.keys().cloned());
    }

    if keys.is_empty() {
        return Ok(DataFrame::default());
    }

    use polars::prelude::Column;
    let mut columns = Vec::with_capacity(keys.len());

    for key in keys {
        // Check if schema specifies a type for this column
        let schema_field = schema.and_then(|s| s.get(&key));

        // First pass: determine the column type
        let mut has_string = false;
        let mut has_int = false;
        let mut has_float = false;
        let mut has_bool = false;
        let mut has_datetime = false;
        let mut has_duration = false;

        for row in rows {
            if let Some(val) = row.get(&key) {
                match val {
                    ParsedValue::String(_) => has_string = true,
                    ParsedValue::Integer(_) => has_int = true,
                    ParsedValue::Float(_) => has_float = true,
                    ParsedValue::Boolean(_) => has_bool = true,
                    ParsedValue::DateTime(_) => has_datetime = true,
                    ParsedValue::Duration(_) => has_duration = true,
                }
            }
        }

        // Determine final type based on schema or inferred types
        use crate::logfmt::SchemaField;
        let inferred_type = match schema_field {
            Some(SchemaField::String) => Some("String".to_string()),
            Some(SchemaField::Integer) => Some("Int".to_string()),
            Some(SchemaField::Float) => Some("Float".to_string()),
            Some(SchemaField::Boolean) => Some("Bool".to_string()),
            Some(SchemaField::DateTime) => Some("DateTime".to_string()),
            Some(SchemaField::Duration) => Some("Duration".to_string()),
            Some(SchemaField::Auto) | None => {
                // Use schema auto or infer from values
                if has_datetime {
                    Some("DateTime".to_string())
                } else if has_duration {
                    Some("Duration".to_string())
                } else if has_string {
                    Some("String".to_string())
                } else if has_float {
                    Some("Float".to_string())
                } else if has_int {
                    Some("Int".to_string())
                } else if has_bool {
                    Some("Bool".to_string())
                } else {
                    None
                }
            }
        };

        // Create series based on inferred type
        match inferred_type.as_deref() {
            Some("String") => {
                let col: Vec<String> = rows
                    .iter()
                    .map(|row| row.get(&key).map(|v| v.as_string()).unwrap_or_default())
                    .collect();
                columns.push(Column::new(key.as_str().into(), col));
            }
            Some("Int") => {
                let col: Vec<i64> = rows
                    .iter()
                    .map(|row| match row.get(&key) {
                        Some(ParsedValue::Integer(i)) => *i,
                        Some(ParsedValue::Float(f)) => *f as i64,
                        Some(ParsedValue::Boolean(b)) => {
                            if *b {
                                1
                            } else {
                                0
                            }
                        }
                        Some(ParsedValue::String(s)) => s.parse().unwrap_or(0),
                        Some(ParsedValue::DateTime(_)) => 0,
                        Some(&ParsedValue::Duration(dur)) => {
                            (dur.as_secs() as i64) * 1_000_000 + (dur.subsec_micros() as i64)
                        }

                        None => 0,
                    })
                    .collect();
                columns.push(Column::new(key.as_str().into(), col));
            }
            Some("Float") => {
                let col: Vec<f64> = rows
                    .iter()
                    .map(|row| match row.get(&key) {
                        Some(ParsedValue::Duration(dur)) => {
                            dur.as_secs() as f64 + (dur.subsec_micros() as f64) / 1_000_000.0
                        }
                        Some(ParsedValue::Float(f)) => *f,
                        Some(ParsedValue::Integer(i)) => *i as f64,
                        Some(ParsedValue::Boolean(b)) => {
                            if *b {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Some(ParsedValue::String(s)) => s.parse().unwrap_or(0.0),
                        Some(ParsedValue::DateTime(_)) => 0.0,
                        None => 0.0,
                    })
                    .collect();
                columns.push(Column::new(key.as_str().into(), col));
            }
            Some("Bool") => {
                let col: Vec<bool> = rows
                    .iter()
                    .map(|row| match row.get(&key) {
                        Some(ParsedValue::Boolean(b)) => *b,
                        Some(ParsedValue::Integer(i)) => *i != 0,
                        Some(ParsedValue::Float(f)) => *f != 0.0,
                        Some(ParsedValue::String(s)) => {
                            matches!(s.to_lowercase().as_str(), "true" | "yes" | "on" | "1")
                        }
                        Some(ParsedValue::DateTime(_)) => false,
                        Some(ParsedValue::Duration(_)) => false,

                        None => false,
                    })
                    .collect();
                columns.push(Column::new(key.as_str().into(), col));
            }
            Some("DateTime") => {
                let col: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| match row.get(&key) {
                        Some(ParsedValue::DateTime(dt)) => Some(dt.timestamp_micros()),
                        Some(ParsedValue::String(s)) => DateTime::parse_from_rfc3339(s)
                            .ok()
                            .map(|dt| dt.timestamp_micros()),
                        Some(v) => DateTime::parse_from_rfc3339(&v.as_string())
                            .ok()
                            .map(|dt| dt.timestamp_micros()),
                        None => None,
                    })
                    .collect();
                // PolarsのSeriesとしてDatetime型で生成
                use polars::prelude::NamedFrom;
                use polars::prelude::{DataType, Series, TimeUnit};
                let s = Series::new(polars::prelude::PlSmallStr::from_str(key.as_str()), col);
                let s = s.cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
                columns.push(s.into());
            }
            _ => {
                let col: Vec<String> = rows
                    .iter()
                    .map(|row| row.get(&key).map(|v| v.as_string()).unwrap_or_default())
                    .collect();
                columns.push(Column::new(key.as_str().into(), col));
            }
        }
    }

    Ok(DataFrame::new_infer_height(columns)?)
}

// ...existing code...

#[allow(dead_code)]
fn _example_usage() -> Result<DataFrame> {
    // let q = LazyLogFmtReader::new(SshSource::new(
    //     "ssh://user@host/path/to/app.log",
    // ))
    // .finish()?
    // .filter(col("msg").eq(lit("finish to process/write ")))
    // .agg(vec![col("bytes").cast(DataType::Float64).sum()]);
    // Ok(q)
    unimplemented!("example usage")
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::logfmt::SchemaField;
    // use polars::prelude::{DataType, Series};
    // use std::io::Cursor;

    //#[test]
    // fn schema_builder_applies_types() -> anyhow::Result<()> {
    //     let data = b"id=\"123\" rate=\"2.5\" flag=yes ts=2026-01-22T12:00:00+09:00\n";
    //     let mut schema = Schema::new();
    //     schema.insert("id".to_string(), SchemaField::Integer);
    //     schema.insert("rate".to_string(), SchemaField::Float);
    //     schema.insert("flag".to_string(), SchemaField::Boolean);
    //     schema.insert("ts".to_string(), SchemaField::DateTime);

    //     let df = LazyLogFmtReader::from_cursor(Cursor::new(data.to_vec()))
    //         .schema(schema)
    //         .scan()?
    //         .collect()?;

    //     assert_eq!(df.column("id")?.dtype(), &DataType::Int64);
    //     assert_eq!(df.column("id")?.i64()?.get(0), Some(123));

    //     assert_eq!(df.column("rate")?.dtype(), &DataType::Float64);
    //     assert_eq!(df.column("rate")?.f64()?.get(0), Some(2.5));

    //     assert_eq!(df.column("flag")?.dtype(), &DataType::Boolean);
    //     assert_eq!(df.column("flag")?.bool()?.get(0), Some(true));

    //     // DateTime is stored as Polars Datetime(ms)
    //     let expected_ts = DateTime::parse_from_rfc3339("2026-01-22T12:00:00+09:00")
    //         .unwrap()
    //         .timestamp_millis();
    //     assert_eq!(
    //         df.column("ts")?.dtype(),
    //         &DataType::Datetime(TimeUnit::Milliseconds, None)
    //     );
    //     let ts_col = df.column("ts")?.i64().expect("datetime col");
    //     assert_eq!(ts_col.get(0), Some(expected_ts));

    //     Ok(())
    // }
    #[test]
    fn append_dataframe_with_union_aligns_columns() -> anyhow::Result<()> {
        // let df_left = DataFrame::new(vec![col!("a", [1i64]), col!("b", ["x"])])?;
        // let df_right = DataFrame::new(vec![col!("b", ["y"]), col!("c", [10i64])])?;
        // let mut acc = Some(df_left);
        // append_dataframe_with_union(&mut acc, df_right)?;
        // let merged = acc.expect("merged dataframe");
        // assert_eq!(merged.height(), 2);
        // assert_eq!(merged.get_column_names_str(), vec!["a", "b", "c"]);
        // assert_eq!(merged.column("a")?.i64()?.get(0), Some(1));
        // assert_eq!(merged.column("a")?.i64()?.get(1), None);
        // assert_eq!(merged.column("b")?.str()?.get(0), Some("x"));
        // assert_eq!(merged.column("b")?.str()?.get(1), Some("y"));
        // assert_eq!(merged.column("c")?.i64()?.get(0), None);
        // assert_eq!(merged.column("c")?.i64()?.get(1), Some(10));
        Ok(())
        //unimplemented!("append_dataframe_with_union test");
    }
}
