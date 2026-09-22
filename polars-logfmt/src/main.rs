use anyhow::{Result, anyhow};
use chrono::DateTime;
use clap::Parser;
use humantime::format_duration;
use memory_stats::memory_stats;
use polars::{
    error::PolarsError,
    prelude::{
        AnyValue, AsOfOptions, AsofStrategy, DataFrame, DataType, IntoLazy, JoinArgs, JoinType,
        ParquetWriteOptions, PolarsResult, SortMultipleOptions, TimeUnit, col, lit,
    },
};
use polars_logfmt::{
    cli::Args,
    lazy::LazyFrameFn,
    lazy::LazyLogFmtReaderBuilder,
    ssh::{SshStream, connect_ssh, parse_ssh_source},
};
use std::io::{BufRead, BufReader};
use std::time::Instant;
use std::{fs::File, path::PathBuf};
use zstd::stream::read::Decoder;

use itertools::Itertools;

use polars::prelude::ScanArgsAnonymous;
use polars::prelude::{Field, ParquetCompression, ParquetWriter, Schema, SinkTarget};
use std::sync::Arc;

fn to_ann_scan(
    builder: polars_logfmt::lazy::LazyLogFmtReaderBuilder,
) -> Result<polars::lazy::frame::LazyFrame, PolarsError> {
    let _schema = Arc::new(
        Schema::from_iter_check_duplicates(vec![
            Field::new(
                "time".into(),
                DataType::Datetime(TimeUnit::Microseconds, None),
            ),
            // Field::new(
            //     "left_time".into(),
            //     DataType::Datetime(TimeUnit::Milliseconds, None),
            // ),
            // Field::new("table".into(), DataType::String),
            Field::new("msg".into(), DataType::String),
            Field::new("new".into(), DataType::String),
            Field::new("old".into(), DataType::String),
            // Field::new("totals".into(), DataType::String),
            Field::new("level".into(), DataType::String),
            Field::new("writer_number".into(), DataType::Int32),
            Field::new("handler_number".into(), DataType::Int32),
            Field::new("old_file_bytes".into(), DataType::UInt64),
            Field::new("bytes".into(), DataType::UInt64),
        ])
        .unwrap(),
    );

    polars::lazy::frame::LazyFrame::anonymous_scan(
        Arc::new(builder.build().unwrap()),
        ScanArgsAnonymous {
            // schema: Some(schema),
            ..Default::default()
        },
    )
}

fn df_run_finished(builder: LazyLogFmtReaderBuilder) -> BuilderWithLazyFrame {
    builder_with_lazy_frame(builder, |b| {
        b.build()
            .unwrap()
            .scan()
            .unwrap()
            .sort(["time"], Default::default())
            .with_row_index("id", None)
            .select([col("time"), col("id"), col("totals")])
    })
    //fn df_run_finished(builder: ReaderBuilder) -> polars::prelude::LazyFrame {
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct RunFinishedRecord {
    time: DateTime<chrono::FixedOffset>,
    totals: Option<String>,
}

#[allow(dead_code)]
struct RunFinishedAggregator {
    rows: Vec<RunFinishedRecord>,
}

impl RunFinishedAggregator {
    #[allow(dead_code)]
    fn new() -> Self {
        Self { rows: Vec::new() }
    }
}

#[allow(dead_code)]
struct ReaderHandle {
    _hold: Option<ssh2::Session>,
    reader: Box<dyn BufRead>,
}

#[allow(dead_code)]
fn open_stream_reader(source: &str) -> Result<ReaderHandle> {
    if source.starts_with("ssh://") {
        let source = parse_ssh_source(source)?;
        let cmd = format!("cat {}", source.path);
        let SshStream { _sess, channel } = connect_ssh(&source, None, None, &cmd)?;
        let channel = channel.ok_or_else(|| anyhow!("No channel in SshStream"))?;
        let reader: Box<dyn BufRead> = if source.path.ends_with(".zst") {
            let decoder = Decoder::new(BufReader::new(channel))?;
            Box::new(BufReader::new(decoder))
        } else {
            Box::new(BufReader::new(channel))
        };
        Ok(ReaderHandle {
            _hold: Some(_sess),
            reader,
        })
    } else {
        let file = File::open(source)?;
        if source.ends_with(".zst") {
            let decoder = Decoder::new(BufReader::new(file))?;
            Ok(ReaderHandle {
                _hold: None,
                reader: Box::new(BufReader::new(decoder)),
            })
        } else {
            Ok(ReaderHandle {
                _hold: None,
                reader: Box::new(BufReader::new(file)),
            })
        }
    }
}

#[allow(dead_code)]
fn extract_table_from_new(value: &str) -> Option<String> {
    let marker = "/out/";
    let start = value.find(marker)? + marker.len();
    let rest = value.get(start..)?;
    let end = rest.find('/')?;
    if end == 0 {
        None
    } else {
        Some(rest[..end].to_string())
    }
}

#[allow(dead_code)]
fn find_finished_id(
    run_finished: &[RunFinishedRecord],
    time: &DateTime<chrono::FixedOffset>,
) -> Option<usize> {
    let idx = run_finished
        .binary_search_by(|row| row.time.cmp(time))
        .unwrap_or_else(|idx| idx);
    if idx < run_finished.len() {
        Some(idx)
    } else {
        None
    }
}

fn rename_finished_id(mut df: DataFrame) -> PolarsResult<DataFrame> {
    df.rename("id", "finished_id".into())?;
    Ok(df)
}

fn setup_logger(loglevel: &str) {
    #[cfg(debug_assertions)]
    {
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
}

fn simple_benchmark(source: Option<String>) -> Result<()> {
    let _finder = memchr::memmem::Finder::new(b"msg=\"finish to process/write \"");
    //let pred = |line: &str| finder.find(line.as_bytes()).is_some();

    // let f = to_ann_scan(
    //     LazyLogFmtReaderBuilder::new()
    //         .source(source.clone())
    //         .aligned_cols_cnt(true)
    //         .line_filter(move |line: &str| _finder.find(line.as_bytes()).is_some()),
    // )?;

    let finder2 = memchr::memmem::Finder::new(b"msg=\"finish to process/write \"");
    let f = LazyLogFmtReaderBuilder::new()
        .source(source.clone())
        .aligned_cols_cnt(true)
        .line_filter(move |line: &str| finder2.find(line.as_bytes()).is_some())
        .build()?
        .scan()?;

    let ldf = f
        .filter(col("old").str().contains_literal(lit("health_check")).not())
        .sort(["time"], Default::default())
        // .with_columns([col("time")])
        .with_columns([col("time").alias("left_time")]);
    // .collect()?;
    // .collect_with_engine(polars::prelude::Engine::Streaming)?;

    println!("{}", ldf.explain(true)?);
    let df = ldf
        // .lazy()
        .with_streaming(true)
        // .with_streaming(true)
        // .collect_with_engine(polars::prelude::Engine::Streaming)?;
        .collect()?;

    // let a = df.tail(1).get_row(0);
    dbg!(df);

    //    tracing::debug!(df = ?df, source = ?source, "simple_benchmark");

    Ok(())
}

fn simple_benchmark2(source: Option<String>) -> Result<()> {
    let finder2 = memchr::memmem::Finder::new(b"msg=\"finish to process/write \"");
    let f = LazyLogFmtReaderBuilder::new()
        .source(source.clone())
        .aligned_cols_cnt(true)
        .line_filter(move |line: &str| finder2.find(line.as_bytes()).is_some())
        .build()?
        .scan()?;

    let opath = PathBuf::from(source.clone().unwrap()).with_extension("parquet");

    // sink_parquet is not working wihtout streaming. enable after to support streaming.
    // let reusult = f.sink_parquet(
    //     SinkTarget::Path(polars::prelude::PlPath::Local(opath.into())),
    //     Default::default(),
    //     None,
    //     Default::default(),
    // )?;
    //
    let mut df = f.collect()?;
    let mut file = File::create(opath).expect("Could not create file");

    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df.clone())?;

    // let df = f
    //     // .lazy()
    //     .with_streaming(true)
    //     // .with_streaming(true)
    //     // .collect_with_engine(polars::prelude::Engine::Streaming)?;
    //     .collect()?;

    // // let a = df.tail(1).get_row(0);
    // dbg!(df);

    //    tracing::debug!(df = ?df, source = ?source, "simple_benchmark");

    Ok(())
}

fn main() -> Result<()> {
    // Initialize tracing subscriber and bridge `log` -> `tracing` so existing `log` calls
    // are emitted through the tracing subscriber (structured output).

    setup_logger("debug");

    let mut args = Args::parse();
    args.streaming = false;

    // MENTION: if support streaming. enable or check
    // unsafe {
    //     std::env::set_var("POLARS_AUTO_NEW_STREAMING", "1");
    //     std::env::set_var("POLARS_FORCE_NEW_STREAMING", "1");
    // };

    // return simple_benchmark(args.source.clone());

    return simple_benchmark2(args.source.clone());

    let rss_start: Option<u64> = current_rss_kb();
    if args.streaming {
        streaming_output2(&args, rss_start)
    } else {
        non_streaming_output(&args, rss_start)
    }
}

fn streaming_output2(args: &Args, _rss_start: Option<u64>) -> Result<()> {
    let _dur = Instant::now();

    let _df_run_finished = df_run_finished(
        LazyLogFmtReaderBuilder::new()
            .source(args.source.clone())
            .line_filter(|line: &str| line.contains("msg=\"csl-etl run finished\"")),
    )
    .to_lazy()?
    .collect();

    Ok(())
}
fn non_streaming_output(args: &Args, rss_start: Option<u64>) -> Result<()> {
    let dur = Instant::now();
    unsafe {
        std::env::set_var("POLARS_FMT_MAX_ROWS", "30");
        std::env::set_var("POLARS_FMT_MAX_COLS", "-1");
    };

    let ldf_run_finished = to_ann_scan(
        LazyLogFmtReaderBuilder::new()
            .source(args.source.clone())
            .aligned_cols_cnt(true)
            .line_filter(|line: &str| line.contains("msg=\"csl-etl run finished\"")),
    )?;
    let df_run_finished = ldf_run_finished
        .clone()
        .with_streaming(true)
        .sort(["time"], Default::default())
        .with_row_index("id", None)
        .collect()?;

    println!("elapsed:{}", format_duration(dur.elapsed()));
    println!("csl-etl run cnt:{}", df_run_finished.height());
    println!("{:?}", df_run_finished.schema());
    dbg!(df_run_finished.schema());
    println!("df_run_finished: {}", df_run_finished);

    // let before_df_base_before_joiin = to_ann_scan(
    let a = to_ann_scan(
        LazyLogFmtReaderBuilder::new()
            .source(args.source.clone())
            .aligned_cols_cnt(true)
            .line_filter(|line: &str| line.contains("msg=\"finish to process/write \"")),
    )?
    .with_streaming(true)
    .filter(col("old").str().contains_literal(lit("health_check")).not())
    .sort(["time"], Default::default())
    .with_row_index("id", None);

    let before_df_base_before_joiin = a
        .with_columns([col("new")
            .str()
            .extract(lit("/out/([^/]+)/"), 1)
            .alias("table")])
        .with_columns([col("time").alias("left_time")]);

    let df_base_before_join: DataFrame = before_df_base_before_joiin
        .clone()
        .with_streaming(true)
        .collect()?;
    // .collect_with_engine(polars::prelude::Engine::Streaming)?;
    // println!(
    //     "df_base_before_join: {}",
    //     df_base_before_join.describe_plan()?,
    // );
    // println!(
    //     "ldf_run_finished: {}",
    //     ldf_run_finished
    //         .clone()
    //         .to_lazy()?
    //         .with_columns([col("time").alias("right_time")])
    //         .describe_plan()?,
    // );
    println!("df_base_before_join: {}", df_base_before_join.clone());
    println!("df_base_before_join: len={}", df_base_before_join.height());

    let df_base = rename_finished_id(
        df_base_before_join
            .clone()
            .lazy()
            .join(
                ldf_run_finished.with_columns([col("time").alias("right_time")]),
                vec![col("left_time")],
                vec![col("right_time")],
                JoinArgs::new(JoinType::AsOf(Box::new(AsOfOptions {
                    strategy: AsofStrategy::Forward,
                    tolerance: None,
                    tolerance_str: None,
                    left_by: None,
                    right_by: None,
                    allow_eq: false,
                    check_sortedness: false,
                }))),
            )
            // 必要なカラムをselect（left_time, right_time, ...）
            // .select([
            //     col("left_time"),
            //     col("right_time"),
            //     // ...他の必要なカラムをここに追加...
            // ])
            .with_streaming(true)
            .collect()?,
    )?;

    println!("elapsed:{}", format_duration(dur.elapsed()));

    println!(
        "df_summary_table_null: {}",
        df_base
            .clone()
            .lazy()
            .filter(col("table").is_null())
            .select([col("new"), col("table")])
            .collect()?
    );

    println!("df_summary: {}", df_base.clone());

    let df_base_lazy = df_base.lazy();

    let df_bytes_sum = df_base_lazy
        .clone()
        .select([
            col("bytes").cast(DataType::Int64).sum().alias("bytes_sum"),
            col("new"),
        ])
        .collect()?;
    let df_bytes_sum_per_table_and_finished_id = df_base_lazy
        .group_by([col("table"), col("finished_id")])
        .agg([
            col("bytes").cast(DataType::Int64).sum().alias("bytes_sum"),
            col("totals").first().alias("totals"),
        ])
        .sort(["finished_id", "table"], Default::default());

    let df_sort_bytes_sum = df_bytes_sum_per_table_and_finished_id.clone().sort(
        ["bytes_sum"],
        SortMultipleOptions::default().with_order_descending(true),
        // Default::default().with_order_descending(true),
    );

    println!("elapsed:{}", format_duration(dur.elapsed()));
    println!("df_bytes_sum: {}", df_bytes_sum);
    println!(
        "df_bytes_sum_per_table_and_finished_id: {}",
        df_bytes_sum_per_table_and_finished_id.collect()?
    );
    println!(
        "df_sort_bytes_sum: {}",
        df_sort_bytes_sum.clone().collect()?
    );

    let a = df_sort_bytes_sum.clone().collect()?;
    let b: Vec<_> = a.column("table")?.str()?.iter().unique().collect();
    println!("table: {:?}", b);
    print_rss_delta("end", rss_start, current_rss_kb());
    Ok(())
}

struct BuilderWithLazyFrame {
    builder: LazyLogFmtReaderBuilder,
    func: LazyFrameFn,
}

impl Clone for BuilderWithLazyFrame {
    fn clone(&self) -> Self {
        Self {
            builder: self.builder.clone(),
            func: self.func,
        }
    }
}

fn builder_with_lazy_frame(
    builder: LazyLogFmtReaderBuilder,
    func: LazyFrameFn,
) -> BuilderWithLazyFrame {
    BuilderWithLazyFrame {
        builder: builder,
        func: func,
    }
}

impl BuilderWithLazyFrame {
    fn to_lazy(self) -> Result<polars::prelude::LazyFrame> {
        let hoge = self.func;
        Ok(hoge(self.builder))
    }
}

fn current_rss_kb() -> Option<u64> {
    memory_stats().map(|stats| stats.physical_mem as u64)
}

fn print_rss_delta(label: &str, before: Option<u64>, after: Option<u64>) {
    match (before, after) {
        (Some(b), Some(a)) => {
            let diff = a as i64 - b as i64;
            println!(
                "rss {}: {} (delta {})",
                label,
                format_bytes(a),
                format_bytes_signed(diff)
            );
        }
        (None, Some(a)) => println!("rss {}: {}", label, format_bytes(a)),
        _ => println!("rss {}: n/a", label),
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}

fn format_bytes_signed(bytes: i64) -> String {
    if bytes < 0 {
        format!("-{}", format_bytes(bytes.unsigned_abs()))
    } else {
        format_bytes(bytes as u64)
    }
}

fn anyvalue_to_string(value: AnyValue) -> Option<String> {
    match value {
        AnyValue::Null => None,
        AnyValue::String(value) => Some(value.to_string()),
        AnyValue::StringOwned(value) => Some(value.to_string()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_table_from_new_parses_table() {
        let value = "/data/out/VTEL_MME_FCT/2/20260113/00/2_chy1-MME.tbl.zst";
        assert_eq!(
            extract_table_from_new(value),
            Some("VTEL_MME_FCT".to_string())
        );
        assert_eq!(extract_table_from_new("/out//"), None);
        assert_eq!(extract_table_from_new("/data/out/"), None);
    }

    #[test]
    fn find_finished_id_returns_next_or_equal() {
        let rows = vec![
            RunFinishedRecord {
                time: DateTime::parse_from_rfc3339("2026-01-13T00:00:00+09:00").unwrap(),
                totals: Some("1".to_string()),
            },
            RunFinishedRecord {
                time: DateTime::parse_from_rfc3339("2026-01-13T01:00:00+09:00").unwrap(),
                totals: Some("2".to_string()),
            },
        ];

        let t0 = DateTime::parse_from_rfc3339("2026-01-13T00:00:00+09:00").unwrap();
        let t_half = DateTime::parse_from_rfc3339("2026-01-13T00:30:00+09:00").unwrap();
        let t_after = DateTime::parse_from_rfc3339("2026-01-13T02:00:00+09:00").unwrap();

        assert_eq!(find_finished_id(&rows, &t0), Some(0));
        assert_eq!(find_finished_id(&rows, &t_half), Some(1));
        assert_eq!(find_finished_id(&rows, &t_after), None);
    }
}
