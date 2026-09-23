use crate::SeekableVfs;
use crate::lazy::{
    LineFilterFn, LineReader, LogFmtReaderState, LogFmtSource, RowFilter, infer_schema_from_reader,
    infer_schema_from_row, infer_schema_from_source, lock_seekable, logfmt_schema_to_polars_schema,
    rows_to_dataframe_filled, ssh_reader,
};
use crate::logfmt::{ParsedValue, Row, Schema, SchemaField};
use crate::ssh::{SshSource, connect_ssh};
use chrono::DateTime;
use hashbrown::HashMap;
use itertools::Itertools;
use polars::prelude::{
    AnonymousScan, AnonymousScanArgs, ChunkedBuilder, Column, DataFrame, Expr, LazyFrame,
    PolarsResult, SchemaRef, TimeUnit,
};
use polars::prelude::{DataType, NamedFrom};
use polars::series::{IntoSeries, Series};
use polars_core::runtime::RAYON;
use polars_core::utils::accumulate_dataframes_vertical;
use std::any::Any;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// use arrayvec::ArrayVec;

use polars::prelude::StringChunkedBuilder;
// use polars::prelude::PrimitiveChunkedBuilder<polars::prelude::Int64Type>;
// use polars::prelude::BooleanChunkedBuilder;
use polars_core::prelude::BooleanChunkedBuilder;
use polars_core::prelude::PrimitiveChunkedBuilder;
/// A small descriptor for a byte-range frame when SeekTable is not available.
#[derive(Clone)]
pub struct FrameRange {
    pub idx: usize,
    pub start: u64,
    pub len: u64,
    pub is_zst: bool,
    pub comp_start: u64,
    pub comp_len: u64,
}

pub struct LazyLogFmtReader {
    pub source: LogFmtSource,
    pub cmd: Option<String>,
    pub batch_size: Option<usize>,
    pub row_filter: Option<RowFilter>,
    pub line_filter: Option<LineFilterFn>,
    pub schema: Option<Schema>,
    pub aligned_cols_cnt: bool,
    pub reader_state: Arc<Mutex<LogFmtReaderState>>,
    pub n_threads: Option<usize>,
    pub use_parallel: bool,
    /// Rows read to infer the schema when none is given.
    pub infer_schema_length: usize,
}

/// Rows read to infer the schema when neither the caller nor polars gives a
/// number: the first accepted line only.
pub const DEFAULT_INFER_SCHEMA_LENGTH: usize = 1;

#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}

impl AnonymousScan for LazyLogFmtReader {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        LazyLogFmtReader::debug_scan_opts(true, &scan_opts);

        // The probe stream to continue from: left by `scan_logfmt` for the
        // first collect, or opened here when no schema was given.
        let mut probe = self.lock_state().reader.take();
        let _schema = if let Some(ref schema) = self.schema {
            schema.clone()
        } else {
            let (schema_opt, probe_opt) = infer_schema_from_source(
                &self.source,
                &self.cmd,
                self.line_filter.clone(),
                self.infer_schema_length,
                crate::ssh::connect_ssh,
                ssh_reader,
            )
            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;
            probe = probe_opt;
            if let Some(s) = schema_opt {
                s
            } else {
                polars::error::polars_bail!(ComputeError: "could not infer schema from source");
            }
        };
        // create a local reader clone with the inferred schema so all batches
        // use a consistent schema when `self.schema` was not provided.
        let scan_reader = self
            .clone_with_fresh_handle()?
            .set_schema_opt(Some(_schema));
        scan_reader.lock_state().reader = probe;
        tracing::debug!(with_columns = ?scan_opts.with_columns, "scan with_columns");

        let mut acc: Option<DataFrame> = None;
        loop {
            match scan_reader.wrrap_next_batch(&scan_opts) {
                Ok(Some(df)) => {
                    // skip unexpected empty DataFrames
                    if df.width() == 0 {
                        continue;
                    }
                    if let Some(ref mut acc_df) = acc {
                        let acc_names = acc_df.get_column_names();
                        let df_names = df.get_column_names();
                        if acc_names != df_names {
                            return Err(polars::error::PolarsError::ComputeError(
                                format!(
                                    "column mismatch between accumulated frame and new batch: acc={:?} df={:?}",
                                    acc_names, df_names
                                )
                                .into(),
                            ));
                        }
                        acc_df
                            .vstack_mut(&df)
                            .map_err(|e: polars::error::PolarsError| {
                                polars::error::PolarsError::ComputeError(e.to_string().into())
                            })?;
                    } else {
                        acc = Some(df);
                    }
                    // polars removes the Slice node after pushing `n_rows` down to an
                    // anonymous scan, so the cap has to be honoured here.
                    if let Some(n_rows) = scan_opts.n_rows
                        && acc
                            .as_ref()
                            .map(|df| df.height() >= n_rows)
                            .unwrap_or(false)
                    {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        let out_df = acc.unwrap_or_else(DataFrame::default);
        let out_df = match scan_opts.n_rows {
            Some(n_rows) if out_df.height() > n_rows => out_df.slice(0, n_rows),
            _ => out_df,
        };

        Ok(out_df)
    }

    /// `infer_schema_length` from polars wins over the reader's own value.
    fn schema(&self, infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        tracing::debug!(infer_schema_length = ?infer_schema_length, "schema requested");
        let infer_schema_length = infer_schema_length.unwrap_or(self.infer_schema_length);
        if let Some(ref schema) = self.schema {
            use polars::prelude::{DataType, Field, Schema};
            let mut fields = Vec::new();
            for (k, v) in schema.iter() {
                let dtype = match v {
                    crate::logfmt::SchemaField::String => DataType::String,
                    crate::logfmt::SchemaField::Integer => DataType::Int64,
                    crate::logfmt::SchemaField::Float => DataType::Float64,
                    crate::logfmt::SchemaField::Boolean => DataType::Boolean,
                    crate::logfmt::SchemaField::DateTime => {
                        DataType::Datetime(polars::prelude::TimeUnit::Microseconds, None)
                    }
                    crate::logfmt::SchemaField::Duration => DataType::Int64,
                    crate::logfmt::SchemaField::Auto => DataType::String,
                };
                fields.push(Field::new(k.into(), dtype));
            }
            let schema = Schema::from_iter_check_duplicates(fields)?;
            Ok(Arc::new(schema))
        } else {
            // Seekable sources are probed through a cloned handle; ssh and cursor
            // sources fall back to reading the first lines from the stream.
            let pred_schema = match self.predict_schema_from_first_line(infer_schema_length)? {
                Some(s) => Some(s),
                None => {
                    infer_schema_from_source(
                        &self.source,
                        &self.cmd,
                        self.line_filter.clone(),
                        infer_schema_length,
                        crate::ssh::connect_ssh,
                        ssh_reader,
                    )
                    .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?
                    .0
                }
            };
            if let Some(pred_schema) = pred_schema {
                let polars_schema = logfmt_schema_to_polars_schema(&pred_schema);
                Ok(Arc::new(polars_schema))
            } else {
                polars::error::polars_bail!(ComputeError: "no logfmt line found in the source; pass a schema to scan an empty source");
            }
        }
    }

    fn allows_predicate_pushdown(&self) -> bool {
        true
    }
    fn allows_projection_pushdown(&self) -> bool {
        true
    }
    // FIXME: support streaming after support streaming of anonymous scan.
}

impl LazyLogFmtReader {
    /// Pull the next batch. `scan_opts.n_rows` is the pushed-down slice, not a
    /// per-call batch size: once that many raw rows were read the reader is
    /// finished and further calls return `Ok(None)`.
    pub fn next_batch(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
        LazyLogFmtReader::debug_scan_opts(true, &scan_opts);

        self.wrrap_next_batch(&scan_opts)
    }

    fn debug_scan_opts(use_tracing: bool, scan_opts: &AnonymousScanArgs) -> String {
        if use_tracing {
            tracing::debug!(
                n_rows = ?scan_opts.n_rows,
                with_columns = ?scan_opts.with_columns,
                schema = ?scan_opts.output_schema,
                predicate = ?scan_opts.predicate,
                "dump scan_opts"
            );
        }

        format!(
            "n_rows:{:?} with_coumns:{:?} schema: {:?} predicate: {:?}",
            scan_opts.n_rows, scan_opts.with_columns, scan_opts.output_schema, scan_opts.predicate,
        )

        //String::new()
    }

    /// A poisoned lock is recovered: the state is a reader position that
    /// stays usable.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, LogFmtReaderState> {
        self.reader_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn max_inflight(&self) -> usize {
        match self.n_threads {
            Some(n_trhreads) => n_trhreads,
            None => {
                RAYON.current_num_threads()
                // let max_inflight = std::thread::available_parallelism()
                //     .map(|n| n.get())
                //     .unwrap_or(4);
                // std::cmp::max(1, max_inflight)
            }
        }
    }
    pub fn set_schema(&mut self, schema: Schema) {
        self.schema = Some(schema);
    }

    fn predict_schema_from_first_line(
        &self,
        infer_schema_length: usize,
    ) -> PolarsResult<Option<Schema>> {
        // extract the inner Arc<Mutex<...>> when source is Seekable
        let file_arc = match &self.source {
            LogFmtSource::Seekable(file_arc) => file_arc,
            _ => return Ok(None),
        };

        let file_opt = lock_seekable(file_arc);

        let Some(orig_file) = file_opt.as_ref() else {
            return Ok(None);
        };

        let Ok(mut probe) = orig_file.clone_handle() else {
            return Ok(None);
        };
        let frame_result = Self::frames_from_seekable(&mut *probe);
        probe
            .seek(0)
            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;

        if frame_result.is_none() {
            return Ok(None);
        }

        // Delegate to the handle-based helper and return its result.
        self.predict_schema_from_first_line_handle(probe, infer_schema_length)
    }

    fn predict_schema_from_first_line_handle(
        &self,
        probe: Box<dyn crate::SeekableVfsFile + Send>,
        infer_schema_length: usize,
    ) -> PolarsResult<Option<Schema>> {
        let mut br = BufReader::new(probe);
        infer_schema_from_reader(
            &mut br,
            self.line_filter.as_ref(),
            infer_schema_length,
            &mut Vec::new(),
        )
        .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))
    }

    fn parallel_batch(
        &self,
        scan_opts: &AnonymousScanArgs,
        schema: &mut Option<Schema>,
        line_filter: Option<LineFilterFn>,
        row_filter: Option<&RowFilter>,
        state: &mut LogFmtReaderState,
    ) -> PolarsResult<Option<DataFrame>> {
        match self._parallel_batch(scan_opts, schema, line_filter, row_filter, state) {
            Ok(r) => match r {
                Some(df) => Ok(Some(df)),
                None => {
                    tracing::debug!("empty result parallel_batch: fall down to single");
                    Ok(None)
                }
            },

            Err(e) => {
                tracing::debug!(error = %e, "fail parallel_batch");
                Err(e)
            }
        }
    }

    fn _parallel_batch(
        &self,
        scan_opts: &AnonymousScanArgs,
        schema: &mut Option<Schema>,
        line_filter: Option<LineFilterFn>,
        row_filter: Option<&RowFilter>,
        state: &mut LogFmtReaderState,
    ) -> PolarsResult<Option<DataFrame>> {
        let n_rows = scan_opts.n_rows;
        // Only attempt when source is Seekable
        tracing::debug!(
            n_rows = ?n_rows,
            n_threads = ?self.n_threads,
            "tparallel_batch entry",
        );

        let file_arc = match &self.source {
            LogFmtSource::Seekable(file_arc) => file_arc,
            _ => return Ok(None),
        };

        let file_opt = lock_seekable(file_arc);

        let Some(orig_file) = file_opt.as_ref() else {
            return Ok(None);
        };

        let Ok(mut probe) = orig_file.clone_handle() else {
            return Ok(None);
        };
        let Some(frames) = Self::frames_from_seekable(&mut *probe) else {
            return Ok(None);
        };

        // optional single-thread schema inference (extracted to helper)
        if schema.is_none()
            && let Some(pred) =
                self.predict_schema_from_first_line_handle(probe, self.infer_schema_length)?
        {
            *schema = Some(pred);
        }

        // process frames in bounded batches to avoid spawning excessive
        // queued tasks and cloning too many handles at once. Use
        // available parallelism as a conservative concurrency bound.
        let frame_ranges = frames;
        tracing::debug!(frames_len = frame_ranges.len(), "frames_len");
        let max_inflight = self.max_inflight();
        // the slice: `n_rows` slots the workers claim one raw row at a time
        let slots = AtomicUsize::new(0);
        let row_cap: Option<(usize, &AtomicUsize)> = n_rows.map(|n| (n, &slots));
        let schema_owned = schema.as_ref().cloned();
        let row_filter_owned: Option<RowFilter> = row_filter.map(|r| Arc::clone(r));
        let pred_owned = scan_opts.predicate.clone();

        // Process all frames with a single POOL.install: partition full frame_ranges
        // among `workers` and let each worker process its chunk sequentially.
        use rayon::prelude::*;
        let frame_vec: Vec<FrameRange> = frame_ranges.clone();
        let frames_len = frame_vec.len();
        if frames_len == 0 {
            return Ok(None);
        }
        let workers = std::cmp::min(max_inflight, frames_len);
        if workers == 0 {
            return Ok(None);
        }
        let workers = std::cmp::min(8, workers);
        let chunk_size = (frames_len + workers - 1) / workers;
        tracing::info!(
            frames_len = frames_len,
            workers = workers,
            chunk_size = chunk_size,
            max_inflight = max_inflight,
            n_threads = ?self.n_threads,
            "parallel scan summary",
        );
        tracing::debug!(
            workers = workers,
            chunk_size = chunk_size,
            "workers/chunk_size"
        );

        let schema_clone = schema_owned.clone();
        let row_filter_clone_outer = row_filter_owned.clone();
        let line_filter_copy = line_filter;
        let with_columns_owned: Option<Vec<String>> = scan_opts
            .with_columns
            .as_ref()
            .map(|v| v.iter().map(|s| s.to_string()).collect());

        const CLONE_HANDLE_FAILED_MSG: &str = "clone_handle failed";

        // Each worker will process its chunk of frames sequentially and return a DataFrame.
        let worker_results: PolarsResult<Vec<DataFrame>> = RAYON.install(|| {
            (0..workers)
                .into_par_iter()
                .filter(|w| *w * chunk_size < frames_len)
                .map(|w| {
                    let start = w * chunk_size;
                    let end_idx = std::cmp::min(start + chunk_size, frames_len);

                    match orig_file.clone_handle() {
                        Ok(h) => {
                            tracing::debug!(
                                worker = w,
                                start = start,
                                end = end_idx,
                                thread = ?std::thread::current().id(),
                                "worker start frames",
                            );

                            let start_time = std::time::Instant::now();

                            let pred_clone = pred_owned.clone();
                            let with_cols_local = with_columns_owned.clone();
                            let aligned_cols_cnt = self.aligned_cols_cnt;
                            let df = Self::read_and_parse_from_frames(
                                w,
                                h,
                                &frame_vec[start..end_idx],
                                schema_clone.as_ref(),
                                line_filter_copy.clone(),
                                row_filter_clone_outer.as_ref(),
                                pred_clone,
                                row_cap,
                                aligned_cols_cnt,
                                with_cols_local.as_ref(),
                            )?;

                            tracing::debug!(
                                worker = w,
                                start = start,
                                end = end_idx,
                                rows = df.height(),
                                thread = ?std::thread::current().id(),
                                elapsed = %humantime::format_duration(start_time.elapsed()),
                                "worker finished frames",
                            );
                            Ok(df)
                        }
                        Err(_) => Err(polars::error::PolarsError::ComputeError(
                            CLONE_HANDLE_FAILED_MSG.into(),
                        )),
                    }
                })
                .filter(|x| x.is_err() || x.as_ref().map(|df| df.height() > 0).unwrap_or(true))
                .collect::<PolarsResult<Vec<DataFrame>>>()
        });
        let df_all = match worker_results {
            Ok(df) => df,
            Err(e) => {
                if e.to_string().contains(CLONE_HANDLE_FAILED_MSG) {
                    tracing::debug!(
                        warning = %e,
                        "warning: clone_handle failed — falling back to single-threaded scan",
                    );
                    return Ok(None);
                } else {
                    tracing::debug!(error = %e, "error: multi thread scan ---end");
                    return Err(e);
                }
            }
        };
        if df_all.is_empty() {
            return Ok(None);
        }

        let start_acc_time = std::time::Instant::now();
        let df = match accumulate_dataframes_vertical(df_all) {
            Ok(df) => df,
            Err(e) => {
                tracing::debug!(error = %e, "error:accumulate_dataframes_vertical ");
                return Ok(None);
            }
        };
        tracing::debug!(elapsed = %humantime::format_duration(start_acc_time.elapsed()),
                "finish global accumulate_dataframes_vertical");
        // With a slice the workers returned at most `n_rows` raw rows in total:
        // filter and project them now. The slice is final, so the reader is
        // finished even when nothing passes the predicate.
        let df = match n_rows {
            Some(_) => apply_pushdown(
                df,
                scan_opts.predicate.as_ref(),
                with_columns_owned.as_ref(),
            )?,
            None => df,
        };
        state.finished = true;
        Ok(Some(df))
    }
    fn wrrap_next_batch(&self, scan_opts: &AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
        let batch_size = scan_opts.n_rows.or(self.batch_size);
        let mut schema = self.schema.as_ref().cloned();
        let line_filter = self.line_filter.clone();
        let row_filter = self.row_filter.as_ref();
        let mut state = self.lock_state();

        tracing::debug!(
            state_finish = state.finished,
            state_offset = state.offset,
            state_reader_is_none = state.reader.is_none(),
            "dump parameter to check pararell"
        );

        if state.finished {
            return Ok(None);
        }
        // Use a local copy because `self` is borrowed immutably.
        let use_parallel = self.use_parallel;
        // Try the parallel first-batch path when appropriate
        if state.reader.is_none()
            && use_parallel
            && state.offset == 0
            && let Some(df) = self.parallel_batch(
                &scan_opts,
                &mut schema,
                line_filter.clone(),
                row_filter,
                &mut state,
            )?
        {
            return Ok(Some(df));
        } else {
            self.single_batch(
                &scan_opts,
                batch_size,
                &mut schema,
                line_filter.clone(),
                row_filter,
                &mut state,
            )
        }
    }
    pub fn single_batch(
        &self,
        scan_opts: &AnonymousScanArgs,
        batch_size: Option<usize>,
        schema: &mut Option<Schema>,
        line_filter: Option<LineFilterFn>,
        row_filter: Option<&RowFilter>,
        state: &mut LogFmtReaderState,
    ) -> PolarsResult<Option<DataFrame>> {
        // Only attempt when source is Seekable
        tracing::debug!(
            batch_size = ?batch_size,
            n_threads = ?self.n_threads,
            "single_bacth entry",
        );
        if state.reader.is_none() {
            let reader: LineReader = match &self.source {
                LogFmtSource::Cursor(cursor) => {
                    let mut c = cursor.clone();
                    c.set_position(0);
                    Box::new(BufReader::new(c))
                }
                LogFmtSource::Ssh(ssh_source) => {
                    let real_cmd = self
                        .cmd
                        .clone()
                        .unwrap_or_else(|| format!("cat {}", ssh_source.path));
                    let stream = connect_ssh(ssh_source, None, None, &real_cmd).map_err(|e| {
                        polars::error::PolarsError::ComputeError(e.to_string().into())
                    })?;
                    let (_sess, reader) = ssh_reader(stream, ssh_source.path.ends_with(".zst"))
                        .map_err(|e| {
                            polars::error::PolarsError::ComputeError(e.to_string().into())
                        })?;
                    reader
                }
                LogFmtSource::Seekable(file_arc) => {
                    let file_opt = lock_seekable(file_arc);
                    let Some(file) = file_opt.as_ref() else {
                        polars::error::polars_bail!(ComputeError: "SeekableVfsFile missing");
                    };
                    let cloned_file = file.clone_handle().map_err(|e| {
                        polars::error::PolarsError::ComputeError(
                            format!("SeekableVfsFile clone failed: {e}").into(),
                        )
                    })?;
                    Box::new(std::io::BufReader::new(cloned_file)) as LineReader
                }
            };
            state.reader = Some(reader);
        }

        let reader = state.reader.as_mut().unwrap();

        // If no schema yet, try to predict it from a cloned seekable handle.
        if schema.is_none()
            && let Some(pred_schema) =
                self.predict_schema_from_first_line(self.infer_schema_length)?
        {
            *schema = Some(pred_schema);
        }

        // Accumulate parsed rows until we have enough (post-filter) or hit EOF.
        let mut accumulated: Vec<Row> = match batch_size {
            Some(bs) => Vec::with_capacity(bs),
            None => Vec::new(),
        };
        let mut total_lines_read = 0usize;
        let mut first_row_opt: Option<Row> = None;
        loop {
            let remaining_opt: Option<usize> = match batch_size {
                Some(bs) => {
                    if accumulated.len() >= bs {
                        Some(0)
                    } else {
                        Some(bs - accumulated.len())
                    }
                }
                None => None,
            };
            if remaining_opt == Some(0) {
                break;
            }
            let (mut rows, first_row, lines_read, eof) = Self::parse_batch_from_reader(
                reader,
                remaining_opt,
                schema.as_ref(),
                line_filter.as_ref(),
                row_filter,
                self.aligned_cols_cnt,
            )?;
            if first_row_opt.is_none() {
                first_row_opt = first_row;
            }
            total_lines_read += lines_read;
            accumulated.append(&mut rows);
            if match batch_size {
                Some(bs) => accumulated.len() >= bs,
                None => false,
            } || eof
            {
                if eof && accumulated.is_empty() {
                    state.offset += total_lines_read;
                    state.finished = true;
                    return Ok(None);
                }
                break;
            }
        }

        if schema.is_none()
            && let Some(first_row) = first_row_opt.as_ref()
        {
            *schema = Some(infer_schema_from_row(first_row));
        }

        let used_schema = match schema.as_ref() {
            Some(s) => s,
            None => {
                // No schema could be inferred and no rows; treat as finished.
                state.offset += total_lines_read;
                if total_lines_read == 0 {
                    state.finished = true;
                    return Ok(None);
                } else {
                    return Ok(None);
                }
            }
        };

        // A pushed-down `n_rows` means "the first n_rows raw rows, then the
        // predicate": `batch_size` equals `n_rows` in that case, so a full batch is
        // the whole slice and nothing further may be read.
        let raw_rows = accumulated.len();
        if scan_opts.n_rows.is_some_and(|n| raw_rows >= n) {
            state.finished = true;
        }

        let df = rows_to_dataframe_filled(&accumulated, Some(used_schema))
            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;

        // apply predicate/with_columns after accumulation (aligns with parallel path)
        let with_columns: Option<Vec<String>> = scan_opts
            .with_columns
            .as_ref()
            .map(|cols| cols.iter().map(|s| s.to_string()).collect());
        let df = apply_pushdown(df, scan_opts.predicate.as_ref(), with_columns.as_ref())?;

        // enforce batch_size on the returned frame (None = unlimited)
        let out = match batch_size {
            Some(bs) => {
                if df.height() > bs {
                    df.slice(0, bs)
                } else {
                    df
                }
            }
            None => df,
        };

        state.offset += total_lines_read;
        if total_lines_read == 0 {
            state.finished = true;
        }
        Ok(Some(out))
    }

    pub fn clone_with_fresh_handle(&self) -> PolarsResult<Self> {
        let lf: LineFilterFn = self
            .line_filter
            .clone()
            .unwrap_or_else(|| Arc::new(|_line: &str| true));

        LazyLogFmtReaderBuilder::new()
            .line_filter(move |s: &str| (lf)(s))
            .batch_size(self.batch_size)
            .aligned_cols_cnt(self.aligned_cols_cnt)
            .n_threads(self.n_threads)
            .infer_schema_length(self.infer_schema_length)
            .cmd(self.cmd.clone())
            .row_filter(self.row_filter.clone())
            .schema(self.schema.clone())
            .clone_source(&self.source)
            .and_then(LazyLogFmtReaderBuilder::build)
            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))
    }
    pub fn with_fresh_state(&self) -> PolarsResult<Self> {
        self.clone_with_fresh_handle()
    }

    // (moved helper to the builder)

    /// Derive coarse-grained frame ranges from a seekable file.
    /// Returns `None` on error (caller should fallback to single-threaded path).
    pub fn frames_from_seekable(file: &mut dyn crate::SeekableVfsFile) -> Option<Vec<FrameRange>> {
        // Prefer to consult SeekTable (decompressed-frame boundaries) when available.
        match file.seek_table_decomp_frames() {
            Some(frames) => Self::frames_from_seekable_with_seekable_zst(frames),
            None => Self::frames_from_seekable_without_seekable_zst(file),
        }
    }

    fn frames_from_seekable_with_seekable_zst(frames: Vec<(u64, u64)>) -> Option<Vec<FrameRange>> {
        let mut out: Vec<FrameRange> = Vec::with_capacity(frames.len());
        for (i, (start, len)) in frames.into_iter().enumerate() {
            out.push(FrameRange {
                idx: i,
                start,
                len,
                is_zst: true,
                comp_start: 0,
                comp_len: 0,
            });
        }
        Some(out)
    }

    fn frames_from_seekable_without_seekable_zst(
        file: &mut dyn crate::SeekableVfsFile,
    ) -> Option<Vec<FrameRange>> {
        // Fallback: create frames with custom end delimiter (cont)
        // MAX_FRAME_SIZE: 32MB (例として大きく)
        const MAX_FRAME_SIZE: u64 = 4 * 1024 * 1024; // 32MB
        const DELIM: u8 = b'\n'; // デフォルトは改行
        const CHECK_SIZE: usize = 4096; // DELIM探索時のバッファサイズ

        match file.size() {
            Ok(total) => {
                if total == 0 {
                    return None;
                }
                let mut out = Vec::new();
                let mut start = 0u64;
                let mut idx = 0;
                while start < total {
                    let mut end = std::cmp::min(start + MAX_FRAME_SIZE, total);
                    // 最終フレーム以外はDELIMで区切る
                    if end < total {
                        // DELIMの位置をCHECK_SIZEバイト単位で後ろから探す
                        let mut pos = end;
                        while pos > start {
                            let read_size = std::cmp::min(CHECK_SIZE as u64, pos - start) as usize;
                            let seek_pos = pos - read_size as u64;
                            if let Ok(_) = file.seek(seek_pos) {
                                let mut buf = vec![0u8; read_size];
                                if let Ok(n) = crate::SeekableVfsFile::read(file, &mut buf)
                                    && n > 0
                                {
                                    // バッファを後ろから前に向かってDELIM探索
                                    if let Some(relpos) = buf.iter().rposition(|&b| b == DELIM) {
                                        end = seek_pos + relpos as u64 + 1; // DELIMを含める
                                        break;
                                    }
                                }
                            }
                            if pos < read_size as u64 + start {
                                break;
                            }
                            pos -= read_size as u64;
                        }
                        // DELIMが見つからなければendはそのまま
                    }
                    let len = end - start;
                    out.push(FrameRange {
                        idx,
                        start,
                        len,
                        is_zst: false,
                        comp_start: 0,
                        comp_len: 0,
                    });
                    start = end;
                    idx += 1;
                }
                Some(out)
            }
            Err(_) => None,
        }
    }

    /// Read and parse contiguous frames from a cloned seekable handle.
    /// With `row_cap = (n, counter)`, stop once `counter` has reached `n`.
    pub fn read_and_parse_from_frames(
        worker: usize,
        mut handle: Box<dyn crate::SeekableVfsFile + Send>,
        ranges: &[FrameRange],
        schema: Option<&Schema>,
        line_filter: Option<LineFilterFn>,
        row_filter: Option<&RowFilter>,
        predicate: Option<Expr>,
        row_cap: Option<(usize, &AtomicUsize)>,
        aligned_cols_cnt: bool,
        with_columns: Option<&Vec<String>>,
    ) -> PolarsResult<DataFrame> {
        use crate::logfmt::{parse_logfmt_line, parse_logfmt_line_with_schema};

        let mut df_maker = DataFrameMakerBuilder::new()
            .schema(schema)
            .mode(Mode::ColumnerChunkedBuilder)
            .build();

        let mut column_keys: Vec<String> = Vec::new();
        let use_df_maker = true;
        // helper to process a single frame into optional DataFrame
        let process_one = |range: &FrameRange,
                           handle: &mut Box<dyn crate::SeekableVfsFile + Send>,
                           schema: Option<&Schema>,
                           column_keys: &mut Vec<String>,
                           df_maker: &mut DataFrameMaker,
                           buf: &mut Vec<u8>,
                           use_df_maker: bool,
                           capped: &mut bool|
         -> PolarsResult<Option<DataFrame>> {
            tracing::trace!(
                worker = worker,
                frame_idx = range.idx,
                start = range.start,
                len = range.len,
                "process_one start",
            );
            // the slice was filled by the other workers: do not read this frame
            if let Some((n, slots)) = row_cap
                && slots.load(Ordering::Relaxed) >= n
            {
                *capped = true;
                return Ok(None);
            }

            if range.len == 0 {
                return Ok(None);
            }

            handle
                .seek(range.start)
                .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;
            // Read the full (decompressed) frame into a single buffer using
            // larger reads to avoid many small SFTP/decoder read calls.

            buf.resize(range.len as usize, 0);

            let n = handle
                .read(buf)
                .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;

            buf.truncate(n);

            let cursor = Cursor::new(buf);
            let mut br = BufReader::new(cursor);
            let mut rows: Vec<Row> = Vec::new();
            let mut first_row: Option<Row> = None;

            // for line in buf
            //     .split(|&b| b == b'\n')
            //     .filter(|x| !x.is_empty())
            //     .filter_map(|x| std::str::from_utf8(x).ok())
            // {
            // for slice in buf.split(|&b| b == b'\n') {
            //     if slice.is_empty() {
            //         continue;
            //     }
            //     let line = match std::str::from_utf8(slice) {
            //         Ok(line) => line,
            //         Err(_) => continue,
            //     };

            loop {
                //line.clear();
                let mut line = String::new();

                let n = br.read_line(&mut line).map_err(|e: std::io::Error| {
                    polars::error::PolarsError::ComputeError(e.to_string().into())
                })?;
                if n == 0 {
                    tracing::trace!(worker = worker, frame_idx = range.idx, "process_one EOF");
                    break;
                }
                if line.trim().is_empty() {
                    continue;
                }
                if let Some(filter) = line_filter.as_ref()
                    && !filter(&line)
                {
                    continue;
                }
                if let Some((n, slots)) = row_cap
                    && slots.fetch_add(1, Ordering::Relaxed) >= n
                {
                    *capped = true;
                    break;
                }
                if first_row.is_none() && schema.is_none() {
                    let r = parse_logfmt_line(&line);
                    first_row = Some(r.clone());
                }
                if use_df_maker && first_row.is_none() && column_keys.is_empty() {
                    let r = parse_logfmt_line(&line);
                    first_row = Some(r.clone());
                }

                if aligned_cols_cnt
                    && column_keys.is_empty()
                    && let Some(first_row) = first_row.as_ref()
                {
                    *column_keys = first_row.keys().cloned().sorted().collect();
                }

                if aligned_cols_cnt && df_maker.column_keys.is_empty() && !column_keys.is_empty() {
                    df_maker.column_keys = column_keys.clone();
                }

                if use_df_maker {
                    // FIXME:  check aligned_cols_cnt. row_filter is not used?
                    if df_maker.schema.is_none()
                        && let Some(first_row) = first_row.as_ref()
                    {
                        df_maker.schema = Some(infer_schema_from_row(first_row));
                    }
                    if df_maker.n_rows_hint.is_none() {
                        df_maker.n_rows_hint = Some(120 * range.len as usize / line.len() / 100);
                    }

                    df_maker.push_line(&line);

                    continue;
                }

                let mut row = parse_logfmt_line_with_schema(&line, schema);
                if let Some(rf) = row_filter
                    && !rf(&row)
                {
                    continue;
                }
                if aligned_cols_cnt
                    && column_keys.len() > 0
                    // && !rows.is_empty()
                    && row.len() != column_keys.len()
                {
                    row.retain(|k, _| column_keys.contains(k));
                    tracing::trace!(
                        retain_keys = ?row.keys().cloned().collect::<Vec<String>>(),
                        "retain row",
                    );
                }
                if aligned_cols_cnt && column_keys.len() == 0 {
                    *column_keys = row.keys().cloned().collect();
                }

                rows.push(row);
            }

            let mut used_schema = schema.map(|s| s.clone());
            if used_schema.is_none()
                && let Some(first_row) = first_row.as_ref()
            {
                used_schema = Some(infer_schema_from_row(first_row));
            }
            if !use_df_maker && rows.is_empty() {
                tracing::trace!(
                    worker = worker,
                    frame_idx = range.idx,
                    "process_one parsed 0 rows"
                );
                return Ok(None);
            } else {
                tracing::trace!(
                    worker = worker,
                    frame_idx = range.idx,
                    parsed = rows.len(),
                    "process_one parsed rows"
                );
            }
            if use_df_maker {
                return Ok(None);
            }

            let df = rows_to_dataframe_filled(&rows, used_schema.as_ref())
                .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;

            Ok(Some(apply_pushdown(df, predicate.as_ref(), with_columns)?))
        };

        // Process each range sequentially until the chunk or the slice runs out.
        let mut parts: Vec<DataFrame> = Vec::new();
        let mut capped = false;

        // per-worker reusable buffer to avoid repeated allocations across frames
        let mut buf: Vec<u8> = Vec::new();

        for (i, range) in ranges.iter().enumerate() {
            let process_one_df = process_one(
                range,
                &mut handle,
                schema,
                &mut column_keys,
                &mut df_maker,
                &mut buf,
                use_df_maker,
                &mut capped,
            )?;

            if !use_df_maker && process_one_df.is_none() {
                continue;
            }
            if use_df_maker && i != ranges.len() - 1 && !capped {
                continue;
            }

            let mut df = if use_df_maker {
                df_maker.to_df()?
            } else {
                process_one_df.ok_or_else(|| {
                    polars::error::PolarsError::ComputeError("process_one returned no frame".into())
                })?
            };
            // `DataFrameMaker` only emits the columns listed in `column_keys`, which
            // stays empty unless `aligned_cols_cnt` is set. A 0-column frame is
            // discarded by the caller, which then falls back to the single-threaded
            // path; applying the pushed-down predicate/projection to it would fail.
            if use_df_maker && df.width() > 0 && row_cap.is_none() {
                df = apply_pushdown(df, predicate.as_ref(), with_columns)?;
            }

            if df.width() > 0 && df.height() > 0 {
                parts.push(df);
            }
            if capped {
                break;
            }
        }

        if parts.is_empty() {
            return Ok(DataFrame::default());
        }
        let start_acc_time = std::time::Instant::now();
        match accumulate_dataframes_vertical(parts) {
            Ok(df) => {
                tracing::debug!(elapsed = %humantime::format_duration(start_acc_time.elapsed()),
                "finish frame accumulate_dataframes_vertical");
                Ok(df)
            }
            Err(e) => {
                tracing::debug!(error = %e, "err: read_and_parse_from_frame");
                Ok(DataFrame::default())
            }
        }
    }

    /// Parse up to `batch_size` rows from a BufRead reader, applying filters and
    /// performing optional first-row capture for schema inference. Returns
    /// (rows, first_row_opt, lines_read, eof_flag).
    pub fn parse_batch_from_reader(
        reader: &mut dyn std::io::BufRead,
        batch_size: Option<usize>,
        schema: Option<&Schema>,
        line_filter: Option<&LineFilterFn>,
        row_filter: Option<&RowFilter>,
        aligned_cols_cnt: bool,
    ) -> PolarsResult<(Vec<Row>, Option<Row>, usize, bool)> {
        use crate::logfmt::{parse_logfmt_line, parse_logfmt_line_with_schema};

        let mut rows = match batch_size {
            Some(bs) => Vec::with_capacity(bs),
            None => Vec::new(),
        };
        let mut lines_read = 0usize;
        let mut first_row: Option<Row> = None;
        let mut column_keys: Vec<String> = Vec::new();

        loop {
            if let Some(bs) = batch_size {
                if rows.len() >= bs {
                    return Ok((rows, first_row, lines_read, false));
                }
            }
            let mut line = String::new();
            let n = reader.read_line(&mut line).map_err(|e: std::io::Error| {
                polars::error::PolarsError::ComputeError(e.to_string().into())
            })?;
            if n == 0 {
                return Ok((rows, first_row, lines_read, true));
            }
            if line.trim().is_empty() {
                continue;
            }
            if let Some(filter) = line_filter
                && !filter(&line)
            {
                continue;
            }
            lines_read += 1;
            let mut row = if let Some(s) = schema {
                parse_logfmt_line_with_schema(&line, Some(s))
            } else {
                let r = parse_logfmt_line(&line);
                if first_row.is_none() {
                    first_row = Some(r.clone());
                }
                r
            };

            if let Some(rf) = row_filter
                && !rf(&row)
            {
                continue;
            }
            if aligned_cols_cnt
                && column_keys.len() > 0
                // && !rows.is_empty()
                && row.len() != column_keys.len()
            {
                row.retain(|k, _| column_keys.contains(k));
                tracing::trace!(
                    retain_keys = ?row.keys().cloned().collect::<Vec<String>>(),
                    "retain row",
                );
            }
            if aligned_cols_cnt && column_keys.len() == 0 {
                column_keys = row.keys().cloned().collect();
            }

            rows.push(row);
        }
    }
    pub fn line_filter<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.line_filter = Some(Arc::new(f) as LineFilterFn);
        self
    }
    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }
    pub fn set_schema_opt(mut self, schema: Option<Schema>) -> Self {
        self.schema = schema;
        self
    }
    pub fn aligned_cols_cnt(mut self, v: bool) -> Self {
        self.aligned_cols_cnt = v;
        self
    }
    pub fn from_cursor(cursor: Cursor<Vec<u8>>) -> Self {
        LazyLogFmtReaderBuilder::new()
            .from_cursor(cursor)
            .build()
            .unwrap()
    }
    pub fn from_seekable_vfs_file(file: Box<dyn crate::SeekableVfsFile + Send>) -> Self {
        LazyLogFmtReaderBuilder::new()
            .from_seekable_vfs_file(file)
            .build()
            .unwrap()
    }
    pub fn scan_logfmt(mut self) -> Result<LazyFrame, polars::error::PolarsError> {
        use polars::prelude::ScanArgsAnonymous;
        use std::sync::Arc;
        // Infer the schema once here. Otherwise polars asks `schema()` and then
        // `scan()` infers again, which for an ssh command source means one extra
        // connection per collect. The probe stream is kept in `reader_state`
        // for the first `scan()` to continue from.
        if self.schema.is_none() {
            let (schema, probe) = infer_schema_from_source(
                &self.source,
                &self.cmd,
                self.line_filter.clone(),
                self.infer_schema_length,
                crate::ssh::connect_ssh,
                ssh_reader,
            )
            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;
            self.schema = Some(schema.ok_or_else(|| {
                polars::error::PolarsError::ComputeError(
                    "no logfmt line found in the source; pass a schema to scan an empty source"
                        .into(),
                )
            })?);
            self.lock_state().reader = probe;
        }
        let schema = self.schema.clone();

        let scan_args = ScanArgsAnonymous {
            schema: schema.map(|s| Arc::new(logfmt_schema_to_polars_schema(&s))),
            ..Default::default()
        };

        LazyFrame::anonymous_scan(Arc::new(self), scan_args)
    }
    pub fn scan(self) -> Result<LazyFrame, polars::error::PolarsError> {
        self.scan_logfmt()
    }
    pub fn new(source: SshSource) -> Self {
        LazyLogFmtReaderBuilder::new()
            .from_ssh_source(source)
            .build()
            .unwrap()
    }
}

/// Apply the pushed-down predicate and projection to a batch, in that order.
fn apply_pushdown(
    mut df: DataFrame,
    predicate: Option<&Expr>,
    with_columns: Option<&Vec<String>>,
) -> PolarsResult<DataFrame> {
    if let Some(pred) = predicate {
        use polars::prelude::IntoLazy;
        df = df
            .lazy()
            .filter(pred.clone())
            .collect()
            .map_err(|e| polars::error::PolarsError::ComputeError(e.to_string().into()))?;
    }
    if let Some(cols) = with_columns
        && !cols.is_empty()
    {
        df = df.select(cols)?;
    }
    Ok(df)
}

pub enum Mode {
    PerColumn,
    Columner,
    ColumnerChunkedBuilder,
}

struct DataFrameMakerBuilder {
    schema: Option<Schema>,
    mode: Mode,
}
impl DataFrameMakerBuilder {
    pub fn new() -> Self {
        Self {
            schema: None,
            mode: Mode::ColumnerChunkedBuilder,
        }
    }
    fn schema(mut self, schema: Option<&Schema>) -> Self {
        self.schema = schema.cloned();
        self
    }
    fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }
    pub fn build(self) -> DataFrameMaker {
        DataFrameMaker {
            columns: Vec::<Column>::new(),
            schema: self.schema,
            column_keys: Vec::<String>::new(),
            columns_as_line: HashMap::<String, Vec<ParsedValue>>::new(),
            mode: self.mode,
            n_rows_hint: None,
            column_infos: Vec::<ColumnInfo>::new(),
            row_seen: Vec::<usize>::new(),
            rows_pushed: 0,
            array_i64s: Vec::<
                polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Int64Type>,
            >::new(),
            array_timestamps: Vec::<
                polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Int64Type>,
            >::new(),
            array_duration: Vec::<
                polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Int64Type>,
            >::new(),
            array_f64s: Vec::<
                polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Float64Type>,
            >::new(),
            array_bools: Vec::<BooleanChunkedBuilder>::new(),
            array_strings: Vec::<StringChunkedBuilder>::new(),
        }
    }
}

pub fn set_parsed_value_to_col(col: &mut Column, key: &str, pv: &ParsedValue) {
    // Create a single-value Series from ParsedValue and append to column.
    let s = match *pv {
        ParsedValue::String(ref v) => polars::prelude::Series::new(key.into(), vec![v.clone()]),
        ParsedValue::Integer(i) => polars::prelude::Series::new(key.into(), &[i]),
        ParsedValue::Float(f) => polars::prelude::Series::new(key.into(), &[f]),
        ParsedValue::Boolean(b) => polars::prelude::Series::new(key.into(), &[b]),
        ParsedValue::DateTime(ref dt) => {
            // store datetime as RFC3339 string for now (owned)
            polars::prelude::Series::new(key.into(), vec![dt.to_rfc3339()])
        }
        ParsedValue::Duration(ref d) => polars::prelude::Series::new(
            key.into(),
            vec![format!("{}", humantime::format_duration(*d))],
        ),
    };
    col.into_materialized_series().append(&s).unwrap();
}

#[derive(Clone)]
struct ColumnInfo {
    name: String,
    data_type: DataType,
    index: usize,
}

struct DataFrameMaker {
    columns: Vec<Column>,
    schema: Option<Schema>,
    column_keys: Vec<String>,
    columns_as_line: hashbrown::HashMap<String, Vec<ParsedValue>>,
    mode: Mode,
    n_rows_hint: Option<usize>,
    // using mutable index mode
    column_infos: Vec<ColumnInfo>,
    // per `column_infos` entry: id (`rows_pushed + 1`) of the last row that carried it
    row_seen: Vec<usize>,
    rows_pushed: usize,
    array_i64s: Vec<PrimitiveChunkedBuilder<polars::prelude::Int64Type>>,
    array_timestamps: Vec<PrimitiveChunkedBuilder<polars::prelude::Int64Type>>,
    array_duration: Vec<PrimitiveChunkedBuilder<polars::prelude::Int64Type>>,
    array_f64s: Vec<PrimitiveChunkedBuilder<polars::prelude::Float64Type>>,
    array_bools: Vec<BooleanChunkedBuilder>,
    array_strings: Vec<StringChunkedBuilder>,
}

impl DataFrameMaker {
    #[allow(dead_code)]
    fn clear(&mut self) -> &mut Self {
        self.columns.clear();
        for (_k, v) in self.columns_as_line.iter_mut() {
            v.clear();
        }
        self
    }

    fn to_df(&mut self) -> PolarsResult<DataFrame> {
        if let Mode::Columner = self.mode {
            tracing::debug!("to_df: use mode columner");
            self.columns_as_lines_to_columns();
        } else if let Mode::ColumnerChunkedBuilder = self.mode {
            tracing::debug!("to_df: use mode chunked builder columner");
            // finalize mutable arrays into Series and build columns
            self.mutal_array_to_to_columns();
        } else {
            tracing::debug!("to_df: use mode per column");
        }

        // move out the columns vector without requiring ownership of self
        let cols = std::mem::take(&mut self.columns);
        DataFrame::new_infer_height(cols)
    }
    fn push_line(&mut self, line: &str) {
        match self.mode {
            Mode::PerColumn => self.push_line_to_column(line),
            Mode::Columner => self.push_line_to_columner_hash(line),
            Mode::ColumnerChunkedBuilder => self.push_line_to_column_mutable_array(line),
        }
    }

    fn push_line_to_column_mutable_array(&mut self, line: &str) {
        let column_info = &mut self.column_infos;
        let row_seen = &mut self.row_seen;
        // this row's id in `row_seen`; 0 is "never seen"
        let row_id = self.rows_pushed + 1;
        let mut hits = 0usize;
        let array_i64s = &mut self.array_i64s;
        let array_f64s = &mut self.array_f64s;
        let array_bools = &mut self.array_bools;
        let array_timestamps = &mut self.array_timestamps;
        let array_duration = &mut self.array_duration;
        let array_strings = &mut self.array_strings;

        crate::logfmt::handle_parse_logfmt_line_with_schema_base(line, |key, value| {
            if !self.column_keys.is_empty() && !self.column_keys.iter().any(|s| s.as_str() == key) {
                return;
            }

            let pos = match column_info.iter().position(|ci| ci.name == key) {
                Some(pos) => pos,
                None => {
                    let mut ci = match self.schema.as_ref().and_then(|s| s.get(key)).copied() {
                        Some(SchemaField::String) => ColumnInfo {
                            name: key.to_string(),
                            data_type: DataType::String,
                            index: 0,
                        },
                        Some(SchemaField::Integer) => ColumnInfo {
                            name: key.to_string(),
                            data_type: DataType::Int64,
                            index: 0,
                        },
                        Some(SchemaField::Float) => ColumnInfo {
                            name: key.to_string(),
                            data_type: DataType::Float64,
                            index: 0,
                        },
                        Some(SchemaField::Boolean) => ColumnInfo {
                            name: key.to_string(),
                            data_type: DataType::Boolean,
                            index: 0,
                        },
                        Some(SchemaField::DateTime) => ColumnInfo {
                            name: key.to_string(),
                            data_type: DataType::Datetime(TimeUnit::Microseconds, None),
                            index: 0,
                        },
                        Some(SchemaField::Duration) => ColumnInfo {
                            name: key.to_string(),
                            data_type: DataType::Duration(TimeUnit::Microseconds),
                            index: 0,
                        },
                        Some(SchemaField::Auto) | None => ColumnInfo {
                            name: key.to_string(),
                            data_type: DataType::String,
                            index: 0,
                        },
                    };
                    // let idx = self.append_column_arrays(&ci);
                    let idx = DataFrameMaker::base_append_column_arrays(
                        array_i64s,
                        array_f64s,
                        array_bools,
                        array_timestamps,
                        array_duration,
                        array_strings,
                        &ci,
                    );
                    ci.index = idx;
                    column_info.push(ci);
                    row_seen.push(0);
                    column_info.len() - 1
                }
            };
            row_seen[pos] = row_id;
            hits += 1;
            let col_info = &column_info[pos];

            match col_info.data_type {
                DataType::Int64 => {
                    let arr = &mut array_i64s[col_info.index];
                    arr.append_value(value.parse::<i64>().unwrap_or(0));
                }
                DataType::Float64 => {
                    let arr = &mut array_f64s[col_info.index];
                    arr.append_value(value.parse::<f64>().unwrap_or(0.0));
                }
                DataType::Boolean => {
                    let arr = &mut array_bools[col_info.index];
                    let v = match value.to_lowercase().as_str() {
                        "true" | "yes" | "on" => true,
                        "false" | "no" | "off" => false,
                        _ => false,
                    };
                    arr.append_value(v);
                }
                DataType::Datetime(TimeUnit::Microseconds, None) => {
                    let arr = &mut array_timestamps[col_info.index];
                    let v = DateTime::parse_from_rfc3339(value)
                        .map(|dt| dt.timestamp_micros())
                        .unwrap_or(0);
                    arr.append_value(v);
                }
                DataType::Duration(TimeUnit::Microseconds) => {
                    let arr = &mut array_duration[col_info.index];
                    let v = humantime::parse_duration(value)
                        .map(|d| (d.as_secs() as i64) * 1_000_000 + (d.subsec_micros() as i64))
                        .unwrap_or(0);
                    arr.append_value(v);
                }
                DataType::String => {
                    let arr = &mut array_strings[col_info.index];
                    arr.append_value(value);
                }
                _ => {
                    // unknown type output warning
                }
            };
        });

        // a column the line did not carry is null on this row
        if hits < column_info.len() {
            for (seen, ci) in row_seen.iter().zip(column_info.iter()) {
                if *seen != row_id {
                    DataFrameMaker::append_null_to_column_array(
                        array_i64s,
                        array_f64s,
                        array_bools,
                        array_timestamps,
                        array_duration,
                        array_strings,
                        ci,
                    );
                }
            }
        }
        self.rows_pushed = row_id;
    }

    /// Append a null to the builder of `ci`. Kept out of line: it runs only
    /// for ragged rows and would otherwise weigh on the per-key closure.
    #[cold]
    #[inline(never)]
    fn append_null_to_column_array(
        array_i64s: &mut [PrimitiveChunkedBuilder<polars::prelude::Int64Type>],
        array_f64s: &mut [PrimitiveChunkedBuilder<polars::prelude::Float64Type>],
        array_bools: &mut [BooleanChunkedBuilder],
        array_timestamps: &mut [PrimitiveChunkedBuilder<polars::prelude::Int64Type>],
        array_duration: &mut [PrimitiveChunkedBuilder<polars::prelude::Int64Type>],
        array_strings: &mut [StringChunkedBuilder],
        ci: &ColumnInfo,
    ) {
        match ci.data_type {
            DataType::Int64 => array_i64s[ci.index].append_null(),
            DataType::Float64 => array_f64s[ci.index].append_null(),
            DataType::Boolean => array_bools[ci.index].append_null(),
            DataType::Datetime(TimeUnit::Microseconds, None) => {
                array_timestamps[ci.index].append_null()
            }
            DataType::Duration(TimeUnit::Microseconds) => array_duration[ci.index].append_null(),
            DataType::String => array_strings[ci.index].append_null(),
            _ => {}
        }
    }
    #[allow(dead_code)]
    fn append_column_arrays(&mut self, ci: &ColumnInfo) -> usize {
        // let array_i64s = &mut self.array_i64s;
        // let array_f64s = &mut self.array_f64s;
        // let array_bools = &mut self.array_bools;
        // let array_timestamps = &mut self.array_timestamps;
        // let array_duration = &mut self.array_duration;
        // let array_strings = &mut self.array_strings;

        Self::base_append_column_arrays(
            &mut self.array_i64s,
            &mut self.array_f64s,
            &mut self.array_bools,
            &mut self.array_timestamps,
            &mut self.array_duration,
            &mut self.array_strings,
            &ci,
        )
    }
    pub fn base_append_column_arrays(
        array_i64s: &mut Vec<
            polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Int64Type>,
        >,
        array_f64s: &mut Vec<
            polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Float64Type>,
        >,
        array_bools: &mut Vec<polars_core::prelude::BooleanChunkedBuilder>,
        array_timestamps: &mut Vec<
            polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Int64Type>,
        >,
        array_duration: &mut Vec<
            polars_core::prelude::PrimitiveChunkedBuilder<polars::prelude::Int64Type>,
        >,
        array_strings: &mut Vec<polars_core::prelude::StringChunkedBuilder>,
        ci: &ColumnInfo,
    ) -> usize {
        match ci.data_type {
            DataType::Int64 => {
                let arr = polars_core::prelude::PrimitiveChunkedBuilder::<
                    polars::prelude::Int64Type,
                >::new(ci.name.as_str().into(), 1024);
                array_i64s.push(arr);
                array_i64s.len() - 1
            }
            DataType::Float64 => {
                let arr = polars_core::prelude::PrimitiveChunkedBuilder::<
                    polars::prelude::Float64Type,
                >::new(ci.name.as_str().into(), 1024);
                array_f64s.push(arr);
                array_f64s.len() - 1
            }
            DataType::Boolean => {
                let arr = BooleanChunkedBuilder::new(ci.name.as_str().into(), 1024);
                array_bools.push(arr);
                array_bools.len() - 1
            }
            DataType::Datetime(TimeUnit::Microseconds, None) => {
                let arr = polars_core::prelude::PrimitiveChunkedBuilder::<
                    polars::prelude::Int64Type,
                >::new(ci.name.as_str().into(), 1024);
                array_timestamps.push(arr);
                array_timestamps.len() - 1
            }
            DataType::Duration(TimeUnit::Microseconds) => {
                let arr = polars_core::prelude::PrimitiveChunkedBuilder::<
                    polars::prelude::Int64Type,
                >::new(ci.name.as_str().into(), 1024);
                array_duration.push(arr);
                array_duration.len() - 1
            }
            DataType::String => {
                let arr = StringChunkedBuilder::new(ci.name.as_str().into(), 1024);
                array_strings.push(arr);
                array_strings.len() - 1
            }
            _ => 0,
        }
    }

    fn push_line_to_columner_hash(&mut self, line: &str) {
        let schema_ref = self.schema.as_ref();
        let n_rows_hnt = self.n_rows_hint.unwrap_or(1024);

        crate::logfmt::handle_parse_logfmt_line_with_schema(line, schema_ref, |key, pv| {
            if !self.column_keys.is_empty() && !self.column_keys.iter().any(|s| s.as_str() == key) {
                return;
            }

            self.columns_as_line
                .raw_entry_mut()
                .from_key(key)
                .or_insert_with(|| {
                    (
                        key.to_string(),
                        Vec::<ParsedValue>::with_capacity(n_rows_hnt),
                        //Vec::<ParsedValue>::new(),
                    )
                })
                .1
                .push(pv)
            //.push(key.to_string(), pv)
            // .entry(key.to_owned())
            // .or_insert_with(Vec::<ParsedValue>::new)
            // .push(pv);

            // Try to reuse existing Vec (no allocation). Only allocate key String when missing.
            // if let Some(vec) = self.columns_as_line.get_mut(key) {
            //     vec.push(pv);
            // } else {
            //     self.columns_as_line.insert(key.to_owned(), vec![pv]);
            // }
        });
    }

    fn mutal_array_to_to_columns(&mut self) {
        for key in self.column_keys.iter() {
            let Some(ci) = self
                .column_infos
                .iter()
                .find(|ci| ci.name == key.to_string())
            else {
                continue;
            };
            let col =
                match ci.data_type {
                    DataType::Int64 => {
                        polars::prelude::Column::from(
                            std::mem::replace(
                                &mut self.array_i64s[ci.index],
                                polars::prelude::PrimitiveChunkedBuilder::<
                                    polars::prelude::Int64Type,
                                >::new(
                                    polars::prelude::PlSmallStr::EMPTY, 0
                                ),
                            )
                            .finish()
                            .into_series(),
                        )
                    }
                    DataType::Float64 => {
                        polars::prelude::Column::from(
                            std::mem::replace(
                                &mut self.array_f64s[ci.index],
                                polars::prelude::PrimitiveChunkedBuilder::<
                                    polars::prelude::Float64Type,
                                >::new(
                                    polars::prelude::PlSmallStr::EMPTY, 0
                                ),
                            )
                            .finish()
                            .into_series(),
                        )
                    }
                    DataType::Boolean => polars::prelude::Column::from(
                        std::mem::replace(
                            &mut self.array_bools[ci.index],
                            polars::prelude::BooleanChunkedBuilder::new(
                                polars::prelude::PlSmallStr::EMPTY,
                                0,
                            ),
                        )
                        .finish()
                        .into_series(),
                    ),
                    DataType::Datetime(TimeUnit::Microseconds, None) => {
                        polars::prelude::Column::from(
                            std::mem::replace(
                                &mut self.array_timestamps[ci.index],
                                polars::prelude::PrimitiveChunkedBuilder::<
                                    polars::prelude::Int64Type,
                                >::new(
                                    polars::prelude::PlSmallStr::EMPTY, 0
                                ),
                            )
                            .finish()
                            .into_datetime(TimeUnit::Microseconds, None)
                            .into_series(),
                        )
                    }
                    DataType::Duration(TimeUnit::Microseconds) => {
                        polars::prelude::Column::from(
                            std::mem::replace(
                                &mut self.array_duration[ci.index],
                                polars::prelude::PrimitiveChunkedBuilder::<
                                    polars::prelude::Int64Type,
                                >::new(
                                    polars::prelude::PlSmallStr::EMPTY, 0
                                ),
                            )
                            .finish()
                            .into_series(),
                        )
                    }
                    _ => polars::prelude::Column::from(
                        std::mem::replace(
                            &mut self.array_strings[ci.index],
                            polars::prelude::StringChunkedBuilder::new(
                                polars::prelude::PlSmallStr::EMPTY,
                                0,
                            ),
                        )
                        .finish()
                        .into_series(),
                    ),
                };

            self.columns.push(col);
        }
    }

    fn columns_as_lines_to_columns(&mut self) {
        let columns = &mut self.columns;

        for key in self.column_keys.iter() {
            //        for (key, values) in self.columns_as_line.iter() {
            let values = self.columns_as_line.remove(key).unwrap();
            if values.first().is_none() {
                continue;
            }
            //let v = values.first().unwrap();
            let v = &values[0];
            let col = match v {
                ParsedValue::Duration(_) => {
                    let cvalues: Vec<i64> = values
                        .into_iter()
                        .map(|pv| match pv {
                            ParsedValue::Duration(dur) => {
                                (dur.as_secs() as i64) * 1_000_000 + (dur.subsec_micros() as i64)
                            }
                            ParsedValue::Integer(i) => i,
                            ParsedValue::Float(f) => f as i64,
                            ParsedValue::Boolean(b) => {
                                if b {
                                    1
                                } else {
                                    0
                                }
                            }
                            ParsedValue::String(s) => s.parse().unwrap_or(0),
                            ParsedValue::DateTime(_) => 0,
                        })
                        .collect();
                    Column::new(key.as_str().into(), cvalues)
                }
                ParsedValue::String(_) => {
                    let cvalues: Vec<String> =
                        values.into_iter().map(|x| x.into_string()).collect();
                    Column::new(key.as_str().into(), cvalues)
                }
                ParsedValue::Integer(_) => {
                    let cvalues: Vec<i64> = values
                        .into_iter()
                        .map(|pv| match pv {
                            ParsedValue::Integer(i) => i,
                            ParsedValue::Float(f) => f as i64,
                            ParsedValue::Boolean(b) => {
                                if b {
                                    1
                                } else {
                                    0
                                }
                            }
                            ParsedValue::String(s) => s.parse().unwrap_or(0),
                            ParsedValue::DateTime(_) => 0,
                            ParsedValue::Duration(dur) => {
                                (dur.as_secs() as i64) * 1_000_000 + (dur.subsec_micros() as i64)
                            }
                        })
                        .collect();
                    Column::new(key.as_str().into(), cvalues)
                }
                ParsedValue::Float(_) => {
                    let cvalues: Vec<f64> = values
                        .into_iter()
                        .map(|pv| match pv {
                            ParsedValue::Integer(i) => i as f64,
                            ParsedValue::Float(f) => f,
                            ParsedValue::Boolean(b) => {
                                if b {
                                    1.0
                                } else {
                                    0.0
                                }
                            }
                            ParsedValue::String(s) => s.parse().unwrap_or(0.0),
                            ParsedValue::DateTime(_) => 0.0,
                            ParsedValue::Duration(dur) => dur.as_secs_f64(),
                        })
                        .collect();
                    Column::new(key.as_str().into(), cvalues)
                }
                ParsedValue::Boolean(_) => {
                    let cvalues: Vec<bool> = values
                        .into_iter()
                        .map(|pv| match pv {
                            ParsedValue::Boolean(b) => b,
                            ParsedValue::Integer(i) => i != 0,
                            ParsedValue::Float(f) => f != 0.0,
                            ParsedValue::String(s) => {
                                matches!(s.to_lowercase().as_str(), "true" | "yes" | "on" | "1")
                            }
                            ParsedValue::DateTime(_) => false,
                            ParsedValue::Duration(dur) => {
                                dur.as_secs() != 0 || dur.subsec_micros() != 0
                            }
                        })
                        .collect();
                    Column::new(key.as_str().into(), cvalues)
                }
                ParsedValue::DateTime(_) => {
                    let cvalues: Vec<i64> = values
                        .into_iter()
                        .map(|pv| match pv {
                            ParsedValue::DateTime(dt) => dt.timestamp_micros(),
                            ParsedValue::String(s) => {
                                DateTime::parse_from_rfc3339(&s).unwrap().timestamp_micros()
                            }
                            ParsedValue::Integer(i) => i,
                            ParsedValue::Float(f) => chrono::DateTime::from_timestamp(f as i64, 0)
                                .unwrap()
                                .timestamp_micros(),
                            ParsedValue::Boolean(_) => chrono::DateTime::from_timestamp(0, 0)
                                .unwrap()
                                .timestamp_micros(),
                            ParsedValue::Duration(_) => chrono::DateTime::from_timestamp(0, 0)
                                .unwrap()
                                .timestamp_micros(),
                        })
                        .collect();
                    let series = Series::new(key.as_str().into(), &cvalues)
                        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                        .unwrap();
                    Column::Series(series.into())
                }
            };
            columns.push(col);
        }
    }

    fn push_line_to_column(&mut self, line: &str) {
        let schema_ref = self.schema.as_ref();
        let columns = &mut self.columns;

        // First pass: determine the column type

        crate::logfmt::handle_parse_logfmt_line_with_schema(line, schema_ref, |key, pv| {
            //map.insert(key.to_string(), pv);

            let _found = self.column_keys.iter().any(|s| s.as_str() == key);
            let _is_empty = self.column_keys.is_empty();

            if !self.column_keys.is_empty() && !self.column_keys.iter().any(|s| s.as_str() == key) {
                return;
            }

            let succ = match columns.iter_mut().find(|c| c.name() == key) {
                Some(col) => {
                    set_parsed_value_to_col(col, key, &pv);
                    true
                }
                None => false,
            };
            if succ {
                return;
            }

            let schema_field = schema_ref.and_then(|s| s.get(key));

            // First pass: determine the column type
            let mut has_string = false;
            let mut has_int = false;
            let mut has_float = false;
            let mut has_bool = false;
            let mut has_datetime = false;
            let mut has_duration = false;

            match pv {
                ParsedValue::String(_) => has_string = true,
                ParsedValue::Integer(_) => has_int = true,
                ParsedValue::Float(_) => has_float = true,
                ParsedValue::Boolean(_) => has_bool = true,
                ParsedValue::DateTime(_) => has_datetime = true,
                ParsedValue::Duration(_) => has_duration = true,
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
            match inferred_type.as_deref() {
                Some("String") => {
                    columns.push(Column::new(key.into(), vec![pv.as_string()]));
                }
                Some("Int") => {
                    columns.push(
                        //polars::prelude::Series::new(
                        match pv {
                            ParsedValue::Integer(i) => Column::new(key.into(), vec![i]),
                            ParsedValue::Float(f) => Column::new(key.into(), vec![f as i64]),
                            ParsedValue::Boolean(b) => {
                                Column::new(key.into(), vec![if b { 1 } else { 0 }])
                            }
                            ParsedValue::String(s) => {
                                Column::new(key.into(), vec![s.parse().unwrap_or(0)])
                            }
                            ParsedValue::DateTime(_) => Column::new(key.into(), vec![0]),
                            ParsedValue::Duration(dur) => Column::new(
                                key.into(),
                                vec![
                                    (dur.as_secs() as i64) * 1_000_000
                                        + (dur.subsec_micros() as i64),
                                ],
                            ),
                        },
                    );
                }
                Some("Float") => {
                    columns.push(match pv {
                        ParsedValue::Float(f) => Column::new(key.into(), vec![f]),
                        ParsedValue::Integer(i) => Column::new(key.into(), vec![i as f64]),
                        ParsedValue::Boolean(b) => {
                            Column::new(key.into(), vec![if b { 1.0 } else { 0.0 }])
                        }
                        ParsedValue::String(s) => {
                            Column::new(key.into(), vec![s.parse().unwrap_or(0.0)])
                        }
                        ParsedValue::DateTime(_) => Column::new(key.into(), vec![0.0]),
                        ParsedValue::Duration(dur) => {
                            Column::new(key.into(), vec![dur.as_secs_f64()])
                        }
                    });
                }
                Some("Bool") => {
                    columns.push(match pv {
                        ParsedValue::Boolean(b) => Column::new(key.into(), vec![b]),
                        ParsedValue::Integer(i) => Column::new(key.into(), vec![i != 0]),
                        ParsedValue::Float(f) => Column::new(key.into(), vec![f != 0.0]),
                        ParsedValue::String(s) => Column::new(
                            key.into(),
                            vec![matches!(
                                s.to_lowercase().as_str(),
                                "true" | "yes" | "on" | "1"
                            )],
                        ),
                        ParsedValue::DateTime(_) => Column::new(key.into(), vec![false]),
                        ParsedValue::Duration(dur) => Column::new(
                            key.into(),
                            vec![dur.as_secs() != 0 || dur.subsec_micros() != 0],
                        ),
                    });
                }
                Some("DateTime") => {
                    columns.push(Column::new(
                        key.into(),
                        match pv {
                            ParsedValue::DateTime(dt) => vec![dt.to_rfc3339()],
                            ParsedValue::String(s) => {
                                vec![s]
                            }
                            ParsedValue::Integer(i) => {
                                vec![chrono::DateTime::from_timestamp(i, 0).unwrap().to_rfc3339()]
                            }
                            ParsedValue::Float(f) => {
                                vec![
                                    chrono::DateTime::from_timestamp(f as i64, 0)
                                        .unwrap()
                                        .to_rfc3339(),
                                ]
                            }
                            ParsedValue::Boolean(_) => {
                                vec![chrono::DateTime::from_timestamp(0, 0).unwrap().to_rfc3339()]
                            }
                            ParsedValue::Duration(dur) => {
                                vec![format!("{}", humantime::format_duration(dur))]
                            }
                        },
                    ));
                }
                _ => {
                    columns.push(Column::new(key.into(), vec![pv.as_string()]));
                }
            };
        });
        //map
    }
}

pub type LazyFrameFn = fn(LazyLogFmtReaderBuilder) -> polars::prelude::LazyFrame;

/// Builder source kinds
pub enum SourceSpec {
    Path(String),
    Cursor(Cursor<Vec<u8>>),
    Seekable(Box<dyn crate::SeekableVfsFile + Send>),
    Ssh(SshSource),
}

impl Clone for SourceSpec {
    fn clone(&self) -> Self {
        match self {
            SourceSpec::Path(s) => SourceSpec::Path(s.clone()),
            SourceSpec::Cursor(c) => SourceSpec::Cursor(c.clone()),
            SourceSpec::Seekable(f) => {
                // Attempt to produce a fresh independent handle via clone_handle()
                // If clone_handle fails, fall back to the original handle by attempting
                // to open a new handle via the trait (panic on failure as a last resort).
                match f.clone_handle() {
                    Ok(h) => SourceSpec::Seekable(h),
                    Err(_) => SourceSpec::Seekable(
                        f.clone_handle()
                            .expect("SeekableVfsFile clone_handle failed"),
                    ),
                }
            }
            SourceSpec::Ssh(s) => SourceSpec::Ssh(s.clone()),
        }
    }
}

pub struct LazyLogFmtReaderBuilder {
    source: Option<SourceSpec>,
    line_filter: LineFilterFn,
    cmd: Option<String>,
    batch_size: Option<usize>,
    row_filter: Option<RowFilter>,
    schema: Option<Schema>,
    aligned_cols_cnt: bool,
    n_threads: Option<usize>,
    use_parallel: bool,
    infer_schema_length: usize,
}

impl Clone for LazyLogFmtReaderBuilder {
    fn clone(&self) -> Self {
        Self {
            source: self.source.as_ref().map(|s| s.clone()),
            line_filter: self.line_filter.clone(),
            cmd: self.cmd.clone(),
            batch_size: self.batch_size,
            row_filter: self.row_filter.as_ref().map(Arc::clone),
            schema: self.schema.clone(),
            aligned_cols_cnt: self.aligned_cols_cnt,
            n_threads: self.n_threads,
            use_parallel: true,
            infer_schema_length: self.infer_schema_length,
        }
    }
}

impl LazyLogFmtReaderBuilder {
    pub fn new_with_default(line_filter: LineFilterFn) -> Self {
        Self {
            source: None,
            line_filter,
            cmd: None,
            batch_size: None,
            row_filter: None,
            schema: None,
            aligned_cols_cnt: false,
            n_threads: None,
            use_parallel: true,
            infer_schema_length: DEFAULT_INFER_SCHEMA_LENGTH,
        }
    }

    pub fn new() -> Self {
        Self::new_with_default(Arc::new(|_line: &str| true))
    }

    pub fn line_filter<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        self.line_filter = Arc::new(f) as LineFilterFn;
        self
    }

    /// Set source by path (will be opened as seekable VFS during `build`).
    pub fn source(mut self, source: Option<String>) -> Self {
        self.source = source.map(SourceSpec::Path);
        self
    }

    /// Use an in-memory cursor as source.
    pub fn from_cursor(mut self, cursor: Cursor<Vec<u8>>) -> Self {
        self.source = Some(SourceSpec::Cursor(cursor));
        self
    }

    /// Use an already-open SeekableVfsFile as source.
    pub fn from_seekable_vfs_file(mut self, file: Box<dyn crate::SeekableVfsFile + Send>) -> Self {
        self.source = Some(SourceSpec::Seekable(file));
        self
    }

    /// Use an explicit SSH source.
    pub fn from_ssh_source(mut self, ssh: SshSource) -> Self {
        self.source = Some(SourceSpec::Ssh(ssh));
        self
    }

    pub fn cmd(mut self, cmd: Option<String>) -> Self {
        self.cmd = cmd;
        self
    }

    pub fn batch_size(mut self, bs: Option<usize>) -> Self {
        self.batch_size = bs;
        self
    }

    pub fn row_filter(mut self, rf: Option<RowFilter>) -> Self {
        self.row_filter = rf;
        self
    }

    pub fn schema(mut self, schema: Option<Schema>) -> Self {
        self.schema = schema;
        self
    }

    pub fn aligned_cols_cnt(mut self, v: bool) -> Self {
        self.aligned_cols_cnt = v;
        self
    }

    pub fn n_threads(mut self, n: Option<usize>) -> Self {
        self.n_threads = n;
        self
    }

    /// Rows read to infer the schema when none is given; how their types
    /// combine is described on [`crate::LogfmtScanOpts::infer_schema_length`].
    pub fn infer_schema_length(mut self, n: usize) -> Self {
        self.infer_schema_length = n;
        self
    }

    // keep single `line_filter` implementation (builder). If duplicate exists elsewhere,
    // remove to avoid multiple definitions.
    fn spec_2_log_fmt_src(spec: Option<SourceSpec>) -> anyhow::Result<LogFmtSource> {
        match spec {
            Some(SourceSpec::Path(path)) => {
                let vfs = crate::ssh_vfs::SshSeekableZstdVfs::new();
                let file = vfs
                    .open(&path)
                    .map_err(|e| anyhow::anyhow!("open {path}: {e}"))?;
                Ok(LogFmtSource::Seekable(std::sync::Arc::new(
                    std::sync::Mutex::new(Some(file)),
                )))
            }
            Some(SourceSpec::Seekable(file)) => Ok(LogFmtSource::Seekable(std::sync::Arc::new(
                std::sync::Mutex::new(Some(file)),
            ))),
            Some(SourceSpec::Cursor(c)) => Ok(LogFmtSource::Cursor(c)),
            Some(SourceSpec::Ssh(s)) => Ok(LogFmtSource::Ssh(s)),
            None => Err(anyhow::anyhow!("LazyLogFmtReaderBuilder.source is not set")),
        }
    }
    fn clone_source(mut self, o_src: &LogFmtSource) -> anyhow::Result<Self> {
        self.source = Some(match o_src {
            LogFmtSource::Seekable(file_arc) => {
                let file_opt = lock_seekable(file_arc);
                let file = file_opt
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("SeekableVfsFile missing"))?;
                let handle = file
                    .clone_handle()
                    .map_err(|e| anyhow::anyhow!("SeekableVfsFile clone failed: {e}"))?;
                SourceSpec::Seekable(handle)
            }
            LogFmtSource::Cursor(c) => SourceSpec::Cursor(c.clone()),
            LogFmtSource::Ssh(s) => SourceSpec::Ssh(s.clone()),
        });
        Ok(self)
    }

    pub fn build(self) -> anyhow::Result<LazyLogFmtReader> {
        if self.source.is_none() {
            return Err(anyhow::anyhow!("LazyLogFmtReaderBuilder.source is not set"));
        }

        let source = self.source;
        let line_filter = self.line_filter.clone();
        let cmd = self.cmd;
        let batch_size = self.batch_size;
        let row_filter = self.row_filter;
        let aligned_cols_cnt = self.aligned_cols_cnt;
        let n_threads = self.n_threads;
        let schema = self.schema;

        let lf = line_filter;

        let source_final = Self::spec_2_log_fmt_src(source)?;

        Ok(LazyLogFmtReader {
            source: source_final,
            cmd,
            batch_size,
            row_filter: row_filter.as_ref().map(Arc::clone),
            line_filter: Some(lf),
            schema,
            aligned_cols_cnt,
            reader_state: Arc::new(Mutex::new(LogFmtReaderState {
                reader: None,
                offset: 0,
                finished: false,
            })),
            n_threads,
            use_parallel: self.use_parallel,
            infer_schema_length: self.infer_schema_length,
        })
    }
}
