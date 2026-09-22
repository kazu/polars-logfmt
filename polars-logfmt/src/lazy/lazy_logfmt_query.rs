// LazyLogFmtQuery: logfmtクエリ構造体とその実装
use crate::lazy::{LineFilter, LogFmtSource, RowFilter};
use polars::prelude::{DataFrame, Expr};

pub struct LazyLogFmtQuery {
    source: LogFmtSource,
    cmd: Option<String>,
    batch_size: usize,
    filter_expr: Option<Expr>,
    row_filter: Option<RowFilter>,
    line_filter: Option<LineFilter>,
}

impl LazyLogFmtQuery {
    pub fn filter(mut self, expr: Expr) -> Self {
        self.filter_expr = Some(expr);
        self
    }

    pub fn agg(self, _aggs: Vec<Expr>) -> DataFrame {
        self.try_agg(vec![]).unwrap_or_else(|err| {
            eprintln!("aggregation failed: {err}");
            DataFrame::new(vec![]).unwrap_or_else(|_| DataFrame::default())
        })
    }

    pub fn try_agg(self, _aggs: Vec<Expr>) -> polars::prelude::Result<DataFrame> {
        anyhow::bail!(
            "scan_logfmt: streaming/batchはAnonymousScan/next_batchで実装されています。LazyFrameはscan()で取得してください。"
        );
    }
}
