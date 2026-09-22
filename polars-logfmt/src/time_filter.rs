use crate::logfmt::Row;
use anyhow::Result;
use chrono::{DateTime, FixedOffset};
use polars::prelude::{AnyValue, BooleanChunked, DataFrame, NewChunkedArray};

pub fn parse_rfc3339(value: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(value).ok()
}

pub fn row_in_time_range(
    row: &Row,
    column: &str,
    start: &DateTime<FixedOffset>,
    end: &DateTime<FixedOffset>,
) -> bool {
    row.get(column)
        .map(|value| value.as_string())
        .and_then(|value| parse_rfc3339(&value))
        .is_some_and(|time| time >= *start && time <= *end)
}

pub fn filter_df_by_time_range(
    df: &DataFrame,
    column: &str,
    start: &DateTime<FixedOffset>,
    end: &DateTime<FixedOffset>,
) -> Result<DataFrame> {
    let Ok(series) = df.column(column) else {
        return Ok(df.clone());
    };

    let mut mask = Vec::with_capacity(series.len());
    for idx in 0..series.len() {
        let keep = match series.get(idx) {
            Ok(AnyValue::String(value)) => {
                parse_rfc3339(value).is_some_and(|time| time >= *start && time <= *end)
            }
            Ok(AnyValue::StringOwned(value)) => {
                parse_rfc3339(&value).is_some_and(|time| time >= *start && time <= *end)
            }
            _ => false,
        };
        mask.push(keep);
    }

    let mask = BooleanChunked::from_slice(polars::prelude::PlSmallStr::from("mask"), &mask);
    Ok(df.filter(&mask)?)
}

#[cfg(test)]
mod tests {
    use super::{filter_df_by_time_range, row_in_time_range};
    use crate::logfmt::{ParsedValue, Row};
    use chrono::{DateTime, FixedOffset};
    use polars::prelude::{DataFrame, IntoColumn, NamedFrom, Series};

    fn rfc3339(value: &str) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(value).unwrap()
    }

    #[test]
    fn row_in_time_range_matches() {
        let mut row = Row::new();
        row.insert(
            "time".to_string(),
            ParsedValue::String("2026-01-21T12:00:00+09:00".to_string()),
        );
        let start = rfc3339("2026-01-21T00:00:00+09:00");
        let end = rfc3339("2026-01-21T23:59:59+09:00");
        assert!(row_in_time_range(&row, "time", &start, &end));
    }

    #[test]
    fn row_in_time_range_outside() {
        let mut row = Row::new();
        row.insert(
            "time".to_string(),
            ParsedValue::String("2026-01-22T00:00:00+09:00".to_string()),
        );
        let start = rfc3339("2026-01-21T00:00:00+09:00");
        let end = rfc3339("2026-01-21T23:59:59+09:00");
        assert!(!row_in_time_range(&row, "time", &start, &end));
    }

    #[test]
    fn filter_df_by_time_range_filters_rows() {
        let time_col = Series::new(
            "time".into(),
            vec![
                Some("2026-01-21T01:00:00+09:00".to_string()),
                Some("2026-01-22T01:00:00+09:00".to_string()),
            ],
        );
        let df = DataFrame::new_infer_height(vec![time_col.into_column()]).unwrap();
        let start = rfc3339("2026-01-21T00:00:00+09:00");
        let end = rfc3339("2026-01-21T23:59:59+09:00");
        let filtered = filter_df_by_time_range(&df, "time", &start, &end).unwrap();
        assert_eq!(filtered.height(), 1);
    }
}
