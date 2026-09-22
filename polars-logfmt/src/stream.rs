use crate::logfmt::Row;

use anyhow::Result;
use polars::prelude::DataFrame;

use std::collections::BTreeSet;

use polars::prelude::Column;

pub fn rows_to_dataframe(rows: &[Row]) -> Result<DataFrame> {
    let mut keys = BTreeSet::new();
    for row in rows {
        keys.extend(row.keys().cloned());
    }

    let mut columns: Vec<Column> = Vec::with_capacity(keys.len());
    for key in keys {
        let mut col_data: Vec<Option<String>> = Vec::with_capacity(rows.len());
        for row in rows {
            col_data.push(row.get(&key).map(|v| v.as_string()));
        }
        columns.push(Column::new(key.as_str().into(), col_data));
    }

    Ok(DataFrame::new_infer_height(columns)?)
}
