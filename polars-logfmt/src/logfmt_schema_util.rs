use crate::logfmt::SchemaField;
use polars::prelude::TimeUnit;
use polars::prelude::{DataType, Field, Schema};
use std::collections::BTreeMap;

pub fn logfmt_schema_to_polars_schema(map: &BTreeMap<String, SchemaField>) -> Schema {
    let mut fields = Vec::new();
    for (k, v) in map.iter() {
        let dtype = match v {
            SchemaField::String => DataType::String,
            SchemaField::Integer => DataType::Int64,
            SchemaField::Float => DataType::Float64,
            SchemaField::Boolean => DataType::Boolean,
            SchemaField::DateTime => DataType::Datetime(TimeUnit::Milliseconds, None),
            SchemaField::Auto => DataType::String,
        };
        fields.push(Field::new(k.into(), dtype));
    }
    Schema::from(fields)
}
