use crate::logfmt::Row;
use anyhow::{Result, anyhow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterSpec {
    Always,
    Eq { key: String, value: String },
    NotEq { key: String, value: String },
    Contains { key: String, value: String },
    NotContains { key: String, value: String },
}

pub fn parse_filter_spec(spec: Option<&str>) -> Result<FilterSpec> {
    let Some(spec) = spec else {
        return Ok(FilterSpec::Always);
    };
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(FilterSpec::Always);
    }

    for (op, kind) in [
        ("!~", "not_contains"),
        ("~", "contains"),
        ("!=", "not_eq"),
        ("=", "eq"),
    ] {
        if let Some((key, value)) = spec.split_once(op) {
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() || value.is_empty() {
                return Err(anyhow!("invalid filter spec: {spec}"));
            }
            return Ok(match kind {
                "not_contains" => FilterSpec::NotContains {
                    key: key.to_string(),
                    value: value.to_string(),
                },
                "contains" => FilterSpec::Contains {
                    key: key.to_string(),
                    value: value.to_string(),
                },
                "not_eq" => FilterSpec::NotEq {
                    key: key.to_string(),
                    value: value.to_string(),
                },
                _ => FilterSpec::Eq {
                    key: key.to_string(),
                    value: value.to_string(),
                },
            });
        }
    }

    Err(anyhow!("invalid filter spec: {spec}"))
}

pub fn filter_row(row: &Row, filter: &FilterSpec) -> bool {
    match filter {
        FilterSpec::Always => true,
        FilterSpec::Eq { key, value } => row
            .get(key)
            .is_some_and(|current| current.as_string() == *value),
        FilterSpec::NotEq { key, value } => row
            .get(key)
            .is_none_or(|current| current.as_string() != *value),
        FilterSpec::Contains { key, value } => row
            .get(key)
            .is_some_and(|current| current.as_string().contains(value)),
        FilterSpec::NotContains { key, value } => row
            .get(key)
            .is_none_or(|current| !current.as_string().contains(value)),
    }
}
