use chrono::{DateTime, FixedOffset};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    DateTime(DateTime<FixedOffset>),
    Duration(Duration),
}

impl ParsedValue {
    pub fn as_string(&self) -> String {
        match self {
            ParsedValue::String(s) => s.clone(),
            ParsedValue::Integer(i) => i.to_string(),
            ParsedValue::Float(f) => f.to_string(),
            ParsedValue::Boolean(b) => b.to_string(),
            ParsedValue::DateTime(dt) => dt.to_rfc3339(),
            ParsedValue::Duration(d) => format!("{}", humantime::format_duration(*d)),
        }
    }

    pub fn into_string(self) -> String {
        match self {
            ParsedValue::String(s) => s,
            ParsedValue::Integer(i) => i.to_string(),
            ParsedValue::Float(f) => f.to_string(),
            ParsedValue::Boolean(b) => b.to_string(),
            ParsedValue::DateTime(dt) => dt.to_rfc3339(),
            ParsedValue::Duration(d) => format!("{}", humantime::format_duration(d)),
        }
    }
    pub fn infer_type(value: &str) -> Self {
        // Try parsing as RFC3339 DateTime first
        if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
            return ParsedValue::DateTime(dt);
        }
        // Try parsing as integer
        if let Ok(i) = i64::from_str(value) {
            return ParsedValue::Integer(i);
        }
        // Try parsing as Duration first (e.g., "1s", "500ms")
        if let Ok(dur) = humantime::parse_duration(value) {
            return ParsedValue::Duration(dur);
        }

        // Try parsing as float
        if let Ok(f) = f64::from_str(value) {
            return ParsedValue::Float(f);
        }
        // Try parsing as boolean
        match value.to_lowercase().as_str() {
            "true" | "yes" | "on" => return ParsedValue::Boolean(true),
            "false" | "no" | "off" => return ParsedValue::Boolean(false),
            _ => {}
        }
        // Default to string
        ParsedValue::String(value.to_string())
    }
}

pub type Row = HashMap<String, ParsedValue>;
pub type Schema = BTreeMap<String, SchemaField>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaField {
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
    Duration,
    Auto,
}

pub fn parse_logfmt_line(line: &str) -> Row {
    parse_logfmt_line_with_schema(line, None)
}

pub fn handle_parse_logfmt_line_with_schema_base<C>(line: &str, mut callback: C)
where
    C: FnMut(&str, &str),
{
    // Byte-slice based parser: faster and simpler for ASCII separators.
    let bytes = line.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;

    while i < n {
        // skip ASCII whitespace
        while i < n && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }

        // parse key start..end (byte indices)
        let key_start = i;
        while i < n && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key_end = i;
        if key_end == key_start {
            // skip until whitespace
            while i < n && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        let key = match std::str::from_utf8(&bytes[key_start..key_end]) {
            Ok(s) => s,
            Err(_) => {
                // invalid utf8 key; skip token
                while i < n && !bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                continue;
            }
        };

        // skip spaces to '='
        while i < n && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= n || bytes[i] != b'=' {
            // malformed, skip to next token
            while i < n && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        // consume '='
        i += 1;

        // parse value
        if i >= n {
            continue;
        }

        let parsed_value = if bytes[i] == b'"' {
            // quoted value: find closing quote, respect simple escapes
            i += 1; // after opening quote
            let val_start = i;
            let mut val_end = val_start;
            while i < n {
                if bytes[i] == b'\\' {
                    // skip escaped byte (keep it in slice)
                    i = i.saturating_add(2);
                    continue;
                }
                if bytes[i] == b'"' {
                    val_end = i;
                    i += 1; // consume closing quote
                    break;
                }
                i += 1;
            }
            if val_end == val_start {
                val_end = std::cmp::min(i, n);
            }
            let value_slice = &bytes[val_start..val_end];
            unsafe { std::str::from_utf8_unchecked(value_slice) }
            //std::str::from_utf8(value_slice).unwrap_or("")
        } else {
            // unquoted value: up to next whitespace
            let val_start = i;
            while i < n && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            let val_end = i;
            let value_slice = &bytes[val_start..val_end];

            unsafe { std::str::from_utf8_unchecked(value_slice) }
        };

        callback(key, parsed_value);
    }
}

pub fn handle_parse_logfmt_line_with_schema<C>(line: &str, schema: Option<&Schema>, mut callback: C)
where
    C: FnMut(&str, ParsedValue),
{
    handle_parse_logfmt_line_with_schema_base(line, |key, value_str| {
        let parsed_value = if let Some(schema) = schema {
            match schema.get(key) {
                Some(SchemaField::String) => ParsedValue::String(value_str.to_string()),
                Some(SchemaField::Integer) => {
                    ParsedValue::Integer(i64::from_str(value_str).unwrap_or(0))
                }
                Some(SchemaField::Float) => {
                    ParsedValue::Float(f64::from_str(value_str).unwrap_or(0.0))
                }
                Some(SchemaField::Boolean) => {
                    let b = matches!(value_str.to_lowercase().as_str(), "true" | "yes" | "on");
                    ParsedValue::Boolean(b)
                }
                Some(SchemaField::Duration) => humantime::parse_duration(value_str)
                    .map(ParsedValue::Duration)
                    .unwrap_or_else(|_| ParsedValue::String(value_str.to_string())),
                Some(SchemaField::DateTime) => DateTime::parse_from_rfc3339(value_str)
                    .map(ParsedValue::DateTime)
                    .unwrap_or_else(|_| ParsedValue::String(value_str.to_string())),
                Some(SchemaField::Auto) | None => ParsedValue::String(value_str.to_string()),
            }
        } else {
            ParsedValue::infer_type(value_str)
        };
        callback(key, parsed_value);
    });
}

pub fn parse_logfmt_line_with_schema(line: &str, schema: Option<&Schema>) -> Row {
    let mut map: HashMap<String, ParsedValue> = HashMap::new();

    handle_parse_logfmt_line_with_schema(line, schema, |key, pv| {
        map.insert(key.to_string(), pv);
    });
    map
}

// DEPREATED:
pub fn old_parse_logfmt_line_with_schema(line: &str, schema: Option<&Schema>) -> Row {
    let mut map = HashMap::new();
    let mut it = line.char_indices().peekable();

    while let Some(&(idx, ch)) = it.peek() {
        if ch.is_whitespace() {
            it.next();
            continue;
        }

        // key
        let key_start = idx;
        let mut key_end = idx;
        while let Some(&(i, c)) = it.peek() {
            if c == '=' || c.is_whitespace() {
                key_end = i;
                break;
            }
            key_end = i + c.len_utf8();
            it.next();
        }

        if key_end == key_start {
            while let Some(&(_, c)) = it.peek() {
                if c.is_whitespace() {
                    break;
                }
                it.next();
            }
            continue;
        }

        let key = &line[key_start..key_end];

        if it.peek().map(|p| p.1) != Some('=') {
            while let Some(&(_, c)) = it.peek() {
                if c.is_whitespace() {
                    break;
                }
                it.next();
            }
            continue;
        }
        it.next(); // consume '='

        // value
        let parsed_value = if let Some(&(_, next_ch)) = it.peek() {
            if next_ch == '"' {
                it.next(); // consume '"'
                let mut value = String::new();
                while let Some((_, c)) = it.next() {
                    if c == '\\' {
                        if let Some((_, nextc)) = it.next() {
                            value.push(nextc);
                        }
                        continue;
                    }
                    if c == '"' {
                        break;
                    }
                    value.push(c);
                }

                if let Some(schema) = schema {
                    match schema.get(key) {
                        Some(SchemaField::String) => ParsedValue::String(value),
                        Some(SchemaField::Integer) => {
                            ParsedValue::Integer(i64::from_str(&value).unwrap_or(0))
                        }
                        Some(SchemaField::Float) => {
                            ParsedValue::Float(f64::from_str(&value).unwrap_or(0.0))
                        }
                        Some(SchemaField::Boolean) => {
                            let b = matches!(value.to_lowercase().as_str(), "true" | "yes" | "on");
                            ParsedValue::Boolean(b)
                        }
                        Some(SchemaField::Duration) => humantime::parse_duration(&value)
                            .map(ParsedValue::Duration)
                            .unwrap_or_else(|_| ParsedValue::String(value)),
                        Some(SchemaField::DateTime) => DateTime::parse_from_rfc3339(&value)
                            .map(ParsedValue::DateTime)
                            .unwrap_or_else(|_| ParsedValue::String(value)),
                        Some(SchemaField::Auto) | None => ParsedValue::String(value),
                    }
                } else {
                    ParsedValue::String(value)
                }
            } else {
                // unquoted -> zero-copy
                let val_start = it.peek().map(|p| p.0).unwrap_or(line.len());
                let mut val_end = val_start;
                while let Some(&(i, c)) = it.peek() {
                    if c.is_whitespace() {
                        val_end = i;
                        break;
                    }
                    val_end = i + c.len_utf8();
                    it.next();
                }
                if it.peek().is_none() {
                    val_end = line.len();
                }
                let value_slice = &line[val_start..val_end];

                if let Some(schema) = schema {
                    match schema.get(key) {
                        Some(SchemaField::String) => ParsedValue::String(value_slice.to_string()),
                        Some(SchemaField::Integer) => {
                            ParsedValue::Integer(i64::from_str(value_slice).unwrap_or(0))
                        }
                        Some(SchemaField::Float) => {
                            ParsedValue::Float(f64::from_str(value_slice).unwrap_or(0.0))
                        }
                        Some(SchemaField::Boolean) => {
                            let b = matches!(
                                value_slice.to_lowercase().as_str(),
                                "true" | "yes" | "on"
                            );
                            ParsedValue::Boolean(b)
                        }
                        Some(SchemaField::Duration) => humantime::parse_duration(value_slice)
                            .map(ParsedValue::Duration)
                            .unwrap_or_else(|_| ParsedValue::String(value_slice.to_string())),
                        Some(SchemaField::DateTime) => DateTime::parse_from_rfc3339(value_slice)
                            .map(ParsedValue::DateTime)
                            .unwrap_or_else(|_| ParsedValue::String(value_slice.to_string())),
                        Some(SchemaField::Auto) | None => ParsedValue::infer_type(value_slice),
                    }
                } else {
                    ParsedValue::infer_type(value_slice)
                }
            }
        } else {
            continue;
        };

        map.insert(key.to_string(), parsed_value);
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_type_datetime() {
        let val = ParsedValue::infer_type("2026-01-22T12:00:00+09:00");
        assert!(matches!(val, ParsedValue::DateTime(_)));
    }

    #[test]
    fn test_infer_type_integer() {
        let val = ParsedValue::infer_type("42");
        assert_eq!(val, ParsedValue::Integer(42));
    }

    #[test]
    fn test_infer_type_float() {
        let val = ParsedValue::infer_type(std::f64::consts::PI.to_string().as_str());
        assert_eq!(val, ParsedValue::Float(std::f64::consts::PI));
    }

    #[test]
    fn test_infer_type_boolean_true() {
        assert_eq!(ParsedValue::infer_type("true"), ParsedValue::Boolean(true));
        assert_eq!(ParsedValue::infer_type("yes"), ParsedValue::Boolean(true));
    }

    #[test]
    fn test_infer_type_boolean_false() {
        assert_eq!(
            ParsedValue::infer_type("false"),
            ParsedValue::Boolean(false)
        );
        assert_eq!(ParsedValue::infer_type("no"), ParsedValue::Boolean(false));
    }

    #[test]
    fn test_infer_type_string() {
        let val = ParsedValue::infer_type("hello");
        assert_eq!(val, ParsedValue::String("hello".to_string()));
    }

    #[test]
    fn test_parse_logfmt_with_datetime() {
        let line = "time=2026-01-13T00:01:01.727+09:00 level=INFO msg=\"test\"";
        let row = parse_logfmt_line(line);

        assert!(matches!(row.get("time"), Some(ParsedValue::DateTime(_))));
        assert_eq!(
            row.get("level"),
            Some(&ParsedValue::String("INFO".to_string()))
        );
        assert_eq!(
            row.get("msg"),
            Some(&ParsedValue::String("test".to_string()))
        );
    }

    #[test]
    fn test_parse_logfmt_quoted_datetime() {
        let line = "time=\"2026-01-22T12:00:00+09:00\" status=200";
        let row = parse_logfmt_line(line);
        let row_str = row.get("time").unwrap().as_string();
        // Quoted values are treated as strings
        assert_eq!(
            row.get("time"),
            Some(&ParsedValue::infer_type("2026-01-22T12:00:00+09:00")),
            "actual={}",
            row_str,
        );
        assert_eq!(row.get("status"), Some(&ParsedValue::Integer(200)));
    }

    #[test]
    fn test_schema_datetime_override() {
        let mut schema = Schema::new();
        schema.insert("ts".to_string(), SchemaField::DateTime);

        let line = "ts=2026-01-22T12:00:00+09:00";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));

        assert!(matches!(row.get("ts"), Some(ParsedValue::DateTime(_))));
    }

    #[test]
    fn test_schema_string_override() {
        let mut schema = Schema::new();
        schema.insert("code".to_string(), SchemaField::String);

        let line = "code=200";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));

        // Override integer inference to string
        assert_eq!(
            row.get("code"),
            Some(&ParsedValue::String("200".to_string()))
        );
    }

    #[test]
    fn test_as_string_datetime() {
        let dt = DateTime::parse_from_rfc3339("2026-01-22T12:00:00+09:00").unwrap();
        let val = ParsedValue::DateTime(dt);
        let s = val.as_string();
        assert_eq!(s, "2026-01-22T12:00:00+09:00");
    }

    #[test]
    fn test_schema_integer_override() {
        let mut schema = Schema::new();
        schema.insert("count".to_string(), SchemaField::Integer);

        // Quoted value "123" would normally be parsed as String
        let line = "count=\"123\"";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));

        // Schema override forces Integer parsing
        assert_eq!(row.get("count"), Some(&ParsedValue::Integer(123)));
    }

    #[test]
    fn test_schema_float_override() {
        let mut schema = Schema::new();
        schema.insert("rate".to_string(), SchemaField::Float);

        // "2.5" without schema might be parsed as String depending on quotes
        let line = "rate=\"2.5\"";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));

        // Schema override forces Float parsing
        assert_eq!(row.get("rate"), Some(&ParsedValue::Float(2.5)));
    }

    #[test]
    fn test_schema_boolean_override() {
        let mut schema = Schema::new();
        schema.insert("active".to_string(), SchemaField::Boolean);

        // "true" would normally be parsed as String if quoted
        let line = "active=true";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));

        // Schema override forces Boolean parsing
        assert_eq!(row.get("active"), Some(&ParsedValue::Boolean(true)));
    }

    #[test]
    fn test_schema_boolean_false_override() {
        let mut schema = Schema::new();
        schema.insert("enabled".to_string(), SchemaField::Boolean);

        // "false" would normally be parsed as String if quoted
        let line = "enabled=false";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));

        // Schema override forces Boolean parsing to false
        assert_eq!(row.get("enabled"), Some(&ParsedValue::Boolean(false)));
    }

    #[test]
    fn test_schema_mixed_types() {
        let mut schema = Schema::new();
        schema.insert("id".to_string(), SchemaField::Integer);
        schema.insert("score".to_string(), SchemaField::Float);
        schema.insert("enabled".to_string(), SchemaField::Boolean);
        schema.insert("timestamp".to_string(), SchemaField::DateTime);

        let line = "id=\"999\" score=\"95.5\" enabled=yes timestamp=2026-01-22T12:00:00+09:00";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));

        assert_eq!(row.get("id"), Some(&ParsedValue::Integer(999)));
        assert_eq!(row.get("score"), Some(&ParsedValue::Float(95.5)));
        assert_eq!(row.get("enabled"), Some(&ParsedValue::Boolean(true)));
        assert!(matches!(
            row.get("timestamp"),
            Some(ParsedValue::DateTime(_))
        ));
    }

    #[test]
    fn test_infer_type_duration() {
        let val = ParsedValue::infer_type("1s");
        assert!(matches!(val, ParsedValue::Duration(_)));

        let val2 = ParsedValue::infer_type("500ms");
        assert!(matches!(val2, ParsedValue::Duration(_)));

        // exact equality with humantime parse
        let expected = humantime::parse_duration("1s").unwrap();
        assert_eq!(val, ParsedValue::Duration(expected));
    }

    #[test]
    fn test_parse_logfmt_with_duration_unquoted() {
        let line = "dur=1s level=INFO msg=ok";
        let row = parse_logfmt_line(line);
        assert!(matches!(row.get("dur"), Some(ParsedValue::Duration(_))));
        assert_eq!(
            row.get("level"),
            Some(&ParsedValue::String("INFO".to_string()))
        );
        assert_eq!(row.get("msg"), Some(&ParsedValue::String("ok".to_string())));
    }

    #[test]
    fn test_parse_logfmt_quoted_duration_is_string() {
        let line = "dur=\"1s\"";
        let row = parse_logfmt_line(line);
        // Quoted value should be treated as string by default
        let expected = humantime::parse_duration("1s").unwrap();
        assert_eq!(row.get("dur"), Some(&ParsedValue::Duration(expected)));
    }

    #[test]
    fn test_schema_duration_override_parses_quoted() {
        let mut schema = Schema::new();
        schema.insert("dur".to_string(), SchemaField::Duration);

        let line = "dur=\"1s\"";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));
        assert!(matches!(row.get("dur"), Some(ParsedValue::Duration(_))));
        // and its value equals parsed duration
        let expected = humantime::parse_duration("1s").unwrap();
        assert_eq!(row.get("dur"), Some(&ParsedValue::Duration(expected)));
    }

    #[test]
    fn test_infer_type_fractional_duration() {
        let val = ParsedValue::infer_type("0.123s");
        assert!(matches!(val, ParsedValue::Duration(_)));

        let expected = humantime::parse_duration("0.123s").unwrap();
        assert_eq!(val, ParsedValue::Duration(expected));
    }
    #[test]
    fn test_infer_type_zero() {
        let val = ParsedValue::infer_type("0");
        assert!(matches!(val, ParsedValue::Integer(_)));
        assert_eq!(val, ParsedValue::Integer(0));
    }
    #[test]
    fn test_infer_type_zero_sec() {
        let val = ParsedValue::infer_type("0s");
        assert!(matches!(val, ParsedValue::Duration(_)));

        let expected = humantime::parse_duration("0s").unwrap();
        assert_eq!(val, ParsedValue::Duration(expected));
    }

    #[test]
    fn test_parse_logfmt_with_fractional_duration_unquoted() {
        let line = "dur=0.123s level=INFO msg=ok";
        let row = parse_logfmt_line(line);
        assert!(matches!(row.get("dur"), Some(ParsedValue::Duration(_))));
        let expected = humantime::parse_duration("0.123s").unwrap();
        assert_eq!(row.get("dur"), Some(&ParsedValue::Duration(expected)));
    }

    #[test]
    fn test_schema_fractional_duration_override_parses_quoted() {
        let mut schema = Schema::new();
        schema.insert("dur".to_string(), SchemaField::Duration);

        let line = "dur=\"0.123s\"";
        let row = parse_logfmt_line_with_schema(line, Some(&schema));
        assert!(matches!(row.get("dur"), Some(ParsedValue::Duration(_))));
        let expected = humantime::parse_duration("0.123s").unwrap();
        assert_eq!(row.get("dur"), Some(&ParsedValue::Duration(expected)));
    }
}
