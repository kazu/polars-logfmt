pub mod cli;
pub mod filter;
pub mod lazy;
pub mod logfmt;
pub mod scan;
pub mod ssh;
pub use lazy::LazyLogFmtReader;
pub use scan::{LogfmtScanOpts, scan_logfmt};
pub use ssh::SshSource;
pub mod seekable_vfs;
pub mod ssh_vfs;
pub mod stream;
pub mod time_filter;
pub use seekable_vfs::*;

#[cfg(test)]
mod tests {
    use super::{
        filter::{FilterSpec, filter_row, parse_filter_spec},
        logfmt::parse_logfmt_line,
        ssh::parse_ssh_source,
        stream::rows_to_dataframe,
    };

    #[test]
    fn parse_valid_source() {
        let source = parse_ssh_source("ssh://user@example.com/var/log/app.log").unwrap();
        assert_eq!(source.user, "user");
        assert_eq!(source.host, "example.com");
        assert_eq!(source.port, 22);
        assert_eq!(source.path, "/var/log/app.log");
    }

    #[test]
    fn parse_valid_source_with_port() {
        let source = parse_ssh_source("ssh://user@example.com:2222/var/log/app.log").unwrap();
        assert_eq!(source.port, 2222);
    }

    #[test]
    fn parse_invalid_scheme() {
        let err = parse_ssh_source("http://user@example.com/var/log/app.log").unwrap_err();
        assert!(err.to_string().contains("scheme"));
    }

    #[test]
    fn parse_missing_user() {
        let err = parse_ssh_source("ssh://example.com/var/log/app.log").unwrap_err();
        assert!(err.to_string().contains("user"));
    }

    #[test]
    fn parse_missing_path() {
        let err = parse_ssh_source("ssh://user@example.com/").unwrap_err();
        assert!(err.to_string().contains("path"));
    }

    #[test]
    fn parse_logfmt_basic() {
        use crate::logfmt::ParsedValue;
        let row = parse_logfmt_line("level=info msg=hello code=200");
        assert_eq!(
            row.get("level"),
            Some(&ParsedValue::String("info".to_string()))
        );
        assert_eq!(
            row.get("msg"),
            Some(&ParsedValue::String("hello".to_string()))
        );
        assert_eq!(row.get("code"), Some(&ParsedValue::Integer(200)));
    }

    #[test]
    fn parse_logfmt_quoted() {
        use crate::logfmt::ParsedValue;
        let row = parse_logfmt_line("msg=\"hello world\" user=alice");
        assert_eq!(
            row.get("msg"),
            Some(&ParsedValue::String("hello world".to_string()))
        );
        assert_eq!(
            row.get("user"),
            Some(&ParsedValue::String("alice".to_string()))
        );
    }

    #[test]
    fn rows_to_dataframe_union_keys() {
        let row1 = parse_logfmt_line("a=1 b=2");
        let row2 = parse_logfmt_line("b=3 c=4");
        let df = rows_to_dataframe(&[row1, row2]).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3);
    }

    #[test]
    fn parse_filter_spec_contains() {
        let filter = parse_filter_spec(Some("msg~finish")).unwrap();
        assert_eq!(
            filter,
            FilterSpec::Contains {
                key: "msg".to_string(),
                value: "finish".to_string()
            }
        );
    }

    #[test]
    fn filter_row_not_contains() {
        let filter = FilterSpec::NotContains {
            key: "new".to_string(),
            value: "healthcheck".to_string(),
        };
        let row = parse_logfmt_line("new=alpha msg=ok");
        assert!(filter_row(&row, &filter));
        let row = parse_logfmt_line("new=healthcheck msg=ok");
        assert!(!filter_row(&row, &filter));
    }
}
