use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "polars_logfmt",
    version,
    about = "Stream logfmt over SSH and process with Polars"
)]
pub struct Args {
    /// Source: a local path or an SSH URL (e.g. ssh://user@host/path)
    #[arg(long)]
    pub source: Option<String>,

    /// SSH private key path (optional)
    #[arg(long)]
    pub key_path: Option<String>,

    /// SSH password (optional)
    #[arg(long)]
    pub password: Option<String>,

    /// Remote command to execute (optional). If omitted, uses `cat <path>` from source.
    #[arg(long)]
    pub cmd: Option<String>,

    /// Number of rows per batch
    #[arg(long, default_value_t = 1000)]
    pub batch_size: usize,

    /// Preview rows to display per batch
    #[arg(long, default_value_t = 5)]
    pub preview_rows: usize,

    /// Aggregation plan (comma-separated). e.g. "count,count_by:status"
    #[arg(long, value_delimiter = ',', default_value = "count")]
    pub agg: Vec<String>,

    /// Aggregation filter. e.g. "level=info", "new!~healthcheck"
    #[arg(long)]
    pub agg_filter: Option<String>,

    /// Use streaming pipeline
    #[arg(long)]
    pub streaming: bool,
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::Parser;

    #[test]
    fn parses_streaming_flag() {
        let args = Args::parse_from(["polars_logfmt", "--streaming"]);
        assert!(args.streaming);
    }
}
