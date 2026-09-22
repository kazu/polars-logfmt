

DEBUG_BIN=../target/debug/polars_logfmt
# Log file to profile: make profile SOURCE=/path/to/app.log.seek.zst
SOURCE?=/path/to/app.log.seek.zst

profile: ../target/profiling/polars_logfmt
	samply record ../target/profiling/polars_logfmt --source "$(SOURCE)"

../target/debug/polars_logfmt: src/*.rs
	cargo build

../target/profiling/polars_logfmt: src/*.rs
	RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile profiling

debug: ../target/debug/polars_logfmt
	../target/debug/polars_logfmt

cov:
	cargo llvm-cov --lcov --output-path lcov.info
