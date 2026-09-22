## spec

rust で実装する。Polars を使う

### データのサンプル

```
time=2026-01-13T00:01:01.688+09:00 level=INFO msg="finish to process/write but fail to remove input file" inputs.0=/data/in/4g_mme_ericsson/chy1-MME-501n_202601122346_371.csv.gz writer_number=1 handler_number=6 error="remove /data/in/4g_mme_ericsson/chy1-MME-501n_202601122346_371.csv.gz: no such file or directory"
time=2026-01-13T00:01:01.688+09:00 level=INFO msg="finish to process/write " old=/data/out/VTEL_MME_FCT/2/20260113/00/2_chy1-MME-501n_202601122346_371.cur new=/data/out/VTEL_MME_FCT/2/20260113/00/2_chy1-MME-501n_202601122346_371.tbl.zst writer_number=1 handler_number=6 old_file_bytes=233318 bytes=260217
```

## 作業

1. ssh 先のlogfmt のファイルをストリームとして受け取りcursor として扱う。
2. Dataframe としてそのデータをストリームとして扱う。
3. ssh source は debug 実行用のデフォルトとして　ssh://user@host/path/to/app.log を指定
4. process_stream 関数に closure を引数として条件として与えられるようにする。条件のサンプルとして  msg="finish to process/write " にしてください。

5. 集計時の条件を指定できるようにしてください。
6. log を時刻で範囲検索する。
7. DataFrame で bytes カラムの値を数値として扱って全て加算してください。


## 修正案

書き方が複雑すぎます。

```rust
let q = LazyLogFmtReader::new(
    SshSource::new("ssh://user@host/path/to/app.log"))
    .finish?()
    .filter(col("msg").eq("finish to process/write "))
    .agg([col("bytes").sum()]);
```

## sshSource の改良

```rust
let _df = LazyLogFmtReader::new(SshSource::new(
        "ssh://user@host/path/to/app.log.1000").contain_line('"msg"="finish to process/write "'))
```

こう呼び出して、 SshSource側で入力行をフィルタする機能を実装する。

## build_reader のbuilder パターン化

build_reader を builder パターン化をして以下のように動作するように変更してください。

```rust
ReaderBuilder::new()
.source(args.source.clone())
.line_filter(|line: &str| line.contains("msg=\"finish to process/write \"") )
.build()
```

## main 内の df_base のカラム追加

現在 df_base では　new カラムには

```
new=/data/out/VTEL_MME_FCT/2/20260113/00/2_chy1-MME-501n_202601122346_371.tbl.zst
```

こういう値が入っている。これの `VTEL_MME_FCT` の部分を取り出して table というカラムで登録する。

## table と finished_id ごとの bytes のsum

df_base から派生して　table と finished_id で group_by して
それごとに bytes を合計したものを格納する df_bytes_sum_per_table_and_finished_id というのを作成してください。


## LazyLogFmtReader の ssh source でのstreaming 対応

ssh ソース先が1TB でも動作するように。

## LazyLogFmt の AnonymousScan 実装のscan のマルチスレッド対応


### 要件

- 現在のSshSource とは別に russh-sftp を使った実装を使う
- seek や offset アクセス stat の所得を可能なinterface を備える。
- LazyLogFmtReader とのアクセスインターフェースもそれらに対応
- ssh 先、ローカルファイルのseekable zstd 対応
- seekable zst　はframe の終端が改行になるように前提
- plain な logfmt のファイルを frame 終端が 改行になるような zst ファイルに変換するコマンド(rust 実装)の作成

## LazyLogFmtReader のrayon によるマルチスレッド化および最適化

## 要件

- LazyLogFmtReader.scan でハンドリングしてないパターンの AnonymousScanArgs を全て実装する。（モッキングではない）
- LazyLogFmtReader.allows_predicate_pushdown をtrue にして scan 時に　filter をできるようにする。
- LazyLogFmtReader.scan をpolars_core::POOL を使いマルチスレッド化する。
- seekable zstd の処理は zeekstd を必ず使う。
- local/ssh の seekable zst の場合は、scan のthread で　SeekTable ごとに処理を行い並列化する。
- LazyLogFmtReader.scan は AnonymousScanArgs のさまざまなパターンのパラメータテストを作成する。マクロ実装で。


