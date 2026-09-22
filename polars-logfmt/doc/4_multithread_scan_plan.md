#!/usr/bin/env/markdown

# スキャンのマルチスレッド設計（SeekTableベース）

日付: 2026-01-25

目的
- `LazyLogFmtReader` に対して、入力が seekable（seekable zst／ローカルファイル）の場合に SeekTable に基づくフレーム単位で並列処理を行う。既存の API と macro テストの互換性は維持する。

前提・制約
- リポジトリには `SeekableVfsFile` の実装があり、`clone_handle()` とランダムアクセス（seek/read）が利用できることを前提とする。zeekstd の `SeekTable` 概念が利用可能。
- 並列化の手段としては `polars_core::POOL`（Polars が管理するスレッドプール）を使用する。rayon を直接呼び出す必要はなく、Polars 側が提供するタスク実行 API（プールへのタスク投入/実行）を利用して並列処理を行う設計とする。
- 各ワーカーは独立ハンドル（`clone_handle()`）を使い、共有リーダーを回避する。
- 各フレームで得られた行はローカルで `DataFrame` に変換し、フレーム順で結合（縦結合）して最終バッチを生成する。

設計概要
1. `LazyLogFmtReader::wrrap_next_batch` または `scan_logfmt` の経路でソースが seekable か判定する。
2. SeekTable が利用できる場合、フレーム一覧（`(frame_idx, comp_start, comp_end, decomp_start, decomp_end)` もしくは最小限で `(start, length)`）を作成する。
3. フレームリストに対して `polars_core::POOL` にフレームごとのタスクを投入して並列実行する。各フレーム内で以下を行う：
   - `file.clone_handle()` で独立ハンドルを取得
   - フレーム先頭へ `seek()` して必要バイトを読み出す（`read_frame()` API があればそれを利用）
   - ZST の場合はデコード/展開（既存の `zeekstd` ヘルパーや `ssh_reader` を利用）
   - 行単位でパースし、`line_filter` / `row_filter` をローカル適用
   - 全体スキーマが未指定ならフレーム内でスキーマ推定（first-row ヒューリスティック）し、`rows_to_dataframe_filled` を使って `DataFrame` を生成
   - `(frame_idx, DataFrame)` を返す
4. ワーカーの戻りを集め `frame_idx` でソートし、`vstack`（または `polars::functions::concat_df`）で結合してバッチ DataFrame を生成する。

統合詳細
- `src/lazy` か `lazy_logfmt_reader.rs` に以下の補助関数を追加する：
  - `fn frames_from_seekable(file: &dyn SeekableVfsFile) -> Option<Vec<FrameRange>>` — 可能なら zeekstd の SeekTable を使い、なければファイルサイズに基づいた粗い byte-range 分割を行う。
  - `fn read_and_parse_frame(handle: Box<dyn SeekableVfsFile + Send>, range: FrameRange, schema: Option<&Schema>, filters: ...) -> PolarsResult<(usize, DataFrame)>` — ワーカー関数。
- `FrameRange` は小さな構造体とする: `{ idx: usize, start: u64, len: u64, is_zst: bool, comp_start: u64, comp_len: u64 }` 等。

スレッド安全性と順序性
- 各ワーカーは `clone_handle()` を使いロックを避ける。
- スキーマが未指定の場合、全体のスキーマ推定を複雑にするよりも、簡潔化のために先にシングルスレッドで先頭 N 行を読みスキーマを推定してから並列パースする方針を取る（例: `schema_infer_lines = 100`）。

バッチサイズとバックプレッシャ
- 既存の `batch_size` 振る舞いは維持する。最初の実装ではフレーム全体を処理し、結合後に先頭 `batch_size` 行を返す方式にする。

フォールバックと障害対応
- `clone_handle()` に失敗する、または SeekTable が得られない場合は既存のシングルスレッド経路にフォールバックする。
- 任意のワーカーでエラーが発生した場合はバッチ処理を中止し、エラーを返す。

テスト戦略
- `tests/lazy_logfmt.rs` に macro ベースのテストを追加し、単一スレッド実装と並列実装で結果が一致すること、`clone_handle()` の同時利用が安全であることを検証する。
- `frames_from_seekable` の単体テスト（小さな zst テストファイルやモック `SeekableVfsFile` を使用）を追加する。

実装手順（フェーズ）
1. `frames_from_seekable` と `read_and_parse_frame` の実装
2. `wrrap_next_batch` に seekable ソース用の並列経路を追加（`polars_core::POOL` を使ってフレーム単位タスクを投入）
3. `self.schema` が無い場合の事前スキーマ読み取り（`schema_infer_lines`）の実装
4. ワーカー結果の結合、`predicate`/`with_columns` の適用、`Ok(Some(df))` の返却
5. macro テスト追加および全テスト実行、回帰対応

見積もり
- 実装＋テスト: `SeekableVfsFile`／zst API の詳細により 4–8 時間程度を想定。

備考
- 並列実行の制御は `polars_core::POOL` に委ねる設計とする。将来的には Polars 側のスケジューリングポリシーやプール設定に合わせた最適化（バッチ単位での並列度調整や優先度設定など）を検討する。

