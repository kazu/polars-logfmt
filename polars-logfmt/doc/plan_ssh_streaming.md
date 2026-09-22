# LazyLogFmtReader SSH streaming plan

## 目的
- SSH ソースが 1TB 規模でも動作するよう、全件読み込みを避けてストリーミング処理に寄せる。

## 方針
1. `LazyLogFmtReader::scan` の SSH 経路をバッチ読み込みに変更し、行バッファの一括保持を回避する。
2. バッチごとに `DataFrame` を生成し、列集合が変化する場合は union（欠損列は null で補完）する。
3. `line_filter` と `row_filter` は行単位の判定を維持しつつ、処理順を最小限にして無駄な parse を抑制する。
4. 既存の `LazyLogFmtQuery::try_agg` はストリーミング済みのため、互換性を維持する。
5. `main` から `non_streaming()` と `run_streaming_pipeline()` を CLI で切り替えられるようにする。

## 実装タスク
- [x] `LazyLogFmtReader::scan` の SSH 経路で `read_streaming_dataframe` を使用
- [x] `append_dataframe_with_union` と `align_dataframes` を追加
- [x] 型不一致の補正（右側を左側の dtype にキャスト）
- [x] 行フィルタ/列フィルタ適用の順序維持
- [x] CLI `--streaming` で `main` の実行パスを切り替え
- [x] 実行終了時に開始からのメモリ増加量（RSS）を表示

## テスト
- [x] 列が異なるバッチ同士の union で列順と null 補完が正しいことを検証
- [x] `run_streaming_pipeline` のファイル入力で期待値が得られることを検証
- [x] `--streaming` の CLI パースを検証

## リスク/注意
- 列数が増加すると union コストが上がるため、batch_size の調整が必要になる可能性がある。
- 型推論の不一致はキャストで解決するが、情報落ちが起こる場合がある。
