# convert_text_to_seekable_zst_reader() 関数のテスト作成


## 作業概要

@polars_logfmt/src/seekzstdsep_lib.rs の　convert_text_to_seekable_zst_reader()　のテストを作成する。

## 開発要件

- テストコードは polars_logfmt/tests/seekzstdsep_lib.rs で作成
- 当然rust で実装する。
- test コードの追加以外してはいけない。convert_text_to_seekable_zst_reader内の処理は変更してはいけない。
- macro を使ったパラメータブルテストで行う
- テストで利用するデータはテスト毎に作成し、テスト終了後失敗、成功にかかわらず削除
- テストファイルは並列実行を考慮し、乱数を使ったユニークなファイル名とする。乱数は必ず乱数の関数を使う。seed は現在時刻を利用してもよい。
- 作成したテスト込みで @polars_logfmt 以下でビルドが通るかのどうかのチェックをする。
- テスト項目
する。
    - 圧縮したファイルとその後それを解凍して元のファイルと同一かどうかをチェックする。
    - 区切り文字の長さは1-10 バイトとする。
    - 区切り文字の長さが０の場合はエラーが発生するか？　
    - frame_size は1024 以上 で 8192*5 のパターンも用意
    - 区切り文字の出現が frame_size * 4　より少ないパターンで正常に終了するか。
    - frame_size は 0 以下のテストは作成しない。
    - 区切り文字の出現が frame_size * 4　より多い時にエラーが発生するか
