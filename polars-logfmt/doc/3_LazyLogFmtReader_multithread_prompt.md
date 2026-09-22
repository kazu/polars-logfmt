# AIプロンプト: LazyLogFmtReader のマルチスレッド化・最適化

このプロンプトは、Rust/Polarsプロジェクトにおける LazyLogFmtReader の scan 処理をマルチスレッド化・最適化するための要件をAIに指示するものです。

## 要件

- LazyLogFmtReader.scan でハンドリングしていないパターンの AnonymousScanArgs を全て実装する（モックではなく本実装）。
- LazyLogFmtReader.allows_predicate_pushdown を true にし、scan 時に filter を適用できるようにする。
- LazyLogFmtReader.scan を polars_core::POOL を使いマルチスレッド化する。
- seekable zstd の処理は zeekstd を必ず使う。
- local/ssh の seekable zst の場合、scan の thread で SeekTable ごとに処理を行い並列化する。
- LazyLogFmtReader.scan は AnonymousScanArgs の様々なパターンのパラメータテストを作成する（マクロ実装）。
- test　は変更をした関数のものは必ず作成か更新をしてください。またそれらは全てmacro を使ったパラメータテストで作成してください。
- git commit 時のmessage は全て英語でお願いします。
- worklog は doc/4_ をprefix にしてmd で作成してください。
- これらは全てmock ではなく 本実装で行なってください。
- main() からの動作が正常に動くことを最後にかくにんする。
- 作業は全て　polars_logfmt　以下で行う。
- test 時にssh 接続のものが必要な場合は　tests/ssh_vfs_palin_and_zst.rs にならいいかのことをしてください。
  - 以下の定義を使い テストデータは LOCAL_BASE 以下に作成し、ssh でアクセスするテストでは SSH_BASE 以下でアクセスするように実装してください。 

```
// テスト用のSSHベースパス（必要に応じて編集）
const SSH_BASE: &str = "ssh://user@host/path/to/test"; // ←必要に応じて編集
const LOCAL_BASE: &str =
    "/path/to/test";
```

---
この要件を満たす Rust コード・テスト・設計をAIに生成させる際の指示文として利用してください。
