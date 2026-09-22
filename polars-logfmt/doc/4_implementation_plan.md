# 4_implementation_plan.md

## Implementation Plan: LazyLogFmtReader & Related Features (2026-01-25, merged with 3_LazyLogFmtReader_multithread_prompt.md)

### 1. 要件・現状分析と実装計画の再構築
- 全要件（AnonymousScanArgs, predicate pushdown, 並列化、zeekstd、テスト、main、worklog、commit、テストmacro化、英語commit、ssh/localテストパス指定）を再整理。
- 各項目ごとに「本実装→build error確認→worklog記載→commit」のサイクルで進行。
- 関数分割・共通化・可読性も常に意識。
- テストは必ずmacroでパラメータ化し、関数変更時は必ず作成・更新。
- commit messageは全て英語。
- worklogはdoc/4_でprefixを付与。
- main()からの動作確認を最終工程とする。
- テスト時のssh/localパスは以下を利用：
	- const SSH_BASE: &str = "ssh://user@host/path/to/test";
	- const LOCAL_BASE: &str = "/path/to/test";

### 2. 実装サイクル
2. AnonymousScanArgsパターンの本実装（scan/next_batch）
3. predicate pushdownの本実装（allows_predicate_pushdown=true, scan時にfilter適用）
4. scanのマルチスレッド化（polars_core::POOL, SeekTableごと並列化）
5. seekable zstdはzeekstdで実装
6. 関数分割・共通化・可読性向上
7. macroベースの網羅的パラメータテスト作成・修正（関数変更時は必ず作成/更新）
8. main()からの動作確認
9. worklog(doc/4_*)への記録・英語commit

### 3. 進行ルール
- 各ステップごとにbuild errorゼロを維持。
- 主要関数は適切なサイズに分割し、重複処理は共通化。
- 進捗・設計意図・工夫点は必ずworklogに記載。
- commitは英語で粒度細かく行う。
- テストはmacroでパラメータ化し、ssh/localパスは指定定数を利用。
- main()での動作確認を必須とする。

---

（以降、各ステップの詳細・進捗を追記していく）
