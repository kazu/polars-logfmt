## 2026-01-25: AnonymousScanArgsパターン本実装（scan/next_batch）

- 既存のscan/next_batchは最低限の動作はするが、
  - コードが肥大化・重複が多い
  - エラー処理や分岐が煩雑
  - フィールド・関数の未使用警告が多い
- 今回は「バッチ取得の共通処理」を関数分割し、
  - scan/next_batchの責務を明確化
  - 主要分岐（source種別/フィルタ/スキーマ推論）を整理
  - すべてのAnonymousScanArgsパターン（バッチサイズ/スキーマ/フィルタ）に対応
- まずはscan/next_batchの本実装・分割・呼び出し設計を行い、build errorゼロを維持
- 完了後、predicate pushdownや並列化、テスト等に進む

---

（この内容でscan/next_batchの本実装・分割を進める）
