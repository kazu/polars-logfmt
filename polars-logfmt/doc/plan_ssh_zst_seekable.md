# SSH経由zstファイル対応 未実装機能 実装計画

## 概要
現状、polars_logfmtのVFS層で「SSH経由zstファイル（seekable zstd）」のread/seek/stat/size等が未実装です。
本ドキュメントでは、未実装箇所の洗い出しと、順次実装するための計画を記載します。
※zstd-seekable ではなく Rust製の zeekstd crate を利用して実装します。

---

## 1. 未実装箇所一覧

### 1-1. SshSeekableZstdFile (is_zst==true)
- read_sync, read_async
- seek_sync, seek_async
- stat_sync, stat_async
- SeekableVfsFile::read, ::seek, ::size, ::stat
- open: sftp_file: None のまま（zstファイルの実体アクセス未実装）

### 1-2. SshSeekableZstdVfs
- open: is_zst==true の場合の本実装（zstd-seekableクレート利用）
- stat: is_zst==true の場合の本実装

### 1-3. ssh_zst_file::SshSeekableZstFile
- open/seek/read/stat等のスケルトン（未実装）

---

## 2. 実装計画

### ステップ1: SSH経由zstファイルのread/seek/stat/sizeのAPI設計
- SSH上のzstファイルをSFTPでダウンロードせず、ランダムアクセス可能なストリームとして扱う設計を明確化
- Rust製の zeekstd crate を利用し、SFTP経由で必要なブロックのみ取得する
- trait/struct設計を整理

### ステップ2: SshSeekableZstdFile::openのzst対応
- is_zst==trueの場合、SFTP経由でファイルハンドルを開き、zeekstdデコーダを初期化
- sftp_file, zeekstdデコーダ等のフィールドを追加

### ステップ3: read/seek/stat/sizeの本実装
- read: 指定位置のデータをSFTP経由で取得し、zeekstdでデコード
- seek: zeekstdのインデックスを利用し、SFTPで必要なブロックのみ取得
- stat/size: SFTPのstatで元ファイルサイズ取得、zeekstdで展開後サイズ取得

### ステップ4: SshSeekableZstdVfs::open/statのzst対応
- open/statでzstファイルの場合、SshSeekableZstdFileのzeekstd対応インスタンスを返す

### ステップ5: テスト追加・既存テスト修正
- SSH経由zstファイルのread/seek/stat/sizeの正常系・異常系テストを追加
- 既存テスト（test_multithreaded_clone_handle_ssh_zst等）がパスすることを確認

### ステップ6: ドキュメント・サンプル整備
- 使い方・制約事項・注意点をdoc/以下に記載（zeekstd利用例も記載）

---

## 3. 優先順位・スケジュール
1. API設計・設計方針整理（1日）
2. open/内部構造実装（1日）
3. read/seek/stat/size本実装（2日）
4. Vfs層のzst対応（0.5日）
5. テスト追加・修正（0.5日）
6. ドキュメント整備（0.5日）

---

## 4. 備考
- zeekstdクレートのAPI調査・最適な使い方の検討が必要
- SFTP経由でのランダムアクセス効率化のため、キャッシュやプリフェッチも検討
- 既存のplainファイル/ローカルzstファイルの実装を参考にする

---

以上、順次この計画に従い実装を進めます。
