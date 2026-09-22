# Ssh/Local 圧縮・非圧縮ファイル対応 実装計画

## 対象
- seekzstdsepで圧縮したseekable zstファイル（ローカル/ssh経由）
- 非圧縮テキストファイル（ローカル/ssh経由）

## 実装方針
- ローカル/ssh/圧縮/非圧縮すべてのファイル種別に対応
- SFTP経由・ローカル両方で自動判定・ラップ切替
- API/CLI/テスト・ドキュメント一貫性を重視

## 実装タスク
1. SFTPセッション・ファイルハンドル管理
   - russh-sftpでSSH接続・認証・SFTPセッション確立
   - ファイルごとにSFTPハンドルを保持
2. SFTPでファイルopen実装（zst/非圧縮両対応）
   - SFTP openでファイルハンドル取得
   - 拡張子・ヘッダ等でzst/非圧縮を判定し、ラップ切替
3. SFTPでseek実装（zst/非圧縮両対応）
   - SFTPハンドルのオフセット管理
   - SFTP read_at等で任意位置から読み込み
4. SFTPでread実装（zst/非圧縮両対応）
   - 現在オフセットからデータ取得
   - 内部でseek+readまたはread_atを利用
5. SFTPでファイルサイズ取得実装（zst/非圧縮両対応）
   - SFTP stat/lstatからサイズ取得
6. SFTPでstat取得実装（zst/非圧縮両対応）
   - SFTP stat/lstatから各種情報取得
7. ローカル/ssh/圧縮/非圧縮の自動判定・ラップ切替
   - open時にzst/非圧縮を判定し、適切なVfsFile/ラッパを返す
8. テスト・動作確認・worklog記載
   - ユニットテスト・統合テストで動作検証
   - worklogに作業・設計・課題を記載

## 備考
- 既存LocalSeekableZstdVfs/ローカル非圧縮は現状で対応済み
- SshSeekableZstdVfs/SshSeekableFileでzstd判定・ラップ/非ラップ切替を実装
- CLI/設計書/テストも全パターン網羅を目指す
