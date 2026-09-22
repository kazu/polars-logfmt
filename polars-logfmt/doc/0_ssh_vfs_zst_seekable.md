# russh-sftp/seekable zstd仮想ファイルシステム 実装AI依頼用プロンプト

以下の要件を満たすRust実装のAI依頼用プロンプトです。

---

## russh-sftp/seekable zstd仮想ファイルシステム 実装AI依頼

### 要件

1. 現在のSshSourceとは別に、russh-sftpを利用した新たなSshSource実装を作成してください。
2. seekやoffsetアクセス、stat取得が可能なインターフェースを備えてください。
3. LazyLogFmtReaderとのアクセスインターフェースも上記機能に対応させてください。
4. ssh先・ローカルファイルの両方でseekableなzstdファイルに対応してください。
5. seekableなzstファイルは「各frameの終端が改行である」ことを前提とします。
6. plainなlogfmtファイルを「frame終端が改行となるzstファイル」に変換するRustコマンドを作成してください。
    1. このコマンドには　zstdのフレームの状態を表示可能なサブコマンドを用意してください。
    2. フレームの状態とはフレームごとの解凍後の解凍後のオフセットの開始位置とサイズ、seekable の対応有無、frame 終端が改行になっているかどうかです。


---

### 補足
- Rust言語で実装してください。
- 必要に応じてテストやサンプルコードも含めてください。
- 既存のSshSourceやLazyLogFmtReaderの設計・利用例も参考にしてください。
- 変換コマンドはCLIツールとして動作すること。
- worklog も作成してください。
- 設計して作成する予定のinterface は doc/かぶらない番号_ssh_vfs_zst_seekable_api.md ファイルに書いてください。
- 作業毎にworklog への追加git commit を行う。
- commit 時は必ず コンパイルが通る状態にする。
- seekable zstd は　https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md　準拠

---

### 追加要件

- seekable zst のコマンドがlogfmt 前提ではないので、`seekzstdsep` に変更して下さい。（seekable で separator 指定があるという意味)
- 変換時のコマンド引数はzstd 準拠にして下さい
    -　標準入力からの入力がある場合はそれを入力にする。
    - 出力ファイル名が指定されてない場合は、入力ファイル名が指定された場合は 入力ファイル名.seek.zst にする。
    - stdin からの入力と 出力ファイル名が指定されていない場合は 標準出力に出力する
    - --rm が指定された場合は, 入力ファイル名が必須で作成後、入力ファイルを消す

---


この要件を満たす実装・設計・サンプルコードを出力して現在のディレクトリに反映してください。