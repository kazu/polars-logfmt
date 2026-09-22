// LogFmtReaderState: ストリーミング状態を保持する構造体
use std::io::BufRead;

pub struct LogFmtReaderState {
    pub reader: Option<Box<dyn BufRead + Send>>, // SSH/ファイル/カーソル
    pub offset: usize,                           // 何行読んだか
    pub finished: bool,
}
