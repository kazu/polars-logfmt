// LogFmtReaderState: ストリーミング状態を保持する構造体
use crate::lazy::LineReader;

pub struct LogFmtReaderState {
    pub reader: Option<LineReader>, // SSH/ファイル/カーソル
    pub offset: usize,              // 何行読んだか
    pub finished: bool,
}
