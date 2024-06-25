use super::offset::Offset;

/**
 * 索引文件，wal的索引落盘
 */
#[derive(Debug, Clone)]
pub struct IndexFile;

/**
 * 索引
 */
pub struct Index {
    name: String,
    offsets: Vec<Offset>,
}

impl IndexFile {
    async fn init(path: &str) -> Self {
        todo!()
    }

    async fn load(&self) {
        todo!()
    }

    async fn save(&self) {
        todo!()
    }
}
