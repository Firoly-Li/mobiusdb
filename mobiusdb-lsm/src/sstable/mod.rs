use anyhow::Result;
use arrow::array::RecordBatch;
pub mod parquet;

// 1、sstable是一个分层的文件结构，每一层都是多个sstable文件，一张表是一个sstable文件，
// 2、每个sstable文件都是一个完整的Parquet数据文件，可以使用Parquet工具查看。
// 3、每一层的文件大小是固定的，每个sstable文件的大小是固定的。每个文件都有一个索引(时间序列)，通过索引可以快速确认数据是否在文件中

pub trait SsTable {
    // 合并不同的sstable文件
    // 合并氛围两种：
    // 1、同一层级的多个sstable文件合并为一个较大的sstable文件
    fn merge<'a>(table_names: impl IntoIterator<Item = &'a str>) -> Result<bool>;

    fn load(table_name: &str, batch: &RecordBatch) -> Result<bool>;
}

#[cfg(test)]
mod tests {

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sstable() {}
}
