use crate::utils::{
    data_utils::{self, get_timestamp_from_batch},
    table_name::TableName,
};
use anyhow::Result;
use arrow::array::RecordBatch;

const MAX_BATCH_SIZE: usize = 1024 * 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTable {
    pub(crate) name: TableName,
    // 判断是mutable_memtable还是immutable_memtable
    pub(crate) mutable: bool,
    // 文件大小
    pub(crate) size: usize,
    // 开始时间
    pub(crate) start: u64,
    // 结束时间
    pub(crate) end: u64,
}

impl MemTable {
    pub async fn new_with_batch(name: &TableName, batch: &RecordBatch) -> Result<Self> {
        let (start, end) = get_timestamp_from_batch(batch).await?;
        let batch_size = data_utils::batch_size(batch);
        println!("batch_size:{}", batch_size);
        let mutable = !(batch_size > MAX_BATCH_SIZE);
        println!("mutable:{}", mutable);
        Ok(Self {
            name: name.clone(),
            mutable,
            size: batch_size,
            start,
            end,
        })
    }
    pub fn new(name: TableName, start: u64, end: u64) -> Self {
        Self {
            name,
            mutable: true,
            size: 0,
            start,
            end,
        }
    }

    pub fn name(&self) -> &TableName {
        &self.name
    }
}
