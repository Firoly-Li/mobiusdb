use crate::utils::{
    file_utils::{get_sstable_path, Level},
    table_name::TableName,
};
use anyhow::Result;
use arrow::array::RecordBatch;
use datafusion::{dataframe::DataFrameWriteOptions, prelude::SessionContext};

use super::SsTable;

#[derive(Debug, Clone)]
pub struct ParquetSsTable {
    // sstable文件名称
    pub(crate) name: TableName,
    // 字段名称
    pub(crate) fields: Vec<String>,
    // 文件层级
    pub(crate) level: Level,
    // 文件大小
    pub(crate) size: usize,
    // 开始时间
    pub(crate) start: u64,
    // 结束时间
    pub(crate) end: u64,
}

impl ParquetSsTable {
    pub fn new_with_table(name: impl AsRef<str>) -> Self {
        let sstable_name = TableName::new_ss_name(name);
        Self {
            name: sstable_name,
            fields: Vec::new(),
            level: Level::L0,
            size: 0,
            start: 0,
            end: 0,
        }
    }
    pub fn get_sstable_name(&self) -> String {
        self.name.get_sstable_name()
    }

    pub fn get_table_name(&self) -> TableName {
        self.name.clone()
    }
    pub fn new_with_opts(
        name: impl AsRef<str>,
        level: Level,
        fields: &Vec<String>,
        size: usize,
        start: u64,
        end: u64,
    ) -> Self {
        let sstable_name = TableName::new_ss_name(name);
        Self {
            name: sstable_name,
            fields: fields.clone(),
            level,
            size,
            start,
            end,
        }
    }

    /**
     * 将RecordBatch数据写入文件
     * 默认：
     *  1、文件只能写入L0层级
     *  2、RecordBatch默认已排序(以时间)
     */
    pub async fn write(&self, batch: &RecordBatch) -> Result<bool> {
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch.clone())?;
        let path = get_sstable_path(&self.name.get_sstable_name(), Level::L0);
        let _ = df
            .write_parquet(&path, DataFrameWriteOptions::new(), None)
            .await?;
        Ok(true)
    }
}

impl SsTable for ParquetSsTable {
    fn merge<'a>(table_names: impl IntoIterator<Item = &'a str>) -> anyhow::Result<bool> {
        todo!()
    }

    fn load(table_name: &str, batch: &RecordBatch) -> Result<bool> {
        todo!()
    }
}
