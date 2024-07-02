use crate::utils::table_name::TableName;

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
