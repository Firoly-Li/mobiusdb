use dashmap::DashMap;

use crate::utils::table_name::TableName;

use super::memory::MemTable;

/**
 * Mutables只是一个前缀管理的表，
 * 并不存储真实的数据，真实的数据在SessionContext中维护
 */
#[derive(Debug, Clone, Default)]
pub struct Mutables {
    // 这是<前缀、TableNameList>结构
    tables_name: DashMap<String, TableName>,
    // 这是<前缀、MemTableList>结构
    tables: DashMap<String, MemTable>,
}

impl Mutables {
    pub fn new() -> Self {
        Self {
            tables_name: DashMap::new(),
            tables: DashMap::new(),
        }
    }

    pub fn contains_key(&self, prefix: impl AsRef<str>) -> bool {
        self.tables_name.contains_key(prefix.as_ref())
    }

    pub fn insert(&mut self, memtable: MemTable) -> bool {
        let table_name = memtable.name();
        let prefix = table_name.get_prefix_name();
        let tb = self.tables_name.insert(prefix.clone(), table_name.clone());
        let mb = self.tables.insert(prefix, memtable);
        let resp = match (tb, mb) {
            (Some(_b), Some(_s)) => true,
            (_, _) => false,
        };
        resp
    }

    pub fn get_table(&self, prefix: impl AsRef<str>) -> Option<MemTable> {
        let r = self.tables.get(prefix.as_ref());
        match r {
            Some(memtable) => Some(memtable.clone()),
            None => None,
        }
    }

    pub fn get_table_name(&self, prefix: impl AsRef<str>) -> Option<TableName> {
        let r = self.tables_name.get(prefix.as_ref());
        match r {
            Some(table_name) => Some(table_name.clone()),
            None => None,
        }
    }
}
