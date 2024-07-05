use anyhow::Result;
use dashmap::DashMap;

use crate::utils::table_name::TableName;

use super::memory::MemTable;

/**
 * 存储所有的Immtable
 */
#[derive(Debug, Default, Clone)]
pub struct Immutables {
    // 这是<前缀、TableNameList>结构
    tables_name: DashMap<String, Vec<TableName>>,
    // 这是<前缀、MemTableList>结构
    tables: DashMap<String, Vec<MemTable>>,
}

impl Immutables {
    pub fn insert(&self, memtable: MemTable) -> bool {
        let mem_table_name = memtable.name();
        let prefix = memtable.name.get_prefix_name();

        match self.tables_name.get_mut(prefix.as_str()) {
            Some(mut vs) => vs.push(mem_table_name.clone()),
            None => {
                let mut vs = Vec::new();
                vs.push(mem_table_name.clone());
                self.tables_name.insert(prefix.clone(), vs);
            }
        }
        match self.tables.get_mut(prefix.as_str()) {
            Some(mut vs) => vs.push(memtable),
            None => {
                let mut vs = Vec::new();
                vs.push(memtable);
                self.tables.insert(prefix, vs);
            }
        }
        true
    }

    /**
     *  true: 可写数据
     *  false: 不可写数据
     */
    pub fn mutable(&self, table_name: &TableName) -> Result<bool> {
        let prefix = table_name.get_prefix_name();
        match self.tables.contains_key(prefix.as_str()) {
            true => {
                if let Some(v) = self.tables_name.get(prefix.as_str()) {
                    match v.contains(&table_name) {
                        true => Ok(false),
                        false => Ok(true),
                    }
                } else {
                    // let msg = format!("there is no immutables for prefix:{}",prefix);
                    // Err(anyhow::Error::msg(msg))
                    Ok(true)
                }
            }
            false => Ok(true),
        }
    }
}

impl Immutables {
    pub fn get_tables(&self, prefix: &str) -> Vec<TableName> {
        let tables = self.tables_name.get(prefix);
        match tables {
            Some(ts) => ts.clone(),
            None => Vec::new(),
        }
    }
}
