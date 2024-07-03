use anyhow::Result;
use crate::utils::table_name::TableName;

use super::{immtables::Immutables, memory::MemTable, mutables::Mutables};

/**
 *  mutables: 正在写入的memtable，mutables中每个key只对应一个value
 * immutables: 已经写入的memtable，immutables中每个key可能对应多个value
 */
#[derive(Debug,Clone,Default)]
pub struct TableIndexs {
    mutables: Mutables,
    immutables: Immutables,
} 

impl TableIndexs {
    pub fn new() -> Self {
        Self::default()
    }

    /**
     * 添加一个新的memtable,如果这个memtable的mutable状态是true
     */
    pub fn insert(&mut self, memtable: MemTable) -> bool {
        let table_name = memtable.name();
        let prefix = table_name.get_prefix_name();
        let r = self.mutables.get_table(prefix.as_str());
        match r {
            Some(old_memtable) => {
                match old_memtable.mutable {
                    true => {
                        // memtable状态是可写，说明old_memtable对应的数据已经被merge到新数据中，直接替换即可！
                        self.mutables.insert(memtable);
                    },
                    false => {
                        let old_mem_table = old_memtable.name().get_memtable_name();
                        println!("old_mem_table:{} mutable状态为: false,需要写入到immtables中！",old_mem_table);
                        println!("当前immutabes中包含:{},的memtable有：{:?}",prefix,self.immutables);
                        // memtable状态是不可写，说明old_memtable对应的数据没有被merge到新数据中，需要将old_memtable转移到immtables中
                        self.immutables.insert(old_memtable);
                        self.mutables.insert(memtable);
                    },
                };
                true
            },
            None => self.mutables.insert(memtable)
        }
    }

    pub fn contains_key(&self,prefix: impl AsRef<str>) -> bool {
        self.mutables.contains_key(prefix)
    }

    pub fn get_mutables(&self) -> &Mutables {
        &self.mutables
    }

    pub fn get_immutables(&self) -> &Immutables {
        &self.immutables
    }
}

impl TableIndexs {
    pub fn get_tables_with_prefix(&self,prefix: &str) -> Vec<TableName> {
        let mut ts = self.immutables.get_tables(prefix);
        let tables = self.mutables.get_table_name(prefix);
        match tables {
            Some(t) => {
                ts.push(t);
                ts
            }
            None => ts,
        }
    }

    pub fn mutable(&self,table_name: &TableName) -> Result<bool> {
        let b = self.immutables.mutable(&table_name).is_ok();
        if b {
            if let Some(v) = self.mutables.get_table(table_name.get_prefix_name()) {
                let b = v.mutable;
                Ok(b)
            } else {
                let msg = format!("not find table: 【{}】", table_name.get_memtable_name());
                Err(anyhow::Error::msg(msg))
            }
        } else {
            Ok(false)
        }
    }
}