use std::{
    collections::HashSet,
    sync::Arc,
};

use anyhow::Result;
use array_data_utils::{merge_batches_with_schema, merge_schema};
use arrow::{array::RecordBatch, datatypes::Schema};
use dashmap::DashMap;
use datafusion::{error::DataFusionError, prelude::SessionContext, sql::TableReference};
use table_name::TableName;
use table_size::{batch_size, TableSize};

use crate::TABLE_NAME;

pub mod array_data_utils;
pub mod sql_utils;
pub mod table_name;
pub mod table_size;

#[derive(Clone)]
pub struct MemTable {
    ctx: SessionContext,
    tables: HashSet<TableName>,
    table_size: TableSize, // 每100行合并一次
    table_opts: DashMap<TableName, Arc<Schema>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            tables: HashSet::new(),
            table_size: TableSize::default(),
            table_opts: DashMap::new(),
        }
    }

    pub async fn batch_insert(&mut self, batches: Vec<RecordBatch>) {
        for batch in batches {
            let _resp = self.insert(batch).await;
        }
    }

    pub async fn insert(&mut self, batch: RecordBatch) {
        let schema = batch.schema();
        let batch_size = batch_size(&batch);
        if let Some(device_id) = schema.metadata().get(TABLE_NAME) {
        self.tables.insert(TableName::new(device_id));
        println!("准备向: {:?} 表添加数据！", device_id);
        let sql = format!("select * from {}", device_id);
        let old_table = self.ctx.sql(sql.as_str()).await;
        match old_table {
            Ok(df) => {
                if let Ok(mut resp) = df.collect().await {
                    println!("合并之前的 resp: {:?}", resp.len());
                    let old_table = &resp[0];
                    let schema = merge_schema(&old_table.schema(), &batch.schema());
                    println!("合并之后的 schema: {:?}", schema);
                    resp.push(batch);
                    println!("合并之后的 resp: {:?}", resp.len());
                    if let Ok(r) = merge_batches_with_schema(&Arc::new(schema), &resp) {
                        let _ = self.ctx.deregister_table(TableReference::from(device_id));
                        
                        let _ = self.ctx.register_batch(&device_id, r);
                    }
                }
            }
            Err(_e) => {
                // println!("error: {:?}",e);
                let _ = self.ctx.register_batch(&device_id, batch);
            }
        }
        let size = self.table_size.insert(device_id, batch_size);
        println!("{} size: {:?}", device_id,size);
    }else {
        println!("没有找到表名");
    }
    }
}

/**
 * Query接口
 */
impl MemTable {
    // 获取所有表名
    pub async fn tables(&self) -> Result<Vec<TableName>, DataFusionError> {
        Ok(self.tables.clone().into_iter().collect())
    }


    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
        let resp = self.ctx.sql(sql).await;
        let resp1 = match resp {
            Ok(df) => df.collect().await,
            Err(e) => {
                return Err(e.into());
            }
        };
        resp1
    }

    /**
     * 查询指定表(所有数据，多用于测试，一般不能这么使用，类似select * from table_name)
     */
    pub async fn query_with_table(
        &self,
        table_name: &str,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let resp = self.ctx.table(table_name).await;
        let resp1 = match resp {
            Ok(df) => df.collect().await,
            Err(e) => {
                return Err(e.into());
            }
        };
        resp1
    }
}


impl MemTable {
    async fn flush(&self,table: impl Into<String>) -> Result<(), DataFusionError> {
        
        Ok(())
    }
}