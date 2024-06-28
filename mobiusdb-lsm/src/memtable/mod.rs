use std::sync::Arc;

use anyhow::Result;
use array_data_utils::{merge_batches_with_schema, merge_schema};
use arrow::array::RecordBatch;
use datafusion::{error::DataFusionError, prelude::SessionContext, sql::TableReference};

pub mod array_data_utils;

#[derive(Clone)]
pub struct MemTable {
    ctx: SessionContext
}

impl MemTable {
    pub fn new() -> Self {
        Self{
            ctx: SessionContext::new()
        }
    }

    pub async fn query(&self,sql: &str) -> Result<Vec<RecordBatch>,DataFusionError> {
        let resp = self.ctx.sql(sql).await;
        let resp1 = match resp {
            Ok(df) => df.collect().await,
            Err(e) => {
                return Err(e.into());
            }
        };
        resp1
    }

    pub async fn insert(&self,batch: RecordBatch) { 
        let schema = batch.schema();
        let device_id = schema.metadata().get("table_name").unwrap();
        let sql = format!("select * from {}",device_id);
        let old_table = self.ctx.sql(sql.as_str()).await;
        match old_table {
            Ok(df) => {
                if let Ok(mut resp) = df.collect().await {
                    println!("合并之前的 resp: {:?}",resp.len());
                    let old_table = &resp[0];
                    let schema = merge_schema(&old_table.schema(), &batch.schema());
                    println!("合并之后的 schema: {:?}", schema);
                    resp.push(batch);
                    println!("合并之后的 resp: {:?}",resp.len());
                    if let Ok(r) = merge_batches_with_schema(&Arc::new(schema), &resp) {
                        let _ = self.ctx.deregister_table(TableReference::from(device_id));
                        let _ = self.ctx.register_batch(&device_id, r);
                    }
                }

            },
            Err(e) => {
                // println!("error: {:?}",e);
                let _ = self.ctx.register_batch(&device_id, batch);
            },
        }
    }
}