use std::sync::Arc;

use anyhow::Result;
use array_data_utils::{merge_batches, merge_batches_with_schema, merge_schema};
use arrow::{array::RecordBatch, datatypes::Schema};
use dashmap::{DashMap, DashSet};
use datafusion::{
    dataframe::DataFrameWriteOptions, error::DataFusionError, prelude::SessionContext,
    sql::TableReference,
};
use memory::MemTable;
use table_index::TableIndexs;
use table_size::TableSize;

use crate::{
    utils::{
        data_utils, file_utils::{create_sstable_path, Level, SSTABLE_FILE_SUFFIX}, table_name::TableName
    },
    TABLE_NAME,
};

pub mod array_data_utils;
pub mod immtables;
pub mod memory;
pub mod mutables;
pub mod sql_utils;
pub mod table_size;
pub mod table_index;

#[derive(Clone)]
pub struct MemTableService {
    ctx: SessionContext,
    table_names: DashSet<TableName>,
    table_size: TableSize, // 每100行合并一次
    table_opts: DashMap<TableName, Arc<Schema>>,
    table_indexs: TableIndexs,
}

impl MemTableService {}

impl MemTableService {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            table_names: DashSet::new(),
            table_size: TableSize::default(),
            table_opts: DashMap::new(),
            table_indexs: TableIndexs::new(),
        }
    }

    pub async fn batch_insert(&mut self, batches: Vec<RecordBatch>) {
        println!("batchs_len = {:?}", batches.len());
        for batch in batches {
            let _resp = self.insert_batch(&batch).await;
        }
    }

    /**
     * todo:
     *  1、从batch中获取表名 prefix
     *  2、根据前缀，获取对应的mutable_table
     *  3、将数据合并到mutableTable
     *  4、判断合并之后的mutabletable的大小，如果过大就转换为immutable_table
     */
    pub async fn insert_batch(&mut self, batch: &RecordBatch) -> Result<bool> {
        // 1、首先生成相应的table_name
        if let Some(prefix) = batch.schema().metadata().get(TABLE_NAME) {
            let b = self.table_indexs.get_mutables().contains_key(prefix);
            
            println!("prefix:{:?} 是否存在于mutables: {:?}",prefix, b);
            let new_batch = match b {
                true => {
                    let mem_table = self.table_indexs.get_mutables().get_table(prefix).unwrap();
                    if mem_table.mutable {
                        let table_name = self.table_indexs.get_mutables().get_table_name(prefix).unwrap();
                        let old_mem_table_name = table_name.get_memtable_name();
                        // println!("old_mem_table_name: {:?}", old_mem_table_name);
                        let mut old_batch = self
                            .query_with_table(&old_mem_table_name)
                            .await?;
                        // println!("old_batch: {:?}", old_batch);
                        // println!("old_batch_len: {:?}", old_batch.len());
                        // println!("old_batch_num_rows: {:?}", old_batch.first().unwrap().num_rows());
                        old_batch.push(batch.clone());
                        let new_batch = merge_batches(&old_batch)?;
                        new_batch
                    }else {
                        let schema = batch.schema();
                        let empty_batch = RecordBatch::new_empty(schema);
                        let new_batch = merge_batches(vec![batch,&empty_batch])?;
                        new_batch
                    }
                    
                }
                false => {
                    let schema = batch.schema();
                    let empty_batch = RecordBatch::new_empty(schema);
                    let new_batch = merge_batches(vec![batch,&empty_batch])?;
                    new_batch
                }
            };
            // println!("new_batch: {:?}", new_batch);
            // println!("new_batch_size: {:?}", data_utils::batch_size(&new_batch));
            let new_table_name = TableName::new_mem_name(prefix);
            let mem_table = MemTable::new_with_batch(&new_table_name, &new_batch).await?;
            // println!("新的mem_table: {:?}", mem_table);
            let _ = self
                .ctx
                .register_batch(&new_table_name.get_memtable_name(), new_batch);
            self.table_indexs.insert(mem_table);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /**
     * todo:
     *  1、从batch中获取表名 prefix
     *  2、根据前缀，获取对应的mutable_table
     *  3、将数据合并到mutableTable
     *  4、判断合并之后的mutabletable的大小，如果过大就转换为immutable_table
     */
    #[deprecated]
    pub async fn insert(&mut self, batch: RecordBatch) {
        let schema = batch.schema();
        if let Some(device_id) = schema.metadata().get(TABLE_NAME) {
            self.table_names.insert(TableName::new_mem_name(device_id));
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
                            let table_name = TableName::new(device_id);
                            let mem_table =
                                MemTable::new_with_batch(&table_name, &r).await.unwrap();
                            let _ = self.ctx.deregister_table(TableReference::from(device_id));
                            let _ = self.ctx.register_batch(&device_id, r);
                            // 更新Mutables
                            println!(
                                "更新Mutables,新表名是：{:?},新memtable:{:?}",
                                table_name, mem_table
                            );
                            self.table_indexs.insert(mem_table);
                        }
                    }
                }
                Err(_e) => {
                    // println!("error: {:?}",e);
                    let _ = self.ctx.register_batch(&device_id, batch);
                }
            }
            // let size = self.table_size.insert(device_id, batch_size);
            // println!("{} size: {:?}", device_id, size);
        } else {
            println!("没有找到表名");
        }
    }
}

/**
 * Query接口
 */
impl MemTableService {
    // 获取所有表名
    pub async fn tables(&self) -> Result<Vec<TableName>, DataFusionError> {
        Ok(self.table_names.clone().into_iter().collect())
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
    pub async fn query_with_table_prefix(
        &self,
        table_prefix_name: &str,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut resp = Vec::new();
        let table_names = self.table_indexs.get_tables_with_prefix(table_prefix_name);
        for table in table_names {
            let table_name = table.get_memtable_name();
            if let Ok(df) = self.ctx.table(table_name).await {
                let fs = df.collect().await?;
                resp.extend(fs);
            }
        };
        Ok(resp)
    }

    pub async fn query_with_table(
        &self,
        table_mem_name: &str,
    ) -> Result<Vec<RecordBatch>> {
        if let Ok(df) = self.ctx.table(table_mem_name).await {
            let vs = df.collect().await?;
            Ok(vs)
        }else {
            Err(anyhow::Error::msg("111"))
        }
    }
}

impl MemTableService {
    // 判断指定memtable是否可写
    fn mutable(&self, table_name: &TableName) -> Result<bool> {
        self.table_indexs.mutable(table_name)
    }

    async fn flush(&self, memtable_name: impl AsRef<str>) -> Result<()> {
        let table_name = TableName::new_with_mem_name(memtable_name.as_ref());
        let mutable = self.mutable(&table_name)?;
        if mutable {
            let sstable_name = format!("{}{}", memtable_name.as_ref(), SSTABLE_FILE_SUFFIX);
            let sql = format!("select * from {}", memtable_name.as_ref());
            let prefix_name = TableName::get_prefix_name_with_name(&sstable_name);
            let path = create_sstable_path(&prefix_name, Level::L0);
            let _df = self
                .ctx
                .sql(sql.as_str())
                .await?
                .write_parquet(path.as_str(), DataFrameWriteOptions::new(), None)
                .await?;
            Ok(())
        } else {
            let msg = format!(
                "the table: 【{}】 is mutable!",
                table_name.get_memtable_name()
            );
            Err(anyhow::Error::msg(msg))
        }
    }
}
