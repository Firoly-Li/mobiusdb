use anyhow::Result;
use common::data_utils::create_teacher;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use mobiusdb_lsm::{
    sstable::parquet::ParquetSsTable,
    utils::file_utils::{get_sstable_path, Level},
};

pub mod common {
    pub mod data_utils;
}

#[tokio::test]
async fn parquet_sstable_save_test() -> Result<()> {
    let batch = create_teacher("class_1");
    let parquet_table = ParquetSsTable::new_with_table("class_1");
    let sstable_name = parquet_table.get_sstable_name();
    println!("sstable_name: {}", sstable_name);
    let s = parquet_table.write(&batch).await?;
    assert!(s);
    let ctx = SessionContext::new();
    let path = get_sstable_path(sstable_name.as_str(), Level::L0);
    let mut opts = ParquetReadOptions::default();
    opts.file_extension = ".sst";
    let df = ctx.read_parquet(path, opts).await?;
    println!("df: {:?}", df.collect().await);
    Ok(())
}
