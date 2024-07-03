use arrow::array::RecordBatch;
use common::data_utils::{create_students, create_teacher_batch2_with_times};
use mobiusdb_lsm::memtable::{array_data_utils::merge_batches_with_schema, MemTableService};

pub mod common {
    mod batch_merge;
    pub mod data_utils;
}

fn create_group1_student() -> RecordBatch {
    let names = vec![
        "A".to_string(),
        "B".to_string(),
        "C".to_string(),
        "D".to_string(),
        "E".to_string(),
    ];
    let ages = vec![10, 11, 12, 13, 14];
    let address = vec![
        "BeiJing".to_string(),
        "ShangHai".to_string(),
        "ChengDu".to_string(),
        "GuangZhou".to_string(),
        "ChongQing".to_string(),
    ];
    let resp = create_students("三年级二班", names, ages, address);
    resp
}
fn create_group2_student() -> RecordBatch {
    let names = vec!["F".to_string(), "G".to_string(), "H".to_string()];
    let ages = vec![10, 11, 12];
    let address = vec![
        "BeiJing".to_string(),
        "ShangHai".to_string(),
        "ChengDu".to_string(),
    ];
    let resp = create_students("三年级二班", names, ages, address);
    resp
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_test() {
    let mut mem_table = MemTableService::new();
    let group1 = create_teacher_batch2_with_times("class",30);
    let r = mem_table.insert_batch(&group1).await;
    let resp = mem_table.query_with_table_prefix("class").await;
    println!("resp: {:?}", resp);
}

#[test]
fn merge_batchs_test() {
    let group1 = create_group1_student();
    let group2 = create_group2_student();
    let schema = group1.schema();
    let resp = merge_batches_with_schema(&schema, &[group1, group2]);
    println!("resp: {:?}", resp);
}
