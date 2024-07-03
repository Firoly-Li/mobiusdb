pub mod common {
    pub mod data_utils;
}

use std::{sync::atomic::Ordering, time::Duration};

use arrow::array::RecordBatch;
use arrow_flight::utils::{batches_to_flight_data, flight_data_to_batches};
use common::data_utils::{create_batch_with_opts, create_data, create_diff_data, create_students, create_teacher_batch2_with_times};
use datafusion::prelude::SessionContext;
use mobiusdb_lsm::{server, utils::data_utils::{self, flight_data_to_batch}};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_append_should_be_work() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
    let wal_size = 1024 * 1024;
    let client = server(path, wal_size).await.unwrap();

    for i in 0..5 {
        let fds = if i % 2 == 0 {
            create_data("test".to_string() + &i.to_string())
        } else {
            create_diff_data(i)
        };
        let resp = client.append_fds(fds).await;
        println!("append: {:?}", resp);
        sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn table_size_test() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
    let wal_size = 1024 * 1024;
    let client = server(path, wal_size).await.unwrap();
    for _i in 0..5 {
        let fds = create_data("test");
        let resp = client.append_fds(fds).await;
        println!("append: {:?}", resp);
        sleep(Duration::from_millis(2)).await;
    }
}

#[test]
fn batch_size_test() {
    // let batch = create_data("test");
    // println!("batch: {:?}", batch[0].data_header.len() + batch[0].data_body.len());
    // let batch1 = flight_data_to_batches(&batch).unwrap();
    // println!("batch: {:?}", batch_size(&batch1[0]));

    let batch = create_batch_with_opts(2000, "table_name");
    let schema = batch.schema();
    println!("batch: {:?} kb", (data_utils::batch_size(&batch)) / 1000);
    let mut vs = Vec::new();
    vs.push(batch);
    let batch1 = batches_to_flight_data(&schema, vs).unwrap();
    println!(
        "fd: {:?} kb",
        (batch1[0].data_header.len() + batch1[0].data_body.len()) / 1000
    );
}


/**
 * todo 这个测试不能通过！
 */
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn batch_size_test3() {
    let batch = create_teacher_batch2_with_times("class_1", 20);
    let schema = batch.schema();
    let batch1_size = data_utils::batch_size(&batch);
    let mut vs = Vec::new();
    vs.push(batch);
    let fds = batches_to_flight_data(&schema, vs).unwrap();
    let batchs = flight_data_to_batch(&fds).unwrap();
    println!("batchs = {:?}",batchs);
    let batchs_size = data_utils::batch_size(&batchs);
    let b = batch1_size == batchs_size;
    assert!(!b);
}

/**
 *
 */
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn batch_size_test1() {
    let batch = create_teacher_batch2_with_times("class_1", 20);
    let schema = batch.schema();
    let mut vs = Vec::new();
    vs .push(batch);
    let batch1 = batches_to_flight_data(&schema, vs).unwrap();
    let fd = flight_data_to_batches(&batch1).unwrap();
    let fd = fd.first().unwrap();
    println!("fd: {:?}", fd);
    println!("batch_len: {:?}", data_utils::batch_size(&fd));

    let batch1 = create_teacher_batch2_with_times("class_1", 20);
    println!("batch1: {:?}", batch1);
    println!("batch1_len: {:?}", data_utils::batch_size(&batch1));
    
    // let ctx = SessionContext::new();
    // let df = ctx.read_batch(batch).unwrap();
    // let _ = df.write_parquet("/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/batch_size.parquet", DataFrameWriteOptions::new(), None).await;
}

fn create_group_student(n: usize, class_name: &str) -> RecordBatch {
    let names = vec![
        "A".to_string(),
        "B".to_string(),
        "C".to_string(),
        "D".to_string(),
        "E".to_string(),
    ];
    let ages = vec![n as i32, 11, 12, 13, 14];
    let address = vec![
        "BeiJing".to_string(),
        "ShangHai".to_string(),
        "ChengDu".to_string(),
        "GuangZhou".to_string(),
        "ChongQing".to_string(),
    ];
    let resp = create_students(class_name, names, ages, address);
    resp
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn lsm_query_should_be_work() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
    let wal_size = 1024 * 1024;
    let client = server(path, wal_size).await.unwrap();
    for i in 0..5 {
        let class_name = if i % 2 == 0 {
            format!("name_{}", i)
        } else {
            format!("name_{}0", i)
        };
        println!("cname: {:?}", class_name);
        let batch = create_group_student(i, class_name.as_str());
        let resp = client.append_batch(batch).await;
        println!("append: {:?}", resp);
    }
    let resp = client.table_list().await;
    println!("table_list: {:?}", resp);
    let table = client.table("name_10").await;
    println!("table: {:?}", table);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn lsm_query_sql_should_be_work() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/wal/";
    let wal_size = 1024 * 1024;
    let client = server(path, wal_size).await.unwrap();
    for i in 0..100 {
        // let class_name = if i % 2 == 0 {
        //     format!("name_{}", i)
        // } else {
        //     format!("name_{}0", i)
        // };
        let class_name = "class_1";
        // println!("cname: {:?}", class_name);
        let batch = create_teacher_batch2_with_times(class_name,i * 10);
        let size = data_utils::batch_size(&batch);
        // println!("客户端发送的数据 batch size: {:?}", size);
        let schema = batch.schema();
        // println!("schema: {:?}", schema);
        let fds = batches_to_flight_data(&schema, vec![batch]).unwrap();
        let resp = client.append_fds(fds).await;
        // println!("resp: {:?}", resp);
    }

    // let sql = format!("select * from class_1 where age > 12");
    let resp = client.table("class_1").await;
    println!("tables: {:?}", resp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn data_size_test() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
    let wal_size = 1024 * 1024;
    let client = server(path, wal_size).await.unwrap();

    let batch = create_group1_student();
    let schema = batch.schema();
    println!("schema: {:?}", schema);
    let fds = batches_to_flight_data(&schema, vec![batch]).unwrap();
    let resp = client.append_fds(fds).await;
    println!("resp: {:?}", resp);
}
