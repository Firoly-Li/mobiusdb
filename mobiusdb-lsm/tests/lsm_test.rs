pub mod common {
    pub mod data_utils;
}

use std::time::Duration;

use arrow::array::RecordBatch;
use arrow_flight::utils::batches_to_flight_data;
use common::data_utils::{create_data, create_diff_data, create_student_batch1, create_students, create_teacher};
use mobiusdb_lsm::{server, LsmCommand};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_append_should_be_work() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
    let wal_size = 1024 * 1024;
    let sender = server(path, wal_size).await.unwrap();

    for i in 0..1500 {
        let fds = if i % 2 == 0 {
            create_data("test".to_string() + &i.to_string())
        } else {
            create_diff_data(i)
        };
        let (cmd, response) = LsmCommand::create_append(fds);
        let _ = sender.send(cmd).await;
        let resp = response.await.unwrap();
        // println!("append: {:?}",resp);
        sleep(Duration::from_millis(2)).await;
    }
}

fn create_group_student(n: usize,class_name: &str) -> RecordBatch {
    let names = vec!["A".to_string(),"B".to_string(),"C".to_string(),"D".to_string(),"E".to_string()];
    let ages = vec![n as i32,11,12,13,14];
    let address = vec!["BeiJing".to_string(),"ShangHai".to_string(),"ChengDu".to_string(),"GuangZhou".to_string(),"ChongQing".to_string()];
    let resp = create_students(class_name, names, ages, address);
    resp
}


fn create_group1_student() -> RecordBatch {
    let names = vec!["A".to_string(),"B".to_string(),"C".to_string(),"D".to_string(),"E".to_string()];
    let ages = vec![10,11,12,13,14];
    let address = vec!["BeiJing".to_string(),"ShangHai".to_string(),"ChengDu".to_string(),"GuangZhou".to_string(),"ChongQing".to_string()];
    let resp = create_students("三年级二班", names, ages, address);
    resp
}
fn create_group2_student() -> RecordBatch {
    let names = vec!["F".to_string(),"G".to_string(),"H".to_string()];
    let ages = vec![10,11,12];
    let address = vec!["BeiJing".to_string(),"ShangHai".to_string(),"ChengDu".to_string()];
    let resp = create_students("三年级二班", names, ages, address);
    resp
}


#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn lsm_query_should_be_work() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
    let wal_size = 1024 * 1024;
    let sender = server(path, wal_size).await.unwrap();
    for i in 0..5 {
        let class_name = if i % 2 == 0 {
            format!("name_{}",i)
        }else {
            format!("name_{}0",i)
        };
        println!("cname: {:?}",class_name);
        let batch = create_group_student(i, class_name.as_str());
        let schema = batch.schema();
        println!("schema: {:?}",schema);
        let fds = batches_to_flight_data(&schema,vec![batch]).unwrap();
        let (cmd, response) = LsmCommand::create_append(fds);
        let _ = sender.send(cmd).await;
        let resp = response.await.unwrap();
        println!("append: {:?}",resp);
    }

    let (cmd,receiver) = LsmCommand::create_tables();
    let _ = sender.send(cmd).await;
    let resp = receiver.await.unwrap();
    println!("tables: {:?}",resp);
    let (cmd,receiver) = LsmCommand::create_table("name_10".to_string());
    let _ = sender.send(cmd).await;
    let resp = receiver.await.unwrap();
    println!("table: {:?}",resp);
}



#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn lsm_query_sql_should_be_work() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
    let wal_size = 1024 * 1024;
    let sender = server(path, wal_size).await.unwrap();
    for i in 0..5 {
        let class_name = if i % 2 == 0 {
            format!("name_{}",i)
        }else {
            format!("name_{}0",i)
        };
        println!("cname: {:?}",class_name);
        let batch = create_group_student(i, class_name.as_str());
        let schema = batch.schema();
        println!("schema: {:?}",schema);
        let fds = batches_to_flight_data(&schema,vec![batch]).unwrap();
        let (cmd, response) = LsmCommand::create_append(fds);
        let _ = sender.send(cmd).await;
        let resp = response.await.unwrap();
        println!("append: {:?}",resp);
    }

    let sql = format!("select * from name_10 where age > 12");
    let (cmd,receiver) = LsmCommand::create_query(sql);
    let _ = sender.send(cmd).await;
    let resp = receiver.await.unwrap();
    println!("tables: {:?}",resp);
    // let (cmd,receiver) = LsmCommand::create_table("name_10".to_string());
    // let _ = sender.send(cmd).await;
    // let resp = receiver.await.unwrap();
    // println!("table: {:?}",resp);
}