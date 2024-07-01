use common::data_utils::{create_data, create_student_batch2};
use mobiusdb_lsm::lsm_client::LsmClient;
use tokio::sync::mpsc;

pub mod common {
    pub mod data_utils;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn client_should_be_work() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = LsmClient::new(sender);
    tokio::spawn(async move {
        let _ = client.table_list().await;
    });
    let resp = receiver.recv().await.unwrap();
    println!("resp: {:?}", resp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn client_append_batch_test() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = LsmClient::new(sender);
    tokio::spawn(async move {
        let batch = create_student_batch2();
        let _ = client.append_batch(batch).await;
    });
    let resp = receiver.recv().await.unwrap();
    println!("resp: {:?}", resp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn client_append_fds_test() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = LsmClient::new(sender);
    tokio::spawn(async move {
        let fds = create_data("test".to_string() + &"1".to_string());
        let _ = client.append_fds(fds).await;
    });
    let resp = receiver.recv().await.unwrap();
    println!("resp: {:?}", resp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn client_query_test() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = LsmClient::new(sender);
    tokio::spawn(async move {
        let _ = client.query("select * from test").await;
    });
    let resp = receiver.recv().await.unwrap();
    println!("resp: {:?}", resp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn client_query_table_test() {
    let (sender, mut receiver) = mpsc::channel(1);
    let client = LsmClient::new(sender);
    tokio::spawn(async move {
        let _ = client.table("batch").await;
    });
    let resp = receiver.recv().await.unwrap();
    println!("resp: {:?}", resp)
}
