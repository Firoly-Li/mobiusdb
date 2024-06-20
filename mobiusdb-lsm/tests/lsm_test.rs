pub mod common {
    pub mod data_utils;
}

use std::time::Duration;

use common::data_utils::{create_data, create_diff_data};
use mobiusdb_lsm::{server, LsmCommand};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_append_should_be_work() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/test2.wal";
    let wal_size = 1024 * 1024;
    let sender = server(path,wal_size).await;

    for i in 0..1000 {
        let fds = if i % 2 == 0 {
            create_data("test".to_string() + &i.to_string())
        } else {
            create_diff_data(i)
        };
        let _ = sender.send(LsmCommand::Append(fds)).await;
        sleep(Duration::from_millis(2)).await;
    }
}
