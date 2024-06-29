use std::default;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow_flight::{utils::flight_data_to_batches, FlightData};

use memtable::MemTable;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use wal::{offset::Offset, Append, WalService};

pub mod memtable;
mod sstable;
mod utils;
pub mod wal;


pub const TABLE_NAME: &str = "table";





#[derive(Debug)]
pub enum LsmCommand {
    Append((Vec<FlightData>, oneshot::Sender<bool>)),
    OffsetList((String, oneshot::Sender<Vec<Offset>>)),
    // 查询指定表的数据
    Table((String, oneshot::Sender<Option<RecordBatch>>)),
    // 查询语句
    Query((String, oneshot::Sender<Option<RecordBatch>>)),
    // 查询表列表
    TableList(oneshot::Sender<Option<Vec<String>>>),
}

impl LsmCommand {
    pub fn create_append(fds: Vec<FlightData>) -> (Self, oneshot::Receiver<bool>) {
        let (sendre, receiver) = oneshot::channel();
        (LsmCommand::Append((fds, sendre)), receiver)
    }

    pub fn create_offset_list(file_name: String) -> (Self, oneshot::Receiver<Vec<Offset>>) {
        let (sendre, receiver) = oneshot::channel();
        (LsmCommand::OffsetList((file_name, sendre)), receiver)
    }

    pub fn create_table(file_name: String) -> (Self, oneshot::Receiver<Option<RecordBatch>>) {
        let (sendre, receiver) = oneshot::channel();
        (LsmCommand::Table((file_name, sendre)), receiver)
    }

    pub fn create_query(query: String) -> (Self, oneshot::Receiver<Option<RecordBatch>>) {
        let (sendre, receiver) = oneshot::channel();
        (LsmCommand::Query((query, sendre)), receiver)
    }

    pub fn create_tables() -> (Self, oneshot::Receiver<Option<Vec<String>>>) {
        let (sendre, receiver) = oneshot::channel();
        (LsmCommand::TableList(sendre), receiver)
    }
}

pub struct LsmServer {
    wal_service: WalService,
    memtable: MemTable,
    receiver: Receiver<LsmCommand>,
}

impl LsmServer {
    async fn run(mut self) {
        loop {
            if let Some(cmd) = self.receiver.recv().await {
                match cmd {
                    LsmCommand::Append((fds, response)) => {
                        let mut resp = false;
                        if let true = self.wal_service.append(fds.clone()).await {
                            if let Ok(batches) = flight_data_to_batches(&fds) {
                                let _ = self.memtable.batch_insert(batches).await; 
                                resp = true;
                            }
                        }
                        println!("append resp: {:?}", resp);
                        let _ = response.send(resp);
                    }
                    LsmCommand::OffsetList((file_name, response)) => {
                        let resp = self.wal_service.indexs_map.get(file_name.as_str());
                        let resp = match resp {
                            Some(r) => r.clone(),
                            None => Vec::new(),
                        };
                        let _ = response.send(resp);
                    },
                    LsmCommand::Table((file_name, response)) => {
                        let tables = self.memtable.tables().await;
                        println!("查询表: {:?}", tables);
                        if let Ok(table) = self.memtable.query_with_table(file_name.as_str()).await {
                            let resp_table = table.first().unwrap().clone();
                            let _ = response.send(Some(resp_table));
                        }else {
                            let _ = response.send(None);
                        }
                    },
                    LsmCommand::Query((query, response)) => {
                        if let Ok(table) = self.memtable.query(query.as_str()).await {
                            let resp_table = table.first().unwrap().clone();
                            let _ = response.send(Some(resp_table));
                        }else {
                            let _ = response.send(None);
                        }
                    },
                    LsmCommand::TableList(response) => {
                        if let Ok(table) = self.memtable.tables().await {
                            let _ = response.send(Some(table));
                        }else {
                            let _ = response.send(None);
                        }
                    },
                    _ => (),
                }
            }
        }
    }
}

/**
 * 构建一个 LSM 存储服务
 */
pub async fn server(path: impl Into<String>, wal_size: usize) -> Result<Sender<LsmCommand>> {
    let (sender, receiver) = mpsc::channel(1024);
    let wal_service = WalService::init(path, wal_size).await;
    match wal_service {
        Ok(service) => {
            let server = LsmServer {
                wal_service: service,
                memtable: MemTable::new(),// 这里可能会有问题，因为内存表是空的 
                receiver,
            };
            tokio::spawn(async move { server.run().await });
            Ok(sender)
        }
        Err(e) => Err(e.into()),
    }
}
