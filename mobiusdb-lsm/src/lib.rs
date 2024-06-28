use anyhow::Result;
use arrow_flight::FlightData;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use wal::{offset::Offset, Append, WalService};

pub mod memtable;
mod sstable;
mod utils;
pub mod wal;

#[derive(Debug)]
pub enum LsmCommand {
    Append((Vec<FlightData>, oneshot::Sender<bool>)),
    OffsetList((String, oneshot::Sender<Vec<Offset>>)),
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
}

pub struct LsmServer {
    wal_service: WalService,
    receiver: Receiver<LsmCommand>,
}

impl LsmServer {
    async fn run(mut self) {
        let mut n = 0;
        loop {
            if let Some(cmd) = self.receiver.recv().await {
                match cmd {
                    LsmCommand::Append((fds, response)) => {
                        let b = self.wal_service.append(fds).await;
                        let _ = response.send(b);
                    }
                    LsmCommand::OffsetList((file_name, response)) => {
                        let resp = self.wal_service.indexs_map.get(file_name.as_str());
                        let resp = match resp {
                            Some(r) => r.clone(),
                            None => Vec::new(),
                        };
                        let _ = response.send(resp);
                    }
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
                receiver,
            };
            tokio::spawn(async move { server.run().await });
            Ok(sender)
        }
        Err(e) => Err(e.into()),
    }
}
