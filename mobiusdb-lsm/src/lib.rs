use arrow_flight::FlightData;
use tokio::sync::mpsc::{self, Receiver, Sender};
use wal::{Append, WalService};

mod memtable;
mod sstable;
mod utils;
pub mod wal;

#[derive(Debug)]
pub enum LsmCommand {
    Append(Vec<FlightData>),
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
                    LsmCommand::Append(fds) => {
                        let b = self.wal_service.append(fds).await;

                        println!("append: {:?},n = {}",b,n);
                        n += 1;
                        if !b {
                            break;
                        }
                    }
                }
            }
        }
    }
}

/**
 * 构建一个 LSM 存储服务
 */
pub async fn server(path: impl Into<String>,wal_size: usize) -> Sender<LsmCommand> {
    let (sender, receiver) = mpsc::channel(1024);
    let wal_service = WalService::init(path,wal_size).await;
    let server = LsmServer {
        wal_service,
        receiver,
    };
    tokio::spawn(async move { server.run().await });
    sender
}
