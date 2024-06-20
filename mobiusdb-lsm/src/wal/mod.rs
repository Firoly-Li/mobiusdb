pub mod active_wal;
pub(crate) mod offset;
pub mod wal_msg;

use std::collections::HashMap;

use active_wal::ActiveWal;
use offset::Offset;
use wal_msg::IntoWalMsg;

#[allow(async_fn_in_trait)]
pub trait Append<T> {
    type Result;
    async fn append(&mut self, data: T) -> Self::Result;
}

#[derive(Debug)]
pub struct WalService {
    wal: ActiveWal,
    wal_max_size: usize,
    indexs: Vec<Offset>,
    indexs_map: HashMap<String,Vec<Offset>>
}

impl WalService {
    pub async fn init(path: impl Into<String>,wal_size: usize) -> Self {
        Self {
            wal: ActiveWal::with_size(&path.into(),wal_size).await,
            wal_max_size: wal_size,
            indexs: Vec::new(),
            indexs_map: HashMap::new()
        }
    }
}

impl<T> Append<T> for WalService
where
    T: IntoWalMsg,
{
    type Result = bool;

    async fn append(&mut self, data: T) -> Self::Result {
        let resp = self.wal.append(data).await;
        match resp {
            Ok(_) => true,
            Err(e) => {
                println!("e = {:?}",e);
                self.indexs_map.insert(self.wal.name(), self.indexs.clone());
                false
            }
        }
    }
}
