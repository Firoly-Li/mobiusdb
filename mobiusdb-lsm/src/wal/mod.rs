pub mod active_wal;
pub(crate) mod offset;
pub mod wal_msg;

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
}

impl WalService {
    pub async fn init(path: impl Into<String>,wal_size: usize) -> Self {
        Self {
            wal: ActiveWal::with_size(&path.into(),wal_size).await,
            wal_max_size: wal_size,
            indexs: Vec::new()
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
                false
            }
        }
    }
}
