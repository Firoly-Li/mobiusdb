pub mod active_wal;
pub mod index_file;
pub(crate) mod offset;
pub mod serialization;
pub mod wal_msg;

use std::collections::HashMap;

use anyhow::Result;

use active_wal::ActiveWal;
use offset::Offset;
use wal_msg::IntoWalMsg;

use crate::utils::file_utils::{get_files_name, has_file_in_path};

#[allow(async_fn_in_trait)]
pub trait Wal {
    async fn append(&mut self, data: impl IntoWalMsg) -> Result<Offset>;

    async fn load(&self, path: impl Into<String>) -> Result<bool>;
}

#[allow(async_fn_in_trait)]
pub trait Append<T> {
    type Result;
    async fn append(&mut self, data: T) -> Self::Result;
}

#[derive(Debug)]
pub struct WalService {
    path: String,
    wal: ActiveWal,
    wal_max_size: usize,
    // 记录ActiveWal文件中的offset
    pub(crate) indexs: Vec<Offset>,
    // 记录ActiveWal文件中的offset, key为wal文件名, value为offset
    pub(crate) indexs_map: HashMap<String, Vec<Offset>>,
}
impl WalService {
    // 初始化walService
    pub async fn init(path: impl Into<String>, wal_size: usize) -> Result<Self> {
        let path: String = path.into();
        match has_file_in_path(&path).await {
            Ok(b) => match b {
                true => {
                    println!("wal文件存在,读取wal文件");
                    // 存在wal文件
                    if let Ok(mut files_name) = get_files_name(path.as_str()).await {
                        files_name.sort();
                        let file_name = files_name.last().unwrap();
                        let file_path = path.clone() + "/"+file_name;
                        let a: Vec<&str> = file_name.split(".").collect();
                        // print!("file_path = {}",file_path);
                        // 1、加载wal文件
                        let (wal,offsets) = ActiveWal::load(file_path.as_str()).await.unwrap();
                        // println!("offsets = {:?}",offsets);

                        // 3、创建
                        Ok(Self {
                            path: path.clone(),
                            wal,
                            wal_max_size: wal_size,
                            indexs: offsets,
                            indexs_map: HashMap::new(),
                        })
                    } else {
                        Err(anyhow::Error::msg("wal文件获取失败"))
                    }
                }
                false => {
                    println!("wal文件不存在,新建wal文件");
                    // 不存在wal文件,是第一次启动
                    let wal = ActiveWal::with_size(&path, wal_size).await?;
                    Ok(Self {
                        path: path.clone(),
                        wal,
                        wal_max_size: wal_size,
                        indexs: Vec::new(),
                        indexs_map: HashMap::new(),
                    })
                }
            },
            Err(e) => Err(e.into()),
        }
    }
    async fn update_wal(&mut self) -> Result<bool> {
        let old_wal_name = self.wal.name();
        let old_indexs = self.indexs.clone();
        let new_wal = ActiveWal::with_size(&self.path, self.wal_max_size).await?;
        self.indexs_map.insert(old_wal_name, old_indexs);
        self.wal = new_wal;
        Ok(true)
    }
}

impl<T> Append<T> for WalService
where
    T: IntoWalMsg + Clone,
{
    type Result = bool;

    async fn append(&mut self, data: T) -> Self::Result {
        loop {
            // 1、数据落盘
            let resp = self.wal.append(data.clone()).await;
            let resp = match resp {
                Ok(offset) => {
                    self.indexs.push(offset);
                    true
                }
                Err(_e) => {
                    println!("wal 写入已满,新建wal文件：【{:?}】", self.wal.name());
                    let resp = self.update_wal().await;
                    match resp {
                        Ok(_b) => continue,
                        Err(e) => {
                            println!("wal 更新失败：{:?}", e);
                            return false;
                        }
                    }
                }
            };
            return resp;
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::WalService;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn wal_service_init_test() {
        let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp";
        let service = WalService::init(path, 1024 * 1024).await;
        println!("service:{:?}", service);
    }
}
