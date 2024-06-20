use std::{io::SeekFrom, sync::Arc};

use anyhow::{Ok, Result};
use arrow_flight::FlightData;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::{utils::{file_utils::{async_open_flie, async_open_only_flie}, time_utils::now}, wal::wal_msg::WalMsg};

use super::{offset::Offset, wal_msg::IntoWalMsg, Append};

// 默认wal文件大小: 1G
const MAX_SIZE: usize = 1024 * 1024 * 1024;

/**
 * 活动的wal文件，wal文件是顺序写入的
 */
#[derive(Debug, Clone)]
pub struct ActiveWal {
    name: String,
    write_enable: bool,
    wal: Arc<Mutex<File>>,
    max_size: usize,
    size: usize,
}

impl ActiveWal {
    pub async fn new(path: &str) -> Self {
        let file_name = now().to_string() + ".wal";
        let path = if path.ends_with("/") {
            format!("{}{}", path, file_name)
        }else {
            format!("{}/{}", path, file_name)
        };
        let file = async_open_flie(path.as_str()).await;
        let position = file.metadata().await.unwrap().len() as usize;
        Self {
            name: file_name,
            write_enable: true,
            wal: Arc::new(Mutex::new(file)),
            max_size: MAX_SIZE,
            size: position,
        }
    }

    /**
     * 打开wal文件,此时wal文件为只读模式
     */
    pub async fn open(path: &str) -> Self {
        let file_name = path.split("/").collect::<Vec<&str>>().last().unwrap().to_string();
        let file = async_open_only_flie(path).await;
        let position = file.metadata().await.unwrap().len() as usize;
        Self {
            name: file_name,
            write_enable: false,
            wal: Arc::new(Mutex::new(file)),
            max_size: MAX_SIZE,
            size: position,
        }
    }

    pub async fn with_size(path: &str, max_size: usize) -> Self {
        let file_name = now().to_string() + ".wal";
        let path = if path.ends_with("/") {
            format!("{}{}", path, file_name)
        }else {
            format!("{}/{}", path, file_name)
        };
        let file = async_open_flie(path.as_str()).await;
        let position = file.metadata().await.unwrap().len() as usize;
        Self {
            name: file_name,
            write_enable: true,
            wal: Arc::new(Mutex::new(file)),
            max_size,
            size: position,
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

/**
 * wal文件数据写入相关的方法
 */
impl ActiveWal {
    /**
     * 新增一条数据
     */
    async fn append<T: ::prost::Message>(&mut self, fds: Vec<T>) -> Result<Offset> {
        if !self.write_enable {
            return Err(anyhow::Error::msg("wal file is not writeable"));
        }
        let wal_msg = WalMsg::from(fds);
        self.append_wal_msg(wal_msg).await
    }

    async fn append_wal_msg(&mut self, wal_msg: WalMsg) -> Result<Offset> {
        if !self.write_enable {
            return Err(anyhow::Error::msg("wal file is not writeable"));
        }
        let mut wal_buf = BytesMut::new();
        wal_msg.encode(&mut wal_buf);
        self.append_bytes(wal_buf.freeze()).await
    }

    async fn append_bytes(&mut self, bytes: Bytes) -> Result<Offset> {
        if !self.write_enable {
            return Err(anyhow::Error::msg("wal file is not writeable"));
        }
        let mut file = self.wal.lock().await;
        // 当前文件的下标
        if self.size > self.max_size {
            return Err(anyhow::Error::msg("Wal file is full"));
        }
        // let mut v_len = 0;
        let mut index = Offset::from((self.size + 4) as usize);
        let v_len = bytes.len() as u32;
        println!("wal_buf len: {}", v_len);
        let mut new_bytes = BytesMut::new();
        new_bytes.put_u32(v_len);
        new_bytes.put(bytes);
        file.write_all(&new_bytes).await.expect("Failed to write");
        index.update((v_len) as usize);
        let add_size = (v_len + 4) as usize;
        self.size += add_size;
        Ok(index)
    }
}

/**
 * wal 文件读取相关的方法
 */
impl ActiveWal {
    /**
     * 根据index读取一条数据,这个offset是WalHeader + WalMsg的偏移量
     */
    pub async fn read_with_offset(&self, offset: usize) -> Result<WalMsg> {
        let mut file = self.wal.lock().await;
        file.seek(SeekFrom::Start(offset as u64))
            .await
            .expect("Failed to seek");
        let mut lens = vec![0; 4];
        let _resp = file.read_exact(&mut lens).await;
        let len = BytesMut::from(lens.as_slice()).get_u32();
        let mut buf = vec![0; len as usize];
        file.read_exact(&mut buf).await.expect("Failed to read");
        let buf_mut = BytesMut::from(buf.as_slice());
        let wal_msg = WalMsg::decode(buf_mut.freeze());
        Ok(wal_msg)
    }

    pub async fn read_with_index(&self, offset: Offset) -> Result<WalMsg> {
        let mut file = self.wal.lock().await;
        file.seek(SeekFrom::Start(offset.offset as u64))
            .await
            .expect("Failed to seek");
        let mut buf = vec![0; offset.len as usize];
        file.read_exact(&mut buf).await.expect("Failed to read");
        let buf_mut = BytesMut::from(buf.as_slice());
        let wal_msg = WalMsg::decode(buf_mut.freeze());
        Ok(wal_msg)
    }
}

/**
 * impl <T>Append<T> for ActiveWal
 */
impl<T> Append<T> for ActiveWal
where
    T: IntoWalMsg,
{
    type Result = Result<Offset>;
    async fn append(&mut self, data: T) -> Self::Result {
        if !self.write_enable {
            return Err(anyhow::Error::msg("wal file is not writeable"));
        }
        self.append_wal_msg(data.into_wal_msg()).await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Ok, Result};
    use arrow::{
        array::{Int32Array, RecordBatch, StringArray, UInt64Array},
        datatypes::*,
    };
    use arrow_flight::{
        utils::{batches_to_flight_data, flight_data_to_batches},
        FlightData,
    };
    use bytes::Bytes;
    use prost::Message;
    use std::sync::Arc;

    use crate::wal::{active_wal::ActiveWal, offset::Offset, wal_msg::WalMsg};


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn active_wal_open_test() {
        let mut active_wal =
            ActiveWal::open("/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/test2.wal")
                .await;
        let resp = active_wal.append_bytes(Bytes::from_static(b"test")).await;
        // println!("resp: {:?}", resp);
        assert_eq!(resp.is_err(), true);
    }



    /**
     * 测试ActiveWal的读写
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn activewal_should_be_work() {
        let mut active_wal =
            ActiveWal::new("/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/")
                .await;
        let fds = create_datas("test");
        let index = active_wal.append(fds).await.unwrap();
        println!("index: {:?}", index);
        let wal_msg = active_wal.read_with_offset(index.offset - 4).await.unwrap();
        let resp = wal_msg_to_batch(wal_msg).unwrap();
    }

    /**
     * 测试ActiveWal的读
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_test() {
        let active_wal =
            ActiveWal::open("/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/test.wal")
                .await;
        let wal_msg = active_wal.read_with_offset(0).await.unwrap();
        let resp = wal_msg_to_batch(wal_msg).unwrap();
        let index = Offset {
            offset: 4,
            len: 2051,
        };
        let wal_msg = active_wal.read_with_index(index).await.unwrap();
        let resp1 = wal_msg_to_batch(wal_msg).unwrap();
    }

    /**
     * 测试ActiveWal的写入不同的数据
     */
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn active_wal_write_diff_data_test() {
        let mut active_wal =
            ActiveWal::new("/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/")
                .await;
        let mut indexs = Vec::new();
        // 写入数据
        for i in 0..10000 {
            let fds = if i % 2 == 0 {
                create_data("test".to_string() + &i.to_string())
            } else {
                create_diff_data(i)
            };
            let index = active_wal.append(fds).await.unwrap();
            indexs.push(index);
        }
        let indexs_len = indexs.len();
        println!("indexs: {:?}", indexs_len);
        // 读取数据
        let mut n = 0;
        for index in indexs {
            let wal_msg1 = active_wal.read_with_index(index).await.unwrap();
            let resp = wal_msg_to_batch(wal_msg1).unwrap();
            n += 1;
        }
        assert_eq!(n, indexs_len);

        // let wal_msg = active_wal.read_with_offset(index.offset - 4).await.unwrap();
        // let resp = wal_msg_to_batch(wal_msg).unwrap();
        // let wal_msg1 = active_wal.read_with_index(index1).await.unwrap();
        // let resp = wal_msg_to_batch(wal_msg1).unwrap();
    }

    fn create_data(n: impl Into<String>) -> Vec<FlightData> {
        let batch = create_short_batch(n);
        let schema = batch.schema();
        let mut v = Vec::new();
        v.push(batch);
        let fds = batches_to_flight_data(&schema, v).unwrap();
        fds
    }

    fn create_diff_data(n: i32) -> Vec<FlightData> {
        // let batch = create_short_batch(n);
        let batch = create_short_batch1(n);
        let schema = batch.schema();
        let mut v = Vec::new();
        v.push(batch);
        let fds = batches_to_flight_data(&schema, v).unwrap();
        fds
    }

    fn create_datas(n: impl Into<String>) -> Vec<FlightData> {
        let name = n.into();
        let batch = create_short_batch(name.clone());
        let batch1 = create_short_batch(name + "1");
        let schema = batch.schema();
        let mut v = Vec::new();
        v.push(batch);
        v.push(batch1);
        let fds = batches_to_flight_data(&schema, v).unwrap();
        fds
    }

    pub fn create_short_batch(n: impl Into<String>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("address", DataType::Int32, false),
            Field::new("time", DataType::UInt64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    n.into(),
                    "2".to_string(),
                    "3".to_string(),
                ])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(UInt64Array::from(vec![None, None, Some(9)])),
            ],
        )
        .unwrap();
        batch
    }

    pub fn create_short_batch1(n: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("time", DataType::UInt64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![n, 5, 6])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(UInt64Array::from(vec![None, None, Some(9)])),
            ],
        )
        .unwrap();
        batch
    }

    fn wal_msg_to_batch(wal_msg: WalMsg) -> Result<Vec<RecordBatch>> {
        let indexs = wal_msg.indexs();
        let mut buf_mut = wal_msg.bytes();
        let mut resp = Vec::new();
        for offset in indexs.clone() {
            // println!("offset: {}", offset);
            let mut s = buf_mut.split_to(offset as usize);
            let fd = FlightData::decode(&mut s).unwrap();
            resp.push(fd);
        }
        let resp = flight_data_to_batches(&resp).unwrap();
        // println!("resp: {:?}", resp);
        Ok(resp)
    }
}
