use std::{io::SeekFrom, sync::Arc};

use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::{utils::file_utils::async_open_flie, Index};

use super::WalMsg;

/**
 *
 */
#[derive(Debug)]
pub struct ActiveWal {
    wal: Arc<Mutex<File>>,
}

impl ActiveWal {
    pub async fn new(path: &str) -> Self {
        let file = async_open_flie(path).await;
        Self {
            wal: Arc::new(Mutex::new(file)),
        }
    }

    /**
     * 新增一条数据
     */
    pub async fn append<T: ::prost::Message>(&self, fds: Vec<T>) -> Result<Index> {
        let mut file = self.wal.lock().await;
        // 当前文件的下标
        let position = file.metadata().await.unwrap().len();
        // let mut v_len = 0;
        let mut index = Index::from(position as usize);
        // println!("初始偏移量: {}", position);
        let mut wal_buf = BytesMut::new();
        // for fd in fds {
        //     println!("偏移量: {}", position);
        //     let mut buf = BytesMut::new();
        //     let _ = fd.encode(&mut buf);
        //     let len = buf.len() as u32;
        //     v_len += len;
        //     data_buf.put(buf);
        //     index.add_index(len);
        //     position += len as u64;
        // }
        let wal_msg = WalMsg::from(fds);
        wal_msg.encode(&mut wal_buf);
        let v_len = wal_buf.len() as u32;
        println!("wal_buf len: {}", v_len);
        let mut bytes = BytesMut::new();
        bytes.put_u32(v_len);
        bytes.put(wal_buf);
        file.write_all(&bytes.freeze()).await.expect("Failed to write");
        index.update((v_len + 4) as usize);
        Ok(index)
    }

    /**
     * 根据index读取一条数据
     */
    pub async fn read(&self, offset: usize) -> Result<WalMsg> {
        let mut file = self.wal.lock().await;
        file.seek(SeekFrom::Start(offset as u64)).await
        .expect("Failed to seek");
        let mut lens = vec![0; 4];
        let resp = file.read_exact(&mut lens).await;
        println!("lens: {:?}", lens);
        let len = BytesMut::from(lens.as_slice()).get_u32();
        println!("len: {}", len);
        let mut buf = vec![0; len as usize];
        file.read_exact(&mut buf).await.expect("Failed to read");
        let buf_mut = BytesMut::from(buf.as_slice());
        let wal_msg = WalMsg::decode(buf_mut.freeze());
        Ok(wal_msg)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use anyhow::{Ok, Result};
    use arrow::{
        array::{Int32Array, RecordBatch, StringArray, UInt64Array},
        datatypes::*,
    };
    use arrow_flight::{utils::{batches_to_flight_data, flight_data_to_batches}, FlightData};
    use prost::Message;

    use crate::{wal::{active_wal::ActiveWal, WalMsg}, Index};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn activewal_should_be_work() {
        let active_wal =
            ActiveWal::new("/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/test.wal")
                .await;
        let fds = create_datas("test");
        let index = active_wal.append(fds).await.unwrap();
        println!("index: {:?}", index);
        let wal_msg = active_wal.read(index.offset).await.unwrap();
        let resp = wal_msg_to_batch(wal_msg).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_test() {
        let active_wal =
            ActiveWal::new("/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/test.wal")
                .await;
        let wal_msg = active_wal.read(2055).await.unwrap();
        let resp = wal_msg_to_batch(wal_msg).unwrap();
    }


    fn create_data(n: impl Into<String>) -> Vec<FlightData> {
        let batch = create_short_batch(n);
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

    

    fn wal_msg_to_batch(wal_msg: WalMsg) -> Result<Vec<RecordBatch>> {
        let indexs = wal_msg.indexs;
        let mut buf_mut = wal_msg.bytes;
        let mut resp = Vec::new();
        for offset in indexs.clone() {
            println!("offset: {}", offset);
            let mut s = buf_mut.split_to(offset as usize);
            let fd = FlightData::decode(&mut s).unwrap();
            resp.push(fd);
        }
        let resp = flight_data_to_batches(&resp).unwrap();
        println!("resp: {:?}", resp);
        Ok(resp)
    }
}
