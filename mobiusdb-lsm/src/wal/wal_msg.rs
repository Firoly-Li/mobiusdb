use arrow_flight::FlightData;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{offset::Offset, serialization::Decoder};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WalMsg {
    num: u16,
    indexs: Vec<u32>,
    bytes: Bytes,
}

impl WalMsg {
    pub fn indexs(&self) -> &Vec<u32> {
        &self.indexs
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn encode_len(&self) -> usize {
        let mut n = (self.num * 4) as usize;
        n += self.bytes.len();
        n
    }
}

impl<T: ::prost::Message> From<Vec<T>> for WalMsg {
    fn from(fds: Vec<T>) -> Self {
        let mut indexs = Vec::new();
        let mut data_buf = BytesMut::new();
        let num = fds.len() as u16;
        fds.iter().for_each(|fd| {
            let mut buf = BytesMut::new();
            let _ = fd.encode(&mut buf);
            let len = buf.len();
            data_buf.put(buf);
            indexs.push(len as u32);
        });
        Self {
            num,
            indexs,
            bytes: data_buf.freeze(),
        }
    }
}

impl<T: ::prost::Message> From<&Vec<T>> for WalMsg {
    fn from(fds: &Vec<T>) -> Self {
        let mut indexs = Vec::new();
        let mut data_buf = BytesMut::new();
        let num = fds.len() as u16;
        fds.iter().for_each(|fd| {
            let mut buf = BytesMut::new();
            let _ = fd.encode(&mut buf);
            let len = buf.len();
            data_buf.put(buf);
            indexs.push(len as u32);
        });
        Self {
            num,
            indexs,
            bytes: data_buf.freeze(),
        }
    }
}

impl WalMsg {
    pub fn encode(&self, buf: &mut BytesMut) {
        let indexs = self.indexs.clone();
        buf.put_u16(self.num);
        for index in indexs {
            buf.put_u32(index);
        }
        buf.put(self.bytes.clone());
    }

    pub fn decode(mut buf: Bytes) -> Self {
        let num = buf.get_u16();
        let mut indexs = Vec::new();
        for _ in 0..num {
            let index = buf.get_u32();
            indexs.push(index);
        }
        Self {
            num,
            indexs,
            bytes: buf,
        }
    }
}

impl Decoder for Vec<WalMsg> {
    type Error = anyhow::Error;

    fn decode(mut bytes: Bytes) -> anyhow::Result<Self, Self::Error> {
        println!("start decode bytes len = {}", bytes.len());
        let mut wal_msgs = Vec::new();
        while bytes.len() > 4 {
            let wal_msg_len = bytes.get_u32();
            println!(
                "本次循环：wal_msg_len = {}，bytes len = {}",
                wal_msg_len,
                bytes.len()
            );
            if bytes.len() < wal_msg_len as usize {
                println!("bytes len = {},wal_msg_len = {}", bytes.len(), wal_msg_len);
                return Err(anyhow::Error::msg("1234"));
            }
            let buf = bytes.split_to(wal_msg_len as usize);
            println!("after split off bytes len = {}", bytes.len());
            let wal_msg = WalMsg::decode(buf);
            wal_msgs.push(wal_msg);
        }
        Ok(wal_msgs)
    }
}

pub fn walmsgs_to_offsets(walmsgs: &Vec<WalMsg>) -> Vec<Offset> {
    let mut offsets = Vec::new();
    let mut offset = 0;
    for wal_msg in walmsgs {
        let wal_msg_len = wal_msg.encode_len();
        let offset_obj = Offset {
            offset,
            len: wal_msg_len,
        };
        offsets.push(offset_obj);
        offset += wal_msg_len;
    }
    offsets
}

pub trait IntoWalMsg {
    fn into_wal_msg(&self) -> WalMsg;
}

impl IntoWalMsg for Vec<FlightData> {
    fn into_wal_msg(&self) -> WalMsg {
        WalMsg::from(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, RecordBatch, StringArray, UInt64Array},
        datatypes::*,
    };
    use arrow_flight::{utils::batches_to_flight_data, FlightData};
    use bytes::{BufMut, Bytes, BytesMut};

    use crate::wal::serialization::Decoder;

    use super::WalMsg;

    #[test]
    fn wal_msg_encode_and_decode_test() {
        let fds = create_datas("test");
        let wal_msg = WalMsg::from(fds);
        println!("wal_msg: {:?}", wal_msg);
        let mut buf = BytesMut::new();
        wal_msg.encode(&mut buf);
        let wal_msg1 = WalMsg::decode(buf.freeze());
        println!("wal_msg1: {:?}", wal_msg1);
        assert_eq!(wal_msg, wal_msg1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn wal_msg_encode_and_decode_test1() {
        let buf = create_bytes_from_walmsgs(20);
        let vecs = Vec::<WalMsg>::decode(buf);
        println!("v: {:?}", vecs);
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

    pub fn create_bytes_from_walmsgs(num: usize) -> Bytes {
        let mut resp = BytesMut::new();
        let b = Bytes::from("hello world !");
        for i in 0..num {
            let wal_msg = WalMsg {
                num: i as u16,
                indexs: vec![45; i],
                bytes: b.clone(),
            };
            let mut buf = BytesMut::new();
            wal_msg.encode(&mut buf);
            resp.put_u32(buf.len() as u32);
            resp.put(buf);
        }
        resp.freeze()
    }
}
