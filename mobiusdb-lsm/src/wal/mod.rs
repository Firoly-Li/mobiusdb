use bytes::{Buf, BufMut, Bytes, BytesMut};
mod active_wal;

#[derive(Debug,Default,Clone,PartialEq, Eq)]
pub struct WalMsg {
    num: u16,
    indexs: Vec<u32>,
    bytes: Bytes,
}

impl<T: ::prost::Message> From<Vec<T>> for WalMsg {
    fn from(fds: Vec<T>) -> Self {
        let mut indexs = Vec::new();
        let mut data_buf = BytesMut::new();
        let num = fds.len() as u16;
        for fd in fds {
            let mut buf = BytesMut::new();
            let _ = fd.encode(&mut buf);
            let len = buf.len();
            data_buf.put(buf);
            indexs.push(len as u32);
        }
        Self {
            num,
            indexs,
            bytes: data_buf.freeze(),
        }
    }
}

impl WalMsg {

    pub fn encode(&self,buf: &mut BytesMut){
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
            bytes: buf
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::{Int32Array, RecordBatch, StringArray, UInt64Array}, datatypes::*};
    use arrow_flight::{utils::batches_to_flight_data, FlightData};
    use bytes::BytesMut;

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
        assert_eq!(wal_msg,wal_msg1);
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
}
