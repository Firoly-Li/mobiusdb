use std::sync::Arc;
use anyhow::Result;
use arrow::{array::{Int32Array, RecordBatch, StringArray, UInt64Array}, datatypes::*};
use arrow_flight::{utils::{batches_to_flight_data, flight_data_to_batches}, FlightData};
use mobiusdb_lsm::wal::wal_msg::WalMsg;
use prost::Message;


pub fn create_data(n: impl Into<String>) -> Vec<FlightData> {
    let batch = create_short_batch(n);
    let schema = batch.schema();
    let mut v = Vec::new();
    v.push(batch);
    let fds = batches_to_flight_data(&schema, v).unwrap();
    fds
}

pub fn create_diff_data(n: i32) -> Vec<FlightData> {
    // let batch = create_short_batch(n);
    let batch = create_short_batch1(n);
    let schema = batch.schema();
    let mut v = Vec::new();
    v.push(batch);
    let fds = batches_to_flight_data(&schema, v).unwrap();
    fds
}

pub fn create_datas(n: impl Into<String>) -> Vec<FlightData> {
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

pub fn wal_msg_to_batch(wal_msg: WalMsg) -> Result<Vec<RecordBatch>> {
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