use anyhow::Result;
use arrow::{
    array::{Int32Array, RecordBatch, StringArray, UInt64Array},
    datatypes::*,
};
use arrow_flight::{
    utils::{batches_to_flight_data, flight_data_to_batches},
    FlightData,
};
use mobiusdb_lsm::{wal::wal_msg::WalMsg, TABLE_NAME};
use prost::Message;
use std::{collections::HashMap, sync::Arc};

pub fn create_data(n: impl Into<String>) -> Vec<FlightData> {
    let batch = create_short_batch(n.into().as_str());
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

pub fn create_teacher(class_name: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("teach", DataType::Utf8, true),
    ]).with_metadata({
        let mut map = HashMap::new();
        map.insert(TABLE_NAME.to_string(), class_name.to_string());
        map
    }));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["James".to_string(), "Michael".to_string(), "David".to_string()])),
            Arc::new(Int32Array::from(vec![18, 19, 20])),
            Arc::new(StringArray::from(vec!["Computer".to_string(), "language".to_string(), "Music".to_string()])),
        ],
    )
    .unwrap();
    batch
}

pub fn create_students(class_name: &str, names: Vec<String>,ages: Vec<i32>,address: Vec<String>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("address", DataType::Utf8, true),
    ]).with_metadata({
        let mut map = HashMap::new();
        map.insert(TABLE_NAME.to_string(), class_name.to_string());
        map
    }));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(ages)),
            Arc::new(StringArray::from(address)),
        ],
    )
    .unwrap();
    batch
}



pub fn create_student_batch1(name: &str,age: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("address", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![name.to_string(), "Tom".to_string(), "Jack".to_string()])),
            Arc::new(Int32Array::from(vec![age, 19, 20])),
            Arc::new(Int32Array::from(vec![71, 81, 91])),
        ],
    )
    .unwrap();
    batch
}

pub fn create_student_batch2() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["John1".to_string(), "Tom1".to_string(), "Jack1".to_string()])),
            Arc::new(Int32Array::from(vec![18, 19, 20])),
        ],
    )
    .unwrap();
    batch
}

pub fn create_teacher_batch2() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("teach", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["James".to_string(), "Michael".to_string(), "David".to_string()])),
            Arc::new(Int32Array::from(vec![18, 19, 20])),
            Arc::new(StringArray::from(vec!["Computer".to_string(), "language".to_string(), "Music".to_string()])),
        ],
    )
    .unwrap();
    batch
}