use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use arrow::{array::RecordBatch, compute::kernels::length, datatypes::Schema};
use arrow_flight::{
    utils::{batches_to_flight_data, flight_data_to_batches},
    FlightData,
};
use bytes::{Bytes, BytesMut};
use common::{
    batch_utils::{create_batch, create_batch1},
    file_utils::open_file,
};
use prost::Message;
pub mod common {
    pub mod batch_utils;
    pub mod file_utils;
}

fn create_batches() -> (Arc<Schema>, Vec<RecordBatch>) {
    let batch = create_batch();
    let schema = batch.schema();
    let batch1 = create_batch();
    let schema = batch.schema();
    let mut v = Vec::new();
    v.push(batch);
    v.push(batch1);
    (schema, v)
}

#[test]
fn batch_to_flight_data_test() {
    let (schema, batches) = create_batches();
    let fds = batches_to_flight_data(&schema, batches).unwrap();
    for fd in &fds {
        // println!("fd: {:?}", fd);
    }

    let batchs = flight_data_to_batches(&fds).unwrap();
    for batch in batchs {
        println!("batch: {:?}", batch);
    }
}

// #[test]
// fn write_flight_datas_test() {
//     let (schema,batches) = create_batches();
//     let batches_copy = batches.clone();
//     let fds = batches_to_flight_data(&schema, batches).unwrap();
//     let v = write_flight_data(fds.clone());
//     println!("v: {:?}", v);
//     let mut new_fds = Vec::new();
//     for (position, len) in v {
//         let fd = read_flight_data(position, len);
//         new_fds.push(fd);
//     }

//     if new_fds == fds {
//         println!("success");
//     }
//     let batchs = flight_data_to_batches(&new_fds).unwrap();
//     println!("batchs: {:?}", batchs);
//     assert!(batchs == batches_copy)

// }

fn read_flight_data(position: usize, len: usize) -> FlightData {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test5.arrow";
    let mut file = open_file(path);
    file.seek(SeekFrom::Start(position as u64))
        .expect("Failed to seek");
    let mut buf = vec![0; len];
    file.read_exact(&mut buf).expect("Failed to read");
    let fd = FlightData::decode(&mut buf.as_slice()).unwrap();
    fd
}

fn write_flight_data(fds: Vec<FlightData>) -> Index {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test5.arrow";
    let mut v = Vec::new();
    let mut file = open_file(path);
    // 获取当前的写入位置，即偏移量
    // 获取文件的元数据
    let metadata = file.metadata().unwrap();
    // 从元数据中获取文件大小，这个大小可以看作是文件中数据的最终偏移量
    let mut position = metadata.len() as usize;
    let mut v_len = 0;
    let mut index = Index::from(position);
    println!("初始偏移量: {}", position);
    for fd in fds {
        println!("偏移量: {}", position);
        let mut buf = BytesMut::new();
        let _ = fd.encode(&mut buf);
        let len = buf.len();
        v_len += len;
        index.add_index(len);
        file.write_all(&buf).expect("Failed to write");
        println!(
            "写入数据之后的偏移量: {}",
            file.stream_position().expect("Failed to get position")
        );
        v.push((position, len));
        position += len;
    }
    index.update(v_len);
    index
}

fn write_flight_data_with_path(path: &str, fds: Vec<FlightData>) -> Index {
    // let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test5.arrow";
    // let mut v = Vec::new();
    let mut file = open_file(path);
    // 获取当前的写入位置，即偏移量
    // 获取文件的元数据
    let metadata = file.metadata().unwrap();
    // 从元数据中获取文件大小，这个大小可以看作是文件中数据的最终偏移量
    let mut position = metadata.len() as usize;
    let mut v_len = 0;
    let mut index = Index::from(position);
    println!("初始偏移量: {}", position);
    for fd in fds {
        println!("偏移量: {}", position);
        let mut buf = BytesMut::new();
        let _ = fd.encode(&mut buf);
        let len = buf.len();
        v_len += len;
        index.add_index(len);
        file.write_all(&buf).expect("Failed to write");
        println!(
            "写入数据之后的偏移量: {}",
            file.stream_position().expect("Failed to get position")
        );
        // v.push((position, len));
        position += len;
    }
    index.update(v_len);
    index
}

fn read_flight_data_with_path(path: &str, index: &Index) -> Vec<FlightData> {
    // let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test5.arrow";
    let mut resp = Vec::new();
    let mut file = open_file(path);
    let position = index.offset;
    let len = index.len;
    println!("读取偏移量: {},长度 = {}", position, len);
    file.seek(SeekFrom::Start(position as u64))
        .expect("Failed to seek");
    let mut buf = vec![0; len];
    file.read_exact(&mut buf).expect("Failed to read");
    let mut buf_mut = BytesMut::from(buf.as_slice());
    for offset in index.indexs.clone() {
        println!("offset: {}", offset);
        let mut s = buf_mut.split_to(offset);
        let fd = FlightData::decode(&mut s).unwrap();
        resp.push(fd);
    }
    // let fd = FlightData::decode(&mut buf_mut).unwrap();
    // resp.push(fd);
    resp
}

#[derive(Debug, PartialEq, Clone)]
struct Index {
    offset: usize,
    len: usize,
    indexs: Vec<usize>,
}

impl Index {
    fn from(offset: usize) -> Self {
        Self {
            offset,
            len: 0,
            indexs: Vec::new(),
        }
    }

    fn add_index(&mut self, index: usize) {
        self.indexs.push(index);
    }

    fn update(&mut self, len: usize) {
        self.len = len;
    }
}

/**
 * 写入不同schema的batch并读取
 */
#[test]
fn write_batches_test() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test7.arrow";
    //1、创建索引表
    let mut index_map = HashMap::new();
    //2、写第一条数据
    let batch = create_batch();
    let schema = batch.schema();
    let mut first = Vec::new();
    first.push(batch);
    let fds = batches_to_flight_data(&schema, first);
    let index = write_flight_data_with_path(path, fds.unwrap());
    println!("index1: {:?}", index);
    index_map.insert("1".to_string(), index);
    //3、写第二条数据
    let batch1 = create_batch1(100);
    let schema = batch1.schema();
    let mut second = Vec::new();
    second.push(batch1);
    let fds = batches_to_flight_data(&schema, second);
    let index = write_flight_data_with_path(path, fds.unwrap());
    println!("index2: {:?}", index);
    index_map.insert("2".to_string(), index);

    //4、读取数据
    for (key, index) in index_map {
        println!("key: {}", key);
        read_batch_by_index(&index, path);
    }
}

fn read_batch_by_index(index: &Index, path: &str) {
    let fd = read_flight_data_with_path(path, index);
    let v = flight_data_to_batches(&fd).unwrap();
    println!("v: {:?}", v);
}
