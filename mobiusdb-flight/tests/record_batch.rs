use std::fs::File;

use arrow::{
    array::RecordBatch,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use common::{
    batch_utils::{create_batch, create_batch1},
    file_utils::open_file,
};

mod common {
    pub mod batch_utils;
    pub mod file_utils;
}

#[test]
fn read_wal() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test.wal";
    let file = open_file(path);
    let reader = StreamReader::try_new(file, None).unwrap();
    let mut n = 0;
    for batch in reader {
        println!("batch: {:?}", batch);
        n += 1;
        println!("n: {}", n);
    }
    println!("n: {}", n);
}

#[test]
fn write_record_batch() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test.wal";
    let mut file = open_file(path);
    for i in 0..10 {
        let batch = if i % 2 == 0 {
            create_batch1(i)
        } else {
            create_batch1(i)
        };
        let schema = batch.schema();

        let mut writer = StreamWriter::try_new(&mut file, &schema).unwrap();
        let _ = writer.write(&batch);
    }
}


/**
 * 只写一条Batch，序列化写入之后读取，验证也入的数据格式
 */
#[test]
fn write_batch_and_read_test() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test2.wal";
    let mut writed_data = Vec::new();
    let mut read_data = Vec::new();
    {
        let mut file = open_file(path);
        // 2、写入不同的batch数据
        let batch = create_batch();
        write_batch(&batch, &mut file);
        write_batch(&batch, &mut file);
        writed_data.push(batch.clone());
        writed_data.push(batch.clone());
    }
    {
        let file = open_file(path);
        // 3、读取数据
        let reader = StreamReader::try_new(file, None).unwrap();
        // println!("len = {:?}",reader.count());
        for batch in reader {
            match batch {
                Ok(batch) => {
                    // println!("batch = {:?}",batch);
                    read_data.push(batch);
                    println!("success");
                },
                Err(e) => println!("e = {}",e),
            }
        }
    }
    assert_eq!(writed_data, read_data);
}


/**
 * 当一个空白文件通过StreamWirter写入一个Batch的时候，数据可以直接写入
 * 当一个有数据的文件通过StreamWriter写入一个Batch的时候，会先写入一个类似于schema的数据段DataTop，然后才是真实数据
 * 且StreamReader读取到DataTop的时候会报错：Ipc error: Not expecting a schema when messages are read
 */


/**
 * 当文件通过StreamWriter写入相同结构的Batch的时候
 */ 

/**
 * 当文件通过StreamWriter写入不同结构的Batch的时候
 */ 
#[test]
fn write_different_batchs_and_read_test() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test1.wal";
    let mut writed_data = Vec::new();
    let mut read_data = Vec::new();
    {
        // 1、打开文件
        let mut file = open_file(path);
        // 2、写入不同的batch数据
        let batch = create_batch();
        write_batch(&batch, &mut file);
        writed_data.push(batch);
        let batch1 = create_batch1(100);
        write_batch(&batch1, &mut file);
        writed_data.push(batch1);
    }
    {
        let file = open_file(path);
        // 3、读取数据
        let reader = StreamReader::try_new(file, None).unwrap();
        // println!("len = {}",reader.count());
        for batch in reader {
            match batch {
                Ok(batch) => {
                    read_data.push(batch);
                },
                Err(e) => println!("e = {}",e),
            }
        }
    }
    println!("w: {:?}", writed_data);
    println!("r: {:?}", read_data);
    // assert!(read_data == writed_data);
}

// 讲batch写入指定文件
fn write_batch(batch: &RecordBatch, mut file: &mut File) {
    let schema = batch.schema();
    let mut writer = StreamWriter::try_new(&mut file, &schema).unwrap();
    let _ = writer.write(batch);
}
