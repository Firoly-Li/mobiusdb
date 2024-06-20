use std::{
    fmt::{self, Debug},
    fs::File,
    io::Read,
};

use arrow::{
    array::RecordBatch,
    ipc::{
        reader::StreamReader,
        writer::{
            DictionaryTracker, EncodedData, FileWriter, IpcDataGenerator, IpcWriteOptions,
            StreamWriter,
        },
    },
};
use bytes::BytesMut;
use common::{
    batch_utils::{create_batch, create_batch1, create_short_batch},
    file_utils::open_file,
};

mod common {
    pub mod batch_utils;
    pub mod file_utils;
    pub mod universal_batch;
}

mod value {
    pub mod int;
    pub mod long;
    pub mod time;
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
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test.arrow";
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
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test2.arrow";
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
                }
                Err(e) => println!("e = {}", e),
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
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test1.arrow";
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
                Ok(batc) => {
                    read_data.push(batc);
                }
                Err(e) => println!("e = {}", e),
            }
        }
    }
    // println!("w: {:?}", writed_data);
    // println!("r: {:?}", read_data);
    assert!(read_data == writed_data);
}

// 讲batch写入指定文件
fn write_batch(batch: &RecordBatch, mut file: &mut File) {
    let schema = batch.schema();
    let mut writer = StreamWriter::try_new(&mut file, &schema).unwrap();
    let _ = writer.write(batch);
}

#[test]
fn read_file_test() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test1.arrow";
    let mut file = open_file(path);
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    println!("buf = {:?}", buf);
}

#[test]
fn ipc_generator() {
    let batch = create_short_batch();
    let generator = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);
    let opts = IpcWriteOptions::default();
    let (r, s) = generator
        .encoded_batch(&batch, &mut tracker, &opts)
        .unwrap();
    println!("r = {:?}", r.len());
    println!(
        "s_ipc_msg = {:?},s_arrow_data = {:?}",
        s.ipc_message, s.arrow_data
    );
}

/**
 * todo 测试未通过
 */
#[test]
fn file_wirte_test() {
    let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-flight/tests/test3.arrow";
    {
        let file = open_file(path);
        let batch = create_batch();
        let schema = batch.schema();
        let batch1 = create_batch1(10);
        let schema1 = batch1.schema();
        {
            let mut writer = FileWriter::try_new(&file, &schema).unwrap();
            // 写入第一个 RecordBatch
            writer.write(&batch).unwrap();
            let _ = writer.finish();
        }
        {
            let mut writer = FileWriter::try_new(&file, &schema1).unwrap();
            // 写入第一个 RecordBatch
            writer.write(&batch1).unwrap();
            let _ = writer.finish();
        }
    }
    {
        let file = open_file(path);
        let reader = StreamReader::try_new(file, None).unwrap();
        for batch in reader {
            match batch {
                Ok(b) => println!("success"),
                Err(e) => println!("e = {}", e),
            }
        }
    }
}
