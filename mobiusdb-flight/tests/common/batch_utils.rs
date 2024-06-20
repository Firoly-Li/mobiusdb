use std::sync::Arc;

use arrow::{array::*, datatypes::*};

pub fn create_batch() -> RecordBatch {
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
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string(),
                "5".to_string(),
                "6".to_string(),
                "7".to_string(),
                "8".to_string(),
                "9".to_string(),
                "10".to_string(),
                "11".to_string(),
                "12".to_string(),
                "13".to_string(),
                "14".to_string(),
                "15".to_string(),
            ])),
            Arc::new(Int32Array::from(vec![
                4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            ])),
            Arc::new(Int32Array::from(vec![
                7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            ])),
            Arc::new(UInt64Array::from(vec![
                None,
                None,
                Some(9),
                None,
                None,
                Some(10),
                None,
                None,
                Some(11),
                None,
                None,
                Some(12),
                None,
                None,
                Some(13),
            ])),
        ],
    )
    .unwrap();
    batch
}

pub fn create_short_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Binary, false),
        Field::new("age", DataType::Int32, false),
        Field::new("address", DataType::Int32, false),
        Field::new("time", DataType::UInt64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BinaryArray::from(vec![
                "1".as_bytes(),
                "2".as_bytes(),
                "3".as_bytes(),
            ])),
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Arc::new(UInt64Array::from(vec![None, None, Some(9)])),
        ],
    )
    .unwrap();
    batch
}

pub fn create_batch1(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Binary, false),
        Field::new("speed", DataType::Int32, false),
        Field::new("address", DataType::Int32, false),
        Field::new("time", DataType::UInt64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BinaryArray::from(vec![
                "1".as_bytes(),
                "2".as_bytes(),
                "3".as_bytes(),
                "4".as_bytes(),
                "5".as_bytes(),
                "6".as_bytes(),
                "7".as_bytes(),
                "8".as_bytes(),
                "9".as_bytes(),
                "10".as_bytes(),
                "11".as_bytes(),
                "12".as_bytes(),
                "13".as_bytes(),
                "14".as_bytes(),
                "15".as_bytes(),
            ])),
            Arc::new(Int32Array::from(vec![
                (n * 100) as i32,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
            ])),
            Arc::new(Int32Array::from(vec![
                7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            ])),
            Arc::new(UInt64Array::from(vec![
                None,
                None,
                Some(9),
                None,
                None,
                Some(10),
                None,
                None,
                Some(11),
                None,
                None,
                Some(12),
                None,
                None,
                Some(13),
            ])),
        ],
    )
    .unwrap();
    batch
}
