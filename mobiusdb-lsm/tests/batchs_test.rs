use std::sync::Arc;

use arrow::array::Array;
use common::data_utils::{ create_short_batch, create_student_batch1, create_teacher_batch2};
use mobiusdb_lsm::memtable::array_data_utils::{ merge_batches, merge_batches_with_schema, merge_schema};


pub mod common {
    pub mod data_utils;
    mod batch_merge;
}


#[test]
fn merge_batches_test() {
    let batch1 = create_student_batch1("Tom", 19);
    let batch2 = create_teacher_batch2();
    let teacher = create_teacher_batch2();
    let merged = merge_batches(vec![&batch1, &batch2, &teacher]);
    println!("merged: {:?}", merged)
}



#[test]
fn combined_batch_test() {
    let batch1 = create_short_batch("test");
    let batch1_vec = batch1.columns().to_vec();
    let schema = batch1.schema();
    
    let batch2 = create_short_batch("test1");
    let batch2_vec = batch2.columns().to_vec();
    
    let mut merged = batch1_vec;
    merged.extend(batch2_vec);
    println!("merged: {:?}", merged);
}

#[test]
fn test1() {
    let batch1 = create_student_batch1("Tom", 19);
    let batch2 = create_teacher_batch2();
    let schema = merge_schema(&batch1.schema(), &batch2.schema());
    let resp = merge_batches_with_schema(&Arc::new(schema), vec![&batch1, &batch2]);
    println!("resp: {:?}", resp);
}

/*
merged: [StringArray
[
  "test",
  "2",
  "3",
], PrimitiveArray<Int32>
[
  4,
  5,
  6,
], PrimitiveArray<Int32>
[
  7,
  8,
  9,
], PrimitiveArray<UInt64>
[
  null,
  null,
  9,
], StringArray
[
  "test1",
  "2",
  "3",
], PrimitiveArray<Int32>
[
  4,
  5,
  6,
], PrimitiveArray<Int32>
[
  7,
  8,
  9,
], PrimitiveArray<UInt64>
[
  null,
  null,
  9,
]]
*/




/**
 * v1: [StringArray["test","2","3",], PrimitiveArray<Int32>[4,5,6,], PrimitiveArray<Int32>[7,8,9,], PrimitiveArray<UInt64>[null,null,9,]]
 * v2: [StringArray["test1","2","3",], PrimitiveArray<Int32>[4,5,6,], PrimitiveArray<Int32>[7,8,9,], PrimitiveArray<UInt64>[null,null,9,]]
 * resp: v1: [StringArray["test","2","3","test1","2","3"], PrimitiveArray<Int32>[4,5,6,], PrimitiveArray<Int32>[7,8,9,], PrimitiveArray<UInt64>[null,null,9,]]
 */
fn combind_colums(v1: Vec<Arc<dyn Array>>,v2: Vec<Arc<dyn Array>>) -> Vec<Arc<dyn Array>> {
    todo!()
}