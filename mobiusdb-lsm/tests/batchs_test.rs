use std::sync::Arc;

use arrow::{array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, UInt16Array, UInt64Array}, compute::{sort_to_indices, SortOptions}, datatypes::*};
use common::data_utils::{
    create_short_batch, create_student_batch1, create_teacher_batch2,
    create_teacher_batch2_with_times,
};
use datafusion::prelude::SessionContext;
use mobiusdb_lsm::{memtable::array_data_utils::{
    merge_batches, merge_batches_with_schema, merge_schema,
}, utils::data_utils::batch_sort};

pub mod common {
    mod batch_merge;
    pub mod data_utils;
}

#[test]
fn merge_batches_test() {
    let batch1 = create_student_batch1("Tom", 19);
    let batch2 = create_teacher_batch2();
    let teacher = create_teacher_batch2();
    let merged = merge_batches(vec![&batch1, &batch2, &teacher]);
    println!("merged: {:?}", merged)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_times_test() {
    let teacher = create_teacher_batch2_with_times("class",30);
    let ctx = SessionContext::new();
    let df = ctx.read_batch(teacher).unwrap();
    let fd = df
        .select_columns(&["timestamp"])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = fd.first().unwrap();
    let column_array: ArrayRef = batch.column(0).clone();
    let u64_array = column_array.as_any().downcast_ref::<UInt64Array>().unwrap();
    let r: Vec<u64> = u64_array.values().into_iter().map(|x| x.clone()).collect();
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

/**
 * 
 */
#[test]
fn batchsort_fn_test() {
    let batch = create_teacher_batch2_with_times("class", 25);
    let new_batch = batch_sort(&batch, "age");
    println!("new_batch = {:?}",new_batch);
}


#[test]
fn batch_sort_test() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create data for the RecordBatch
    let ids = Arc::new(Int32Array::from(vec![3, 1, 2])) as ArrayRef;
    let names = Arc::new(StringArray::from(vec!["Charlie", "Alice", "Bob"])) as ArrayRef;

    // Create the RecordBatch
    let batch = RecordBatch::try_new(schema.clone(), vec![ids.clone(), names.clone()]).unwrap();

    // Specify the field to sort by (e.g., "id")
    let sort_column_index = schema.index_of("id").unwrap();

    // Create SortOptions to sort in ascending order
    let sort_options = SortOptions::default();

    // Compute the sorted indices
    let sorted_indices = sort_to_indices(&batch.column(sort_column_index), Some(sort_options),None).unwrap();

    // Reorder the RecordBatch based on the sorted indices
    let sorted_ids = arrow::compute::take(&ids, &sorted_indices, None).unwrap();
    let sorted_names = unsafe { arrow::compute::take(&names, &sorted_indices, None).unwrap() };

    // Create a new sorted RecordBatch
    let sorted_batch = RecordBatch::try_new(
        schema,
        vec![sorted_ids, sorted_names]
    ).unwrap();

    // Print the sorted RecordBatch
    println!("Sorted RecordBatch:");
    for row in 0..sorted_batch.num_rows() {
        println!(
            "id: {}, name: {}",
            sorted_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(row),
            sorted_batch.column(1).as_any().downcast_ref::<StringArray>().unwrap().value(row)
        );
    }

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
fn combind_colums(v1: Vec<Arc<dyn Array>>, v2: Vec<Arc<dyn Array>>) -> Vec<Arc<dyn Array>> {
    todo!()
}
