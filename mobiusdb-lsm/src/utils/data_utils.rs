use anyhow::Result;
use arrow::{array::{ArrayRef, RecordBatch, UInt64Array}, compute::{sort_to_indices, SortOptions}};
use arrow_flight::{utils::{batches_to_flight_data, flight_data_to_batches}, FlightData};
use datafusion::prelude::SessionContext;

use crate::memtable::array_data_utils::merge_batches;

pub fn batch_to_flight_data(batch: RecordBatch) -> Result<Vec<FlightData>> {
    let mut vecs = Vec::new();
    let schema = batch.schema();
    vecs.push(batch);
    let fds = batches_to_flight_data(&schema, vecs)?;
    Ok(fds)
}

/**
 * 
 */
pub fn flight_data_to_batch(fds: &Vec<FlightData>) -> Result<RecordBatch> {
    let batches = flight_data_to_batches(&fds)?;
    let batch = merge_batches(&batches)?;
    let schema = batch.schema();
    let empty_batch = RecordBatch::new_empty(schema);
    merge_batches(&vec![batch,empty_batch])
}

pub async fn get_timestamp_from_batch(batch: &RecordBatch) -> Result<(u64, u64)> {
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone()).unwrap();
    let fd = df
        .select_columns(&["timestamp"])
        ?
        .collect()
        .await
        ?;
    let batch = fd.first().unwrap();
    let column_array: ArrayRef = batch.column(0).clone();
    let u64_array = column_array.as_any().downcast_ref::<UInt64Array>().unwrap();
    let mut timestamps: Vec<u64> = u64_array.values().into_iter().map(|x| x.clone()).collect();
    timestamps.sort();
    Ok((timestamps[0], timestamps[timestamps.len() - 1]))
}

pub fn batch_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

/**
 * 描述：对RecordBatch，以其中的某个字段进行排序
 * 状态：初步通过测试
 */
pub fn batch_sort(batch: &RecordBatch,sort_by: &str) -> Result<RecordBatch>{
    let schema = batch.schema();
    let sort_column_index = schema.index_of(sort_by)?;
    // Create SortOptions to sort in ascending order
    let sort_options = SortOptions::default();
    // Compute the sorted indices
    let sorted_indices = sort_to_indices(&batch.column(sort_column_index), Some(sort_options),None)?;
    let columns = batch.columns();
    let mut vs = Vec::new();
    for column in columns {
        let sorted_cloumns = arrow::compute::take(&column, &sorted_indices, None)?;
        vs.push(sorted_cloumns);
    }
    // Create a new sorted RecordBatch
    let sorted_batch = RecordBatch::try_new(
        schema,
        vs
    )?;
    Ok(sorted_batch)

}
