use anyhow::Result;
use arrow::array::RecordBatch;
use arrow_flight::{utils::batches_to_flight_data, FlightData};

pub fn batch_to_flight_data(batch: RecordBatch) -> Result<Vec<FlightData>> {
    let mut vecs = Vec::new();
    let schema = batch.schema();
    vecs.push(batch);
    let fds = batches_to_flight_data(&schema, vecs)?;
    Ok(fds)
}
