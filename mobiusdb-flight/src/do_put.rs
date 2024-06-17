use arrow::array::RecordBatch;
use arrow_flight::PutResult;
use prost::bytes::Bytes;
use tonic::Status;

pub async fn write_batch(batchs: Vec<RecordBatch>) -> Vec<Result<PutResult, Status>> {
    let mut resp = Vec::new();
    let mut file_index = 0;
    for batch in batchs {
        resp.push(Ok(PutResult {
            app_metadata: Bytes::from(vec![file_index]),
        }));
        file_index += 1;
    }
    resp
}

/**
 *
 */
pub(crate) async fn write_to_wal() {}
