use anyhow::Result;
use arrow::array::RecordBatch;
use arrow_flight::FlightData;
use tokio::sync::{mpsc::Sender, oneshot};

use crate::{
    utils::{data_utils::batch_to_flight_data, table_name::TableName},
    LsmCommand,
};

#[derive(Debug, Clone)]
pub struct LsmClient {
    cli: Sender<LsmCommand>,
}

impl LsmClient {
    pub fn new(cli: Sender<LsmCommand>) -> Self {
        Self { cli }
    }
}

impl LsmClient {
    pub async fn append_batch(&self, batch: RecordBatch) -> Result<bool> {
        let (sender, receiver) = oneshot::channel();
        let resp = batch_to_flight_data(batch)?;
        let cmd = LsmCommand::Append((resp, sender));
        self.cli.send(cmd).await?;
        let response = receiver.await?;
        Ok(response)
    }

    pub async fn append_fds(&self, batch: Vec<FlightData>) -> Result<bool> {
        let (sender, receiver) = oneshot::channel();
        let cmd = LsmCommand::Append((batch, sender));
        self.cli.send(cmd).await?;
        let response = receiver.await?;
        Ok(response)
    }

    pub async fn table_list(&self) -> Result<Option<Vec<TableName>>> {
        let (sender, receiver) = oneshot::channel();
        let cmd = LsmCommand::TableList(sender);
        self.cli.send(cmd).await?;
        let response = receiver.await?;
        Ok(response)
    }

    pub async fn table(&self, table_name: &str) -> Result<Option<RecordBatch>> {
        let (sender, receiver) = oneshot::channel();
        let cmd = LsmCommand::Table((table_name.to_string(), sender));
        self.cli.send(cmd).await?;
        let response = receiver.await?;
        Ok(response)
    }

    pub async fn query(&self, query: &str) -> Result<Option<RecordBatch>> {
        let (sender, receiver) = oneshot::channel();
        let cmd = LsmCommand::Query((query.to_string(), sender));
        self.cli.send(cmd).await?;
        let response = receiver.await?;
        Ok(response)
    }
}
