use std::sync::Arc;

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::*, error::ArrowError, json::ReaderBuilder};
use arrow_flight::{
    flight_service_server::FlightServiceServer, utils::batches_to_flight_data, Action, ActionType,
    BasicAuth, Criteria, Empty, FlightClient, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Ticket,
};
use futures::{StreamExt, TryStreamExt};
use mobiusdb_flight::{list_flights::FlightsKey, state::State, ApiServer};
use prost::{bytes::Bytes, Message};
use serde::Serialize;
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Server},
    Status,
};

/// mutable state for the TestFlightServer, captures requests and provides responses
#[derive(Debug, Default)]
struct StateTest {
    /// The last handshake request that was received
    pub handshake_request: Option<HandshakeRequest>,
    /// The next response to return from `handshake()`
    pub handshake_response: Option<Result<HandshakeResponse, Status>>,
    /// The last `get_flight_info` request received
    pub get_flight_info_request: Option<FlightDescriptor>,
    /// The next response to return from `get_flight_info`
    pub get_flight_info_response: Option<Result<FlightInfo, Status>>,
    /// The last `poll_flight_info` request received
    pub poll_flight_info_request: Option<FlightDescriptor>,
    /// The next response to return from `poll_flight_info`
    pub poll_flight_info_response: Option<Result<PollInfo, Status>>,
    /// The last do_get request received
    pub do_get_request: Option<Ticket>,
    /// The next response returned from `do_get`
    pub do_get_response: Option<Vec<Result<RecordBatch, Status>>>,
    /// The last do_put request received
    pub do_put_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_put`
    pub do_put_response: Option<Vec<Result<PutResult, Status>>>,
    /// The last do_exchange request received
    pub do_exchange_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_exchange`
    pub do_exchange_response: Option<Vec<Result<FlightData, Status>>>,
    /// The last list_flights request received
    pub list_flights_request: Option<Criteria>,
    /// The next response returned from `list_flights`
    pub list_flights_response: Option<Vec<Result<FlightInfo, Status>>>,
    /// The last get_schema request received
    pub get_schema_request: Option<FlightDescriptor>,
    /// The next response returned from `get_schema`
    pub get_schema_response: Option<Result<Schema, Status>>,
    /// The last list_actions request received
    pub list_actions_request: Option<Empty>,
    /// The next response returned from `list_actions`
    pub list_actions_response: Option<Vec<Result<ActionType, Status>>>,
    /// The last do_action request received
    pub do_action_request: Option<Action>,
    /// The next response returned from `do_action`
    pub do_action_response: Option<Vec<Result<arrow_flight::Result, Status>>>,
    /// The last request headers received
    pub last_request_metadata: Option<MetadataMap>,
}

impl StateTest {
    fn new() -> Self {
        Default::default()
    }
}

impl State for StateTest {
    fn flight_list(&self, key: &FlightsKey) -> Vec<FlightInfo> {
        Vec::new()
    }
}

pub async fn flight_server() -> Result<()> {
    let test_flight_server = ApiServer::new(StateTest::new());
    let addr = "127.0.0.1:50051".parse()?;
    let server = FlightServiceServer::new(test_flight_server);
    println!("flight server will be starting on :{}", addr);
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}

pub async fn flight_client() -> Result<FlightClient> {
    let local_url = "http://127.0.0.1:50051";
    if let Ok(channel) = Channel::from_static(local_url).connect().await {
        let client = FlightClient::new(channel);
        Ok(client)
    } else {
        Err(anyhow::Error::msg(""))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn handshake_should_be_work() {
    tokio::spawn(async move {
        let _ = flight_server().await;
    });

    let auth = BasicAuth {
        username: String::from("username"),
        password: String::from("password"),
    };

    let mut buf = Vec::new();

    let _ = auth.encode(&mut buf);
    let request = HandshakeRequest {
        protocol_version: 1,
        payload: buf.into(),
    };
    if let Ok(mut client) = flight_client().await {
        let mut buf1 = Vec::new();
        let _ = request.encode(&mut buf1);
        let resp = client.handshake(buf1).await;
        assert!(resp.is_ok());
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn list_flights_should_be_work() {
    tokio::spawn(async move {
        let _ = flight_server().await;
    });

    if let Ok(mut client) = flight_client().await {
        let c = Criteria {
            expression: Bytes::from("table1"),
        };
        let resp = client.list_flights(c.expression).await;
        match resp {
            Ok(resp) => {
                let response: Vec<_> = resp.try_collect().await.expect("Error streaming data");
                println!("flight_info = {:?}", response);
            }
            Err(_) => println!("error"),
        }
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn do_put_should_be_work() {
    // tokio::spawn(async move {
    //     let _ = flight_server().await;
    // });

    if let Ok(mut client) = flight_client().await {
        let batch = create_batch();
        match batch {
            Ok(b) => {
                // 创建一个流，用于发送数据到服务端
                let input_stream = futures::stream::iter(b).map(Ok);
                let a = client.do_put(input_stream).await.unwrap();
            }
            Err(_) => todo!(),
        }
    };
}

#[derive(Serialize)]
struct MyStruct {
    int32: i32,
    string: String,
}
pub fn create_batch() -> Result<Vec<FlightData>, ArrowError> {
    let schema = Schema::new(vec![
        Field::new("int32", DataType::Int32, false),
        Field::new("string", DataType::Utf8, false),
    ]);

    let mut vecs = Vec::new();
    for _ in 0..5 {
        let rows = vec![
            MyStruct {
                int32: 5,
                string: "bar".to_string(),
            },
            MyStruct {
                int32: 8,
                string: "foo".to_string(),
            },
        ];

        let mut decoder = ReaderBuilder::new(Arc::new(schema.clone()))
            .build_decoder()
            .unwrap();
        decoder.serialize(&rows).unwrap();

        let batch: arrow::array::RecordBatch = decoder.flush().unwrap().unwrap();
        vecs.push(batch);
    }
    println!("vecs = {:?}", vecs.len());
    batches_to_flight_data(&schema, vecs)
}
