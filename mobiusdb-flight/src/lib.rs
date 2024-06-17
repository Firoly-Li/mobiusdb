pub mod do_put;
pub mod list_flights;
pub mod state;

use arrow_flight::{
    error::FlightError, flight_descriptor::DescriptorType, flight_service_server::FlightService,
    utils::flight_data_to_batches, Action, ActionType, BasicAuth, Criteria, Empty, FlightData,
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo,
    PutResult, SchemaResult, Ticket,
};
use do_put::write_batch;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use list_flights::FlightsKey;
use prost::{bytes::Bytes, Message};
use state::State;
use tonic::{Request, Response, Status, Streaming};

#[derive(Debug, Clone)]
pub struct ApiServer<S: State> {
    state: S,
}
impl<S: State> ApiServer<S> {
    pub fn new(state: S) -> Self {
        Self { state }
    }
}

/**
 *
 */
#[tonic::async_trait]
impl<S> FlightService for ApiServer<S>
where
    S: State + Send + Sync + 'static,
{
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // 1. 获取 HandshakeRequest
        let handshake_request: HandshakeRequest = request.into_inner().message().await?.unwrap();
        println!(
            "接收到客户端的HandshakeRequest报文：{:?}",
            handshake_request
        );
        let _version = handshake_request.protocol_version;

        let p = handshake_request.payload;
        let auth = BasicAuth::decode(p);
        println!("BasicAuth{:?}", auth);
        match auth {
            Ok(a) => {
                println!("客户端认证信息：{:?}", a);
                let handshake_response = HandshakeResponse {
                    protocol_version: 1,
                    payload: Bytes::from("auth success"),
                };
                let output = futures::stream::iter(std::iter::once(Ok(handshake_response)));
                let stream = output.boxed();
                Ok(Response::new(stream))
            }
            Err(_e) => Err(Status::ok("message")),
        }
    }
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    /**
     * 客户端可以向服务端发送 ListFlights 请求，服务端响应包含可用数据集或服务列表的 FlightInfo 对象。
     * 这些信息通常包括数据集的描述、Schema、分区信息等，帮助客户端了解可访问的数据资源。
     */
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let req = request.into_inner();
        let key: FlightsKey = req.into();
        let flights = self.state.flight_list(&key);
        let batchs: Vec<Result<FlightInfo, FlightError>> =
            flights.iter().map(|x| Ok(x.clone())).collect();
        let flights_stream = futures::stream::iter(batchs).map_err(Into::into);
        Ok(Response::new(flights_stream.boxed()))
    }
    /**
     * 客户端请求特定数据集的详细信息，服务端返回 FlightInfo，
     * 其中包含数据集的完整 Schema、数据分布情况（如有多个分片）、访问凭证（如有）等
     * todo: （当前理解）如果有一批数据是热点数据，服务端会生成相关的FlightInfo，但是数据是动态的，所以FlightInfo都会有过期时间，以避免占用服务端资源。
     *
     * FlightInfo {
     *   schema: b"",
     *   flight_descriptor: None,
     *   endpoint: [
     *       FlightEndpoint {
     *           ticket: Some(Ticket { ticket: b"0" }),
     *           location: [],
     *           expiration_time: None,
     *           app_metadata: b""
     *       }
     *   ],
     *   total_records: 0,
     *   total_bytes: 0,
     *   ordered: false,
     *   app_metadata: b""
     * }
     *
     * FlightInfo 是 Apache Arrow Flight 协议中的一个核心数据结构，它用来描述一个可执行的查询或数据传输任务的信息，包含客户端需要知道的所有元数据以发起和处理一次数据交换。下面是 FlightInfo 结构体中一些关键字段及其作用：
     * Schema: 定义了数据的结构，包括列名、数据类型等，让客户端知道即将接收的数据格式。
     * FlightDescriptor: 描述了这次Flight的唯一标识符和类型，可以是命令描述符、路径描述符或命令字符串，帮助客户端和服务器识别特定的查询或数据集。
     * Endpoints: 包含了可以用来获取数据的一个或多个服务端点信息（IP地址、端口等），客户端可以根据这些信息建立连接以获取数据。这对于高可用性和负载均衡非常重要。
     * TotalBytes: 表示整个数据传输预计的总字节数，这个字段在某些场景下可能不可用或不准确，但它可以用来做预估的资源规划或进度显示。
     * TotalRecords: 表示数据集中预计的记录总数，类似于 TotalBytes，提供数据规模的粗略估计。
     * DescriptorType: 指示 FlightDescriptor 的类型，比如是命令还是路径，这影响了如何解析和理解 FlightDescriptor 的内容。
     * Options: 可选参数集合，可能包含特定于实现或特定于查询的额外元数据，用于定制化数据处理或传输过程。
     * Expiration: 表示 FlightInfo 的有效期限，过了这个时间点，客户端可能需要重新请求 FlightInfo。
     */
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let desc = request.into_inner();
        let desc_type = desc.r#type();
        println!("desc_type: {:?}", desc_type);
        return match desc_type {
            DescriptorType::Unknown => Err(Status::new(tonic::Code::Unknown, "message")),
            DescriptorType::Path => {
                let paths = desc.path;
                let ticket_data = paths[0].as_bytes().to_vec();
                let ticket = Ticket::new(ticket_data);
                let endpoint = FlightEndpoint {
                    ticket: Some(ticket),
                    ..Default::default()
                };
                let flight_info = FlightInfo {
                    endpoint: vec![endpoint],
                    ..Default::default()
                };
                Ok(Response::new(flight_info))
            }
            DescriptorType::Cmd => {
                Err(Status::new(tonic::Code::Unknown, "not support cmd message"))
            }
        };
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented!()
    }
    /**
     * 客户端请求某个数据集的Schema信息，服务端返回详细的Schema定义，便于客户端正确解析接收到的数据。
     */
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let request = request.into_inner();
        let desc_type = request.r#type();
        return match desc_type {
            DescriptorType::Unknown => Err(Status::ok("FlightDescriptor type is unknown")),
            DescriptorType::Path => Err(Status::ok("FlightDescriptor type is unknown")),
            DescriptorType::Cmd => Err(Status::ok("FlightDescriptor type is unknown")),
        };
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    /**
     * 客户端根据 FlightInfo 发送 DoGet 请求以获取数据。服务端以流的形式返回数据，
     * 每个数据包通常是 Arrow 格式的 RecordBatch。
     * 客户端可以逐步接收和处理这些数据包，无需一次性加载全部数据。
     *
     * Ticket：
     * Ticket是一个用于请求数据的凭据或者说是标识。当客户端想要从Flight服务器获取数据时，
     * 它会发送一个包含Ticket的请求给服务器。这个Ticket实质上包含了服务器所需的所有信息
     * 以便准确地定位和检索请求的数据集。
     * 具体来说，Ticket可以包含例如表名、查询ID、数据过滤条件或者其他任何必要的元数据，
     * 这些都是为了能够让服务器识别并处理这个请求，从而返回相应的数据给客户端。
     * 在Flight的协议层面，Ticket是一个二进制序列化的对象，其具体内容和格式取决于实现和使用的上下文。
     */
    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        println!("do_get is point");
        // let inner = request.into_inner();
        // let ticket: Bytes = inner.ticket;
        unimplemented!()
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    /**
     * 客户端使用 DoPut 请求将数据上传至服务端。
     * 客户端以流的形式发送包含 Arrow RecordBatch 的数据包，服务端接收并存储这些数据。
     * 这种方式常用于数据导入或实时数据流传输。
     * todo: 服务端应该生成这批数据的FlightInfo，或者暂存于内存，等经过处理之后生成相关的FlightInfo
     */
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        println!("触发 do_put");
        let stream: Vec<_> = request.into_inner().collect().await;
        // 从第一个FlightData消息中尝试提取schema
        let ss = stream
            .into_iter()
            .filter_map(|fd| fd.ok())
            .collect::<Vec<FlightData>>();
        println!("ss: {:?}", ss);

        let ss1 = ss.clone();
        let batches = flight_data_to_batches(&ss1).unwrap();
        println!("batches: {:?}", batches);
        // 这里需要把RecordBatch存储起来，以便于后续使用
        let write_result = write_batch(batches).await;
        let stream = futures::stream::iter(write_result).map_err(Into::into);
        Ok(Response::new(stream.boxed()))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    /**
     * 支持客户端和服务端之间进行双向数据流交换。
     * 双方可以同时发送和接收 Arrow RecordBatch，
     * 适用于需要实时交互或迭代计算的场景，如查询中间结果的反馈、增量计算等
     */
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented!()
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    /**
     * 用于扩展arrowFlight机制，可以自定义Action
     * 客户端可以发送一个包含特定操作请求的消息（如执行 SQL 查询、触发数据处理任务等）。
     * 服务端执行相应操作，并返回操作结果或状态信息。
     * 此机制扩展了 Arrow Flight 的功能，使其不仅局限于数据传输，还能支持复杂的业务逻辑
     */
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented!()
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented!()
    }
}
