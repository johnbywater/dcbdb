use futures::Stream;
use std::path::Path;
use std::pin::Pin;
use std::thread;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};

use crate::dcbapi::{
    DCBAppendCondition, DCBEvent, DCBEventStore, DCBQuery, DCBQueryItem, DCBSequencedEvent,
    DCBError, DCBResult,
};
use crate::event_store::EventStore;

// Include the generated proto code
pub mod dcbdb {
    tonic::include_proto!("dcbdb");
}

use dcbdb::{
    AppendConditionProto, AppendRequestProto, AppendResponseProto, EventProto, HeadRequestProto,
    HeadResponseProto, QueryItemProto, QueryProto, ReadRequestProto, ReadResponseProto,
    SequencedEventBatchProto, SequencedEventProto,
    event_store_service_server::{EventStoreService, EventStoreServiceServer},
};

// Conversion functions between proto and API types
impl From<EventProto> for DCBEvent {
    fn from(proto: EventProto) -> Self {
        DCBEvent {
            event_type: proto.event_type,
            tags: proto.tags,
            data: proto.data,
        }
    }
}

impl From<DCBEvent> for EventProto {
    fn from(event: DCBEvent) -> Self {
        EventProto {
            event_type: event.event_type,
            tags: event.tags,
            data: event.data,
        }
    }
}

impl From<QueryItemProto> for DCBQueryItem {
    fn from(proto: QueryItemProto) -> Self {
        DCBQueryItem {
            types: proto.types,
            tags: proto.tags,
        }
    }
}

impl From<QueryProto> for DCBQuery {
    fn from(proto: QueryProto) -> Self {
        DCBQuery {
            items: proto.items.into_iter().map(|item| item.into()).collect(),
        }
    }
}

impl From<AppendConditionProto> for DCBAppendCondition {
    fn from(proto: AppendConditionProto) -> Self {
        DCBAppendCondition {
            fail_if_events_match: proto
                .fail_if_events_match
                .map_or_else(DCBQuery::default, |q| q.into()),
            after: proto.after,
        }
    }
}

impl From<DCBSequencedEvent> for SequencedEventProto {
    fn from(event: DCBSequencedEvent) -> Self {
        SequencedEventProto {
            position: event.position,
            event: Some(event.event.into()),
        }
    }
}

// Message types for communication between the gRPC server and the EventStore thread
enum EventStoreRequest {
    Read {
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
        response_tx: oneshot::Sender<DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)>>,
    },
    Append {
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        response_tx: oneshot::Sender<DCBResult<u64>>,
    },
    Head {
        response_tx: oneshot::Sender<DCBResult<Option<u64>>>,
    },
    #[allow(dead_code)]
    Shutdown,
}

// Thread-safe wrapper for EventStore
struct EventStoreHandle {
    request_tx: mpsc::Sender<EventStoreRequest>,
}

impl EventStoreHandle {
    fn new<P: AsRef<Path> + Send + 'static>(path: P) -> std::io::Result<Self> {
        // Create a channel for sending requests to the EventStore thread
        let (request_tx, mut request_rx) = mpsc::channel::<EventStoreRequest>(32);

        // Spawn a thread to run the EventStore
        thread::spawn(move || {
            // Create the EventStore in this thread
            let event_store = match EventStore::new(path) {
                Ok(store) => store,
                Err(e) => {
                    eprintln!("Failed to create EventStore: {e:?}");
                    return;
                }
            };

            // Create a runtime for async operations
            let rt = tokio::runtime::Runtime::new().unwrap();

            // Process requests
            rt.block_on(async {
                while let Some(request) = request_rx.recv().await {
                    match request {
                        EventStoreRequest::Read {
                            query,
                            after,
                            limit,
                            response_tx,
                        } => {
                            let result = event_store.read_with_head(query, after, limit);
                            let _ = response_tx.send(result);
                        }
                        EventStoreRequest::Append {
                            events,
                            condition,
                            response_tx,
                        } => {
                            let result = event_store.append(events, condition);
                            let _ = response_tx.send(result);
                        }
                        EventStoreRequest::Head { response_tx } => {
                            let result = event_store.head();
                            let _ = response_tx.send(result);
                        }
                        EventStoreRequest::Shutdown => {
                            break;
                        }
                    }
                }
            });
        });

        Ok(Self { request_tx })
    }

    async fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(EventStoreRequest::Read {
                query,
                after,
                limit,
                response_tx,
            })
            .await
            .map_err(|_| {
                DCBError::Io(std::io::Error::other(
                    "Failed to send read request to EventStore thread",
                ))
            })?;

        response_rx.await.map_err(|_| {
            DCBError::Io(std::io::Error::other(
                "Failed to receive read response from EventStore thread",
            ))
        })?
    }

    async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(EventStoreRequest::Append {
                events,
                condition,
                response_tx,
            })
            .await
            .map_err(|_| {
                DCBError::Io(std::io::Error::other(
                    "Failed to send append request to EventStore thread",
                ))
            })?;

        response_rx.await.map_err(|_| {
            DCBError::Io(std::io::Error::other(
                "Failed to receive append response from EventStore thread",
            ))
        })?
    }

    async fn head(&self) -> DCBResult<Option<u64>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(EventStoreRequest::Head { response_tx })
            .await
            .map_err(|_| {
                DCBError::Io(std::io::Error::other(
                    "Failed to send head request to EventStore thread",
                ))
            })?;

        response_rx.await.map_err(|_| {
            DCBError::Io(std::io::Error::other(
                "Failed to receive head response from EventStore thread",
            ))
        })?
    }

    #[allow(dead_code)]
    async fn shutdown(&self) {
        let _ = self.request_tx.send(EventStoreRequest::Shutdown).await;
    }
}

// gRPC server implementation
pub struct GrpcEventStoreServer {
    event_store: EventStoreHandle,
}

impl GrpcEventStoreServer {
    pub fn new<P: AsRef<Path> + Send + 'static>(path: P) -> std::io::Result<Self> {
        let event_store = EventStoreHandle::new(path)?;
        Ok(Self { event_store })
    }

    pub fn into_service(self) -> EventStoreServiceServer<Self> {
        EventStoreServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl EventStoreService for GrpcEventStoreServer {
    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<ReadResponseProto, Status>> + Send + 'static>>;

    async fn read(
        &self,
        request: Request<ReadRequestProto>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = request.into_inner();

        // Convert proto types to API types
        let query = req.query.map(|q| q.into());
        let after = req.after;
        let limit = req.limit.map(|l| l as usize);

        // Create a channel for streaming responses
        let (tx, rx) = mpsc::channel(128);
        let event_store = self.event_store.clone();

        // Spawn a task to handle the read operation
        tokio::spawn(async move {
            match event_store.read(query, after, limit).await {
                Ok((events, head)) => {
                    // Create a batch of events
                    let batch = SequencedEventBatchProto {
                        events: events.into_iter().map(|e| e.into()).collect(),
                        head,
                    };

                    // Send the batch as a response
                    let response = ReadResponseProto { batch: Some(batch) };

                    let _ = tx.send(Ok(response)).await;
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::internal(format!("Read error: {e:?}"))))
                        .await;
                }
            }
        });

        // Return the receiver as a stream
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::ReadStream
        ))
    }

    async fn append(
        &self,
        request: Request<AppendRequestProto>,
    ) -> Result<Response<AppendResponseProto>, Status> {
        let req = request.into_inner();

        // Convert proto types to API types
        let events: Vec<DCBEvent> = req.events.into_iter().map(|e| e.into()).collect();
        let condition = req.condition.map(|c| c.into());

        // Call the event store append method
        match self.event_store.append(events, condition).await {
            Ok(position) => {
                // Return the position as a response
                Ok(Response::new(AppendResponseProto { position }))
            }
            Err(e) => {
                // Convert the error to a gRPC status with specific error type information
                match e {
                    DCBError::IntegrityError => Err(Status::failed_precondition(
                        "Integrity error: condition failed",
                    )),
                    _ => Err(Status::internal(format!("Append error: {e:?}"))),
                }
            }
        }
    }

    async fn head(
        &self,
        _request: Request<HeadRequestProto>,
    ) -> Result<Response<HeadResponseProto>, Status> {
        // Call the event store head method
        match self.event_store.head().await {
            Ok(position) => {
                // Return the position as a response
                Ok(Response::new(HeadResponseProto { position }))
            }
            Err(e) => Err(Status::internal(format!("Head error: {e:?}"))),
        }
    }
}

// Clone implementation for EventStoreHandle
impl Clone for EventStoreHandle {
    fn clone(&self) -> Self {
        Self {
            request_tx: self.request_tx.clone(),
        }
    }
}

// gRPC client implementation
pub struct GrpcEventStoreClient {
    client: dcbdb::event_store_service_client::EventStoreServiceClient<tonic::transport::Channel>,
}

impl GrpcEventStoreClient {
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let client =
            dcbdb::event_store_service_client::EventStoreServiceClient::connect(dst).await?;
        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl DCBEventStore for GrpcEventStoreClient {
    fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<Box<dyn crate::dcbapi::DCBReadResponse + '_>> {
        // Convert API types to proto types
        let query_proto = query.map(|q| QueryProto {
            items: q
                .items
                .into_iter()
                .map(|item| QueryItemProto {
                    types: item.types,
                    tags: item.tags,
                })
                .collect(),
        });

        let limit_proto = limit.map(|l| l as u32);

        // Create the read request
        let request = ReadRequestProto {
            query: query_proto,
            after,
            limit: limit_proto,
        };

        // Execute the read operation
        let mut client = self.client.clone();

        if tokio::runtime::Handle::try_current().is_ok() {
            // We're already in a Tokio runtime, use the current one
            let response = futures::executor::block_on(async move { client.read(request).await });

            match response {
                Ok(stream) => {
                    // Create a GrpcReadResponse that implements DCBReadResponse
                    Ok(Box::new(GrpcReadResponse::new_with_current_runtime(
                        stream.into_inner(),
                    ))
                        as Box<dyn crate::dcbapi::DCBReadResponse + '_>)
                }
                Err(status) => Err(DCBError::Io(std::io::Error::other(format!(
                    "gRPC read error: {status}"
                )))),
            }
        } else {
            // No Tokio runtime, create a new one
            let rt = tokio::runtime::Runtime::new().unwrap();
            let response = rt.block_on(async move { client.read(request).await });

            match response {
                Ok(stream) => {
                    // Create a GrpcReadResponse that implements DCBReadResponse
                    Ok(
                        Box::new(GrpcReadResponse::new_with_runtime(rt, stream.into_inner()))
                            as Box<dyn crate::dcbapi::DCBReadResponse + '_>,
                    )
                }
                Err(status) => Err(DCBError::Io(std::io::Error::other(format!(
                    "gRPC read error: {status}"
                )))),
            }
        }
    }

    fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64> {
        // Convert API types to proto types
        let events_proto: Vec<EventProto> = events
            .into_iter()
            .map(|e| EventProto {
                event_type: e.event_type,
                tags: e.tags,
                data: e.data,
            })
            .collect();

        let condition_proto = condition.map(|c| AppendConditionProto {
            fail_if_events_match: Some(QueryProto {
                items: c
                    .fail_if_events_match
                    .items
                    .into_iter()
                    .map(|item| QueryItemProto {
                        types: item.types,
                        tags: item.tags,
                    })
                    .collect(),
            }),
            after: c.after,
        });

        // Create the append request
        let request = AppendRequestProto {
            events: events_proto,
            condition: condition_proto,
        };

        // Execute the append operation
        let mut client = self.client.clone();

        let response = if tokio::runtime::Handle::try_current().is_ok() {
            // We're already in a Tokio runtime, use the current one
            futures::executor::block_on(async move { client.append(request).await })
        } else {
            // No Tokio runtime, create a new one
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move { client.append(request).await })
        };

        match response {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => {
                // Check if the error message indicates an integrity error
                if status.message().contains("Integrity error") {
                    Err(DCBError::IntegrityError)
                } else {
                    Err(DCBError::Io(std::io::Error::other(format!(
                        "gRPC append error: {status}"
                    ))))
                }
            }
        }
    }

    fn head(&self) -> DCBResult<Option<u64>> {
        // Create the head request
        let request = HeadRequestProto {};

        // Execute the head operation
        let mut client = self.client.clone();

        let response = if tokio::runtime::Handle::try_current().is_ok() {
            // We're already in a Tokio runtime, use the current one
            futures::executor::block_on(async move { client.head(request).await })
        } else {
            // No Tokio runtime, create a new one
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move { client.head(request).await })
        };

        match response {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(DCBError::Io(std::io::Error::other(format!(
                "gRPC head error: {status}"
            )))),
        }
    }
}

// Implementation of DCBReadResponse for the gRPC client
enum RuntimeType {
    Owned(tokio::runtime::Runtime),
    Current,
}

struct GrpcReadResponse {
    runtime_type: RuntimeType,
    stream: tonic::codec::Streaming<ReadResponseProto>,
    events: Vec<DCBSequencedEvent>,
    current_index: usize,
    head: Option<u64>,
}

impl GrpcReadResponse {
    fn new_with_runtime(
        rt: tokio::runtime::Runtime,
        stream: tonic::codec::Streaming<ReadResponseProto>,
    ) -> Self {
        Self {
            runtime_type: RuntimeType::Owned(rt),
            stream,
            events: Vec::new(),
            current_index: 0,
            head: None,
        }
    }

    fn new_with_current_runtime(stream: tonic::codec::Streaming<ReadResponseProto>) -> Self {
        Self {
            runtime_type: RuntimeType::Current,
            stream,
            events: Vec::new(),
            current_index: 0,
            head: None,
        }
    }

    fn fetch_next_batch(&mut self) -> DCBResult<()> {
        // Use the appropriate method to get the next message from the stream
        let next_message = match &mut self.runtime_type {
            RuntimeType::Owned(rt) => rt.block_on(async { self.stream.message().await }),
            RuntimeType::Current => {
                futures::executor::block_on(async { self.stream.message().await })
            }
        };

        match next_message {
            Ok(Some(response)) => {
                if let Some(batch) = response.batch {
                    // Update the head
                    self.head = batch.head;

                    // Convert proto events to API events
                    let events: Vec<DCBSequencedEvent> = batch
                        .events
                        .into_iter()
                        .map(|e| {
                            let event_proto = e.event.unwrap();
                            DCBSequencedEvent {
                                position: e.position,
                                event: DCBEvent {
                                    event_type: event_proto.event_type,
                                    tags: event_proto.tags,
                                    data: event_proto.data,
                                },
                            }
                        })
                        .collect();

                    // Add the events to our buffer
                    self.events.extend(events);
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(status) => Err(DCBError::Io(std::io::Error::other(format!(
                "gRPC stream error: {status}"
            )))),
        }
    }
}

impl Iterator for GrpcReadResponse {
    type Item = DCBSequencedEvent;

    fn next(&mut self) -> Option<Self::Item> {
        // If we've gone through all events in our buffer, try to fetch more
        if self.current_index >= self.events.len() {
            match self.fetch_next_batch() {
                Ok(()) => {
                    // If we still don't have any events, we're done
                    if self.current_index >= self.events.len() {
                        return None;
                    }
                }
                Err(_) => return None,
            }
        }

        // Return the next event
        let event = self.events[self.current_index].clone();
        self.current_index += 1;
        Some(event)
    }
}

impl crate::dcbapi::DCBReadResponse for GrpcReadResponse {
    fn head(&self) -> Option<u64> {
        self.head
    }

    fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>) {
        // Collect all events
        let mut events = Vec::new();
        for event in self.by_ref() {
            events.push(event);
        }

        (events, self.head)
    }

    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        // Fetch the next batch if needed
        if self.current_index >= self.events.len() {
            self.fetch_next_batch()?;
        }

        // Return all events in the current batch
        let batch = self.events.drain(..).collect();
        self.current_index = 0;

        Ok(batch)
    }
}

// Function to start the gRPC server
pub async fn start_grpc_server<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let server = GrpcEventStoreServer::new(path)?;

    println!("gRPC server listening on {addr}");

    Server::builder()
        .add_service(server.into_service())
        .serve(addr)
        .await?;

    Ok(())
}

// Function to start the gRPC server with a shutdown signal
pub async fn start_grpc_server_with_shutdown<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let server = GrpcEventStoreServer::new(path)?;
    println!("gRPC server listening on {addr}");

    Server::builder()
        .add_service(server.into_service())
        .serve_with_shutdown(addr, async {
            shutdown_rx.await.ok();
            println!("gRPC server shutdown complete");
        })
        .await?;

    Ok(())
}
