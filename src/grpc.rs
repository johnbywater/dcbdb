use std::path::Path;
use tonic::{Request, Response, Status, transport::Server};
use futures::Stream;
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use std::thread;

use crate::api::{DCBEventStoreAPI, DCBEvent, DCBQuery, DCBQueryItem, DCBAppendCondition, DCBSequencedEvent, EventStoreError, Result as DCBResult};
use crate::store::EventStore;

// Include the generated proto code
pub mod dcbdb {
    tonic::include_proto!("dcbdb");
}

use dcbdb::{
    event_store_service_server::{EventStoreService, EventStoreServiceServer},
    EventProto, SequencedEventProto, SequencedEventBatchProto, QueryItemProto, QueryProto,
    AppendConditionProto, ReadRequestProto, ReadResponseProto, AppendRequestProto, AppendResponseProto,
    HeadRequestProto, HeadResponseProto,
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
            fail_if_events_match: proto.fail_if_events_match.map_or_else(|| DCBQuery::default(), |q| q.into()),
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
    FlushAndShutdown {
        response_tx: oneshot::Sender<DCBResult<()>>,
    },
    FlushAndCheckpoint {
        response_tx: oneshot::Sender<DCBResult<()>>,
    },
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
                    eprintln!("Failed to create EventStore: {:?}", e);
                    return;
                }
            };

            // Create a runtime for async operations
            let rt = tokio::runtime::Runtime::new().unwrap();

            // Process requests
            rt.block_on(async {
                while let Some(request) = request_rx.recv().await {
                    match request {
                        EventStoreRequest::Read { query, after, limit, response_tx } => {
                            let result = event_store.read_with_head(query, after, limit);
                            let _ = response_tx.send(result);
                        }
                        EventStoreRequest::Append { events, condition, response_tx } => {
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
                        EventStoreRequest::FlushAndShutdown { response_tx } => {
                            // Flush and checkpoint before shutting down
                            let result = event_store.flush_and_checkpoint();
                            let _ = response_tx.send(result);
                            break;
                        }
                        EventStoreRequest::FlushAndCheckpoint { response_tx } => {
                            // Flush and checkpoint but continue processing requests
                            let result = event_store.flush_and_checkpoint();
                            let _ = response_tx.send(result);
                        }
                    }
                }
            });
        });

        Ok(Self { request_tx })
    }

    async fn read(&self, query: Option<DCBQuery>, after: Option<u64>, limit: Option<usize>) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx.send(EventStoreRequest::Read {
            query,
            after,
            limit,
            response_tx,
        }).await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send read request to EventStore thread",
        )))?;

        response_rx.await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to receive read response from EventStore thread",
        )))?
    }

    async fn append(&self, events: Vec<DCBEvent>, condition: Option<DCBAppendCondition>) -> DCBResult<u64> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx.send(EventStoreRequest::Append {
            events,
            condition,
            response_tx,
        }).await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send append request to EventStore thread",
        )))?;

        response_rx.await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to receive append response from EventStore thread",
        )))?
    }

    async fn head(&self) -> DCBResult<Option<u64>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx.send(EventStoreRequest::Head {
            response_tx,
        }).await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send head request to EventStore thread",
        )))?;

        response_rx.await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to receive head response from EventStore thread",
        )))?
    }

    #[allow(dead_code)]
    async fn shutdown(&self) {
        let _ = self.request_tx.send(EventStoreRequest::Shutdown).await;
    }

    /// Flushes all pending changes to disk, creates a checkpoint, and shuts down the EventStore
    pub async fn flush_and_shutdown(&self) -> DCBResult<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx.send(EventStoreRequest::FlushAndShutdown {
            response_tx,
        }).await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send flush_and_shutdown request to EventStore thread",
        )))?;

        response_rx.await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to receive flush_and_shutdown response from EventStore thread",
        )))?
    }

    /// Flushes all pending changes to disk and creates a checkpoint
    pub async fn flush_and_checkpoint(&self) -> DCBResult<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx.send(EventStoreRequest::FlushAndCheckpoint {
            response_tx,
        }).await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send flush_and_checkpoint request to EventStore thread",
        )))?;

        response_rx.await.map_err(|_| EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to receive flush_and_checkpoint response from EventStore thread",
        )))?
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
    type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponseProto, Status>> + Send + 'static>>;

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
                    let response = ReadResponseProto {
                        batch: Some(batch),
                    };

                    let _ = tx.send(Ok(response)).await;
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(format!("Read error: {:?}", e)))).await;
                }
            }
        });

        // Return the receiver as a stream
        Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::ReadStream))
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
                    EventStoreError::IntegrityError => {
                        Err(Status::failed_precondition("Integrity error: condition failed"))
                    }
                    _ => {
                        Err(Status::internal(format!("Append error: {:?}", e)))
                    }
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
            Err(e) => {
                Err(Status::internal(format!("Head error: {:?}", e)))
            }
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
        let client = dcbdb::event_store_service_client::EventStoreServiceClient::connect(dst).await?;
        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl DCBEventStoreAPI for GrpcEventStoreClient {
    fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<Box<dyn crate::api::DCBReadResponse + '_>> {
        // Create a blocking runtime for the async read operation
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Convert API types to proto types
        let query_proto = query.map(|q| QueryProto {
            items: q.items.into_iter().map(|item| QueryItemProto {
                types: item.types,
                tags: item.tags,
            }).collect(),
        });

        let limit_proto = limit.map(|l| l as u32);

        // Create the read request
        let request = ReadRequestProto {
            query: query_proto,
            after,
            limit: limit_proto,
        };

        // Execute the read operation in the runtime
        let mut client = self.client.clone();
        let response = rt.block_on(async move {
            client.read(request).await
        });

        match response {
            Ok(stream) => {
                // Create a GrpcReadResponse that implements DCBReadResponse
                Ok(Box::new(GrpcReadResponse::new(rt, stream.into_inner())) as Box<dyn crate::api::DCBReadResponse + '_>)
            }
            Err(status) => {
                Err(EventStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("gRPC read error: {}", status),
                )))
            }
        }
    }

    fn append(&self, events: Vec<DCBEvent>, condition: Option<DCBAppendCondition>) -> DCBResult<u64> {
        // Create a blocking runtime for the async append operation
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Convert API types to proto types
        let events_proto: Vec<EventProto> = events.into_iter().map(|e| EventProto {
            event_type: e.event_type,
            tags: e.tags,
            data: e.data,
        }).collect();

        let condition_proto = condition.map(|c| AppendConditionProto {
            fail_if_events_match: Some(QueryProto {
                items: c.fail_if_events_match.items.into_iter().map(|item| QueryItemProto {
                    types: item.types,
                    tags: item.tags,
                }).collect(),
            }),
            after: c.after,
        });

        // Create the append request
        let request = AppendRequestProto {
            events: events_proto,
            condition: condition_proto,
        };

        // Execute the append operation in the runtime
        let mut client = self.client.clone();
        let response = rt.block_on(async move {
            client.append(request).await
        });

        match response {
            Ok(response) => {
                Ok(response.into_inner().position)
            }
            Err(status) => {
                // Check if the error message indicates an integrity error
                if status.message().contains("Integrity error") {
                    Err(EventStoreError::IntegrityError)
                } else {
                    Err(EventStoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("gRPC append error: {}", status),
                    )))
                }
            }
        }
    }

    fn head(&self) -> DCBResult<Option<u64>> {
        // Create a blocking runtime for the async head operation
        let rt = tokio::runtime::Runtime::new().unwrap();

        // Create the head request
        let request = HeadRequestProto {};

        // Execute the head operation in the runtime
        let mut client = self.client.clone();
        let response = rt.block_on(async move {
            client.head(request).await
        });

        match response {
            Ok(response) => {
                Ok(response.into_inner().position)
            }
            Err(status) => {
                Err(EventStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("gRPC head error: {}", status),
                )))
            }
        }
    }
}

// Implementation of DCBReadResponse for the gRPC client
struct GrpcReadResponse {
    rt: tokio::runtime::Runtime,
    stream: tonic::codec::Streaming<ReadResponseProto>,
    events: Vec<DCBSequencedEvent>,
    current_index: usize,
    head: Option<u64>,
}

impl GrpcReadResponse {
    fn new(rt: tokio::runtime::Runtime, stream: tonic::codec::Streaming<ReadResponseProto>) -> Self {
        Self {
            rt,
            stream,
            events: Vec::new(),
            current_index: 0,
            head: None,
        }
    }

    fn fetch_next_batch(&mut self) -> DCBResult<()> {
        // Use the runtime to get the next message from the stream
        let next_message = self.rt.block_on(async {
            self.stream.message().await
        });

        match next_message {
            Ok(Some(response)) => {
                if let Some(batch) = response.batch {
                    // Update the head
                    self.head = batch.head;

                    // Convert proto events to API events
                    let events: Vec<DCBSequencedEvent> = batch.events.into_iter().map(|e| {
                        let event_proto = e.event.unwrap();
                        DCBSequencedEvent {
                            position: e.position,
                            event: DCBEvent {
                                event_type: event_proto.event_type,
                                tags: event_proto.tags,
                                data: event_proto.data,
                            },
                        }
                    }).collect();

                    // Add the events to our buffer
                    self.events.extend(events);
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(status) => {
                Err(EventStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("gRPC stream error: {}", status),
                )))
            }
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

impl crate::api::DCBReadResponse for GrpcReadResponse {
    fn head(&self) -> Option<u64> {
        self.head
    }

    fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>) {
        // Collect all events
        let mut events = Vec::new();
        while let Some(event) = self.next() {
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
pub async fn start_grpc_server<P: AsRef<Path> + Send + 'static>(path: P, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let server = GrpcEventStoreServer::new(path)?;

    println!("gRPC server listening on {}", addr);

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
    let event_store = server.event_store.clone();

    println!("gRPC server listening on {}", addr);

    // Create a channel to signal the background task to stop
    let (flush_stop_tx, flush_stop_rx) = tokio::sync::watch::channel(false);

    // Spawn a background task that calls flush_and_checkpoint every 100ms
    let flush_event_store = event_store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(250));
        let mut flush_stop_rx = flush_stop_rx;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Call flush_and_checkpoint
                    if let Err(e) = flush_event_store.flush_and_checkpoint().await {
                        eprintln!("Error during periodic flush_and_checkpoint: {:?}", e);
                    }
                }
                _ = flush_stop_rx.changed() => {
                    // Stop the background task if the stop signal is received
                    if *flush_stop_rx.borrow() {
                        println!("Stopping periodic flush task");
                        break;
                    }
                }
            }
        }
    });

    Server::builder()
        .add_service(server.into_service())
        .serve_with_shutdown(addr, async {
            shutdown_rx.await.ok();
            println!("gRPC server shutting down, flushing data to disk...");

            // Signal the background task to stop
            flush_stop_tx.send(true).ok();

            // Flush and checkpoint before shutting down
            if let Err(e) = event_store.flush_and_shutdown().await {
                eprintln!("Error during flush_and_shutdown: {:?}", e);
            } else {
                println!("Data flushed successfully");
            }

            println!("gRPC server shutdown complete");
        })
        .await?;

    Ok(())
}
