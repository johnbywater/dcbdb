use futures::Stream;
use std::path::Path;
use std::pin::Pin;
use std::thread;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};

use crate::dcbapi::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStore, DCBQuery, DCBQueryItem, DCBResult,
    DCBSequencedEvent,
};
use crate::event_store::{EventStore, read_conditional, DEFAULT_PAGE_SIZE};
use crate::lmdb::Lmdb;

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
    // Read {
    //     query: Option<DCBQuery>,
    //     after: Option<u64>,
    //     limit: Option<usize>,
    //     response_tx: oneshot::Sender<DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)>>,
    // },
    Append {
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        response_tx: oneshot::Sender<DCBResult<u64>>,
    },
    // Head {
    //     response_tx: oneshot::Sender<DCBResult<Option<u64>>>,
    // },
    #[allow(dead_code)]
    Shutdown,
}

// Thread-safe wrapper for EventStore
struct EventStoreHandle {
    request_tx: mpsc::Sender<EventStoreRequest>,
    lmdb: std::sync::Arc<crate::lmdb::Lmdb>,
    head_tx: watch::Sender<Option<u64>>,
}

impl EventStoreHandle {
    fn new<P: AsRef<Path> + Send + 'static>(path: P) -> std::io::Result<Self> {
        // Create a channel for sending requests to the EventStore thread (for writes)
        let (request_tx, mut request_rx) = mpsc::channel::<EventStoreRequest>(32);

        // Build a shared LMDB instance (Arc) upfront so reads can proceed concurrently on this thread
        let p = path.as_ref();
        let file_path = if p.is_dir() { p.join("dcb.db") } else { p.to_path_buf() };
        let lmdb = std::sync::Arc::new(
            Lmdb::new(&file_path, DEFAULT_PAGE_SIZE, false)
                .map_err(|e| std::io::Error::other(format!("Failed to init LMDB: {e:?}")))?,
        );

        // Initialize head watch channel with current head
        let init_head = {
            let (_, header) = lmdb
                .get_latest_header()
                .map_err(|e| std::io::Error::other(format!("Failed to read header: {e:?}")))?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { None } else { Some(last) }
        };
        let (head_tx, _head_rx) = watch::channel::<Option<u64>>(init_head);

        // Spawn a thread to run the EventStore for serialized write operations
        let lmdb_for_writer = lmdb.clone();
        let head_tx_writer = head_tx.clone();
        thread::spawn(move || {
            let event_store = EventStore::from_arc(lmdb_for_writer);

            // Create a runtime for async operations
            let rt = tokio::runtime::Runtime::new().unwrap();

            // Process requests (append/head if needed). Note: reads will be served directly without going through this loop.
            rt.block_on(async {
                while let Some(request) = request_rx.recv().await {
                    match request {
                        // EventStoreRequest::Read { .. } => {
                        //     // No-op: reads should not be routed here anymore.
                        // }
                        EventStoreRequest::Append {
                            events,
                            condition,
                            response_tx,
                        } => {
                            // Batch processing: drain any immediately available append requests
                            let mut items: Vec<(Vec<DCBEvent>, Option<DCBAppendCondition>)> = Vec::new();
                            let mut responders: Vec<oneshot::Sender<DCBResult<u64>>> = Vec::new();

                            items.push((events, condition));
                            responders.push(response_tx);

                            // Drain the channel for more pending append requests without awaiting
                            loop {
                                match request_rx.try_recv() {
                                    Ok(EventStoreRequest::Append { events, condition, response_tx }) => {
                                        items.push((events, condition));
                                        responders.push(response_tx);
                                    }
                                    Ok(EventStoreRequest::Shutdown) => {
                                        // Push back the shutdown signal by breaking and letting outer loop handle after batch
                                        // We'll process current batch first, then break outer loop on next iteration when channel is empty
                                        // To ensure shutdown is noticed, we stop draining further
                                        break;
                                    }
                                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                                }
                            }

                            // Execute a single batched append
                            let batch_result = event_store.append_batch(items);
                            match batch_result {
                                Ok(results) => {
                                    for (res, tx) in results.into_iter().zip(responders.into_iter()) {
                                        let _ = tx.send(res);
                                    }
                                    // After successful batch commit, publish updated head
                                    let new_head = event_store.head().ok().flatten();
                                    let _ = head_tx_writer.send(new_head);
                                }
                                Err(e) => {
                                    // If the batch failed as a whole (e.g., commit failed), propagate an error to all responders
                                    // We cannot clone DCBError, so move it into the last sender and send IO-wrapped messages to others.
                                    let total = responders.len();
                                    let mut iter = responders.into_iter();
                                    if total > 1 {
                                        for _ in 0..(total - 1) {
                                            if let Some(tx) = iter.next() {
                                                let _ = tx.send(Err(DCBError::Io(std::io::Error::other(
                                                    "Batched append failed; see a concurrent request error for details",
                                                ))));
                                            }
                                        }
                                    }
                                    if let Some(tx) = iter.next() {
                                        let _ = tx.send(Err(e));
                                    }
                                }
                            }
                        }
                        // EventStoreRequest::Head { response_tx } => {
                        //     let result = event_store.head();
                        //     let _ = response_tx.send(result);
                        // }
                        EventStoreRequest::Shutdown => {
                            break;
                        }
                    }
                }
            });
        });

        Ok(Self { request_tx, lmdb, head_tx })
    }

    async fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        // Concurrent read path: use shared LMDB directly
        let db: &Lmdb = &self.lmdb;
        let reader = db.reader()?;
        let last_committed_position = reader.next_position.0.saturating_sub(1);

        let q = query.unwrap_or(DCBQuery { items: vec![] });
        let after_pos = crate::common::Position(after.unwrap_or(0));
        let events = read_conditional(db, &std::collections::HashMap::new(), reader.events_tree_root_id, reader.tags_tree_root_id, q, after_pos, limit)
            .map_err(|e| DCBError::Corruption(format!("{e}")))?;

        let head = if limit.is_none() {
            if last_committed_position == 0 {
                None
            } else {
                Some(last_committed_position)
            }
        } else {
            events.last().map(|e| e.position)
        };

        Ok((events, head))
    }

    async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64> {
        // Concurrent pre-check of condition using a read-only view. If it fails, return early.
        let mut adjusted_condition = condition;
        if let Some(mut cond) = adjusted_condition.take() {
            // Open a reader and determine current head
            let db: &Lmdb = &self.lmdb;
            let reader = db.reader()?;
            let current_head = {
                let last = reader.next_position.0.saturating_sub(1);
                if last == 0 { None } else { Some(last) }
            };

            // Perform conditional read on the snapshot (limit 1) starting after the provided position
            let after_pos = crate::common::Position(cond.after.unwrap_or(0));
            let found = read_conditional(
                db,
                &std::collections::HashMap::new(),
                reader.events_tree_root_id,
                reader.tags_tree_root_id,
                cond.fail_if_events_match.clone(),
                after_pos,
                Some(1),
            ).map_err(|e| DCBError::Corruption(format!("{e}")))?;

            if let Some(matched) = found.first() {
                let msg = format!(
                    "matching event: {:?} condition: {:?}",
                    matched, cond.fail_if_events_match
                );
                return Err(DCBError::IntegrityError(msg));
            }

            // Advance after to at least the current head observed by this reader
            let new_after = std::cmp::max(cond.after.unwrap_or(0), current_head.unwrap_or(0));
            cond.after = Some(new_after);
            adjusted_condition = Some(cond);
        }

        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(EventStoreRequest::Append {
                events,
                condition: adjusted_condition,
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

    fn watch_head(&self) -> watch::Receiver<Option<u64>> {
        self.head_tx.subscribe()
    }

    async fn head(&self) -> DCBResult<Option<u64>> {
        let db: &Lmdb = &self.lmdb;
        let (_, header) = db
            .get_latest_header()
            .map_err(|e| DCBError::Corruption(format!("{e}")))?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
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
        let mut query: Option<DCBQuery> = req.query.map(|q| q.into());
        let after = req.after;
        let limit = req.limit.map(|l| l as usize);

        // Create a channel for streaming responses
        let (tx, rx) = mpsc::channel(128);
        let event_store = self.event_store.clone();

        // Spawn a task to handle the read operation and stream multiple batches
        tokio::spawn(async move {
            // Ensure we can reuse the same query across batches
            let query_clone = query.take();
            let mut next_after = after;
            let mut sent_any = false;
            let mut remaining = limit;
            let subscribe = req.subscribe.unwrap_or(false);
            // Create a watch receiver for head updates (for subscriptions)
            let mut head_rx = event_store.watch_head();
            // If overall read is unlimited, compute global head once to preserve semantics
            let global_head = if remaining.is_none() && !subscribe {
                match event_store.head().await { Ok(h) => h, Err(_) => None }
            } else { None };
            // Server-side batch cap to ensure streaming in multiple messages
            // Keep this modest so tests expecting multiple batches (e.g., 300 items) will split.
            const GRPC_BATCH_SIZE: usize = 100;
            loop {
                // Determine per-iteration limit: don't exceed remaining (if any), and cap by server batch size
                let per_iter_limit = Some(remaining.unwrap_or(usize::MAX).min(GRPC_BATCH_SIZE));
                // If subscription and remaining exhausted (limit reached), terminate
                if subscribe {
                    if let Some(rem) = remaining { if rem == 0 { break; } }
                }
                match event_store.read(query_clone.clone(), next_after, per_iter_limit).await {
                    Ok((events, head)) => {
                        if events.is_empty() {
                            // Only send an empty batch to communicate head if this is the first batch
                            if !sent_any {
                                // For unlimited overall reads, use precomputed global head (non-subscribe)
                                let head_to_send = if remaining.is_none() && !subscribe { global_head } else { head };
                                let batch = SequencedEventBatchProto { events: vec![], head: head_to_send };
                                let response = ReadResponseProto { batch: Some(batch) };
                                let _ = tx.send(Ok(response)).await;
                            }
                            // For subscriptions, wait for new events instead of terminating
                            if subscribe {
                                // Wait while head <= next_after (or None)
                                loop {
                                    // If channel closed, stop
                                    if tx.is_closed() { break; }
                                    let current_head = *head_rx.borrow();
                                    let na = next_after.unwrap_or(0);
                                    if current_head.map(|h| h > na).unwrap_or(false) {
                                        break; // new events available
                                    }
                                    if head_rx.changed().await.is_err() {
                                        break; // sender dropped
                                    }
                                }
                                continue;
                            }
                            break;
                        }

                        // For unlimited overall reads, clamp events to the starting global head
                        let (events_to_send, reached_end) = if remaining.is_none() {
                            if let Some(h) = global_head {
                                let trimmed: Vec<_> = events.iter().cloned().take_while(|e| e.position <= h).collect();
                                let hit_boundary = trimmed.len() < events.len();
                                (trimmed, hit_boundary)
                            } else {
                                (events.clone(), false)
                            }
                        } else {
                            (events.clone(), false)
                        };

                        if events_to_send.is_empty() {
                            // If we haven't sent anything yet, still send an empty batch with head to convey metadata
                            if !sent_any {
                                let head_to_send = if remaining.is_none() { global_head } else { head };
                                let batch = SequencedEventBatchProto { events: vec![], head: head_to_send };
                                let response = ReadResponseProto { batch: Some(batch) };
                                let _ = tx.send(Ok(response)).await;
                            }
                            break;
                        }

                        // Prepare and send this non-empty (possibly trimmed) batch
                        let batch_head = if remaining.is_none() { global_head } else { head };
                        let batch = SequencedEventBatchProto {
                            events: events_to_send.iter().cloned().map(|e| e.into()).collect(),
                            head: batch_head,
                        };
                        let response = ReadResponseProto { batch: Some(batch) };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                        sent_any = true;

                        // Advance the cursor (use a new reader on the next loop iteration)
                        next_after = events_to_send.last().map(|e| e.position);

                        // If we reached the captured head boundary on non-subscribe, stop streaming further
                        if reached_end && remaining.is_none() && !subscribe {
                            break;
                        }

                        // Decrease remaining overall limit if any, and stop if reached
                        if let Some(rem) = remaining.as_mut() {
                            if *rem <= events_to_send.len() {
                                *rem = 0;
                            } else {
                                *rem -= events_to_send.len();
                            }
                            if *rem == 0 {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("Read error: {e:?}"))))
                            .await;
                        break;
                    }
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
                    DCBError::IntegrityError(_) => Err(Status::failed_precondition(
                        format!("Integrity error: condition failed: {e:?}"),
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
            lmdb: self.lmdb.clone(),
            head_tx: self.head_tx.clone(),
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
        subscribe: bool,
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
            subscribe: Some(subscribe),
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
            let rt = tokio::runtime::Runtime::new()?;
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
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async move { client.append(request).await })
        };

        match response {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => {
                // Check if the error message indicates an integrity error
                if status.message().contains("Integrity error") {
                    Err(DCBError::IntegrityError(status.message().to_string()))
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
            let rt = tokio::runtime::Runtime::new()?;
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
                            let event_proto = e.event.expect("SequencedEventProto should have an EventProto");
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
