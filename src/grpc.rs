use futures::Stream;
use std::path::Path;
use std::pin::Pin;
use std::thread;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Code, transport::Server};

use crate::dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStore, DCBQuery, DCBQueryItem, DCBResult,
    DCBSequencedEvent,
};
use crate::db::{UmaDB, read_conditional, DEFAULT_PAGE_SIZE};
use crate::mvcc::Mvcc;

// Include the generated proto code
pub mod umadb {
    tonic::include_proto!("umadb");
}

use prost::Message;
use prost::bytes::Bytes;

use umadb::{
    ErrorResponseProto,
    AppendConditionProto, AppendRequestProto, AppendResponseProto, EventProto, HeadRequestProto,
    HeadResponseProto, QueryItemProto, QueryProto, ReadRequestProto, ReadResponseProto,
    SequencedEventProto,
    uma_db_service_server::{UmaDbService, UmaDbServiceServer},
};

// Function to start the gRPC server with a shutdown signal
pub async fn start_server<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    // Create a shutdown broadcast channel for terminating ongoing subscriptions
    let (srv_shutdown_tx, srv_shutdown_rx) = watch::channel(false);
    let server = UmaDBServer::new(path, srv_shutdown_rx)?;
    println!("UmaDB server listening on {addr}");

    use std::time::Duration;
    let mut server_builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(5)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024))
        .tcp_nodelay(true)
        .concurrency_limit_per_connection(1024);

    server_builder
        .add_service(server.into_service())
        .serve_with_shutdown(addr, async move {
            // Wait for external shutdown trigger
            let _ = shutdown_rx.await;
            // Broadcast shutdown to all subscription tasks
            let _ = srv_shutdown_tx.send(true);
            println!("UmaDB server shutdown complete");
        })
        .await?;

    Ok(())
}


// gRPC server implementation
pub struct UmaDBServer {
    event_store: EventStoreHandle,
    shutdown_rx: watch::Receiver<bool>,
}

impl UmaDBServer {
    pub fn new<P: AsRef<Path> + Send + 'static>(path: P, shutdown_rx: watch::Receiver<bool>) -> std::io::Result<Self> {
        let event_store = EventStoreHandle::new(path)?;
        Ok(Self { event_store, shutdown_rx })
    }

    pub fn into_service(self) -> UmaDbServiceServer<Self> {
        UmaDbServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl UmaDbService for UmaDBServer {
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

        // Create a channel for streaming responses (deeper buffer to reduce backpressure under concurrency)
        let (tx, rx) = mpsc::channel(2048);
        let event_store = self.event_store.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

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
            const GRPC_BATCH_SIZE_DEFAULT: u32 = 100;
            const GRPC_BATCH_SIZE_MAX: u32 = 5000;
            loop {
                // If this is a subscription, exit if client has gone away or server is shutting down
                if subscribe {
                    if tx.is_closed() { break; }
                    if *shutdown_rx.borrow() { break; }
                }
                // Determine per-iteration limit: take requested batch size (if any), cap to GRPC_BATCH_SIZE, ensure >= 1, then don't exceed remaining (if any)
                let requested_bs = req.batch_size.unwrap_or(GRPC_BATCH_SIZE_DEFAULT);
                let capped_bs = requested_bs.min(GRPC_BATCH_SIZE_MAX);
                let per_iter_limit = Some(remaining.unwrap_or(usize::MAX).min(capped_bs as usize));
                // println!("Per iter limit: {per_iter_limit:?}");
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
                                let response = ReadResponseProto { events: vec![], head: head_to_send };
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
                                    // Wait for either a new head or a server shutdown signal
                                    tokio::select! {
                                        res = head_rx.changed() => {
                                            if res.is_err() { break; }
                                        }
                                        res2 = shutdown_rx.changed() => {
                                            if res2.is_ok() {
                                                // If shutdown flag set to true, exit
                                                if *shutdown_rx.borrow() { break; }
                                            } else {
                                                break; // sender dropped
                                            }
                                        }
                                    }
                                }
                                continue;
                            }
                            break;
                        }

                        // For unlimited overall reads, clamp events to the starting global head without cloning
                        let (slice_start, slice_len, reached_end) = if remaining.is_none() {
                            if let Some(h) = global_head {
                                // find first index > h
                                let idx = events.iter().position(|e| e.position > h).unwrap_or(events.len());
                                (0, idx, idx < events.len())
                            } else {
                                (0, events.len(), false)
                            }
                        } else {
                            (0, events.len(), false)
                        };

                        if slice_len == 0 {
                            // If we haven't sent anything yet, still send an empty batch with head to convey metadata
                            if !sent_any {
                                let head_to_send = if remaining.is_none() { global_head } else { head };
                                let response = ReadResponseProto { events: vec![], head: head_to_send };
                                let _ = tx.send(Ok(response)).await;
                            }
                            break;
                        }

                        // Prepare and send this non-empty (possibly trimmed) batch
                        let batch_head = if remaining.is_none() { global_head } else { head };
                        let mut ev_out = Vec::with_capacity(slice_len);
                        for e in events[slice_start..slice_start + slice_len].iter() {
                            ev_out.push(SequencedEventProto::from(e.clone()));
                        }
                        let response = ReadResponseProto { events: ev_out, head: batch_head };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                        sent_any = true;

                        // Advance the cursor (use a new reader on the next loop iteration)
                        next_after = events.get(slice_start + slice_len - 1).map(|e| e.position);

                        // If we reached the captured head boundary on non-subscribe, stop streaming further
                        if reached_end && remaining.is_none() && !subscribe {
                            break;
                        }

                        // Decrease remaining overall limit if any, and stop if reached
                        if let Some(rem) = remaining.as_mut() {
                            if *rem <= slice_len {
                                *rem = 0;
                            } else {
                                *rem -= slice_len;
                            }
                            if *rem == 0 {
                                break;
                            }
                        }

                        // Yield to let other tasks progress under high concurrency
                        tokio::task::yield_now().await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(status_from_dcb_error(&e)))
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
            Ok(position) => Ok(Response::new(AppendResponseProto { position })),
            Err(e) => Err(status_from_dcb_error(&e)),
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
            Err(e) => Err(status_from_dcb_error(&e)),
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
    mvcc: std::sync::Arc<Mvcc>,
    head_tx: watch::Sender<Option<u64>>,
    request_tx: mpsc::Sender<EventStoreRequest>,
}

impl EventStoreHandle {
    fn new<P: AsRef<Path> + Send + 'static>(path: P) -> std::io::Result<Self> {
        // Create a channel for sending requests to the EventStore thread (for writes)
        let (request_tx, mut request_rx) = mpsc::channel::<EventStoreRequest>(1024);

        // Build a shared LMDB instance (Arc) upfront so reads can proceed concurrently on this thread
        let p = path.as_ref();
        let file_path = if p.is_dir() { p.join("dcb.db") } else { p.to_path_buf() };
        let mvcc = std::sync::Arc::new(
            Mvcc::new(&file_path, DEFAULT_PAGE_SIZE, false)
                .map_err(|e| std::io::Error::other(format!("Failed to init LMDB: {e:?}")))?,
        );

        // Initialize head watch channel with current head
        let init_head = {
            let (_, header) = mvcc
                .get_latest_header()
                .map_err(|e| std::io::Error::other(format!("Failed to read header: {e:?}")))?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { None } else { Some(last) }
        };
        let (head_tx, _head_rx) = watch::channel::<Option<u64>>(init_head);

        // Spawn a thread to run the EventStore for serialized write operations
        let mvcc_for_writer = mvcc.clone();
        let head_tx_writer = head_tx.clone();
        thread::spawn(move || {
            let event_store = UmaDB::from_arc(mvcc_for_writer);

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

                            let mut total_events = 0;
                            total_events += events.len();
                            items.push((events, condition));
                            responders.push(response_tx);

                            // Drain the channel for more pending append requests without awaiting.
                            // Important: do not drop a popped request when hitting the batch limit.
                            // We stop draining BEFORE attempting to recv if we've reached the limit.
                            const MAX_BATCH_EVENTS: usize = 300;
                            loop {
                                if total_events >= MAX_BATCH_EVENTS {
                                    break;
                                }
                                match request_rx.try_recv() {
                                    Ok(EventStoreRequest::Append { events, condition, response_tx }) => {
                                        let ev_len = events.len();
                                        items.push((events, condition));
                                        responders.push(response_tx);
                                        total_events += ev_len;
                                    }
                                    Ok(EventStoreRequest::Shutdown) => {
                                        // Push back the shutdown signal by breaking and letting outer loop handle after batch.
                                        // We'll process current batch first, then break outer loop on next iteration when channel is empty.
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

        Ok(Self { mvcc, head_tx, request_tx })
    }

    async fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        // Offload blocking read to a dedicated threadpool
        let db = self.mvcc.clone();
        tokio::task::spawn_blocking(move || -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
            let reader = db.reader()?;
            let last_committed_position = reader.next_position.0.saturating_sub(1);

            let q = query.unwrap_or(DCBQuery { items: vec![] });
            let after_pos = crate::common::Position(after.unwrap_or(0));
            let events = read_conditional(&db, &std::collections::HashMap::new(), reader.events_tree_root_id, reader.tags_tree_root_id, q, after_pos, limit)
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
        })
        .await
        .map_err(|e| DCBError::Io(std::io::Error::other(format!("Join error: {e}"))))?
    }

    async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64> {
        // Concurrent pre-check of condition using a read-only view in a blocking thread.
        let adjusted_condition = if let Some(cond_in) = condition {
            let db = self.mvcc.clone();
            tokio::task::spawn_blocking(move || -> DCBResult<Option<DCBAppendCondition>> {
                let mut cond = cond_in;
                let reader = db.reader()?;
                let current_head = {
                    let last = reader.next_position.0.saturating_sub(1);
                    if last == 0 { None } else { Some(last) }
                };

                // Perform conditional read on the snapshot (limit 1) starting after the provided position
                let after_pos = crate::common::Position(cond.after.unwrap_or(0));
                let found = read_conditional(
                    &db,
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

                // Advance 'after' to at least the current head observed by this reader
                let new_after = std::cmp::max(cond.after.unwrap_or(0), current_head.unwrap_or(0));
                cond.after = Some(new_after);
                Ok(Some(cond))
            })
            .await
            .map_err(|e| DCBError::Io(std::io::Error::other(format!("Join error: {e}"))))??
        } else { None };

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
        // Offload blocking head read to a dedicated threadpool
        let db = self.mvcc.clone();
        tokio::task::spawn_blocking(move || -> DCBResult<Option<u64>> {
            let (_, header) = db
                .get_latest_header()
                .map_err(|e| DCBError::Corruption(format!("{e}")))?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { Ok(None) } else { Ok(Some(last)) }
        })
        .await
        .map_err(|e| DCBError::Io(std::io::Error::other(format!("Join error: {e}"))))?
    }

    #[allow(dead_code)]
    async fn shutdown(&self) {
        let _ = self.request_tx.send(EventStoreRequest::Shutdown).await;
    }
}

// Clone implementation for EventStoreHandle
impl Clone for EventStoreHandle {
    fn clone(&self) -> Self {
        Self {
            mvcc: self.mvcc.clone(),
            head_tx: self.head_tx.clone(),
            request_tx: self.request_tx.clone(),
        }
    }
}


// Async client implementation
pub struct AsyncUmaDBClient {
    client: umadb::uma_db_service_client::UmaDbServiceClient<tonic::transport::Channel>,
}

// Async read response wrapper that provides batched access and head metadata
pub struct AsyncReadResponse {
    stream: tonic::Streaming<ReadResponseProto>,
    buffered: Vec<crate::dcb::DCBSequencedEvent>,
    last_head: Option<Option<u64>>, // None = unknown yet; Some(x) = known
    ended: bool,
}

impl AsyncUmaDBClient {
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let client =
            umadb::uma_db_service_client::UmaDbServiceClient::connect(dst).await?;
        Ok(Self { client })
    }

    // Optimized connect that configures the transport for lower latency and better concurrency.
    // Takes a URL like "http://127.0.0.1:50051".
    pub async fn connect_optimized_url(url: &str) -> Result<Self, tonic::transport::Error> {
        use std::time::Duration;
        use tonic::transport::Endpoint;

        let endpoint = Endpoint::from_shared(url.to_string())?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(5))
            .keep_alive_timeout(Duration::from_secs(10))
            // Bump the window sizes to reduce flow-control stalls under high throughput
            .initial_stream_window_size(Some(4 * 1024 * 1024))
            .initial_connection_window_size(Some(8 * 1024 * 1024));

        let channel = endpoint.connect().await?;
        let client = umadb::uma_db_service_client::UmaDbServiceClient::new(channel);
        Ok(Self { client })
    }

    // Async inherent methods: use the gRPC client directly (no trait required)
    pub async fn read(
        &self,
        query: Option<crate::dcb::DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
        subscribe: bool,
        batch_size: Option<usize>,
    ) -> crate::dcb::DCBResult<AsyncReadResponse> {
        // Convert API types to proto types
        let query_proto = query.map(|q| QueryProto {
            items: q
                .items
                .into_iter()
                .map(|item| QueryItemProto { types: item.types, tags: item.tags })
                .collect(),
        });
        let limit_proto = limit.map(|l| l as u32);
        let request = ReadRequestProto { query: query_proto, after, limit: limit_proto, subscribe: Some(subscribe), batch_size: batch_size.map(|b| b as u32) };

        let mut client = self.client.clone();
        let response = client.read(request).await.map_err(|status| dcb_error_from_status(status))?;
        let stream = response.into_inner();

        Ok(AsyncReadResponse { stream, buffered: Vec::new(), last_head: None, ended: false })
    }

    pub async fn append(
        &self,
        events: Vec<crate::dcb::DCBEvent>,
        condition: Option<crate::dcb::DCBAppendCondition>,
    ) -> crate::dcb::DCBResult<u64> {
        let events_proto: Vec<EventProto> = events
            .into_iter()
            .map(|e| EventProto { event_type: e.event_type, tags: e.tags, data: e.data })
            .collect();

        let condition_proto = condition.map(|c| AppendConditionProto {
            fail_if_events_match: Some(QueryProto {
                items: c
                    .fail_if_events_match
                    .items
                    .into_iter()
                    .map(|item| QueryItemProto { types: item.types, tags: item.tags })
                    .collect(),
            }),
            after: c.after,
        });

        let request = AppendRequestProto { events: events_proto, condition: condition_proto };
        let mut client = self.client.clone();
        match client.append(request).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(dcb_error_from_status(status)),
        }
    }

    pub async fn head(&self) -> crate::dcb::DCBResult<Option<u64>> {
        let mut client = self.client.clone();
        match client.head(HeadRequestProto {}).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(dcb_error_from_status(status)),
        }
    }
}

impl AsyncReadResponse {
    async fn fetch_next_if_needed(&mut self) -> crate::dcb::DCBResult<()> {
        if !self.buffered.is_empty() || self.ended { return Ok(()); }
        match self.stream.message().await {
            Ok(Some(resp)) => {
                self.last_head = Some(resp.head);
                self.buffered.clear();
                for e in resp.events.into_iter() {
                    if let Some(ev) = e.event {
                        self.buffered.push(crate::dcb::DCBSequencedEvent {
                            position: e.position,
                            event: crate::dcb::DCBEvent { event_type: ev.event_type, tags: ev.tags, data: ev.data },
                        });
                    }
                }
                Ok(())
            }
            Ok(None) => { self.ended = true; Ok(()) }
            Err(status) => Err(dcb_error_from_status(status)),
        }
    }

    pub async fn head(&mut self) -> crate::dcb::DCBResult<Option<u64>> {
        if let Some(h) = self.last_head { return Ok(h); }
        // Need to read at least one message to learn head
        self.fetch_next_if_needed().await?;
        Ok(self.last_head.unwrap_or(None))
    }

    // Returns the next batch of events; empty vec indicates end of stream
    pub async fn next_batch(&mut self) -> crate::dcb::DCBResult<Vec<crate::dcb::DCBSequencedEvent>> {
        // If we have buffered events from a previous fetch, return them first
        if !self.buffered.is_empty() {
            let mut out = Vec::new();
            std::mem::swap(&mut out, &mut self.buffered);
            return Ok(out);
        }
        // Otherwise fetch a new message (which may be empty but carries head)
        self.fetch_next_if_needed().await?;
        if !self.buffered.is_empty() {
            let mut out = Vec::new();
            std::mem::swap(&mut out, &mut self.buffered);
            return Ok(out);
        }
        // No buffered events. If stream ended or message had no events, return empty.
        Ok(Vec::new())
    }
}


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

// Helper: map DCBError -> tonic::Status with structured details
fn status_from_dcb_error(e: &DCBError) -> Status {
    let (code, error_type) = match e {
        DCBError::IntegrityError(_) => (Code::FailedPrecondition, umadb::error_response_proto::ErrorType::Integrity as i32),
        DCBError::Corruption(_) | DCBError::DatabaseCorrupted(_) | DCBError::DeserializationError(_) => (Code::DataLoss, umadb::error_response_proto::ErrorType::Corruption as i32),
        _ => (Code::Internal, umadb::error_response_proto::ErrorType::Io as i32),
    };
    let msg = e.to_string();
    let detail = ErrorResponseProto { message: msg.clone(), error_type };
    let bytes = detail.encode_to_vec();
    Status::with_details(code, msg, Bytes::from(bytes))
}

// Helper: map tonic::Status -> DCBError by decoding details
fn dcb_error_from_status(status: Status) -> DCBError {
    let details = status.details();
    // Try to decode ErrorResponseProto directly from details
    if !details.is_empty() {
        if let Ok(err) = ErrorResponseProto::decode(details) {
            return match err.error_type {
                x if x == umadb::error_response_proto::ErrorType::Integrity as i32 => DCBError::IntegrityError(err.message),
                x if x == umadb::error_response_proto::ErrorType::Corruption as i32 => DCBError::Corruption(err.message),
                x if x == umadb::error_response_proto::ErrorType::Serialization as i32 => DCBError::SerializationError(err.message),
                _ => DCBError::Io(std::io::Error::other(err.message)),
            };
        }
    }
    // Fallback: infer from gRPC code
    match status.code() {
        Code::FailedPrecondition => DCBError::IntegrityError(status.message().to_string()),
        Code::DataLoss => DCBError::Corruption(status.message().to_string()),
        Code::Internal => DCBError::Io(std::io::Error::other(status.message().to_string())),
        _ => DCBError::Io(std::io::Error::other(format!("gRPC error: {}", status))),
    }
}
