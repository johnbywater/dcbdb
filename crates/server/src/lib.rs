use futures::Stream;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, transport::Server};

use umadb_core::db::{DEFAULT_PAGE_SIZE, UmaDB, is_request_idempotent, read_conditional, DEFAULT_DB_FILENAME};
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBQuery, DCBResult, DCBSequencedEvent,
};
use umadb_core::mvcc::Mvcc;

use tokio::runtime::Runtime;
use umadb_core::common::Position;
use umadb_proto::status_from_dcb_error;
use umadb_proto::umadb::{
    AppendRequestProto, AppendResponseProto, HeadRequestProto, HeadResponseProto, ReadRequestProto,
    ReadResponseProto, SequencedEventProto,
    uma_db_service_server::{UmaDbService, UmaDbServiceServer},
};

const APPEND_BATCH_MAX_EVENTS: usize = 2000;
const READ_RESPONSE_BATCH_SIZE_DEFAULT: u32 = 100;
const READ_RESPONSE_BATCH_SIZE_MAX: u32 = 5000;

// Optional TLS configuration helpers
#[derive(Clone, Debug)]
pub struct ServerTlsOptions {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

fn build_server_builder_with_options(tls: Option<ServerTlsOptions>) -> Server {
    use std::time::Duration;
    let mut server_builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(5)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024))
        .tcp_nodelay(true)
        .concurrency_limit_per_connection(1024);

    if let Some(opts) = tls {
        let identity = Identity::from_pem(opts.cert_pem, opts.key_pem);
        server_builder = server_builder
            .tls_config(ServerTlsConfig::new().identity(identity))
            .expect("failed to apply TLS config");
    }

    server_builder
}

// Function to start the gRPC server with a shutdown signal
pub async fn start_server<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    start_server_internal(path, addr, shutdown_rx, None).await
}

/// Start server with TLS using PEM-encoded cert and key.
pub async fn start_server_secure<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let tls = ServerTlsOptions { cert_pem, key_pem };
    start_server_internal(path, addr, shutdown_rx, Some(tls)).await
}

/// Convenience: load cert and key from filesystem paths
pub async fn start_server_secure_from_files<
    P: AsRef<Path> + Send + 'static,
    CP: AsRef<Path>,
    KP: AsRef<Path>,
>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    cert_path: CP,
    key_path: KP,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;
    let cert_pem = fs::read(cert_path)?;
    let key_pem = fs::read(key_path)?;
    start_server_secure(path, addr, shutdown_rx, cert_pem, key_pem).await
}

async fn start_server_internal<P: AsRef<Path> + Send + 'static>(
    path: P,
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
    tls: Option<ServerTlsOptions>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    // Create a shutdown broadcast channel for terminating ongoing subscriptions
    let (srv_shutdown_tx, srv_shutdown_rx) = watch::channel(false);
    let server = UmaDBServer::new(path, srv_shutdown_rx)?;
    if tls.is_some() {
        println!("Started UmaDB server (with TLS) listening on {addr}");
    } else {
        println!("UmaDB server (insecure) listening on {addr}");
    }

    let mut server_builder = build_server_builder_with_options(tls);

    // gRPC Health service setup
    use tonic_health::ServingStatus; // server API expects this enum
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    // Set overall and service-specific health to SERVING
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;
    health_reporter
        .set_service_status("umadb.UmaDBService", ServingStatus::Serving)
        .await;
    let health_reporter_for_shutdown = health_reporter.clone();

    server_builder
        .add_service(health_service)
        .add_service(server.into_service())
        .serve_with_shutdown(addr, async move {
            // Wait for an external shutdown trigger
            let _ = shutdown_rx.await;
            // Mark health as NOT_SERVING before shutdown
            let _ = health_reporter_for_shutdown
                .set_service_status("", ServingStatus::NotServing)
                .await;
            let _ = health_reporter_for_shutdown
                .set_service_status("umadb.UmaDBService", ServingStatus::NotServing)
                .await;
            // Broadcast shutdown to all subscription tasks
            let _ = srv_shutdown_tx.send(true);
            println!("UmaDB server shutdown complete");
        })
        .await?;

    Ok(())
}

// gRPC server implementation
pub struct UmaDBServer {
    request_handler: RequestHandler,
    shutdown_watch_rx: watch::Receiver<bool>,
}

impl UmaDBServer {
    pub fn new<P: AsRef<Path> + Send + 'static>(
        path: P,
        shutdown_rx: watch::Receiver<bool>,
    ) -> std::io::Result<Self> {
        let command_handler = RequestHandler::new(path)?;
        Ok(Self {
            request_handler: command_handler,
            shutdown_watch_rx: shutdown_rx,
        })
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
        let read_request = request.into_inner();

        // Convert protobuf query to DCB types
        let mut query: Option<DCBQuery> = read_request.query.map(|q| q.into());
        let start = read_request.start;
        let backwards = read_request.backwards.unwrap_or(false);
        let limit = read_request.limit;
        // Cap requested batch size.
        let capped_batch_size = read_request
            .batch_size
            .unwrap_or(READ_RESPONSE_BATCH_SIZE_DEFAULT)
            .clamp(1, READ_RESPONSE_BATCH_SIZE_MAX);
        let subscribe = read_request.subscribe.unwrap_or(false);

        // Create a channel for streaming responses (deeper buffer to reduce backpressure under concurrency)
        let (tx, rx) = mpsc::channel(2048);
        // Clone the request handler.
        let request_handler = self.request_handler.clone();
        // Clone the shutdown watch receiver.
        let mut shutdown_watch_rx = self.shutdown_watch_rx.clone();

        // Spawn a task to handle the read operation and stream multiple batches
        tokio::spawn(async move {
            // Ensure we can reuse the same query across batches
            let query_clone = query.take();
            let mut next_start = start;
            let mut sent_any = false;
            let mut remaining = limit;
            // Create a watch receiver for head updates (for subscriptions)
            // TODO: Make this an Option and only do this for subscriptions?
            let mut head_rx = request_handler.watch_head();
            // If overall read is unlimited, compute global head once to preserve semantics
            let global_head = if remaining.is_none() && !subscribe {
                request_handler.head().await.unwrap_or(None)
            } else {
                None
            };
            loop {
                // If this is a subscription, exit if the client
                // has gone away or the server is shutting down.
                if subscribe {
                    if tx.is_closed() {
                        break;
                    }
                    if *shutdown_watch_rx.borrow() {
                        break;
                    }
                }
                // Determine per-iteration limit.
                let per_iter_limit = remaining.unwrap_or(u32::MAX).min(capped_batch_size);
                // If subscription and remaining exhausted (limit reached), terminate
                if subscribe
                    && let Some(rem) = remaining
                    && rem == 0
                {
                    break;
                }
                match request_handler
                    .read(
                        query_clone.clone(),
                        next_start,
                        backwards,
                        Some(per_iter_limit),
                    )
                    .await
                {
                    Ok((events, head)) => {
                        if events.is_empty() {
                            // Only send an empty response to communicate head if this is the first
                            if !sent_any {
                                // For unlimited overall reads, use precomputed global head (non-subscribe)
                                let head_to_send = if remaining.is_none() && !subscribe {
                                    global_head
                                } else {
                                    head
                                };
                                let response = ReadResponseProto {
                                    events: vec![],
                                    head: head_to_send,
                                };
                                let _ = tx.send(Ok(response)).await;
                            }
                            // For subscriptions, wait for new events instead of terminating
                            if subscribe {
                                // Wait while head <= next_after (or None)
                                loop {
                                    // Stop if the channel is closed.
                                    if tx.is_closed() {
                                        break;
                                    }
                                    let current_head = *head_rx.borrow();
                                    let na = next_start.unwrap_or(1);
                                    if current_head.map(|h| h >= na).unwrap_or(false) {
                                        break; // new events available
                                    }
                                    // Wait for either a new head or a server shutdown signal
                                    tokio::select! {
                                        res = head_rx.changed() => {
                                            if res.is_err() { break; }
                                        }
                                        res2 = shutdown_watch_rx.changed() => {
                                            if res2.is_ok() {
                                                // Exit if shutting down.
                                                if *shutdown_watch_rx.borrow() { break; }
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
                                let idx = events
                                    .iter()
                                    .position(|e| e.position > h)
                                    .unwrap_or(events.len())
                                    as u32;
                                (0u32, idx, idx < events.len() as u32)
                            } else {
                                (0u32, events.len() as u32, false)
                            }
                        } else {
                            (0u32, events.len() as u32, false)
                        };

                        if slice_len == 0 {
                            // If we haven't sent anything yet, still send an empty batch with head to convey metadata
                            if !sent_any {
                                let head_to_send = if remaining.is_none() {
                                    global_head
                                } else {
                                    head
                                };
                                let response = ReadResponseProto {
                                    events: vec![],
                                    head: head_to_send,
                                };
                                let _ = tx.send(Ok(response)).await;
                            }
                            break;
                        }

                        // Prepare and send this non-empty (possibly trimmed) batch
                        let batch_head = if remaining.is_none() {
                            global_head
                        } else {
                            head
                        };
                        let mut ev_out = Vec::with_capacity(slice_len as usize);
                        for e in events
                            [slice_start as usize..slice_start as usize + slice_len as usize]
                            .iter()
                        {
                            ev_out.push(SequencedEventProto::from(e.clone()));
                        }
                        let response = ReadResponseProto {
                            events: ev_out,
                            head: batch_head,
                        };

                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                        sent_any = true;

                        // Advance the cursor (use a new reader on the next loop iteration)
                        next_start = events
                            .get(slice_start as usize + slice_len as usize - 1)
                            .map(|e| e.position + 1);

                        // Stop streaming further If we reached the
                        // captured head boundary (non-subscriber only).
                        if reached_end && remaining.is_none() && !subscribe {
                            break;
                        }

                        // Decrease the remaining overall limit if any, and stop if reached
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
                        let _ = tx.send(Err(status_from_dcb_error(&e))).await;
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

        // Convert protobuf types to API types
        let events: Vec<DCBEvent> = match req.events.into_iter().map(|e| e.try_into()).collect() {
            Ok(events) => events,
            Err(e) => {
                return Err(status_from_dcb_error(&e));
            }
        };
        let condition = req.condition.map(|c| c.into());

        // Call the event store append method
        match self.request_handler.append(events, condition).await {
            Ok(position) => Ok(Response::new(AppendResponseProto { position })),
            Err(e) => Err(status_from_dcb_error(&e)),
        }
    }

    async fn head(
        &self,
        _request: Request<HeadRequestProto>,
    ) -> Result<Response<HeadResponseProto>, Status> {
        // Call the event store head method
        match self.request_handler.head().await {
            Ok(position) => {
                // Return the position as a response
                Ok(Response::new(HeadResponseProto { position }))
            }
            Err(e) => Err(status_from_dcb_error(&e)),
        }
    }
}

// Message types for communication between the gRPC server and the request handler's writer thread
enum WriterRequest {
    Append {
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
        response_tx: oneshot::Sender<DCBResult<u64>>,
    },
    Shutdown,
}

// Thread-safe request handler
struct RequestHandler {
    mvcc: Arc<Mvcc>,
    head_watch_tx: watch::Sender<Option<u64>>,
    writer_request_tx: mpsc::Sender<WriterRequest>,
}

impl RequestHandler {
    fn new<P: AsRef<Path> + Send + 'static>(path: P) -> std::io::Result<Self> {
        // Create a channel for sending requests to the writer thread
        let (request_tx, mut request_rx) = mpsc::channel::<WriterRequest>(1024);

        // Build a shared Mvcc instance (Arc) upfront so reads can proceed concurrently
        let p = path.as_ref();
        let file_path = if p.is_dir() {
            p.join(DEFAULT_DB_FILENAME)
        } else {
            p.to_path_buf()
        };
        let mvcc = Arc::new(
            Mvcc::new(&file_path, DEFAULT_PAGE_SIZE, false)
                .map_err(|e| std::io::Error::other(format!("Failed to init LMDB: {e:?}")))?,
        );

        // Initialize the head watch channel with the current head.
        let init_head = {
            let (_, header) = mvcc
                .get_latest_header()
                .map_err(|e| std::io::Error::other(format!("Failed to read header: {e:?}")))?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { None } else { Some(last) }
        };
        let (head_tx, _head_rx) = watch::channel::<Option<u64>>(init_head);

        // Spawn a thread for processing writer requests.
        let mvcc_for_writer = mvcc.clone();
        let head_tx_writer = head_tx.clone();
        thread::spawn(move || {
            let db = UmaDB::from_arc(mvcc_for_writer);

            // Create a runtime for processing writer requests.
            let rt = Runtime::new().unwrap();

            // Process writer requests.
            rt.block_on(async {
                while let Some(request) = request_rx.recv().await {
                    match request {
                        WriterRequest::Append {
                            events,
                            condition,
                            response_tx,
                        } => {
                            // Batch processing: drain any immediately available requests
                            let mut items: Vec<(Vec<DCBEvent>, Option<DCBAppendCondition>)> =
                                Vec::new();
                            let mut responders: Vec<oneshot::Sender<DCBResult<u64>>> = Vec::new();

                            let mut total_events = 0;
                            total_events += events.len();
                            items.push((events, condition));
                            responders.push(response_tx);

                            // Drain the channel for more pending writer requests without awaiting.
                            // Important: do not drop a popped request when hitting the batch limit.
                            // We stop draining BEFORE attempting to recv if we've reached the limit.
                            loop {
                                if total_events >= APPEND_BATCH_MAX_EVENTS {
                                    break;
                                }
                                match request_rx.try_recv() {
                                    Ok(WriterRequest::Append {
                                        events,
                                        condition,
                                        response_tx,
                                    }) => {
                                        let ev_len = events.len();
                                        items.push((events, condition));
                                        responders.push(response_tx);
                                        total_events += ev_len;
                                    }
                                    Ok(WriterRequest::Shutdown) => {
                                        // Push back the shutdown signal by breaking and letting
                                        // outer loop handle after batch. We'll process the
                                        // current batch first, then break the outer loop on
                                        // the next iteration when the channel is empty.
                                        break;
                                    }
                                    Err(mpsc::error::TryRecvError::Empty) => break,
                                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                                }
                            }
                            // println!("Total events: {total_events}");
                            // Execute a single batched append operation.
                            let batch_result = db.append_batch(items);
                            match batch_result {
                                Ok(results) => {
                                    // Send individual results back to requesters
                                    // Also compute the new head as the maximum successful last position in this batch
                                    let mut max_ok: Option<u64> = None;
                                    for (res, tx) in results.into_iter().zip(responders.into_iter())
                                    {
                                        if let Ok(v) = &res {
                                            max_ok = Some(max_ok.map_or(*v, |m| m.max(*v)));
                                        }
                                        let _ = tx.send(res);
                                    }
                                    // After a successful batch commit, publish the updated head.
                                    if let Some(h) = max_ok {
                                        let _ = head_tx_writer.send(Some(h));
                                    }
                                }
                                Err(e) => {
                                    // If the batch failed as a whole (e.g., commit failed), propagate the SAME error to all responders.
                                    // DCBError is not Clone (contains io::Error), so reconstruct a best-effort copy by using its Display text
                                    // for Io and cloning data for other variants.
                                    fn clone_dcb_error(src: &DCBError) -> DCBError {
                                        match src {
                                            DCBError::Io(err) => {
                                                DCBError::Io(std::io::Error::other(err.to_string()))
                                            }
                                            DCBError::IntegrityError(s) => {
                                                DCBError::IntegrityError(s.clone())
                                            }
                                            DCBError::Corruption(s) => {
                                                DCBError::Corruption(s.clone())
                                            }
                                            DCBError::PageNotFound(id) => {
                                                DCBError::PageNotFound(*id)
                                            }
                                            DCBError::DirtyPageNotFound(id) => {
                                                DCBError::DirtyPageNotFound(*id)
                                            }
                                            DCBError::RootIDMismatch(old_id, new_id) => {
                                                DCBError::RootIDMismatch(*old_id, *new_id)
                                            }
                                            DCBError::DatabaseCorrupted(s) => {
                                                DCBError::DatabaseCorrupted(s.clone())
                                            }
                                            DCBError::InternalError(s) => {
                                                DCBError::InternalError(s.clone())
                                            }
                                            DCBError::SerializationError(s) => {
                                                DCBError::SerializationError(s.clone())
                                            }
                                            DCBError::DeserializationError(s) => {
                                                DCBError::DeserializationError(s.clone())
                                            }
                                            DCBError::PageAlreadyFreed(id) => {
                                                DCBError::PageAlreadyFreed(*id)
                                            }
                                            DCBError::PageAlreadyDirty(id) => {
                                                DCBError::PageAlreadyDirty(*id)
                                            }
                                            DCBError::TransportError(err) => {
                                                DCBError::TransportError(err.clone())
                                            }
                                        }
                                    }
                                    let total = responders.len();
                                    let mut iter = responders.into_iter();
                                    for _ in 0..total {
                                        if let Some(tx) = iter.next() {
                                            let _ = tx.send(Err(clone_dcb_error(&e)));
                                        }
                                    }
                                }
                            }
                        }
                        WriterRequest::Shutdown => {
                            break;
                        }
                    }
                }
            });
        });

        Ok(Self {
            mvcc,
            head_watch_tx: head_tx,
            writer_request_tx: request_tx,
        })
    }

    async fn read(
        &self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let reader = self.mvcc.reader()?;
        let last_committed_position = reader.next_position.0.saturating_sub(1);

        let q = query.unwrap_or(DCBQuery { items: vec![] });
        let from = start.map(Position);
        
        let events = read_conditional(
            &self.mvcc,
            &std::collections::HashMap::new(),
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            q,
            from,
            backwards,
            limit,
        )
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

    async fn head(&self) -> DCBResult<Option<u64>> {
        let (_, header) = self.mvcc
            .get_latest_header()
            .map_err(|e| DCBError::Corruption(format!("{e}")))?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
    }
    pub async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64> {
        // Concurrent pre-check of the given condition using a reader in a blocking thread.
        let pre_append_decision = if let Some(condition_binding) = condition {
            let mvcc = self.mvcc.clone();
            let events_clone = events.clone(); // move-safe copy for the blocking thread

            tokio::task::spawn_blocking(move || -> DCBResult<PreAppendDecision> {
                let mut given_condition = condition_binding;
                let reader = mvcc.reader()?;
                let current_head = {
                    let last = reader.next_position.0.saturating_sub(1);
                    if last == 0 { None } else { Some(last) }
                };

                // Perform conditional read on the snapshot (limit 1) starting after the given position
                let from = given_condition.after.map(|after| Position(after + 1));
                let empty_dirty = std::collections::HashMap::new();
                let found = read_conditional(
                    &mvcc,
                    &empty_dirty,
                    reader.events_tree_root_id,
                    reader.tags_tree_root_id,
                    given_condition.fail_if_events_match.clone(),
                    from,
                    false,
                    Some(1),
                )?;

                if let Some(matched) = found.first() {
                    // Found one event — consider if the request is idempotent...
                    return match is_request_idempotent(
                        &mvcc,
                        &empty_dirty,
                        reader.events_tree_root_id,
                        reader.tags_tree_root_id,
                        &events_clone,
                        given_condition.fail_if_events_match.clone(),
                        from,
                    ) {
                        Ok(Some(last_recorded_position)) => {
                            // Request is idempotent; skip actual append
                            Ok(PreAppendDecision::AlreadyAppended(last_recorded_position))
                        }
                        Ok(None) => {
                            // Integrity violation
                            let msg = format!(
                                "condition: {:?} matched: {:?}",
                                given_condition.clone(),
                                matched,
                            );
                            Err(DCBError::IntegrityError(msg))
                        }
                        Err(err) => {
                            // Propagate underlying read error
                            Err(err)
                        }
                    };
                }

                // No match found: we can advance 'after' to the current head observed by this reader
                let new_after = std::cmp::max(
                    given_condition.after.unwrap_or(0),
                    current_head.unwrap_or(0),
                );
                given_condition.after = Some(new_after);

                Ok(PreAppendDecision::UseCondition(Some(given_condition)))
            })
            .await
            .map_err(|e| DCBError::Io(std::io::Error::other(format!("Join error: {e}"))))??
        } else {
            // No condition provided at all
            PreAppendDecision::UseCondition(None)
        };

        // Handle the pre-check decision
        match pre_append_decision {
            PreAppendDecision::AlreadyAppended(last_found_position) => {
                // ✅ Request was idempotent — just return the existing position.
                Ok(last_found_position)
            }
            PreAppendDecision::UseCondition(adjusted_condition) => {
                // ✅ Proceed with append operation on the writer thread.
                let (response_tx, response_rx) = oneshot::channel();

                self.writer_request_tx
                    .send(WriterRequest::Append {
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
        }
    }

    fn watch_head(&self) -> watch::Receiver<Option<u64>> {
        self.head_watch_tx.subscribe()
    }

    #[allow(dead_code)]
    async fn shutdown(&self) {
        let _ = self.writer_request_tx.send(WriterRequest::Shutdown).await;
    }
}

// Clone implementation for EventStoreHandle
impl Clone for RequestHandler {
    fn clone(&self) -> Self {
        Self {
            mvcc: self.mvcc.clone(),
            head_watch_tx: self.head_watch_tx.clone(),
            writer_request_tx: self.writer_request_tx.clone(),
        }
    }
}

#[derive(Debug)]
enum PreAppendDecision {
    /// Proceed with this (possibly adjusted) condition
    UseCondition(Option<DCBAppendCondition>),
    /// Skip append operation because the request was idempotent; return last recorded position
    AlreadyAppended(u64),
}
