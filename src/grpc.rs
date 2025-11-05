use async_trait::async_trait;
use futures::Stream;
use futures::ready;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fs, thread};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig,
};
use tonic::{Code, Request, Response, Status, transport::Server};

use crate::db::{DEFAULT_PAGE_SIZE, UmaDB, is_request_idempotent, read_conditional};
use crate::dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreAsync, DCBEventStoreSync, DCBQuery,
    DCBQueryItem, DCBReadResponseAsync, DCBReadResponseSync, DCBResult, DCBSequencedEvent,
};
use crate::mvcc::Mvcc;

// Include the generated proto code
pub mod umadb {
    tonic::include_proto!("umadb");
}

use crate::common::Position;
use prost::Message;
use prost::bytes::Bytes;
use tokio::runtime::{Handle, Runtime};
use umadb::{
    AppendConditionProto, AppendRequestProto, AppendResponseProto, ErrorResponseProto, EventProto,
    HeadRequestProto, HeadResponseProto, QueryItemProto, QueryProto, ReadRequestProto,
    ReadResponseProto, SequencedEventProto,
    uma_db_service_server::{UmaDbService, UmaDbServiceServer},
};
use uuid::Uuid;

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

    server_builder
        .add_service(server.into_service())
        .serve_with_shutdown(addr, async move {
            // Wait for an external shutdown trigger
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
        let mut query: Option<Arc<DCBQuery>> = read_request.query.map(|q| q.into());
        let start = read_request.start;
        let backwards = read_request.backwards.unwrap_or(false);
        let limit = read_request.limit;
        // Cap requested batch size.
        let capped_batch_size = read_request
            .batch_size
            .unwrap_or(READ_RESPONSE_BATCH_SIZE_DEFAULT)
            .max(1)
            .min(READ_RESPONSE_BATCH_SIZE_MAX);
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
            p.join("dcb.db")
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
        query: Option<Arc<DCBQuery>>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        // Offload blocking read to a dedicated threadpool
        let db = self.mvcc.clone();
        tokio::task::spawn_blocking(
            move || -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
                let reader = db.reader()?;
                let last_committed_position = reader.next_position.0.saturating_sub(1);

                let q = query.unwrap_or(Arc::new(DCBQuery { items: vec![] }));
                let from = start.map(|after| Position(after));
                let events = read_conditional(
                    &db,
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
            },
        )
        .await
        .map_err(|e| DCBError::Io(std::io::Error::other(format!("Join error: {e}"))))?
    }

    async fn head(&self) -> DCBResult<Option<u64>> {
        // Offload blocking head read to a dedicated threadpool
        let mvcc = self.mvcc.clone();
        tokio::task::spawn_blocking(move || -> DCBResult<Option<u64>> {
            let (_, header) = mvcc
                .get_latest_header()
                .map_err(|e| DCBError::Corruption(format!("{e}")))?;
            let last = header.next_position.0.saturating_sub(1);
            if last == 0 { Ok(None) } else { Ok(Some(last)) }
        })
        .await
        .map_err(|e| DCBError::Io(std::io::Error::other(format!("Join error: {e}"))))?
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

// Async client implementation
pub struct AsyncUmaDBClient {
    client: umadb::uma_db_service_client::UmaDbServiceClient<tonic::transport::Channel>,
}

#[derive(Clone, Debug, Default)]
pub struct ClientTlsOptions {
    pub domain: Option<String>,
    pub ca_pem: Option<Vec<u8>>, // trusted CA cert in PEM for self-signed setups
}

fn endpoint_from_url_with_options(
    url: &str,
    tls: Option<ClientTlsOptions>,
) -> Result<Endpoint, tonic::transport::Error> {
    use std::time::Duration;

    // Accept grpcs:// as an alias for https://
    let mut url_owned = url.to_string();
    if url_owned.starts_with("grpcs://") {
        url_owned = url_owned.replacen("grpcs://", "https://", 1);
    }

    let mut endpoint = Endpoint::from_shared(url_owned)?
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(5))
        .keep_alive_timeout(Duration::from_secs(10))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .initial_connection_window_size(Some(8 * 1024 * 1024));

    if let Some(opts) = tls {
        let mut cfg = ClientTlsConfig::new();
        if let Some(domain) = &opts.domain {
            cfg = cfg.domain_name(domain.clone());
        }
        if let Some(ca) = opts.ca_pem {
            cfg = cfg.ca_certificate(Certificate::from_pem(ca));
        }
        endpoint = endpoint.tls_config(cfg)?;
    } else if url.starts_with("https://") {
        // When using https without explicit options, still enable default TLS.
        endpoint = endpoint.tls_config(ClientTlsConfig::new())?;
    }

    Ok(endpoint)
}

impl AsyncUmaDBClient {
    pub async fn connect(url: &str, ca_path: Option<&str>) -> DCBResult<Self> {
        // Try to read the CA certificate.
        let ca_pem = {
            if let Some(ca_path) = ca_path {
                let ca_path = PathBuf::from(ca_path);
                Some(fs::read(&ca_path).expect(&format!("Couldn't read cert_path: {:?}", ca_path)))
            } else {
                None
            }
        };

        let client_tls_options = Some(ClientTlsOptions {
            domain: None,
            ca_pem,
        });

        Self::connect_with_options(url, client_tls_options).await
    }

    pub async fn connect_with_options(
        url: &str,
        client_tls_options: Option<ClientTlsOptions>,
    ) -> DCBResult<AsyncUmaDBClient> {
        match new_channel(url, client_tls_options).await {
            Ok(channel) => Ok(Self {
                client: umadb::uma_db_service_client::UmaDbServiceClient::new(channel),
            }),
            Err(err) => Err(DCBError::TransportError(format!(
                "failed to connect: {:?}",
                err
            ))),
        }
    }
}

async fn new_channel(
    url: &str,
    tls: Option<ClientTlsOptions>,
) -> Result<Channel, tonic::transport::Error> {
    let endpoint = endpoint_from_url_with_options(url, tls)?;
    endpoint.connect().await
}

#[async_trait]
impl DCBEventStoreAsync for AsyncUmaDBClient {
    // Async inherent methods: use the gRPC client directly (no trait required)
    async fn read<'a>(
        &'a self,
        query: Option<Arc<DCBQuery>>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
        batch_size: Option<u32>,
    ) -> DCBResult<Box<dyn DCBReadResponseAsync + Send>> {
        // Convert API types to protobuf types
        let query_proto = query.map(|q| QueryProto {
            items: <Vec<DCBQueryItem> as Clone>::clone(&q.items)
                .into_iter()
                .map(|item| QueryItemProto {
                    types: item.types,
                    tags: item.tags,
                })
                .collect(),
        });
        let request = ReadRequestProto {
            query: query_proto,
            start,
            backwards: Some(backwards),
            limit,
            subscribe: Some(subscribe),
            batch_size,
        };

        let mut client = self.client.clone();
        let response = client.read(request).await.map_err(dcb_error_from_status)?;
        let stream = response.into_inner();

        Ok(Box::new(AsyncReadResponse::new(stream)))
    }

    async fn head(&self) -> DCBResult<Option<u64>> {
        let mut client = self.client.clone();
        match client.head(HeadRequestProto {}).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(dcb_error_from_status(status)),
        }
    }

    async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64> {
        let events_proto: Vec<EventProto> = events.into_iter().map(EventProto::from).collect();

        let condition_proto = condition.map(|c| AppendConditionProto {
            fail_if_events_match: Some(QueryProto {
                items: <Vec<DCBQueryItem> as Clone>::clone(&c.fail_if_events_match.items)
                    .into_iter()
                    .map(|item| QueryItemProto {
                        types: item.types,
                        tags: item.tags,
                    })
                    .collect(),
            }),
            after: c.after,
        });

        let request = AppendRequestProto {
            events: events_proto,
            condition: condition_proto,
        };
        let mut client = self.client.clone();
        match client.append(request).await {
            Ok(response) => Ok(response.into_inner().position),
            Err(status) => Err(dcb_error_from_status(status)),
        }
    }
}

/// Async read response wrapper that provides batched access and head metadata
pub struct AsyncReadResponse {
    stream: tonic::Streaming<ReadResponseProto>,
    buffered: Vec<DCBSequencedEvent>,
    buf_idx: usize,
    last_head: Option<Option<u64>>, // None = unknown yet; Some(x) = known
    ended: bool,
}

impl AsyncReadResponse {
    pub fn new(stream: tonic::Streaming<ReadResponseProto>) -> Self {
        Self {
            stream,
            buffered: Vec::new(),
            buf_idx: 0,
            last_head: None,
            ended: false,
        }
    }

    /// Fetches the next batch if needed, filling the buffer
    async fn fetch_next_if_needed(&mut self) -> DCBResult<()> {
        if self.buf_idx < self.buffered.len() || self.ended {
            return Ok(());
        }

        match self.stream.message().await {
            Ok(Some(resp)) => {
                self.last_head = Some(resp.head);

                let mut buffered = Vec::with_capacity(resp.events.len());
                for e in resp.events {
                    if let Some(ev) = e.event {
                        let event = DCBEvent::try_from(ev)?; // propagate error
                        buffered.push(DCBSequencedEvent {
                            position: e.position,
                            event,
                        });
                    }
                }

                self.buffered = buffered;
                self.buf_idx = 0;
                Ok(())
            }
            Ok(None) => {
                self.ended = true;
                Ok(())
            }
            Err(status) => Err(dcb_error_from_status(status)),
        }
    }
}

#[async_trait]
impl DCBReadResponseAsync for AsyncReadResponse {
    async fn head(&mut self) -> DCBResult<Option<u64>> {
        if let Some(h) = self.last_head {
            return Ok(h);
        }
        // Need to read at least one message to learn head
        self.fetch_next_if_needed().await?;
        Ok(self.last_head.unwrap_or(None))
    }

    async fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        if self.buf_idx < self.buffered.len() {
            let out = self.buffered[self.buf_idx..].to_vec();
            self.buf_idx = self.buffered.len();
            return Ok(out);
        }

        self.fetch_next_if_needed().await?;

        if self.buf_idx < self.buffered.len() {
            let out = self.buffered[self.buf_idx..].to_vec();
            self.buf_idx = self.buffered.len();
            return Ok(out);
        }

        Ok(Vec::new())
    }
}

impl Stream for AsyncReadResponse {
    type Item = DCBResult<DCBSequencedEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Return buffered event if available
            if this.buf_idx < this.buffered.len() {
                let ev = this.buffered[this.buf_idx].clone();
                this.buf_idx += 1;
                return Poll::Ready(Some(Ok(ev)));
            }

            // Stop if the stream ended.
            if this.ended {
                return Poll::Ready(None);
            }

            // Poll the underlying tonic::Streaming
            return match ready!(Pin::new(&mut this.stream).poll_next(cx)) {
                Some(Ok(resp)) => {
                    this.last_head = Some(resp.head);

                    let mut buffered = Vec::with_capacity(resp.events.len());
                    for e in resp.events {
                        if let Some(ev) = e.event {
                            // Propagate any conversion error using DCBResult.
                            let event = match DCBEvent::try_from(ev) {
                                Ok(event) => event,
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            };
                            buffered.push(DCBSequencedEvent {
                                position: e.position,
                                event,
                            });
                        }
                    }

                    this.buffered = buffered;
                    this.buf_idx = 0;

                    // If the batch is empty, loop again to poll the next message
                    if this.buffered.is_empty() {
                        continue;
                    }

                    // Otherwise, return the first event
                    let ev = this.buffered[this.buf_idx].clone();
                    this.buf_idx += 1;
                    Poll::Ready(Some(Ok(ev)))
                }
                Some(Err(status)) => {
                    this.ended = true;
                    Poll::Ready(Some(Err(dcb_error_from_status(status))))
                }
                None => {
                    this.ended = true;
                    Poll::Ready(None)
                }
            };
        }
    }
}

// Conversion functions between proto and API types
impl TryFrom<EventProto> for DCBEvent {
    type Error = DCBError;

    fn try_from(proto: EventProto) -> DCBResult<Self> {
        let uuid = if proto.uuid.is_empty() {
            None
        } else {
            match Uuid::parse_str(&proto.uuid) {
                Ok(uuid) => Some(uuid),
                Err(_) => {
                    return Err(DCBError::DeserializationError(
                        "Invalid UUID in EventProto".to_string(),
                    ));
                }
            }
        };

        Ok(DCBEvent {
            event_type: proto.event_type.clone(),
            tags: proto.tags.clone(),
            data: proto.data.clone(),
            uuid,
        })
    }
}

impl From<DCBEvent> for EventProto {
    fn from(event: DCBEvent) -> Self {
        EventProto {
            event_type: event.event_type,
            tags: event.tags,
            data: event.data,
            uuid: event.uuid.map(|u| u.to_string()).unwrap_or_default(),
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

impl From<QueryProto> for Arc<DCBQuery> {
    fn from(proto: QueryProto) -> Self {
        Arc::new(DCBQuery {
            items: proto.items.into_iter().map(|item| item.into()).collect(),
        })
    }
}

impl From<AppendConditionProto> for DCBAppendCondition {
    fn from(proto: AppendConditionProto) -> Self {
        DCBAppendCondition {
            fail_if_events_match: Arc::new(
                proto
                    .fail_if_events_match
                    .map_or_else(DCBQuery::default, |q| q.into()),
            ),
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
        DCBError::IntegrityError(_) => (
            Code::FailedPrecondition,
            umadb::error_response_proto::ErrorType::Integrity as i32,
        ),
        DCBError::Corruption(_)
        | DCBError::DatabaseCorrupted(_)
        | DCBError::DeserializationError(_) => (
            Code::DataLoss,
            umadb::error_response_proto::ErrorType::Corruption as i32,
        ),
        DCBError::SerializationError(_) => (
            Code::InvalidArgument,
            umadb::error_response_proto::ErrorType::Serialization as i32,
        ),
        DCBError::InternalError(_) => (
            Code::Internal,
            umadb::error_response_proto::ErrorType::Internal as i32,
        ),
        _ => (
            Code::Internal,
            umadb::error_response_proto::ErrorType::Io as i32,
        ),
    };
    let msg = e.to_string();
    let detail = ErrorResponseProto {
        message: msg.clone(),
        error_type,
    };
    let bytes = detail.encode_to_vec();
    Status::with_details(code, msg, Bytes::from(bytes))
}

// Helper: map tonic::Status -> DCBError by decoding details
fn dcb_error_from_status(status: Status) -> DCBError {
    let details = status.details();
    // Try to decode ErrorResponseProto directly from details
    if !details.is_empty()
        && let Ok(err) = ErrorResponseProto::decode(details)
    {
        return match err.error_type {
            x if x == umadb::error_response_proto::ErrorType::Integrity as i32 => {
                DCBError::IntegrityError(err.message)
            }
            x if x == umadb::error_response_proto::ErrorType::Corruption as i32 => {
                DCBError::Corruption(err.message)
            }
            x if x == umadb::error_response_proto::ErrorType::Serialization as i32 => {
                DCBError::SerializationError(err.message)
            }
            x if x == umadb::error_response_proto::ErrorType::Internal as i32 => {
                DCBError::InternalError(err.message)
            }
            _ => DCBError::Io(std::io::Error::other(err.message)),
        };
    }
    // Fallback: infer from gRPC code
    match status.code() {
        Code::FailedPrecondition => DCBError::IntegrityError(status.message().to_string()),
        Code::DataLoss => DCBError::Corruption(status.message().to_string()),
        Code::InvalidArgument => DCBError::SerializationError(status.message().to_string()),
        Code::Internal => DCBError::InternalError(status.message().to_string()),
        _ => DCBError::Io(std::io::Error::other(format!("gRPC error: {}", status))),
    }
}

// --- Sync wrapper around the async client ---
pub struct UmaDBClient {
    async_client: AsyncUmaDBClient,
    _runtime: Option<Runtime>, // Keeps runtime alive if we created it
    handle: Handle,
}

impl UmaDBClient {
    pub fn connect(url: &str, ca_path: Option<&str>) -> DCBResult<Self> {
        // Try to use an existing runtime first
        if let Ok(handle) = Handle::try_current() {
            let async_client = handle.block_on(AsyncUmaDBClient::connect(url, ca_path))?;
            return Ok(Self {
                async_client,
                _runtime: None, // We didn’t create a runtime
                handle,
            });
        }

        // No runtime → create and own one
        let rt = Runtime::new().expect("failed to create Tokio runtime");
        let handle = rt.handle().clone();
        let async_client = rt.block_on(AsyncUmaDBClient::connect(url, ca_path))?;
        Ok(Self {
            async_client,
            _runtime: Some(rt), // Keep runtime alive for the client lifetime
            handle,
        })
    }
}

impl DCBEventStoreSync for UmaDBClient {
    fn read(
        &self,
        query: Option<Arc<DCBQuery>>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
        batch_size: Option<u32>,
    ) -> Result<Box<dyn DCBReadResponseSync + '_>, DCBError> {
        let async_read_response = self.handle.block_on(
            self.async_client
                .read(query, start, backwards, limit, subscribe, batch_size),
        )?;
        Ok(Box::new(SyncClientReadResponse {
            rt: &self.handle,
            resp: async_read_response,
            buffer: VecDeque::new(),
            finished: false,
        }))
    }

    fn head(&self) -> Result<Option<u64>, DCBError> {
        self.handle.block_on(self.async_client.head())
    }

    fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> Result<u64, DCBError> {
        self.handle
            .block_on(self.async_client.append(events, condition))
    }
}

pub struct SyncClientReadResponse<'a> {
    rt: &'a Handle,
    resp: Box<dyn DCBReadResponseAsync + Send + 'a>,
    buffer: VecDeque<DCBSequencedEvent>, // efficient pop_front()
    finished: bool,
}

impl<'a> SyncClientReadResponse<'a> {
    /// Fetch the next batch from the async response, filling the buffer
    fn fetch_next_batch(&mut self) -> Result<(), DCBError> {
        if self.finished {
            return Ok(());
        }

        let batch = self.rt.block_on(self.resp.next_batch())?;
        if batch.is_empty() {
            self.finished = true;
        } else {
            self.buffer = batch.into();
        }
        Ok(())
    }
}

impl<'a> Iterator for SyncClientReadResponse<'a> {
    type Item = Result<DCBSequencedEvent, DCBError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Fetch the next batch if the buffer is empty.
        while self.buffer.is_empty() && !self.finished {
            if let Err(e) = self.fetch_next_batch() {
                return Some(Err(e));
            }
        }

        self.buffer.pop_front().map(Ok)
    }
}

impl<'a> DCBReadResponseSync for SyncClientReadResponse<'a> {
    fn head(&mut self) -> DCBResult<Option<u64>> {
        self.rt.block_on(self.resp.head())
    }

    fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut out = Vec::new();
        for result in self.by_ref() {
            out.push(result?);
        }
        Ok((out, self.head()?))
    }

    fn next_batch(&mut self) -> Result<Vec<DCBSequencedEvent>, DCBError> {
        // If there are remaining events in the buffer, drain them
        if !self.buffer.is_empty() {
            return Ok(self.buffer.drain(..).collect());
        }

        // Otherwise fetch a new batch
        self.fetch_next_batch()?;
        Ok(self.buffer.drain(..).collect())
    }
}
