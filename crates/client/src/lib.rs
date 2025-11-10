use async_trait::async_trait;
use futures::Stream;
use futures::ready;
use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use tokio::runtime::{Handle, Runtime};
use umadb_dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStoreAsync, DCBEventStoreSync, DCBQuery,
    DCBReadResponseAsync, DCBReadResponseSync, DCBResult, DCBSequencedEvent,
};
use umadb_proto::dcb_error_from_status;
use umadb_proto::umadb::uma_db_service_client;
use umadb_proto::umadb::{
    AppendConditionProto, AppendRequestProto, EventProto, HeadRequestProto, ReadRequestProto,
    ReadResponseProto,
};

pub struct UmaDBClient {
    url: String,
    ca_path: Option<String>,
    batch_size: Option<u32>,
}

impl UmaDBClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            ca_path: None,
            batch_size: None,
        }
    }

    pub fn ca_path(self, ca_path: String) -> Self {
        Self {
            ca_path: Some(ca_path),
            ..self
        }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self {
            batch_size: Some(batch_size),
            ..self
        }
    }

    pub fn connect(&self) -> DCBResult<SyncUmaDBClient> {
        SyncUmaDBClient::connect(self.url.clone(), self.ca_path.clone(), self.batch_size)
    }
    pub async fn connect_async(&self) -> DCBResult<AsyncUmaDBClient> {
        AsyncUmaDBClient::connect(self.url.clone(), self.ca_path.clone(), self.batch_size).await
    }
}

// --- Sync wrapper around the async client ---
pub struct SyncUmaDBClient {
    async_client: AsyncUmaDBClient,
    handle: Handle,
    _runtime: Option<Runtime>, // Keeps runtime alive if we created it
}

impl SyncUmaDBClient {
    pub fn connect(
        url: String,
        ca_path: Option<String>,
        batch_size: Option<u32>,
    ) -> DCBResult<Self> {
        let (rt, handle) = Self::get_rt_handle();
        let async_client = handle.block_on(AsyncUmaDBClient::connect(url, ca_path, batch_size))?;
        Ok(Self {
            async_client,
            _runtime: rt, // Keep runtime alive for the client lifetime
            handle,
        })
    }

    pub fn connect_with_tls_options(
        url: String,
        tls_options: Option<ClientTlsOptions>,
        batch_size: Option<u32>,
    ) -> DCBResult<Self> {
        let (rt, handle) = Self::get_rt_handle();
        let async_client = handle.block_on(AsyncUmaDBClient::connect_with_tls_options(
            url,
            tls_options,
            batch_size,
        ))?;
        Ok(Self {
            async_client,
            _runtime: rt, // Keep runtime alive for the client lifetime
            handle,
        })
    }

    fn get_rt_handle() -> (Option<Runtime>, Handle) {
        let (rt, handle) = {
            // Try to use an existing runtime first
            if let Ok(handle) = Handle::try_current() {
                (None, handle)
            } else {
                // No runtime → create and own one
                let rt = Runtime::new().expect("failed to create Tokio runtime");
                let handle = rt.handle().clone();
                (Some(rt), handle)
            }
        };
        (rt, handle)
    }
}

impl DCBEventStoreSync for SyncUmaDBClient {
    fn read(
        &self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> Result<Box<dyn DCBReadResponseSync + '_>, DCBError> {
        let async_read_response = self.handle.block_on(
            self.async_client
                .read(query, start, backwards, limit, subscribe),
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

// Async client implementation
pub struct AsyncUmaDBClient {
    client: uma_db_service_client::UmaDbServiceClient<Channel>,
    batch_size: Option<u32>,
}

impl AsyncUmaDBClient {
    pub async fn connect(
        url: String,
        ca_path: Option<String>,
        batch_size: Option<u32>,
    ) -> DCBResult<Self> {
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

        Self::connect_with_tls_options(url, client_tls_options, batch_size).await
    }

    pub async fn connect_with_tls_options(
        url: String,
        tls_options: Option<ClientTlsOptions>,
        batch_size: Option<u32>,
    ) -> DCBResult<Self> {
        match new_channel(url, tls_options).await {
            Ok(channel) => Ok(Self {
                client: uma_db_service_client::UmaDbServiceClient::new(channel),
                batch_size,
            }),
            Err(err) => Err(DCBError::TransportError(format!(
                "failed to connect: {:?}",
                err
            ))),
        }
    }
}

#[async_trait]
impl DCBEventStoreAsync for AsyncUmaDBClient {
    // Async inherent methods: use the gRPC client directly (no trait required)
    async fn read<'a>(
        &'a self,
        query: Option<DCBQuery>,
        start: Option<u64>,
        backwards: bool,
        limit: Option<u32>,
        subscribe: bool,
    ) -> DCBResult<Box<dyn DCBReadResponseAsync + Send>> {
        // Convert API types to protobuf types
        let query_proto = query.map(|q| q.into());
        let request = ReadRequestProto {
            query: query_proto,
            start,
            backwards: Some(backwards),
            limit,
            subscribe: Some(subscribe),
            batch_size: self.batch_size,
        };

        let mut client = self.client.clone();
        let response = client.read(request).await.map_err(dcb_error_from_status)?;
        let stream = response.into_inner();

        Ok(Box::new(AsyncClientReadResponse::new(stream)))
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
            fail_if_events_match: Some(c.fail_if_events_match.into()),
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
pub struct AsyncClientReadResponse {
    stream: tonic::Streaming<ReadResponseProto>,
    buffered: VecDeque<DCBSequencedEvent>,
    last_head: Option<Option<u64>>, // None = unknown yet; Some(x) = known
    ended: bool,
}

impl AsyncClientReadResponse {
    pub fn new(stream: tonic::Streaming<ReadResponseProto>) -> Self {
        Self {
            stream,
            buffered: VecDeque::new(),
            last_head: None,
            ended: false,
        }
    }

    /// Fetches the next batch if needed, filling the buffer
    async fn fetch_next_if_needed(&mut self) -> DCBResult<()> {
        if !self.buffered.is_empty() || self.ended {
            return Ok(());
        }

        match self.stream.message().await {
            Ok(Some(resp)) => {
                self.last_head = Some(resp.head);

                let mut buffered = VecDeque::with_capacity(resp.events.len());
                for e in resp.events {
                    if let Some(ev) = e.event {
                        let event = DCBEvent::try_from(ev)?; // propagate error
                        buffered.push_back(DCBSequencedEvent {
                            position: e.position,
                            event,
                        });
                    }
                }

                self.buffered = buffered;
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
impl DCBReadResponseAsync for AsyncClientReadResponse {
    async fn head(&mut self) -> DCBResult<Option<u64>> {
        if let Some(h) = self.last_head {
            return Ok(h);
        }
        // Need to read at least one message to learn head
        self.fetch_next_if_needed().await?;
        Ok(self.last_head.unwrap_or(None))
    }

    async fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }

        self.fetch_next_if_needed().await?;

        if !self.buffered.is_empty() {
            return Ok(self.buffered.drain(..).collect());
        }

        Ok(Vec::new())
    }
}

impl Stream for AsyncClientReadResponse {
    type Item = DCBResult<DCBSequencedEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Return buffered event if available
            if let Some(ev) = this.buffered.pop_front() {
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

                    let mut buffered = VecDeque::with_capacity(resp.events.len());
                    for e in resp.events {
                        if let Some(ev) = e.event {
                            // Propagate any conversion error using DCBResult.
                            let event = match DCBEvent::try_from(ev) {
                                Ok(event) => event,
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            };
                            buffered.push_back(DCBSequencedEvent {
                                position: e.position,
                                event,
                            });
                        }
                    }

                    this.buffered = buffered;

                    // If the batch is empty, loop again to poll the next message
                    if this.buffered.is_empty() {
                        continue;
                    }

                    // Otherwise, return the first event
                    let ev = this.buffered.pop_front().unwrap();
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

#[derive(Clone, Debug, Default)]
pub struct ClientTlsOptions {
    pub domain: Option<String>,
    pub ca_pem: Option<Vec<u8>>, // trusted CA cert in PEM for self-signed setups
}

async fn new_channel(
    url: String,
    tls: Option<ClientTlsOptions>,
) -> Result<Channel, tonic::transport::Error> {
    new_endpoint(url, tls)?.connect().await
}

fn new_endpoint(
    url: String,
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
