use std::sync::Arc;

use tonic::transport::Channel;
use tonic::Status;

use crate::{Event, EventStoreError, Query, AppendCondition, SequencedEvent, Result};

// Import the generated proto code directly
pub mod proto {
    tonic::include_proto!("dcbdb");
}

use proto::{
    event_store_service_client::EventStoreServiceClient,
    EventProto, QueryProto, QueryItemProto, AppendConditionProto,
    ReadRequestProto, AppendRequestProto,
};

// EventStore gRPC client
pub struct EventStoreClient {
    client: EventStoreServiceClient<Channel>,
}

impl EventStoreClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let client = EventStoreServiceClient::connect(addr.to_string())
            .await
            .map_err(|e| EventStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to connect to gRPC server: {}", e),
            )))?;

        Ok(Self { client })
    }

    // Implement the same API as EventStore
    pub async fn read(
        &self,
        query: Option<Query>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> Result<(Vec<SequencedEvent>, Option<u64>)> {
        let query_proto = query.map(|q| {
            let items = q.items.into_iter()
                .map(|item| {
                    QueryItemProto {
                        types: item.types,
                        tags: item.tags,
                    }
                })
                .collect();

            QueryProto { items }
        });

        let limit_proto = limit.map(|l| l as u32);

        let request = ReadRequestProto {
            query: query_proto,
            after,
            limit: limit_proto,
        };

        let response = self.client.clone().read(request)
            .await
            .map_err(|e| convert_status_to_error(e))?;

        let response = response.into_inner();

        let events = response.events.into_iter()
            .map(|e| {
                let event_proto = e.event.unwrap();
                let event = Event {
                    event_type: event_proto.event_type,
                    tags: event_proto.tags,
                    data: event_proto.data,
                };

                SequencedEvent {
                    position: e.position,
                    event,
                }
            })
            .collect();

        Ok((events, response.head))
    }

    pub async fn append(
        &self,
        events: Vec<Event>,
        condition: Option<AppendCondition>,
    ) -> Result<u64> {
        let events_proto = events.into_iter()
            .map(|e| EventProto {
                event_type: e.event_type,
                tags: e.tags,
                data: e.data,
            })
            .collect();

        let condition_proto = condition.map(|c| {
            let query = c.fail_if_events_match;
            let items = query.items.into_iter()
                .map(|item| {
                    QueryItemProto {
                        types: item.types,
                        tags: item.tags,
                    }
                })
                .collect();

            let query_proto = QueryProto { items };

            AppendConditionProto {
                fail_if_events_match: Some(query_proto),
                after: c.after,
            }
        });

        let request = AppendRequestProto {
            events: events_proto,
            condition: condition_proto,
        };

        let response = self.client.clone().append(request)
            .await
            .map_err(|e| convert_status_to_error(e))?;

        let response = response.into_inner();

        Ok(response.position)
    }
}

// Helper function to convert gRPC status errors to EventStoreError
fn convert_status_to_error(status: Status) -> EventStoreError {
    match status.code() {
        tonic::Code::FailedPrecondition => EventStoreError::IntegrityError,
        tonic::Code::DataLoss => EventStoreError::Corruption(status.message().to_string()),
        _ => EventStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("gRPC error: {}", status),
        )),
    }
}

// Sync wrapper for the async client
pub struct EventStoreClientSync {
    client: Arc<EventStoreClient>,
}

impl EventStoreClientSync {
    pub fn new(client: EventStoreClient) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    // Implement the same API as EventStore but with sync methods
    pub fn read(
        &self,
        query: Option<Query>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> Result<(Vec<SequencedEvent>, Option<u64>)> {
        let client = self.client.clone();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            client.read(query, after, limit).await
        })
    }

    pub fn append(
        &self,
        events: Vec<Event>,
        condition: Option<AppendCondition>,
    ) -> Result<u64> {
        let client = self.client.clone();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            client.append(events, condition).await
        })
    }
}
