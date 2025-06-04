use std::path::Path;
use std::sync::Arc;

use tonic::{Request, Response, Status};
use async_trait::async_trait;

use crate::{Event, EventStore, EventStoreError, Query, QueryItem, AppendCondition, SequencedEvent};

// Import the generated proto code
pub mod proto {
    tonic::include_proto!("dcbdb");
}

use proto::{
    event_store_service_server::{EventStoreService, EventStoreServiceServer},
    EventProto, SequencedEventProto, QueryItemProto, QueryProto, AppendConditionProto,
    ReadRequestProto, ReadResponseProto, AppendRequestProto, AppendResponseProto,
    // error_response_proto::ErrorType,
};

// Conversion functions between domain types and proto types
impl From<Event> for EventProto {
    fn from(event: Event) -> Self {
        EventProto {
            event_type: event.event_type,
            tags: event.tags,
            data: event.data,
        }
    }
}

impl From<EventProto> for Event {
    fn from(proto: EventProto) -> Self {
        Event {
            event_type: proto.event_type,
            tags: proto.tags,
            data: proto.data,
        }
    }
}

impl From<SequencedEvent> for SequencedEventProto {
    fn from(event: SequencedEvent) -> Self {
        SequencedEventProto {
            position: event.position,
            event: Some(event.event.into()),
        }
    }
}

impl From<QueryItem> for QueryItemProto {
    fn from(item: QueryItem) -> Self {
        QueryItemProto {
            types: item.types,
            tags: item.tags,
        }
    }
}

impl From<QueryItemProto> for QueryItem {
    fn from(proto: QueryItemProto) -> Self {
        QueryItem {
            types: proto.types,
            tags: proto.tags,
        }
    }
}

impl From<Query> for QueryProto {
    fn from(query: Query) -> Self {
        QueryProto {
            items: query.items.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<QueryProto> for Query {
    fn from(proto: QueryProto) -> Self {
        Query {
            items: proto.items.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<AppendCondition> for AppendConditionProto {
    fn from(condition: AppendCondition) -> Self {
        AppendConditionProto {
            fail_if_events_match: Some(condition.fail_if_events_match.into()),
            after: condition.after,
        }
    }
}

impl From<AppendConditionProto> for AppendCondition {
    fn from(proto: AppendConditionProto) -> Self {
        AppendCondition {
            fail_if_events_match: proto.fail_if_events_match.map(Into::into).unwrap_or_default(),
            after: proto.after,
        }
    }
}

impl From<EventStoreError> for Status {
    fn from(error: EventStoreError) -> Self {
        match error {
            EventStoreError::Io(e) => Status::internal(format!("IO error: {}", e)),
            EventStoreError::Serialization(e) => Status::internal(format!("Serialization error: {}", e)),
            EventStoreError::IntegrityError => Status::failed_precondition("Integrity error: condition failed"),
            EventStoreError::Corruption(msg) => Status::data_loss(format!("Corruption detected: {}", msg)),
        }
    }
}

// EventStore gRPC service implementation
pub struct EventStoreServer {
    event_store: Arc<EventStore>,
}

impl EventStoreServer {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, EventStoreError> {
        let event_store = EventStore::open(path)?;
        Ok(Self {
            event_store: Arc::new(event_store),
        })
    }

    pub fn with_event_store(event_store: Arc<EventStore>) -> Self {
        Self { event_store }
    }

    pub fn into_service(self) -> EventStoreServiceServer<Self> {
        EventStoreServiceServer::new(self)
    }
}

#[async_trait]
impl EventStoreService for EventStoreServer {
    async fn read(
        &self,
        request: Request<ReadRequestProto>,
    ) -> Result<Response<ReadResponseProto>, Status> {
        let req = request.into_inner();

        let query = req.query.map(Into::into);
        let after = req.after;
        let limit = req.limit.map(|l| l as usize);

        let (events, head) = self.event_store.read(query, after, limit)
            .map_err(Status::from)?;

        let events_proto: Vec<SequencedEventProto> = events.into_iter()
            .map(Into::into)
            .collect();

        let response = ReadResponseProto {
            events: events_proto,
            head,
        };

        Ok(Response::new(response))
    }

    async fn append(
        &self,
        request: Request<AppendRequestProto>,
    ) -> Result<Response<AppendResponseProto>, Status> {
        let req = request.into_inner();

        let events: Vec<Event> = req.events.into_iter()
            .map(Into::into)
            .collect();

        let condition = req.condition.map(Into::into);

        let position = self.event_store.append(events, condition)
            .map_err(Status::from)?;

        let response = AppendResponseProto {
            position,
        };

        Ok(Response::new(response))
    }
}
