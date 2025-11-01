//! API for Dynamic Consistency Boundaries (DCB) event store
//!
//! This module provides the core interfaces and data structures for working with
//! an event store that supports dynamic consistency boundaries.

use crate::common::PageID;
use async_trait::async_trait;
use futures_core::Stream;
use std::iter::Iterator;
use std::sync::Arc;
use thiserror::Error;

/// Non-async Rust interface for recording and retrieving events
pub trait DCBEventStoreSync {
    /// Reads events from the store based on the provided query and constraints
    ///
    /// Returns a DCBReadResponseSync that provides an iterator over all events,
    /// unless 'after' is given then only those with position greater than 'after',
    /// and unless any query items are given, then only those that match at least one
    /// query item. An event matches a query item if its type is in the item types or
    /// there are no item types, and if all the item tags are in the event tags.
    fn read(
        &self,
        query: Option<Arc<DCBQuery>>,
        after: Option<u64>,
        limit: Option<usize>,
        subscribe: bool,
        batch_size: Option<usize>,
    ) -> DCBResult<Box<dyn DCBReadResponseSync + '_>>;

    /// Reads events from the store and returns them as a tuple of (Vec<DCBSequencedEvent>, Option<u64>)
    fn read_with_head(
        &self,
        query: Option<Arc<DCBQuery>>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut response = self.read(query, after, limit, false, None)?;
        response.collect_with_head()
    }

    /// Returns the current head position of the event store, or None if empty
    ///
    /// Returns the value of last_committed_position, or None if last_committed_position is zero
    fn head(&self) -> DCBResult<Option<u64>>;

    /// Appends given events to the event store, unless the condition fails
    ///
    /// Returns the position of the last appended event
    fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64>;
}

/// Response from a read operation, providing an iterator over sequenced events
pub trait DCBReadResponseSync: Iterator<Item = Result<DCBSequencedEvent, DCBError>> {
    /// Returns the current head position of the event store, or None if empty
    fn head(&mut self) -> DCBResult<Option<u64>>;
    /// Returns a vector of events with head
    fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)>;
    /// Returns a batch of events, updating head with the last event in the batch if there is one and if limit.is_some() is true
    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>>;
}

/// Async Rust interface for recording and retrieving events
#[async_trait]
pub trait DCBEventStoreAsync: Send + Sync {
    /// Reads events from the store based on the provided query and constraints
    ///
    /// Returns a DCBReadResponseSync that provides an iterator over all events,
    /// unless 'after' is given then only those with position greater than 'after',
    /// and unless any query items are given, then only those that match at least one
    /// query item. An event matches a query item if its type is in the item types or
    /// there are no item types, and if all the item tags are in the event tags.
    async fn read<'a>(
        &'a self,
        query: Option<Arc<DCBQuery>>,
        after: Option<u64>,
        limit: Option<usize>,
        subscribe: bool,
        batch_size: Option<usize>,
    ) -> DCBResult<Box<dyn DCBReadResponseAsync + Send>>;

    /// Reads events from the store and returns them as a tuple of (Vec<DCBSequencedEvent>, Option<u64>)
    async fn read_with_head<'a>(
        &'a self,
        query: Option<Arc<DCBQuery>>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut response = self.read(query, after, limit, false, None).await?;
        response.collect_with_head().await
    }

    /// Returns the current head position of the event store, or None if empty
    ///
    /// Returns the value of last_committed_position, or None if last_committed_position is zero
    async fn head(&self) -> DCBResult<Option<u64>>;

    /// Appends given events to the event store, unless the condition fails
    ///
    /// Returns the position of the last appended event
    async fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64>;
}

/// Asynchronous response from a read operation, providing a stream of sequenced events
#[async_trait]
pub trait DCBReadResponseAsync: Stream<Item = DCBResult<DCBSequencedEvent>> + Send + Unpin {
    async fn head(&mut self) -> DCBResult<Option<u64>>;

    async fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        use futures_util::StreamExt;

        let mut events = Vec::new();
        while let Some(result) = self.next().await {
            events.push(result?); // propagate error from stream
        }

        let head = self.head().await?;
        Ok((events, head))
    }

    async fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>>;
}

/// Represents a query item for filtering events
#[derive(Debug, Clone, Default)]
pub struct DCBQueryItem {
    /// Event types to match
    pub types: Vec<String>,
    /// Tags that must all be present in the event
    pub tags: Vec<String>,
}

/// A query composed of multiple query items
#[derive(Debug, Clone, Default)]
pub struct DCBQuery {
    /// List of query items, where events matching any item are included in results
    pub items: Vec<DCBQueryItem>,
}

/// Conditions that must be satisfied for an append operation to succeed
#[derive(Debug, Clone, Default)]
pub struct DCBAppendCondition {
    /// Query that, if matching any events, will cause the append to fail
    pub fail_if_events_match: Arc<DCBQuery>,
    /// Position after which to append; if None, append at the end
    pub after: Option<u64>,
}

/// Represents an event in the event store
#[derive(Debug, Clone)]
pub struct DCBEvent {
    /// Type of the event
    pub event_type: String,
    /// Binary data associated with the event
    pub data: Vec<u8>,
    /// Tags associated with the event
    pub tags: Vec<String>,
}

/// An event with its position in the event sequence
#[derive(Debug, Clone)]
pub struct DCBSequencedEvent {
    /// The event
    pub event: DCBEvent,
    /// Position of the event in the sequence
    pub position: u64,
}

// Error types
#[derive(Error, Debug)]
pub enum DCBError {
    // Generic/system errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // DCB domain errors
    #[error("Integrity error: condition failed: {0}")]
    IntegrityError(String),
    #[error("Corruption detected: {0}")]
    Corruption(String),

    // LMDB/Storage domain errors (unified into DCBError)
    #[error("Page not found: {0:?}")]
    PageNotFound(PageID),
    #[error("Dirty page not found: {0:?}")]
    DirtyPageNotFound(PageID),
    #[error("Root ID mismatched: old {0:?} new {1:?}")]
    RootIDMismatch(PageID, PageID),
    #[error("Database corrupted: {0}")]
    DatabaseCorrupted(String),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    #[error("Page already freed: {0:?}")]
    PageAlreadyFreed(PageID),
    #[error("Page already dirty: {0:?}")]
    PageAlreadyDirty(PageID),
}

pub type DCBResult<T> = Result<T, DCBError>;

#[cfg(test)]
mod tests {
    use super::*;

    // A simple implementation of DCBReadResponseSync for testing
    struct TestReadResponse {
        events: Vec<DCBSequencedEvent>,
        current_index: usize,
        head_position: Option<u64>,
    }

    impl TestReadResponse {
        fn new(events: Vec<DCBSequencedEvent>, head_position: Option<u64>) -> Self {
            Self {
                events,
                current_index: 0,
                head_position,
            }
        }
    }

    impl Iterator for TestReadResponse {
        type Item = Result<DCBSequencedEvent, DCBError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current_index < self.events.len() {
                let event = self.events[self.current_index].clone();
                self.current_index += 1;
                Some(Ok(event))
            } else {
                None
            }
        }
    }

    impl DCBReadResponseSync for TestReadResponse {
        fn head(&mut self) -> DCBResult<Option<u64>> {
            Ok(self.head_position)
        }

        fn collect_with_head(&mut self) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
            todo!()
        }

        fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
            let mut batch = Vec::new();
            while let Some(result) = self.next() {
                match result {
                    Ok(event) => batch.push(event),
                    Err(err) => {
                        panic!("{}", err);
                    }
                }
            }
            Ok(batch)
        }
    }

    #[test]
    fn test_dcb_read_response() {
        // Create some test events
        let event1 = DCBEvent {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let event2 = DCBEvent {
            event_type: "another_event".to_string(),
            data: vec![4, 5, 6],
            tags: vec!["tag2".to_string(), "tag3".to_string()],
        };

        let seq_event1 = DCBSequencedEvent {
            event: event1,
            position: 1,
        };

        let seq_event2 = DCBSequencedEvent {
            event: event2,
            position: 2,
        };

        // Create a test response
        let mut response =
            TestReadResponse::new(vec![seq_event1.clone(), seq_event2.clone()], Some(2));

        // Test head position
        assert_eq!(response.head().unwrap(), Some(2));

        // Test iterator functionality
        assert_eq!(response.next().unwrap().unwrap().position, 1);
        assert_eq!(response.next().unwrap().unwrap().position, 2);
        assert!(response.next().is_none());
    }
}
