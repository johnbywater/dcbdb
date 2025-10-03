//! API for Dynamic Consistency Boundaries (DCB) event store
//!
//! This module provides the core interfaces and data structures for working with
//! an event store that supports dynamic consistency boundaries.

use crate::common::PageID;
use std::iter::Iterator;
use thiserror::Error;

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
    pub fail_if_events_match: DCBQuery,
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

/// Response from a read operation, providing an iterator over sequenced events
pub trait DCBReadResponse: Iterator<Item = DCBSequencedEvent> {
    /// Returns the current head position of the event store, or None if empty
    fn head(&self) -> Option<u64>;
    /// Returns a vector of events with head
    fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>);
    /// Returns a batch of events, updating head with the last event in the batch if there is one and if limit.is_some() is true
    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>>;
}

/// Interface for recording and retrieving events
pub trait DCBEventStore {
    /// Reads events from the store based on the provided query and constraints
    ///
    /// Returns a DCBReadResponse that provides an iterator over all events,
    /// unless 'after' is given then only those with position greater than 'after',
    /// and unless any query items are given, then only those that match at least one
    /// query item. An event matches a query item if its type is in the item types or
    /// there are no item types, and if all the item tags are in the event tags.
    fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<Box<dyn DCBReadResponse + '_>>;

    /// Reads events from the store and returns them as a tuple of (Vec<DCBSequencedEvent>, Option<u64>)
    fn read_with_head(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<(Vec<DCBSequencedEvent>, Option<u64>)> {
        let mut response = self.read(query, after, limit)?;
        Ok(response.collect_with_head())
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

// Error types
#[derive(Error, Debug)]
pub enum DCBError {
    // Generic/system errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // Bincode-specific serialization error (kept for API completeness)
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),

    // DCB domain errors
    #[error("Integrity error: condition failed")]
    IntegrityError,
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

    // A simple implementation of DCBReadResponse for testing
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
        type Item = DCBSequencedEvent;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current_index < self.events.len() {
                let event = self.events[self.current_index].clone();
                self.current_index += 1;
                Some(event)
            } else {
                None
            }
        }
    }

    impl DCBReadResponse for TestReadResponse {
        fn head(&self) -> Option<u64> {
            self.head_position
        }

        fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>) {
            todo!()
        }

        fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
            let mut batch = Vec::new();
            while let Some(event) = self.next() {
                batch.push(event);
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
        assert_eq!(response.head(), Some(2));

        // Test iterator functionality
        assert_eq!(response.next().unwrap().position, 1);
        assert_eq!(response.next().unwrap().position, 2);
        assert!(response.next().is_none());
    }
}
