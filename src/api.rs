//! API for Dynamic Consistency Boundaries (DCB) event store
//! 
//! This module provides the core interfaces and data structures for working with
//! an event store that supports dynamic consistency boundaries.

use std::iter::Iterator;

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
    pub after: Option<i64>,
}

/// Represents an event in the event store
#[derive(Debug, Clone)]
pub struct DCBEvent {
    /// Type of the event
    pub type_: String,
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
    pub position: i64,
}

/// Response from a read operation, providing an iterator over sequenced events
pub trait DCBReadResponse: Iterator<Item = DCBSequencedEvent> {
    /// Returns the current head position of the event store, or None if empty
    fn head(&self) -> Option<i64>;
}

/// Interface for recording and retrieving events
pub trait DCBRecorder {
    /// Reads events from the store based on the provided query and constraints
    ///
    /// Returns all events, unless 'after' is given then only those with position
    /// greater than 'after', and unless any query items are given, then only those
    /// that match at least one query item. An event matches a query item if its type
    /// is in the item types or there are no item types, and if all the item tags are
    /// in the event tags.
    fn read(
        &self,
        query: Option<&DCBQuery>,
        after: Option<i64>,
        limit: Option<usize>,
    ) -> Box<dyn DCBReadResponse>;

    /// Appends given events to the event store, unless the condition fails
    ///
    /// Returns the position of the last appended event
    fn append(&self, events: &[DCBEvent], condition: Option<&DCBAppendCondition>) -> i64;
}

#[cfg(test)]
mod tests {
    use super::*;

    // A simple implementation of DCBReadResponse for testing
    struct TestReadResponse {
        events: Vec<DCBSequencedEvent>,
        current_index: usize,
        head_position: Option<i64>,
    }

    impl TestReadResponse {
        fn new(events: Vec<DCBSequencedEvent>, head_position: Option<i64>) -> Self {
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
        fn head(&self) -> Option<i64> {
            self.head_position
        }
    }

    #[test]
    fn test_dcb_read_response() {
        // Create some test events
        let event1 = DCBEvent {
            type_: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let event2 = DCBEvent {
            type_: "another_event".to_string(),
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
        let mut response = TestReadResponse::new(
            vec![seq_event1.clone(), seq_event2.clone()],
            Some(2),
        );

        // Test head position
        assert_eq!(response.head(), Some(2));

        // Test iterator functionality
        assert_eq!(response.next().unwrap().position, 1);
        assert_eq!(response.next().unwrap().position, 2);
        assert!(response.next().is_none());
    }
}
