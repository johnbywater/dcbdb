use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::api::{DCBEvent, DCBEventStoreAPI, DCBQuery, DCBQueryItem, DCBSequencedEvent, DCBAppendCondition, EventStoreError, Result};
use crate::transactions::TransactionManager;
use crate::wal::Position;

/// EventStore implements the DCBEventStoreAPI using TransactionManager
pub struct EventStore {
    transaction_manager: Mutex<TransactionManager>,
    path: PathBuf,
}

impl EventStore {
    /// Opens an EventStore at the specified path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new_with_options(path, None, None)
    }

    /// Creates a new EventStore with the given path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new_with_options(path, None, None)
    }

    /// Creates a new EventStore with the given path and options
    pub fn new_with_options<P: AsRef<Path>>(
        path: P,
        max_segment_size: Option<usize>,
        cache_size: Option<usize>,
    ) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();

        let transaction_manager = TransactionManager::new(&path_buf, max_segment_size, cache_size)
            .map_err(|e| EventStoreError::Io(e.into()))?;

        Ok(Self {
            transaction_manager: Mutex::new(transaction_manager),
            path: path_buf,
        })
    }

    /// Checks if an event matches a query item
    fn event_matches_query_item(event: &DCBEvent, query_item: &DCBQueryItem) -> bool {
        // If query_item.types is empty, any event type matches
        // Otherwise, the event type must be in the query_item.types list
        let type_matches = query_item.types.is_empty() || 
                          query_item.types.iter().any(|t| t == &event.event_type);

        // All tags in query_item.tags must be present in event.tags
        let tags_match = query_item.tags.iter().all(|tag| event.tags.contains(tag));

        type_matches && tags_match
    }
}

impl DCBEventStoreAPI for EventStore {
    fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<i64>,
        limit: Option<usize>,
    ) -> Result<(Vec<DCBSequencedEvent>, Option<i64>)> {
        let mut result = Vec::new();
        let mut head: Option<i64> = None;

        // Special case for limit 0
        if let Some(0) = limit {
            return Ok((Vec::new(), None));
        }

        // Convert i64 to Position (u64)
        let after_position = after.map(|pos| pos as Position);

        // Lock the transaction manager
        let mut tm = self.transaction_manager.lock().unwrap();

        // First, find the highest position in the store
        let mut current_position = 1;
        loop {
            match tm.read_event_at_position(current_position) {
                Ok(_) => {
                    head = Some(current_position as i64);
                    current_position += 1;
                },
                Err(_) => {
                    break;
                }
            }
        }

        // If there are no events, return empty result
        if head.is_none() {
            return Ok((Vec::new(), None));
        }

        // Start from position 1 (or after+1 if specified)
        let start_position = after_position.map(|p| p + 1).unwrap_or(1);
        current_position = start_position;

        // Read events one by one until we hit an error or reach the limit
        loop {
            match tm.read_event_at_position(current_position) {
                Ok(event) => {
                    // Check if the event matches the query
                    let matches = match &query {
                        None => true,
                        Some(q) => {
                            // If query has no items, it matches all events
                            if q.items.is_empty() {
                                true
                            } else {
                                // Event matches if it matches any query item
                                q.items.iter().any(|item| Self::event_matches_query_item(&event.event, item))
                            }
                        }
                    };

                    if matches {
                        result.push(event);

                        // Apply limit if specified
                        if let Some(limit) = limit {
                            if result.len() >= limit {
                                // If we've reached the limit, set the head to the position of the last event in the result
                                if limit > 0 {
                                    head = Some(result.last().unwrap().position as i64);
                                }
                                break;
                            }
                        }
                    }

                    // Move to the next position
                    current_position += 1;
                },
                Err(_) => {
                    // We've reached the end of the events
                    break;
                }
            }
        }

        Ok((result, head))
    }

    fn append(&self, events: Vec<DCBEvent>, condition: Option<DCBAppendCondition>) -> Result<i64> {
        // Check condition if provided
        if let Some(condition) = condition {
            // Check if any events match the fail_if_events_match query
            let (matching_events, _) = self.read(Some(condition.fail_if_events_match), condition.after, None)?;

            if !matching_events.is_empty() {
                return Err(EventStoreError::IntegrityError);
            }
        }

        // Lock the transaction manager
        let mut tm = self.transaction_manager.lock().unwrap();

        // Begin a transaction
        let txn_id = tm.begin().map_err(|e| EventStoreError::Io(e.into()))?;

        let mut last_position = 0;

        // Append each event
        for event in events {
            last_position = tm.append_event(txn_id, event).map_err(|e| EventStoreError::Io(e.into()))? as i64;
        }

        // Commit the transaction
        tm.commit(txn_id).map_err(|e| EventStoreError::Io(e.into()))?;

        // Flush and checkpoint
        tm.flush_and_checkpoint().map_err(|e| EventStoreError::Io(e.into()))?;

        Ok(last_position)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_event_store_append_read() -> std::io::Result<()> {
        let dir = tempdir()?;
        let store = EventStore::new(dir.path()).unwrap();

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

        // Append events
        let position = store.append(vec![event1.clone(), event2.clone()], None).unwrap();
        assert_eq!(position, 2);

        // Read all events
        let (events, head) = store.read(None, None, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(head, Some(2));

        // Check event contents
        assert_eq!(events[0].event.event_type, "test_event");
        assert_eq!(events[0].event.data, vec![1, 2, 3]);
        assert_eq!(events[0].event.tags, vec!["tag1", "tag2"]);

        assert_eq!(events[1].event.event_type, "another_event");
        assert_eq!(events[1].event.data, vec![4, 5, 6]);
        assert_eq!(events[1].event.tags, vec!["tag2", "tag3"]);

        Ok(())
    }

    #[test]
    fn test_event_store_query_by_tag() -> std::io::Result<()> {
        let dir = tempdir()?;
        let store = EventStore::new(dir.path()).unwrap();

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

        // Append events
        store.append(vec![event1.clone(), event2.clone()], None).unwrap();

        // Query by tag1
        let query1 = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["tag1".to_string()],
            }],
        };

        let (events, _) = store.read(Some(query1), None, None).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type, "test_event");

        // Query by tag2 (should match both events)
        let query2 = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["tag2".to_string()],
            }],
        };

        let (events, _) = store.read(Some(query2), None, None).unwrap();
        assert_eq!(events.len(), 2);

        // Query by tag3
        let query3 = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["tag3".to_string()],
            }],
        };

        let (events, _) = store.read(Some(query3), None, None).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type, "another_event");

        Ok(())
    }

    #[test]
    fn test_event_store_query_by_type() -> std::io::Result<()> {
        let dir = tempdir()?;
        let store = EventStore::new(dir.path()).unwrap();

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

        // Append events
        store.append(vec![event1.clone(), event2.clone()], None).unwrap();

        // Query by type
        let query = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["test_event".to_string()],
                tags: vec![],
            }],
        };

        let (events, _) = store.read(Some(query), None, None).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type, "test_event");

        Ok(())
    }

    #[test]
    fn test_event_store_query_by_type_and_tag() -> std::io::Result<()> {
        let dir = tempdir()?;
        let store = EventStore::new(dir.path()).unwrap();

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

        // Append events
        store.append(vec![event1.clone(), event2.clone()], None).unwrap();

        // Query by type and tag
        let query = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["test_event".to_string()],
                tags: vec!["tag2".to_string()],
            }],
        };

        let (events, _) = store.read(Some(query), None, None).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type, "test_event");

        Ok(())
    }

    #[test]
    fn test_event_store_condition() -> std::io::Result<()> {
        let dir = tempdir()?;
        let store = EventStore::new(dir.path()).unwrap();

        // Create some test events
        let event1 = DCBEvent {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        // Append first event
        store.append(vec![event1.clone()], None).unwrap();

        // Try to append second event with condition that should fail
        let condition = DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec!["test_event".to_string()],
                    tags: vec![],
                }],
            },
            after: None,
        };

        let event2 = DCBEvent {
            event_type: "another_event".to_string(),
            data: vec![4, 5, 6],
            tags: vec!["tag2".to_string(), "tag3".to_string()],
        };

        let result = store.append(vec![event2.clone()], Some(condition));
        assert!(result.is_err());

        // Try to append with condition that should succeed
        let condition = DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec!["non_existent_event".to_string()],
                    tags: vec![],
                }],
            },
            after: None,
        };

        let result = store.append(vec![event2.clone()], Some(condition));
        assert!(result.is_ok());

        Ok(())
    }
}
