use std::path::{Path};
use std::sync::Mutex;
use std::collections::HashMap;

use crate::api::{DCBEvent, DCBEventStoreAPI, DCBQuery, DCBQueryItem, DCBSequencedEvent, DCBAppendCondition, EventStoreError, Result};
use crate::transactions::TransactionManager;
use crate::wal::Position;

/// EventStore implements the DCBEventStoreAPI using TransactionManager
pub struct EventStore {
    transaction_manager: Mutex<TransactionManager>,
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
            // Get the last issued position from the transaction manager
            let tm = self.transaction_manager.lock().unwrap();
            let last_issued_position = tm.get_last_issued_position();
            // If last_issued_position is 0, there are no events, so return None
            if last_issued_position == 0 {
                return Ok((Vec::new(), None));
            }
            return Ok((Vec::new(), None));
        }

        // Convert i64 to Position (u64)
        let after_position = after.map(|pos| pos as Position);

        // Lock the transaction manager
        let mut tm = self.transaction_manager.lock().unwrap();

        // Check if there are any events
        let last_issued_position = tm.get_last_issued_position();
        if last_issued_position == 0 {
            return Ok((Vec::new(), None));
        }

        // If limit is None, set the head to the last_issued_position
        if limit.is_none() {
            head = Some(last_issued_position as i64);
        }

        // Check if we can use the optimized path with tag indexes
        if let Some(ref q) = query {
            // Check if all query items have tags
            let all_items_have_tags = !q.items.is_empty() && q.items.iter().all(|item| !item.tags.is_empty());

            if all_items_have_tags {
                // Use the optimized path with tag indexes
                let mut positions = Vec::new();

                // For each query item, get positions from tag index
                for item in &q.items {
                    // For each tag in the query item, get positions
                    let mut item_positions = Vec::new();

                    for tag in &item.tags {
                        // Get positions for this tag
                        let tag_positions = if let Some(after_pos) = after_position {
                            let iter = tm.lookup_positions_for_tag_after_iter(tag, after_pos).map_err(|e| EventStoreError::Io(e.into()))?;
                            let mut positions = Vec::new();
                            for pos_result in iter {
                                positions.push(pos_result.map_err(|e| EventStoreError::Io(e.into()))?);
                            }
                            positions
                        } else {
                            let iter = tm.lookup_positions_for_tag_iter(tag).map_err(|e| EventStoreError::Io(e.into()))?;
                            let mut positions = Vec::new();
                            for pos_result in iter {
                                positions.push(pos_result.map_err(|e| EventStoreError::Io(e.into()))?);
                            }
                            positions
                        };

                        // If this is the first tag, use its positions
                        if item_positions.is_empty() {
                            item_positions = tag_positions;
                        } else {
                            // Otherwise, keep only positions that are in both lists
                            item_positions.retain(|pos| tag_positions.contains(pos));
                        }

                        // If no positions match all tags so far, we can break early
                        if item_positions.is_empty() {
                            break;
                        }
                    }

                    // Add positions from this query item to the overall list
                    positions.extend(item_positions);
                }

                // Deduplicate positions
                positions.sort_unstable();
                positions.dedup();

                // Get all position records
                let position_records = tm.scan_positions(after_position).map_err(|e| EventStoreError::Io(e.into()))?;

                // Create a map of position to type hash for quick lookup
                let mut position_to_type_hash = HashMap::new();
                for (pos, record) in position_records {
                    position_to_type_hash.insert(pos, record.type_hash);
                }

                // For each position, check if the event type matches and read the event
                for position in positions {
                    // Skip positions that are not in the position index
                    if let Some(type_hash) = position_to_type_hash.get(&position) {
                        // Check if any query item matches this event type
                        let type_matches = q.items.iter().any(|item| {
                            // If item has no types, it matches any type
                            if item.types.is_empty() {
                                true
                            } else {
                                // Otherwise, check if any type in the item matches
                                item.types.iter().any(|t| {
                                    let t_hash = crate::positions::hash_type(t);
                                    t_hash == *type_hash
                                })
                            }
                        });

                        // If the type matches, read the event
                        if type_matches {
                            match tm.read_event_at_position(position) {
                                Ok(event) => {
                                    // Double-check that the event matches the query
                                    // This is needed because the type hash might have collisions
                                    let matches = q.items.iter().any(|item| Self::event_matches_query_item(&event.event, item));

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
                                },
                                Err(_) => {
                                    // Skip this event if we can't read it
                                    continue;
                                }
                            }
                        }
                    }
                }

                // Sort the result by position
                result.sort_by_key(|e| e.position);

                return Ok((result, head));
            }
        }

        // Fall back to a sequential scan to match query items without tags.
        // Use scan() on PositionIndex to get all positions and position index records
        let position_records = tm.scan_positions(after_position).map_err(|e| EventStoreError::Io(e.into()))?;

        // Process each position and position index record
        for (position, record) in position_records {
            // Read the event using the position and position index record
            match tm.read_event_at_position_with_record(position, Some(record)) {
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
                },
                Err(_) => {
                    // Skip this event if we can't read it
                    continue;
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
