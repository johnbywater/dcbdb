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

    /// Helper method to create an iterator of (position, tag, query item indices)
    fn _position_tag_qiid<'a>(
        &'a self,
        tag: &'a str,
        tag_qiids: &'a HashMap<String, Vec<usize>>,
        after: Option<Position>,
    ) -> impl Iterator<Item = (Position, String, Vec<usize>)> + 'a {
        let qiids = tag_qiids.get(tag).cloned().unwrap_or_default();
        let tag_owned = tag.to_string();

        let mut tm = self.transaction_manager.lock().unwrap();
        let after_pos = after.unwrap_or(0);

        // We need to drop the mutex guard before returning the iterator
        // So we collect the positions into a Vec first
        let positions = match tm.lookup_positions_for_tag_after_iter(tag, after_pos) {
            Ok(iter) => {
                let mut positions = Vec::new();
                for pos_result in iter {
                    if let Ok(pos) = pos_result {
                        positions.push(pos);
                    }
                }
                positions
            },
            Err(_) => Vec::new(),
        };

        // Now return an iterator that yields (position, tag, qiids)
        positions.into_iter().map(move |position| {
            (position, tag_owned.clone(), qiids.clone())
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
        let mut result: Vec<DCBSequencedEvent> = Vec::new();
        let mut head;

        // Special case for limit 0
        if let Some(0) = limit {
            return Ok((Vec::new(), None));
        }

        // Convert i64 to Position (u64)
        let after_position = after.map(|pos| pos as Position);

        // Get the last issued position and check if there are any events
        let last_issued_position = {
            let tm = self.transaction_manager.lock().unwrap();
            tm.get_last_issued_position()
        };

        if last_issued_position == 0 {
            return Ok((Vec::new(), None));
        }

        // Set the head to the last_issued_position by default
        // It may be overridden later if we hit the limit
        head = Some(last_issued_position as i64);

        // Check if we can use the optimized path with tag indexes
        if let Some(ref q) = query {
            // Check if all query items have tags
            let all_items_have_tags = !q.items.is_empty() && q.items.iter().all(|item| !item.tags.is_empty());

            if all_items_have_tags {
                // Invert the query items
                let mut qis: HashMap<usize, &DCBQueryItem> = HashMap::new();
                let mut tag_qiis: HashMap<String, Vec<usize>> = HashMap::new();
                let mut qi_tags: Vec<std::collections::HashSet<String>> = Vec::new();
                let mut qi_types: Vec<std::collections::HashSet<String>> = Vec::new();
                let mut qi_type_hashes: Vec<std::collections::HashSet<Vec<u8>>> = Vec::new();

                for (qiid, item) in q.items.iter().enumerate() {
                    qis.insert(qiid, item);

                    // Create sets of tags and types for this query item
                    let tags_set: std::collections::HashSet<String> = item.tags.iter().cloned().collect();
                    qi_tags.push(tags_set);

                    let types_set: std::collections::HashSet<String> = item.types.iter().cloned().collect();
                    qi_types.push(types_set);

                    // Create set of type hashes for this query item
                    let type_hashes_set: std::collections::HashSet<Vec<u8>> = item.types.iter()
                        .map(|t| crate::positions::hash_type(t))
                        .collect();
                    qi_type_hashes.push(type_hashes_set);

                    // Associate each tag with this query item's ordinal
                    for tag in &item.tags {
                        tag_qiis.entry(tag.clone()).or_default().push(qiid);
                    }
                }

                // Create iterators for each tag and merge them
                let mut tag_iterators = Vec::new();
                for tag in tag_qiis.keys() {
                    tag_iterators.push(self._position_tag_qiid(tag, &tag_qiis, after_position));
                }

                // Merge the iterators and group by position
                let mut positions_with_tags = Vec::new();

                // Collect all (position, tag, qiids) tuples
                let mut all_tuples = Vec::new();
                for iter in tag_iterators {
                    all_tuples.extend(iter);
                }

                // Sort by position to prepare for grouping
                all_tuples.sort_by_key(|(pos, _, _)| *pos);

                // Group by position
                let mut current_position = None;
                let mut current_tags = std::collections::HashSet::new();
                let mut current_qiis = std::collections::HashSet::new();

                for (position, tag, qiids) in all_tuples {
                    if current_position.is_none() || current_position.unwrap() != position {
                        // Save the previous group if it exists
                        if let Some(pos) = current_position {
                            positions_with_tags.push((pos, current_tags, current_qiis));
                            current_tags = std::collections::HashSet::new();
                            current_qiis = std::collections::HashSet::new();
                        }
                        current_position = Some(position);
                    }

                    // Add tag and qiids to the current group
                    current_tags.insert(tag);
                    for qii in qiids {
                        current_qiis.insert(qii);
                    }
                }

                // Add the last group if it exists
                if let Some(pos) = current_position {
                    positions_with_tags.push((pos, current_tags, current_qiis));
                }

                // Filter for tag-matching query items
                let mut positions_matching_tags = Vec::new();

                // Lock the transaction manager for scanning positions
                let mut tm = self.transaction_manager.lock().unwrap();

                for (position, tags, qiis) in positions_with_tags {
                    let matching_qiis: std::collections::HashSet<usize> = qiis.into_iter()
                        .filter(|&qii| {
                            // Check if all tags in the query item are in the position's tags
                            qi_tags[qii].is_subset(&tags)
                        })
                        .collect();

                    if !matching_qiis.is_empty() {
                        // Get the position index record
                        let position_records = tm.scan_positions(Some(position - 1)).map_err(|e| EventStoreError::Io(e.into()))?;
                        for (pos, record) in position_records {
                            if pos == position {
                                positions_matching_tags.push((position, record, matching_qiis));
                                break;
                            }
                        }
                    }
                }

                // Filter for type-matching query items
                let mut positions_and_idx_records = Vec::new();
                for (position, position_idx_record, qiis) in positions_matching_tags {
                    let type_matches = qiis.iter().any(|&qii| {
                        qi_type_hashes[qii].is_empty() || qi_type_hashes[qii].contains(&position_idx_record.type_hash)
                    });

                    if type_matches {
                        positions_and_idx_records.push((position, position_idx_record));
                    }
                }

                // Read events at the remaining positions
                for (position, idx_record) in positions_and_idx_records {
                    if let Some(limit) = limit {
                        if result.len() >= limit {
                            if limit > 0 {
                                head = Some(result.last().unwrap().position as i64);
                            }
                            break;
                        }
                    }

                    match tm.read_event_at_position_with_record(position, Some(idx_record)) {
                        Ok(event) => {
                            // Double-check that the event matches the query
                            let matches = q.items.iter().any(|item| Self::event_matches_query_item(&event.event, item));

                            if matches {
                                result.push(event);
                            }
                        },
                        Err(_) => {
                            // Skip this event if we can't read it
                            continue;
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
        let mut tm = self.transaction_manager.lock().unwrap();
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
