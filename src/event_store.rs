// //! EventStore implementation for the event store.
// //!
// //! This module provides an implementation of the EventStoreAPI trait.
// 
// use std::path::Path;
// use std::sync::{Arc, Mutex};
// 
// use crate::api::{DCBAppendCondition, DCBSequencedEvent, Event, DCBEventStoreAPI, EventStoreError, DCBQuery, Result};
// use crate::segments::{Segment, SegmentManager};
// use crate::tags::TagIndex;
// use crate::wal::{Position, TransactionWAL, pack_dcb_event_with_crc};
// 
// /// Implementation of the EventStoreAPI trait
// pub struct EventStore {
//     segment_manager: Arc<Mutex<SegmentManager>>,
//     tag_index: Arc<Mutex<TagIndex>>,
//     wal: Arc<Mutex<TransactionWAL>>,
//     next_position: Arc<Mutex<Position>>,
// }
// 
// impl EventStore {
//     /// Open an EventStore at the specified path
//     pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
//         let path_buf = path.as_ref().to_path_buf();
// 
//         // Create directories if they don't exist
//         std::fs::create_dir_all(&path_buf)?;
//         let segments_path = path_buf.join("segments");
//         std::fs::create_dir_all(&segments_path)?;
//         let indexes_path = path_buf.join("indexes");
//         std::fs::create_dir_all(&indexes_path)?;
// 
//         // Initialize components
//         let segment_manager = Arc::new(Mutex::new(SegmentManager::new(&segments_path, None)
//             .map_err(|e| EventStoreError::Corruption(e.to_string()))?));
// 
//         let tag_index = Arc::new(Mutex::new(TagIndex::new(&indexes_path.join("tags"), 4096)
//             .map_err(|e| EventStoreError::Corruption(e.to_string()))?));
// 
// 
//         let wal = Arc::new(Mutex::new(TransactionWAL::new(&path_buf)
//             .map_err(|e| EventStoreError::Corruption(e.to_string()))?));
// 
//         // Determine the next position by scanning segments
//         let mut next_position = 1; // Start at 1
// 
//         {
//             let segment_manager_lock = segment_manager.lock().unwrap();
//             for segment_result in segment_manager_lock.segments() {
//                 let mut segment = match segment_result {
//                     Ok(segment) => segment,
//                     Err(_) => continue,
//                 };
// 
//                 for record_result in segment.iter_event_records() {
//                     match record_result {
//                         Ok((position, _, _)) => {
//                             next_position = next_position.max(position + 1);
//                         }
//                         Err(_) => continue,
//                     }
//                 }
//             }
//         }
// 
//         Ok(Self {
//             segment_manager,
//             tag_index,
//             wal,
//             next_position: Arc::new(Mutex::new(next_position)),
//         })
//     }
// 
//     /// Internal method to read events
//     fn read_internal(&self, query: Option<DCBQuery>, after: Option<i64>, limit: Option<usize>) -> Result<(Vec<DCBSequencedEvent>, Option<i64>)> {
//         let mut result = Vec::new();
//         let mut head: Option<i64> = None;
// 
//         // Get all segments
//         let segment_manager = self.segment_manager.lock().unwrap();
//         let mut segments: Vec<Segment> = Vec::new();
// 
//         for segment_result in segment_manager.segments() {
//             match segment_result {
//                 Ok(segment) => segments.push(segment),
//                 Err(_) => continue,
//             }
//         }
// 
//         // Process each segment
//         for mut segment in segments {
//             for record_result in segment.iter_event_records() {
//                 match record_result {
//                     Ok((position, event, _)) => {
//                         // Update head position
//                         head = Some(position.max(head.unwrap_or(0)));
// 
//                         // Skip if position is <= after
//                         if let Some(after_pos) = after {
//                             if position <= after_pos {
//                                 continue;
//                             }
//                         }
// 
//                         // Check if event matches query
//                         let matches = match &query {
//                             None => true,
//                             Some(q) => {
//                                 // If query has no items, it matches all events
//                                 if q.items.is_empty() {
//                                     true
//                                 } else {
//                                     // Event matches if it matches any query item
//                                     q.items.iter().any(|item| {
//                                         // Event matches item if:
//                                         // 1. Item has no types OR event type is in item types
//                                         // 2. All item tags are in event tags
//                                         let type_match = item.types.is_empty() || 
//                                                         item.types.contains(&event.event_type);
// 
//                                         let tag_match = item.tags.iter().all(|tag| 
//                                                         event.tags.contains(tag));
// 
//                                         type_match && tag_match
//                                     })
//                                 }
//                             }
//                         };
// 
//                         if matches {
//                             result.push(DCBSequencedEvent {
//                                 event: event.clone(),
//                                 position,
//                             });
//                         }
//                     }
//                     Err(_) => continue,
//                 }
//             }
//         }
// 
//         // Sort by position (should already be sorted, but just to be safe)
//         result.sort_by_key(|e| e.position);
// 
//         // Apply limit if specified
//         if let Some(limit_val) = limit {
//             if limit_val == 0 {
//                 result.clear();
//                 head = None;
//             } else if result.len() > limit_val {
//                 // Update head to the position of the last returned event
//                 head = Some(result[limit_val - 1].position);
//                 result.truncate(limit_val);
//             }
//         }
// 
//         Ok((result, head))
//     }
// 
//     /// Check if any events match the given condition
//     fn check_condition(&self, condition: &DCBAppendCondition) -> Result<bool> {
//         let query = condition.fail_if_events_match.clone();
//         let after = condition.after;
// 
//         // Read events that match the query
//         let (events, _) = self.read_internal(Some(query), after, None)?;
// 
//         // If any events match, return true
//         Ok(!events.is_empty())
//     }
// }
// 
// /// Implementation of the EventStoreAPI trait for EventStore
// impl DCBEventStoreAPI for EventStore {
//     /// Read events from the store based on the provided query and constraints
//     fn read(
//         &self,
//         query: Option<DCBQuery>,
//         after: Option<i64>,
//         limit: Option<usize>,
//     ) -> Result<(Vec<DCBSequencedEvent>, Option<i64>)> {
//         self.read_internal(query, after, limit)
//     }
// 
//     /// Append given events to the event store, unless the condition fails
//     fn append(&self, events: Vec<Event>, condition: Option<DCBAppendCondition>) -> Result<i64> {
//         // Check condition if provided
//         if let Some(cond) = condition {
//             if self.check_condition(&cond)? {
//                 return Err(EventStoreError::IntegrityError);
//             }
//         }
// 
//         // Get the next position
//         let mut next_position = self.next_position.lock().unwrap();
//         let start_position = *next_position;
//         let mut current_position = start_position;
// 
//         // Begin transaction in WAL
//         let txn_id = current_position as u64;
//         let wal = self.wal.lock().unwrap();
//         wal.begin_transaction(txn_id).map_err(|e| EventStoreError::Corruption(e.to_string()))?;
// 
//         // Prepare payloads for segment manager
//         let mut payloads = Vec::with_capacity(events.len());
// 
//         for event in &events {
//             // Pack event with CRC
//             let payload = pack_dcb_event_with_crc(current_position, event.clone());
// 
//             // Write to WAL
//             wal.write_event(txn_id, &payload).map_err(|e| EventStoreError::Corruption(e.to_string()))?;
// 
//             // Add to payloads
//             payloads.push(payload);
// 
//             // Update position
//             current_position += 1;
//         }
// 
//         // Commit transaction in WAL
//         wal.commit_transaction(txn_id).map_err(|e| EventStoreError::Corruption(e.to_string()))?;
// 
//         // Add payloads to segment manager
//         let mut segment_manager = self.segment_manager.lock().unwrap();
//         segment_manager.add_payloads(&payloads).map_err(|e| EventStoreError::Corruption(e.to_string()))?;
//         segment_manager.flush().map_err(|e| EventStoreError::Corruption(e.to_string()))?;
// 
//         // Update indexes
//         let mut position = start_position;
//         for event in &events {
//             // Update tag index
//             let mut tag_index = self.tag_index.lock().unwrap();
//             for tag in &event.tags {
//                 tag_index.insert(tag, position).map_err(|e| EventStoreError::Corruption(e.to_string()))?;
//             }
// 
//             // Update position index
//             // Note: We're not actually using the position index in this implementation,
//             // but we would update it here if needed
// 
//             position += 1;
//         }
// 
//         // Update next position
//         *next_position = current_position;
// 
//         // Return the position of the last appended event
//         Ok(current_position - 1)
//     }
// }
