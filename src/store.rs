// use itertools::Itertools;
// use std::collections::HashMap;
// use std::path::Path;
// use std::sync::Mutex;
// 
// use crate::api::{
//     DCBAppendCondition, DCBEvent, DCBEventStoreAPI, DCBQuery, DCBQueryItem, DCBReadResponse,
//     DCBSequencedEvent, EventStoreError, Result,
// };
// use crate::transactions::TransactionManager;
// use crate::wal::Position;
// 
// /// Iterator that yields (position, tag, query item indices)
// struct PositionTagQiidIterator<I>
// where
//     I: Iterator<Item = crate::transactions::Result<Position>>,
// {
//     // This is the iterator over positions
//     positions: I,
//     // Tag and query item indices
//     tag: String,
//     qiids: Vec<usize>,
// }
// 
// impl<I> PositionTagQiidIterator<I>
// where
//     I: Iterator<Item = crate::transactions::Result<Position>>,
// {
//     fn new(positions: I, tag: String, qiids: Vec<usize>) -> Self {
//         Self {
//             positions,
//             tag,
//             qiids,
//         }
//     }
// }
// 
// impl<I> Iterator for PositionTagQiidIterator<I>
// where
//     I: Iterator<Item = crate::transactions::Result<Position>>,
// {
//     type Item = (Position, String, Vec<usize>);
// 
//     fn next(&mut self) -> Option<Self::Item> {
//         self.positions.next().and_then(|position_result| {
//             position_result
//                 .ok()
//                 .map(|position| (position, self.tag.clone(), self.qiids.clone()))
//         })
//     }
// }
// 
// /// EventStore implements the DCBEventStoreAPI using TransactionManager
// pub struct EventStore {
//     transaction_manager: Mutex<TransactionManager>,
// }
// 
// /// Response from a read operation on the EventStore
// pub struct EventStoreDCBReadResponse<'a> {
//     /// Reference to the event store
//     event_store: &'a EventStore,
//     /// Query to filter events
//     query: Option<DCBQuery>,
//     /// Maximum number of events to read
//     limit: Option<usize>,
//     /// Last committed position in the event store
//     last_committed_position: u64,
//     /// Current head position
//     head: Option<u64>,
//     /// Current position for iteration
//     current_position: u64,
//     /// Current batch of events
//     current_batch: Vec<DCBSequencedEvent>,
//     /// Max batch size
//     max_batch_size: usize,
//     /// Current index in the batch
//     batch_index: usize,
//     /// Count of events returned so far
//     count: usize,
// }
// 
// impl<'a> EventStoreDCBReadResponse<'a> {
//     /// Creates a new EventStoreDCBReadResponse
//     pub fn new(
//         event_store: &'a EventStore,
//         query: Option<DCBQuery>,
//         after: Option<u64>,
//         limit: Option<usize>,
//     ) -> Self {
//         let mut tm = event_store.transaction_manager.lock().unwrap();
//         let last_committed_position = tm.get_last_committed_position();
// 
//         // Initialize with the after position or 0
//         let current_position = after.unwrap_or(0);
// 
//         // Read a batch of events
//         let max_batch_size = 100;
//         let (current_batch, _) = event_store
//             .read_internal(&mut tm, query.clone(), after, Some(max_batch_size))
//             .unwrap();
// 
//         let head = {
//             if limit.is_none() && last_committed_position > 0 {
//                 Some(last_committed_position)
//             } else {
//                 None
//             }
//         };
// 
//         Self {
//             event_store,
//             query,
//             limit,
//             last_committed_position,
//             head,
//             current_position,
//             current_batch,
//             max_batch_size,
//             batch_index: 0,
//             count: 0,
//         }
//     }
// 
//     /// Fetches the next batch of events
//     fn fetch_next_batch(&mut self) -> Result<()> {
//         // Clear the current batch and reset the index
//         self.current_batch.clear();
//         self.batch_index = 0;
// 
//         // Lock the transaction manager
//         let mut tm = self.event_store.transaction_manager.lock().unwrap();
// 
//         // Read a batch of events
//         let (batch, _) = self.event_store.read_internal(
//             &mut tm,
//             self.query.clone(),
//             Some(self.current_position),
//             Some(self.max_batch_size),
//         )?;
// 
//         // Store the batch
//         self.current_batch = batch;
// 
//         Ok(())
//     }
// }
// 
// impl<'a> Iterator for EventStoreDCBReadResponse<'a> {
//     type Item = DCBSequencedEvent;
// 
//     fn next(&mut self) -> Option<Self::Item> {
//         // Check if we've reached the limit
//         if let Some(limit) = self.limit {
//             if self.count >= limit {
//                 return None;
//             }
//         }
// 
//         // If we've exhausted the current batch...
//         if self.batch_index >= self.current_batch.len() {
//             // that was the last batch, we're done.
//             if self.current_batch.len() < self.max_batch_size {
//                 return None;
//             }
//             self.fetch_next_batch().unwrap();
//         }
// 
//         if self.current_batch.is_empty() {
//             return None;
//         }
//         // Get the next event from the current batch
//         let event = self.current_batch[self.batch_index].clone();
// 
//         // Skip events beyond the last committed position
//         if event.position > self.last_committed_position {
//             return None;
//         }
// 
//         // Update the head value based on whether there's a limit
//         if self.limit.is_some() {
//             // When there's a limit, always update the head to the position of the current event
//             // This ensures that when the iterator is exhausted, the head will be the position of the last event
//             self.head = Some(event.position);
//         }
// 
//         // Increment the batch index and count
//         self.batch_index += 1;
//         self.count += 1;
// 
//         // Update the current position for the next fetch
//         self.current_position = event.position;
// 
//         // Return the event
//         Some(event)
//     }
// }
// 
// impl<'a> DCBReadResponse for EventStoreDCBReadResponse<'a> {
//     fn head(&self) -> Option<u64> {
//         self.head
//     }
// 
//     fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>) {
//         let mut head: Option<u64> = self.head;
//         let is_limit_some = self.limit.is_some();
// 
//         let events: Vec<DCBSequencedEvent> = self
//             .inspect(|event| {
//                 if is_limit_some {
//                     head = Some(event.position);
//                 }
//             })
//             .collect();
//         (events, head)
//     }
// 
//     fn next_batch(&mut self) -> Result<Vec<DCBSequencedEvent>> {
//         let mut batch = Vec::new();
//         let is_limit_some = self.limit.is_some();
// 
//         // Check if we've reached the limit
//         if let Some(limit) = self.limit {
//             if self.count >= limit {
//                 return Ok(batch);
//             }
//         }
// 
//         // If we've exhausted the current batch, fetch the next one
//         if self.batch_index >= self.current_batch.len() {
//             // If that was the last batch, we're done
//             if self.current_batch.len() < self.max_batch_size {
//                 return Ok(batch);
//             }
//             self.fetch_next_batch()?;
//         }
// 
//         // If there are no events in the current batch, we're done
//         if self.current_batch.is_empty() {
//             return Ok(batch);
//         }
// 
//         // Get all remaining events from the current batch
//         while self.batch_index < self.current_batch.len() {
//             // Check if we've reached the limit
//             if let Some(limit) = self.limit {
//                 if self.count >= limit {
//                     break;
//                 }
//             }
// 
//             let event = self.current_batch[self.batch_index].clone();
// 
//             // Skip events beyond the last committed position
//             if event.position > self.last_committed_position {
//                 break;
//             }
// 
//             // Update the head value based on whether there's a limit
//             if is_limit_some {
//                 // When there's a limit, always update the head to the position of the current event
//                 self.head = Some(event.position);
//             }
// 
//             // Increment the batch index and count
//             self.batch_index += 1;
//             self.count += 1;
// 
//             // Update the current position for the next fetch
//             self.current_position = event.position;
// 
//             // Add the event to the batch
//             batch.push(event);
//         }
// 
//         Ok(batch)
//     }
// }
// 
// impl EventStore {
//     /// Creates a new EventStore with the given path
//     pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
//         Self::new_with_options(path, None, None)
//     }
// 
//     /// Flushes all pending changes to disk and creates a checkpoint
//     pub fn flush_and_checkpoint(&self) -> Result<()> {
//         self.transaction_manager
//             .lock()
//             .unwrap()
//             .flush_and_checkpoint()
//             .map_err(|e| EventStoreError::Io(e.into()))
//     }
// 
//     /// Creates a new EventStore with the given path and options
//     pub fn new_with_options<P: AsRef<Path>>(
//         path: P,
//         max_segment_size: Option<usize>,
//         cache_size: Option<usize>,
//     ) -> Result<Self> {
//         let path_buf = path.as_ref().to_path_buf();
// 
//         let transaction_manager = TransactionManager::new(&path_buf, max_segment_size, cache_size)
//             .map_err(|e| EventStoreError::Io(e.into()))?;
// 
//         Ok(Self {
//             transaction_manager: Mutex::new(transaction_manager),
//         })
//     }
// 
//     /// Checks if an event matches a query item
//     fn event_matches_query_item(event: &DCBEvent, query_item: &DCBQueryItem) -> bool {
//         // If query_item.types is empty, any event type matches
//         // Otherwise, the event type must be in the query_item.types list
//         let type_matches =
//             query_item.types.is_empty() || query_item.types.iter().any(|t| t == &event.event_type);
// 
//         // All tags in query_item.tags must be present in event.tags
//         let tags_match = query_item.tags.iter().all(|tag| event.tags.contains(tag));
// 
//         type_matches && tags_match
//     }
// 
//     fn read_internal(
//         &self,
//         tm: &mut TransactionManager,
//         query: Option<DCBQuery>,
//         after: Option<u64>,
//         limit: Option<usize>,
//     ) -> Result<(Vec<DCBSequencedEvent>, Option<u64>)> {
//         let mut result: Vec<DCBSequencedEvent> = Vec::new();
//         let mut head;
// 
//         // Special case for limit 0
//         if let Some(0) = limit {
//             return Ok((Vec::new(), None));
//         }
// 
//         // Convert u64 to Position (u64)
//         let after_optional = after.map(|pos| pos as Position);
//         let after_position = after.unwrap_or(0);
// 
//         // Get the last issued position and check if there are any events
//         let last_issued_position = { tm.get_last_issued_position() };
// 
//         if last_issued_position == 0 {
//             return Ok((Vec::new(), None));
//         }
// 
//         // Set the head to the last_issued_position by default
//         // It may be overridden later if we hit the limit
//         head = Some(last_issued_position);
// 
//         // Check if we can use the optimized path with tag indexes
//         if let Some(ref q) = query {
//             // Check if all query items have tags
//             let all_items_have_tags =
//                 !q.items.is_empty() && q.items.iter().all(|item| !item.tags.is_empty());
// 
//             if all_items_have_tags {
//                 // Invert the query items
//                 let mut qis: HashMap<usize, &DCBQueryItem> = HashMap::new();
//                 let mut tag_qiis: HashMap<String, Vec<usize>> = HashMap::new();
//                 let mut qi_tags: Vec<std::collections::HashSet<String>> = Vec::new();
//                 let mut qi_types: Vec<std::collections::HashSet<String>> = Vec::new();
//                 let mut qi_type_hashes: Vec<std::collections::HashSet<Vec<u8>>> = Vec::new();
// 
//                 for (qiid, item) in q.items.iter().enumerate() {
//                     qis.insert(qiid, item);
// 
//                     // Create sets of tags and types for this query item
//                     let tags_set: std::collections::HashSet<String> =
//                         item.tags.iter().cloned().collect();
//                     qi_tags.push(tags_set);
// 
//                     let types_set: std::collections::HashSet<String> =
//                         item.types.iter().cloned().collect();
//                     qi_types.push(types_set);
// 
//                     // Create set of type hashes for this query item
//                     let type_hashes_set: std::collections::HashSet<Vec<u8>> = item
//                         .types
//                         .iter()
//                         .map(|t| crate::positions::hash_type(t))
//                         .collect();
//                     qi_type_hashes.push(type_hashes_set);
// 
//                     // Associate each tag with this query item's ordinal
//                     for tag in &item.tags {
//                         tag_qiis.entry(tag.clone()).or_default().push(qiid);
//                     }
//                 }
// 
//                 // Create iterators for each tag
//                 let mut tag_iterators = Vec::new();
//                 for tag in tag_qiis.keys() {
//                     let qiids = tag_qiis.get(tag).cloned().unwrap_or_default();
//                     let tag_owned = tag.to_string();
// 
//                     // Call lookup to get an iterator of positions
//                     let positions_iter = tm
//                         .lookup_positions_for_tag_after_iter(tag, after_position)
//                         .unwrap();
// 
//                     // Now create an iterator that yields (position, tag, qiids)
//                     let position_tag_qiids_iter =
//                         PositionTagQiidIterator::new(positions_iter, tag_owned, qiids);
// 
//                     tag_iterators.push(position_tag_qiids_iter);
//                 }
// 
//                 // Merge the iterators and group by position
// 
//                 // Merge the iterators by position using itertools::merge_by
//                 let all_tuples_iter = tag_iterators.into_iter().kmerge_by(|a, b| a.0 < b.0);
// 
//                 // Iterator that groups tuples by position
//                 struct GroupByPositionIterator<I>
//                 where
//                     I: Iterator<Item = (Position, String, Vec<usize>)>,
//                 {
//                     all_tuples_iter: I,
//                     current_position: Option<Position>,
//                     current_tags: std::collections::HashSet<String>,
//                     current_qiis: std::collections::HashSet<usize>,
//                     finished: bool,
//                 }
// 
//                 impl<I> GroupByPositionIterator<I>
//                 where
//                     I: Iterator<Item = (Position, String, Vec<usize>)>,
//                 {
//                     fn new(iter: I) -> Self {
//                         Self {
//                             all_tuples_iter: iter,
//                             current_position: None,
//                             current_tags: std::collections::HashSet::new(),
//                             current_qiis: std::collections::HashSet::new(),
//                             finished: false,
//                         }
//                     }
//                 }
// 
//                 impl<I> Iterator for GroupByPositionIterator<I>
//                 where
//                     I: Iterator<Item = (Position, String, Vec<usize>)>,
//                 {
//                     type Item = (
//                         Position,
//                         std::collections::HashSet<String>,
//                         std::collections::HashSet<usize>,
//                     );
// 
//                     fn next(&mut self) -> Option<Self::Item> {
//                         // If we've already processed the last group, return None
//                         if self.finished {
//                             return None;
//                         }
// 
//                         for (position, tag, qiids) in self.all_tuples_iter.by_ref() {
//                             if self.current_position.is_none() {
//                                 // First tuple, initialize the current group
//                                 self.current_position = Some(position);
//                             } else if self.current_position.unwrap() != position {
//                                 // Position changed, return the current group
//                                 let result_position = self.current_position.unwrap();
//                                 let result_tags = std::mem::take(&mut self.current_tags);
//                                 let result_qiis = std::mem::take(&mut self.current_qiis);
// 
//                                 // Start a new group with the current tuple
//                                 self.current_position = Some(position);
//                                 self.current_tags.insert(tag);
//                                 for qii in qiids {
//                                     self.current_qiis.insert(qii);
//                                 }
// 
//                                 return Some((result_position, result_tags, result_qiis));
//                             }
// 
//                             // Add to the current group
//                             self.current_tags.insert(tag);
//                             for qii in qiids {
//                                 self.current_qiis.insert(qii);
//                             }
//                         }
// 
//                         // No more tuples, return the last group if it exists
//                         if let Some(pos) = self.current_position.take() {
//                             self.finished = true;
//                             let result_tags = std::mem::take(&mut self.current_tags);
//                             let result_qiis = std::mem::take(&mut self.current_qiis);
//                             return Some((pos, result_tags, result_qiis));
//                         }
// 
//                         None
//                     }
//                 }
// 
//                 // Create and use the group by position iterator
//                 let group_by_position_iter = GroupByPositionIterator::new(all_tuples_iter);
// 
//                 // Collect the grouped positions
//                 let positions_with_tags: Vec<_> = group_by_position_iter.collect();
// 
//                 // Filter for tag-matching query items using an iterator
//                 let positions_with_tags_iter = positions_with_tags.into_iter();
// 
//                 // Iterator that filters positions based on tag matching
//                 struct TagMatchingIterator<'a, I>
//                 where
//                     I: Iterator<
//                         Item = (
//                             Position,
//                             std::collections::HashSet<String>,
//                             std::collections::HashSet<usize>,
//                         ),
//                     >,
//                 {
//                     positions_with_tags_iter: I,
//                     qi_tags: &'a Vec<std::collections::HashSet<String>>,
//                     tm: &'a mut TransactionManager,
//                 }
// 
//                 impl<'a, I> Iterator for TagMatchingIterator<'a, I>
//                 where
//                     I: Iterator<
//                         Item = (
//                             Position,
//                             std::collections::HashSet<String>,
//                             std::collections::HashSet<usize>,
//                         ),
//                     >,
//                 {
//                     type Item = (
//                         Position,
//                         crate::positions::PositionIndexRecord,
//                         std::collections::HashSet<usize>,
//                     );
// 
//                     fn next(&mut self) -> Option<Self::Item> {
//                         loop {
//                             let (position, tags, qiis) = self.positions_with_tags_iter.next()?;
// 
//                             let matching_qiis: std::collections::HashSet<usize> = qiis
//                                 .into_iter()
//                                 .filter(|&qii| {
//                                     // Check if all tags in the query item are in the position's tags
//                                     self.qi_tags[qii].is_subset(&tags)
//                                 })
//                                 .collect();
// 
//                             if !matching_qiis.is_empty() {
//                                 // Lookup the position index record
//                                 if let Ok(Some(record)) = self
//                                     .tm
//                                     .lookup_position_record(position)
//                                     .map_err(|e| EventStoreError::Io(e.into()))
//                                 {
//                                     return Some((position, record, matching_qiis));
//                                 }
//                             }
//                             // If no match or error, continue to next position
//                         }
//                     }
//                 }
// 
//                 // Create and use the tag matching iterator
//                 let tag_matching_iter = TagMatchingIterator {
//                     positions_with_tags_iter,
//                     qi_tags: &qi_tags,
//                     tm,
//                 };
// 
//                 // Collect the filtered positions and records
//                 let positions_matching_tags: Vec<_> = tag_matching_iter.collect();
// 
//                 // Filter for type-matching query items using an iterator
//                 let positions_matching_tags_iter = positions_matching_tags.into_iter();
// 
//                 // Iterator that filters positions based on type matching
//                 struct TypeMatchingIterator<'a, I>
//                 where
//                     I: Iterator<
//                         Item = (
//                             Position,
//                             crate::positions::PositionIndexRecord,
//                             std::collections::HashSet<usize>,
//                         ),
//                     >,
//                 {
//                     positions_matching_tags_iter: I,
//                     qi_type_hashes: &'a Vec<std::collections::HashSet<Vec<u8>>>,
//                 }
// 
//                 impl<'a, I> Iterator for TypeMatchingIterator<'a, I>
//                 where
//                     I: Iterator<
//                         Item = (
//                             Position,
//                             crate::positions::PositionIndexRecord,
//                             std::collections::HashSet<usize>,
//                         ),
//                     >,
//                 {
//                     type Item = (Position, crate::positions::PositionIndexRecord);
// 
//                     fn next(&mut self) -> Option<Self::Item> {
//                         loop {
//                             let (position, position_idx_record, qiis) =
//                                 self.positions_matching_tags_iter.next()?;
// 
//                             let type_matches = qiis.iter().any(|&qii| {
//                                 self.qi_type_hashes[qii].is_empty()
//                                     || self.qi_type_hashes[qii]
//                                         .contains(&position_idx_record.type_hash)
//                             });
// 
//                             if type_matches {
//                                 return Some((position, position_idx_record));
//                             }
//                             // If no match, continue to next position
//                         }
//                     }
//                 }
// 
//                 // Create and use the type matching iterator
//                 let type_matching_iter = TypeMatchingIterator {
//                     positions_matching_tags_iter,
//                     qi_type_hashes: &qi_type_hashes,
//                 };
// 
//                 // Collect the filtered positions and records
//                 let positions_and_idx_records: Vec<_> = type_matching_iter.collect();
// 
//                 // Read events at the remaining positions using iterators
//                 let positions_and_idx_records_iter = positions_and_idx_records.into_iter();
// 
//                 // Convert positions and index records to DCBSequencedEvent objects
//                 struct ReadEventAtPositionIterator<'a> {
//                     tm: &'a mut TransactionManager,
//                     positions_and_idx_records_iter:
//                         std::vec::IntoIter<(Position, crate::positions::PositionIndexRecord)>,
//                 }
// 
//                 impl<'a> Iterator for ReadEventAtPositionIterator<'a> {
//                     type Item = Result<DCBSequencedEvent>;
// 
//                     fn next(&mut self) -> Option<Self::Item> {
//                         if let Some((position, idx_record)) =
//                             self.positions_and_idx_records_iter.next()
//                         {
//                             Some(
//                                 self.tm
//                                     .read_event_at_position_with_record(position, Some(idx_record))
//                                     .map_err(|e| EventStoreError::Io(e.into())),
//                             )
//                         } else {
//                             None
//                         }
//                     }
//                 }
// 
//                 // Filter actual events based on query matching (not just type and tag hashes)
//                 struct MatchEventsWithQueryItemsIterator<'a> {
//                     events_iter: ReadEventAtPositionIterator<'a>,
//                     query_items: &'a Vec<DCBQueryItem>,
//                 }
// 
//                 impl<'a> Iterator for MatchEventsWithQueryItemsIterator<'a> {
//                     type Item = DCBSequencedEvent;
// 
//                     fn next(&mut self) -> Option<Self::Item> {
//                         // Keep iterating until we find a matching event or run out of events
//                         loop {
//                             match self.events_iter.next()? {
//                                 Ok(event) => {
//                                     // Check if the event matches the query
//                                     let matches = self.query_items.iter().any(|item| {
//                                         EventStore::event_matches_query_item(&event.event, item)
//                                     });
// 
//                                     if matches {
//                                         return Some(event);
//                                     }
//                                     // If no match, continue to next event
//                                 }
//                                 Err(_) => {
//                                     // Skip this event if we can't read it
//                                     continue;
//                                 }
//                             }
//                         }
//                     }
//                 }
// 
//                 // Create and use the iterators
//                 let events_iter = ReadEventAtPositionIterator {
//                     tm,
//                     positions_and_idx_records_iter,
//                 };
// 
//                 let filtered_events_iter = MatchEventsWithQueryItemsIterator {
//                     events_iter,
//                     query_items: &q.items,
//                 };
// 
//                 // Collect the filtered events
//                 result = filtered_events_iter.collect();
// 
//                 // Apply limit using itertools functionality
//                 let mut events_iter: Box<dyn Iterator<Item = DCBSequencedEvent>> =
//                     Box::new(result.into_iter());
// 
//                 if let Some(limit) = limit {
//                     // Get only a limited number of events
//                     events_iter = Box::new(events_iter.take(limit));
//                 }
// 
//                 result = events_iter.collect_vec();
// 
//                 if limit.is_some() {
//                     if !result.is_empty() {
//                         // Set head to the position of the last event in the limited result
//                         head = Some(result.last().unwrap().position);
//                     } else {
//                         head = None;
//                     }
//                 }
// 
//                 return Ok((result, head));
//             }
//         }
// 
//         // Fall back to a sequential scan to match query items without tags.
//         // Use scan() on PositionIndex to get all positions and position index records
//         let position_records = tm
//             .scan_positions(after_optional)
//             .map_err(|e| EventStoreError::Io(e.into()))?;
// 
//         // Process each position and position index record
//         for (position, record) in position_records {
//             // Read the event using the position and position index record
//             match tm.read_event_at_position_with_record(position, Some(record)) {
//                 Ok(event) => {
//                     // Check if the event matches the query
//                     let matches = match &query {
//                         None => true,
//                         Some(q) => {
//                             // If query has no items, it matches all events
//                             if q.items.is_empty() {
//                                 true
//                             } else {
//                                 // Event matches if it matches any query item
//                                 q.items
//                                     .iter()
//                                     .any(|item| Self::event_matches_query_item(&event.event, item))
//                             }
//                         }
//                     };
// 
//                     if matches {
//                         result.push(event);
//                     }
//                 }
//                 Err(_) => {
//                     // Skip this event if we can't read it
//                     continue;
//                 }
//             }
//         }
// 
//         // Apply limit using itertools functionality
//         if let Some(limit) = limit {
//             // Get only the limited number of events
//             let limited_result = result.into_iter().take(limit).collect_vec();
// 
//             if !limited_result.is_empty() {
//                 // Set head to the position of the last event in the limited result
//                 head = Some(limited_result.last().unwrap().position);
//             } else {
//                 head = None;
//             }
// 
//             result = limited_result;
//         }
// 
//         Ok((result, head))
//     }
// }
// 
// impl DCBEventStoreAPI for EventStore {
//     fn read(
//         &self,
//         query: Option<DCBQuery>,
//         after: Option<u64>,
//         limit: Option<usize>,
//     ) -> Result<Box<dyn DCBReadResponse + '_>> {
//         // Create a new EventStoreDCBReadResponse
//         let response = EventStoreDCBReadResponse::new(self, query, after, limit);
// 
//         Ok(Box::new(response))
//     }
// 
//     fn head(&self) -> Result<Option<u64>> {
//         let tm = self.transaction_manager.lock().unwrap();
//         let last_committed_position = tm.get_last_committed_position();
// 
//         if last_committed_position == 0 {
//             Ok(None)
//         } else {
//             Ok(Some(last_committed_position))
//         }
//     }
// 
//     fn append(&self, events: Vec<DCBEvent>, condition: Option<DCBAppendCondition>) -> Result<u64> {
//         // Check condition if provided
//         // Lock the transaction manager
//         let mut tm = self.transaction_manager.lock().unwrap();
// 
//         if let Some(condition) = condition {
//             // Check if any events match the fail_if_events_match query
//             let (matching_events, _) = self.read_internal(
//                 &mut tm,
//                 Some(condition.fail_if_events_match),
//                 condition.after,
//                 None,
//             )?;
// 
//             if !matching_events.is_empty() {
//                 return Err(EventStoreError::IntegrityError);
//             }
//         }
// 
//         // Begin a transaction
//         // let start = std::time::Instant::now();
//         let txn_id = tm.begin().map_err(|e| EventStoreError::Io(e.into()))?;
//         // let duration = start.elapsed();
//         // println!("Transaction begin took: {:?}", duration);
// 
//         let mut last_position = 0;
// 
//         // Append each event
//         for event in events {
//             // let start = std::time::Instant::now();
//             last_position = tm
//                 .append_event(txn_id, event)
//                 .map_err(|e| EventStoreError::Io(e.into()))?;
//             // let duration = start.elapsed();
//             // println!("Append took: {:?}", duration);
//         }
// 
//         // Commit the transaction
//         tm.commit(txn_id)
//             .map_err(|e| EventStoreError::Io(e.into()))?;
// 
//         // // Flush and checkpoint
//         // tm.flush_and_checkpoint().map_err(|e| EventStoreError::Io(e.into()))?;
//         //
//         Ok(last_position)
//     }
// }
// 
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tempfile::tempdir;
// 
//     /// Helper function to convert a DCBReadResponse to a tuple of (Vec<DCBSequencedEvent>, Option<u64>)
//     /// This function is no longer needed since the read method now returns a tuple directly
//     #[allow(dead_code)]
//     fn response_to_tuple(
//         response: Box<dyn DCBReadResponse>,
//     ) -> (Vec<DCBSequencedEvent>, Option<u64>) {
//         let head = response.head().map(|h| h as u64);
//         let events: Vec<DCBSequencedEvent> = response.collect();
//         (events, head)
//     }
// 
//     #[test]
//     fn test_event_store_append_read() -> std::io::Result<()> {
//         let dir = tempdir()?;
//         let store = EventStore::new(dir.path()).unwrap();
// 
//         // Read all events
//         let (events, head) = store.read_with_head(None, None, None).unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, None);
// 
//         // Create some test events
//         let event1 = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: vec![1, 2, 3],
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         let event2 = DCBEvent {
//             event_type: "another_event".to_string(),
//             data: vec![4, 5, 6],
//             tags: vec!["tag2".to_string(), "tag3".to_string()],
//         };
// 
//         // Append events
//         let position = store
//             .append(vec![event1.clone(), event2.clone()], None)
//             .unwrap();
//         assert_eq!(position, 2);
// 
//         // Read all events
//         let (events, head) = store.read_with_head(None, None, None).unwrap();
//         assert_eq!(events.len(), 2);
//         assert_eq!(head, Some(2));
// 
//         // Check event contents
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(events[0].event.data, vec![1, 2, 3]);
//         assert_eq!(events[0].event.tags, vec!["tag1", "tag2"]);
// 
//         assert_eq!(events[1].event.event_type, "another_event");
//         assert_eq!(events[1].event.data, vec![4, 5, 6]);
//         assert_eq!(events[1].event.tags, vec!["tag2", "tag3"]);
// 
//         Ok(())
//     }
// 
//     #[test]
//     fn test_event_store_query_by_tag() -> std::io::Result<()> {
//         let dir = tempdir()?;
//         let store = EventStore::new(dir.path()).unwrap();
// 
//         // Create some test events
//         let event1 = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: vec![1, 2, 3],
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         let event2 = DCBEvent {
//             event_type: "another_event".to_string(),
//             data: vec![4, 5, 6],
//             tags: vec!["tag2".to_string(), "tag3".to_string()],
//         };
// 
//         // Append events
//         store
//             .append(vec![event1.clone(), event2.clone()], None)
//             .unwrap();
// 
//         // Query by tag1
//         let query1 = DCBQuery {
//             items: vec![DCBQueryItem {
//                 types: vec![],
//                 tags: vec!["tag1".to_string()],
//             }],
//         };
// 
//         let (events, head) = store
//             .read_with_head(Some(query1.clone()), None, None)
//             .unwrap();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(head, Some(2));
// 
//         // Query by tag1 - limit 0
//         let (events, head) = store
//             .read_with_head(Some(query1.clone()), None, Some(0))
//             .unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, None);
// 
//         // Query by tag1 - limit 1
//         let read_response = store.read(Some(query1.clone()), None, Some(1));
//         let (events, head) = read_response.unwrap().collect_with_head();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(head, Some(1));
// 
//         // Query by tag1 - after 0
//         let (events, head) = store
//             .read_with_head(Some(query1.clone()), Some(0), None)
//             .unwrap();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(head, Some(2));
// 
//         // Query by tag1 - after 1
//         let (events, head) = store
//             .read_with_head(Some(query1.clone()), Some(1), None)
//             .unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, Some(2));
// 
//         // Query by tag1 - after 1, limit 1
//         let (events, head) = store
//             .read_with_head(Some(query1.clone()), Some(1), Some(1))
//             .unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, None);
// 
//         // Query by tag2 (should match both events)
//         let query2 = DCBQuery {
//             items: vec![DCBQueryItem {
//                 types: vec![],
//                 tags: vec!["tag2".to_string()],
//             }],
//         };
// 
//         let (events, _) = store.read_with_head(Some(query2), None, None).unwrap();
//         assert_eq!(events.len(), 2);
// 
//         // Query by tag1 or tag3 (should match both events)
//         let query2 = DCBQuery {
//             items: vec![
//                 DCBQueryItem {
//                     types: vec![],
//                     tags: vec!["tag1".to_string()],
//                 },
//                 DCBQueryItem {
//                     types: vec![],
//                     tags: vec!["tag3".to_string()],
//                 },
//             ],
//         };
// 
//         let (events, _) = store.read_with_head(Some(query2), None, None).unwrap();
//         assert_eq!(events.len(), 2);
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(events[1].event.event_type, "another_event");
// 
//         // Query by tag3 or tag1 (should return events in correct order)
//         let query2 = DCBQuery {
//             items: vec![
//                 DCBQueryItem {
//                     types: vec![],
//                     tags: vec!["tag3".to_string()],
//                 },
//                 DCBQueryItem {
//                     types: vec![],
//                     tags: vec!["tag1".to_string()],
//                 },
//             ],
//         };
// 
//         let (events, _) = store.read_with_head(Some(query2), None, None).unwrap();
//         assert_eq!(events.len(), 2);
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(events[1].event.event_type, "another_event");
// 
//         // Query by tag3
//         let query3 = DCBQuery {
//             items: vec![DCBQueryItem {
//                 types: vec![],
//                 tags: vec!["tag3".to_string()],
//             }],
//         };
// 
//         let (events, _) = store.read_with_head(Some(query3), None, None).unwrap();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "another_event");
// 
//         // Query by tag4
//         let query4 = DCBQuery {
//             items: vec![DCBQueryItem {
//                 types: vec![],
//                 tags: vec!["tag4".to_string()],
//             }],
//         };
// 
//         let (events, head) = store.read_with_head(Some(query4), None, None).unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, Some(2));
// 
//         Ok(())
//     }
// 
//     #[test]
//     fn test_event_store_query_by_type() -> std::io::Result<()> {
//         let dir = tempdir()?;
//         let store = EventStore::new(dir.path()).unwrap();
// 
//         // Create some test events
//         let event1 = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: vec![1, 2, 3],
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         let event2 = DCBEvent {
//             event_type: "another_event".to_string(),
//             data: vec![4, 5, 6],
//             tags: vec!["tag2".to_string(), "tag3".to_string()],
//         };
// 
//         // Append events
//         store
//             .append(vec![event1.clone(), event2.clone()], None)
//             .unwrap();
// 
//         // Query by type
//         let query = DCBQuery {
//             items: vec![DCBQueryItem {
//                 types: vec!["test_event".to_string()],
//                 tags: vec![],
//             }],
//         };
// 
//         let (events, _) = store
//             .read_with_head(Some(query.clone()), None, None)
//             .unwrap();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "test_event");
// 
//         // Query by tag1 - limit 0
//         let (events, head) = store
//             .read_with_head(Some(query.clone()), None, Some(0))
//             .unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, None);
// 
//         // Query by tag1 - limit 1
//         let (events, head) = store
//             .read_with_head(Some(query.clone()), None, Some(1))
//             .unwrap();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(head, Some(1));
// 
//         // Query by tag1 - after 0
//         let (events, head) = store
//             .read_with_head(Some(query.clone()), Some(0), None)
//             .unwrap();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "test_event");
//         assert_eq!(head, Some(2));
// 
//         // Query by tag1 - after 1
//         let (events, head) = store
//             .read_with_head(Some(query.clone()), Some(1), None)
//             .unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, Some(2));
// 
//         // Query by tag1 - after 1, limit 1
//         let (events, head) = store
//             .read_with_head(Some(query.clone()), Some(1), Some(1))
//             .unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, None);
// 
//         Ok(())
//     }
// 
//     #[test]
//     fn test_event_store_query_by_type_and_tag() -> std::io::Result<()> {
//         let dir = tempdir()?;
//         let store = EventStore::new(dir.path()).unwrap();
// 
//         // Create some test events
//         let event1 = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: vec![1, 2, 3],
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         let event2 = DCBEvent {
//             event_type: "another_event".to_string(),
//             data: vec![4, 5, 6],
//             tags: vec!["tag2".to_string(), "tag3".to_string()],
//         };
// 
//         // Append events
//         store
//             .append(vec![event1.clone(), event2.clone()], None)
//             .unwrap();
// 
//         // Query by test_event and tag2
//         let query = DCBQuery {
//             items: vec![DCBQueryItem {
//                 types: vec!["test_event".to_string()],
//                 tags: vec!["tag2".to_string()],
//             }],
//         };
// 
//         let (events, _) = store.read_with_head(Some(query), None, None).unwrap();
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].event.event_type, "test_event");
// 
//         // Query by non_event and tag2
//         let query = DCBQuery {
//             items: vec![DCBQueryItem {
//                 types: vec!["non_event".to_string()],
//                 tags: vec!["tag2".to_string()],
//             }],
//         };
// 
//         let (events, head) = store.read_with_head(Some(query), None, None).unwrap();
//         assert_eq!(events.len(), 0);
//         assert_eq!(head, Some(2));
// 
//         Ok(())
//     }
// 
//     #[test]
//     fn test_event_store_condition() -> std::io::Result<()> {
//         let dir = tempdir()?;
//         let store = EventStore::new(dir.path()).unwrap();
// 
//         // Create some test events
//         let event1 = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: vec![1, 2, 3],
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         // Append first event
//         store.append(vec![event1.clone()], None).unwrap();
// 
//         // Try to append second event with condition that should fail
//         let condition = DCBAppendCondition {
//             fail_if_events_match: DCBQuery {
//                 items: vec![DCBQueryItem {
//                     types: vec!["test_event".to_string()],
//                     tags: vec![],
//                 }],
//             },
//             after: None,
//         };
// 
//         let event2 = DCBEvent {
//             event_type: "another_event".to_string(),
//             data: vec![4, 5, 6],
//             tags: vec!["tag2".to_string(), "tag3".to_string()],
//         };
// 
//         let result = store.append(vec![event2.clone()], Some(condition));
//         assert!(result.is_err());
// 
//         // Try to append with condition that should succeed
//         let condition = DCBAppendCondition {
//             fail_if_events_match: DCBQuery {
//                 items: vec![DCBQueryItem {
//                     types: vec!["non_existent_event".to_string()],
//                     tags: vec![],
//                 }],
//             },
//             after: None,
//         };
// 
//         let result = store.append(vec![event2.clone()], Some(condition));
//         assert!(result.is_ok());
// 
//         Ok(())
//     }
// 
//     #[test]
//     fn test_head_method() -> std::io::Result<()> {
//         let dir = tempdir()?;
//         let store = EventStore::new(dir.path()).unwrap();
// 
//         // For an empty store, head() should return None
//         let head = store.head().unwrap();
//         assert_eq!(head, None);
// 
//         // Create a test event
//         let event = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: vec![1, 2, 3],
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         // Append the event
//         let position = store.append(vec![event.clone()], None).unwrap();
//         assert_eq!(position, 1);
// 
//         // Now head() should return Some(1)
//         let head = store.head().unwrap();
//         assert_eq!(head, Some(1));
// 
//         // Append another event
//         let event2 = DCBEvent {
//             event_type: "another_event".to_string(),
//             data: vec![4, 5, 6],
//             tags: vec!["tag2".to_string(), "tag3".to_string()],
//         };
// 
//         let position = store.append(vec![event2.clone()], None).unwrap();
//         assert_eq!(position, 2);
// 
//         // Now head() should return Some(2)
//         let head = store.head().unwrap();
//         assert_eq!(head, Some(2));
// 
//         Ok(())
//     }
// 
//     #[test]
//     fn test_event_store_with_many_events() -> std::io::Result<()> {
//         let dir = tempdir()?;
//         let store = EventStore::new(dir.path()).unwrap();
// 
//         let num_appends = 300;
//         let num_events = num_appends * 2;
// 
//         // Create and insert pairs of events with alternating tags
//         let mut expected_positions = Vec::with_capacity(num_events);
//         let mut expected_tags = Vec::with_capacity(num_events);
// 
//         for _ in 0..num_appends {
//             // Create event with tag1
//             let event1 = DCBEvent {
//                 event_type: "test_event".to_string(),
//                 data: vec![1, 2, 3],
//                 tags: vec!["tag1".to_string()],
//             };
// 
//             // Create event with tag2
//             let event2 = DCBEvent {
//                 event_type: "test_event".to_string(),
//                 data: vec![1, 2, 3],
//                 tags: vec!["tag2".to_string()],
//             };
// 
//             // Append events
//             let position = store.append(vec![event1, event2], None).unwrap();
// 
//             // Store expected positions and tags
//             expected_positions.push(position - 1);
//             expected_positions.push(position);
//             expected_tags.push("tag1".to_string());
//             expected_tags.push("tag2".to_string());
//         }
// 
//         // Create query to match events with either tag1 or tag2
//         let query = DCBQuery {
//             items: vec![
//                 DCBQueryItem {
//                     types: vec![],
//                     tags: vec!["tag1".to_string()],
//                 },
//                 DCBQueryItem {
//                     types: vec![],
//                     tags: vec!["tag2".to_string()],
//                 },
//             ],
//         };
// 
//         // Read events with the query
//         let (events, head) = store
//             .read_with_head(Some(query.clone()), None, None)
//             .unwrap();
// 
//         // Check that we got the expected number of events
//         assert_eq!(
//             events.len(),
//             num_events,
//             "Expected {}, got {}",
//             num_events,
//             events.len()
//         );
// 
//         // Check that the head position is correct
//         assert_eq!(
//             head,
//             Some(num_events as u64),
//             "Expected head position to be {}, got {:?}",
//             num_events,
//             head
//         );
// 
//         // Check that positions are in order and tags alternate correctly
//         for (i, event) in events.iter().enumerate() {
//             // Check position
//             assert_eq!(
//                 event.position, expected_positions[i],
//                 "Event at index {} has position {}, expected {}",
//                 i, event.position, expected_positions[i]
//             );
// 
//             // Check tag
//             let tag = &event.event.tags[0];
//             assert_eq!(
//                 tag, &expected_tags[i],
//                 "Event at index {} has tag {}, expected {}",
//                 i, tag, expected_tags[i]
//             );
//         }
// 
//         store
//             .transaction_manager
//             .lock()
//             .unwrap()
//             .flush_and_checkpoint()
//             .unwrap();
// 
//         // Read events with the query using next_batch
//         let mut read_response = store.read(Some(query.clone()), None, None).unwrap();
//         let mut events: Vec<DCBSequencedEvent> = Vec::new();
// 
//         // Collect events from batches until there are no more
//         loop {
//             let batch = read_response.next_batch().unwrap();
//             if batch.is_empty() {
//                 break;
//             }
//             events.extend(batch);
//         }
// 
//         let head = read_response.head();
// 
//         // Check that we got the expected number of events
//         assert_eq!(
//             events.len(),
//             num_events,
//             "Expected {}, got {}",
//             num_events,
//             events.len()
//         );
// 
//         // Check that the head position is correct
//         assert_eq!(
//             head,
//             Some(num_events as u64),
//             "Expected head position to be {}, got {:?}",
//             num_events,
//             head
//         );
// 
//         // Check that positions are in order and tags alternate correctly
//         for (i, event) in events.iter().enumerate() {
//             // Check position
//             assert_eq!(
//                 event.position, expected_positions[i],
//                 "Event at index {} has position {}, expected {}",
//                 i, event.position, expected_positions[i]
//             );
// 
//             // Check tag
//             let tag = &event.event.tags[0];
//             assert_eq!(
//                 tag, &expected_tags[i],
//                 "Event at index {} has tag {}, expected {}",
//                 i, tag, expected_tags[i]
//             );
//         }
// 
//         Ok(())
//     }
// }
