use std::collections::HashMap;
use std::io;
use std::path::Path;

use lru::LruCache;
use thiserror::Error;

use crate::api::{DCBEvent, DCBSequencedEvent};
use crate::checkpoint::CheckpointFile;
use crate::positions::{hash_type, PositionIndex, PositionIndexRecord};
use crate::segments::{SegmentError, SegmentManager};
use crate::tags::TagIndex;
use crate::wal::{pack_dcb_event_with_crc, Position, TransactionWAL, WalError};

/// Error types for transaction operations
#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Segment error: {0}")]
    Segment(#[from] SegmentError),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("Transaction not found: {0}")]
    TransactionNotFound(u64),

    #[error("Position not found: {0}")]
    PositionNotFound(Position),

    #[error("Invalid position: expected {expected}, found {found}")]
    InvalidPosition { expected: Position, found: Position },

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for transaction operations
pub type Result<T> = std::result::Result<T, TransactionError>;

/// Implement From<TransactionError> for std::io::Error
impl From<TransactionError> for std::io::Error {
    fn from(error: TransactionError) -> Self {
        match error {
            TransactionError::Io(io_error) => io_error,
            TransactionError::Segment(segment_error) => segment_error.into(),
            TransactionError::Wal(wal_error) => wal_error.into(),
            TransactionError::TransactionNotFound(txn_id) => std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Transaction not found: {txn_id}"),
            ),
            TransactionError::PositionNotFound(position) => std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Position not found: {position}"),
            ),
            TransactionError::InvalidPosition { expected, found } => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid position: expected {expected}, found {found}"),
            ),
            TransactionError::Serialization(msg) => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Serialization error: {msg}"),
            ),
        }
    }
}

/// Represents the last committed transaction position
#[derive(Debug, Clone, Copy)]
pub struct LastCommittedTxnPosition {
    pub txn_id: u64,
    pub position: Position,
}

/// Transaction Manager for managing transactions, segments, and indexes
pub struct TransactionManager {
    uncommitted: HashMap<u64, Vec<(Position, DCBEvent, Vec<u8>)>>,

    checkpoint: CheckpointFile,
    wal: TransactionWAL,
    segment_manager: SegmentManager,
    position_idx: PositionIndex,
    tags_idx: TagIndex,

    last_issued_txn_id: u64,
    last_issued_position: Position,
    last_committed: LastCommittedTxnPosition,

    /// Cache of events by position
    cache: LruCache<Position, DCBSequencedEvent>,
    /// Cache capacity
    cache_capacity: usize,
}

impl TransactionManager {
    const POSITION_INDEX_FILE_NAME: &'static str = "position_idx.dat";
    const TAG_INDEX_FILE_NAME: &'static str = "tags_idx.dat";

    /// Create a new TransactionManager
    pub fn new<P: AsRef<Path>>(
        path: P,
        max_segment_size: Option<usize>,
        cache_size: Option<usize>,
    ) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path_buf)?;

        // Read the last checkpoint
        let mut checkpoint = CheckpointFile::new(&path_buf)?;
        checkpoint.read_checkpoint()?;

        // Initialize the WAL
        let wal = match TransactionWAL::new(&path_buf) {
            Ok(wal) => wal,
            Err(e) => return Err(TransactionError::Wal(e)),
        };

        // Initialize the segment manager and recover position
        let mut segment_manager = match SegmentManager::new(&path_buf, max_segment_size) {
            Ok(sm) => sm,
            Err(e) => return Err(TransactionError::Segment(e)),
        };

        if let Err(e) =
            segment_manager.recover_position(checkpoint.segment_number, checkpoint.segment_offset)
        {
            return Err(TransactionError::Segment(e));
        }

        // Check if index files exist
        let position_idx_path = path_buf.join(Self::POSITION_INDEX_FILE_NAME);
        let tags_idx_path = path_buf.join(Self::TAG_INDEX_FILE_NAME);

        let position_idx_new = !position_idx_path.exists();
        let tags_idx_new = !tags_idx_path.exists();

        // Initialize the indexes
        let position_idx = PositionIndex::new_with_cache_capacity(
            position_idx_path,
            4096, // page size
            cache_size,
        )?;

        let tags_idx = TagIndex::new_with_cache_capacity(
            tags_idx_path,
            4096, // page size
            cache_size,
        )?;

        let cache_capacity = cache_size.unwrap_or(1000);

        let mut tm = Self {
            uncommitted: HashMap::new(),

            checkpoint,
            wal,
            segment_manager,
            position_idx,
            tags_idx,

            last_issued_txn_id: 0,
            last_issued_position: 0,
            last_committed: LastCommittedTxnPosition {
                txn_id: 0,
                position: 0,
            },

            cache: LruCache::unbounded(),
            cache_capacity,
        };

        // If either index doesn't exist or is invalid, rebuild both from segment files
        if position_idx_new || tags_idx_new {
            tm.build_index()?;
        }

        // Set the last issued transaction ID and position from the checkpoint
        tm.last_issued_txn_id = tm.checkpoint.txn_id;
        tm.last_issued_position = tm.checkpoint.position;
        tm.last_committed = LastCommittedTxnPosition {
            txn_id: tm.checkpoint.txn_id,
            position: tm.checkpoint.position,
        };

        // Restore committed events from the WAL
        let committed_transactions = match tm.wal.read_committed_transactions() {
            Ok(txns) => txns,
            Err(e) => return Err(TransactionError::Wal(e)),
        };

        for (txn_id, payloads) in committed_transactions {
            if txn_id <= tm.checkpoint.txn_id {
                continue;
            }

            tm.last_issued_txn_id = txn_id;

            let mut committed = Vec::new();
            for payload in payloads {
                let (position, event) = tm.deserialize_dcb_event(&payload[8..])?;
                tm.last_issued_position = position;
                committed.push((position, event, payload));
            }

            tm.push_to_segment_and_indexes(&committed)?;

            tm.last_committed = LastCommittedTxnPosition {
                txn_id,
                position: committed.last().unwrap().0,
            };
        }

        Ok(tm)
    }

    /// Begin a new transaction
    pub fn begin(&mut self) -> Result<u64> {
        let txn_id = self.issue_txn_id();

        if let Err(e) = self.wal.begin_transaction(txn_id) {
            return Err(TransactionError::Wal(e));
        }

        self.uncommitted.insert(txn_id, Vec::new());

        Ok(txn_id)
    }

    /// Issue a new transaction ID
    fn issue_txn_id(&mut self) -> u64 {
        self.last_issued_txn_id += 1;
        self.last_issued_txn_id
    }

    /// Issue a new position
    fn issue_position(&mut self) -> Position {
        self.last_issued_position += 1;
        self.last_issued_position
    }

    /// Append an event to a transaction
    pub fn append_event(&mut self, txn_id: u64, event: DCBEvent) -> Result<Position> {
        let position = self.issue_position();
        let payload = pack_dcb_event_with_crc(position, event.clone());

        if let Err(e) = self.wal.write_event(txn_id, &payload) {
            return Err(TransactionError::Wal(e));
        }

        if let Some(events) = self.uncommitted.get_mut(&txn_id) {
            events.push((position, event, payload));
        } else {
            return Err(TransactionError::TransactionNotFound(txn_id));
        }

        Ok(position)
    }

    /// Commit a transaction
    pub fn commit(&mut self, txn_id: u64) -> Result<()> {
        if let Err(e) = self.wal.commit_transaction(txn_id) {
            return Err(TransactionError::Wal(e));
        }

        let committed = self
            .uncommitted
            .remove(&txn_id)
            .ok_or_else(|| TransactionError::TransactionNotFound(txn_id))?;

        self.push_to_segment_and_indexes(&committed)?;

        let last_position = committed.last().map(|(pos, _, _)| *pos).unwrap_or(0);

        self.last_committed = LastCommittedTxnPosition {
            txn_id,
            position: last_position,
        };

        Ok(())
    }

    /// Add an event to both indexes
    fn add_event_to_indexes(
        &mut self,
        position: Position,
        segment_number: u64,
        offset: u64,
        event: &DCBEvent,
    ) -> Result<()> {
        // Add to position index
        self.position_idx
            .insert(
                position,
                PositionIndexRecord {
                    segment: segment_number,
                    offset,
                    type_hash: hash_type(&event.event_type),
                },
            )
            .map_err(TransactionError::Io)?;

        // Add to tags index
        for tag in &event.tags {
            self.tags_idx
                .insert(tag, position)
                .map_err(TransactionError::Io)?;
        }

        Ok(())
    }

    /// Push committed events to segment and indexes
    fn push_to_segment_and_indexes(
        &mut self,
        committed: &[(Position, DCBEvent, Vec<u8>)],
    ) -> Result<()> {
        let payloads: Vec<Vec<u8>> = committed
            .iter()
            .map(|(_, _, payload)| payload.clone())
            .collect();

        let segment_numbers_and_offsets = match self.segment_manager.add_payloads(&payloads) {
            Ok(nos) => nos,
            Err(e) => return Err(TransactionError::Segment(e)),
        };

        for (i, (position, event, _)) in committed.iter().enumerate() {
            let (segment_number, segment_offset) = segment_numbers_and_offsets[i];

            // Push to indexes
            self.add_event_to_indexes(*position, segment_number, segment_offset, event)?;

            // Cache the event
            self.cache.put(
                *position,
                DCBSequencedEvent {
                    event: event.clone(),
                    position: *position,
                },
            );
        }

        Ok(())
    }

    /// Flush and checkpoint
    pub fn flush_and_checkpoint(&mut self) -> Result<()> {
        self.wal.write_flush_and_sync().unwrap();

        if self.last_committed.txn_id == self.checkpoint.txn_id {
            return Ok(());
        }

        // Flush the segment manager
        if let Err(e) = self.segment_manager.flush() {
            return Err(TransactionError::Segment(e));
        }

        // Flush the position index.
        self.position_idx
            .index_pages
            .flush()
            .map_err(TransactionError::Io)?;

        // Flush the tag index.
        let mut tag_index_pages = self.tags_idx.index_pages.borrow_mut();
        tag_index_pages.flush().map_err(TransactionError::Io)?;
        drop(tag_index_pages);

        // Update the checkpoint
        self.checkpoint.txn_id = self.last_committed.txn_id;
        self.checkpoint.position = self.last_committed.position;
        self.checkpoint.segment_number = self.segment_manager.current_segment.number();
        self.checkpoint.segment_offset = self.segment_manager.current_segment.write_offset();
        self.checkpoint.wal_commit_offset = self.wal.commit_offset();

        self.checkpoint
            .write_checkpoint()
            .map_err(TransactionError::Io)?;

        // We can't call wal.cut_before_offset directly, so we use truncate_wal_before_checkpoint
        if let Err(e) = self
            .wal
            .cut_before_offset(self.checkpoint.wal_commit_offset)
        {
            return Err(TransactionError::Wal(e));
        }

        // Reduce the cache to the cache_capacity
        while self.cache.len() > self.cache_capacity {
            self.cache.pop_lru();
        }

        Ok(())
    }

    /// Build the position and tags indexes by scanning all segment files
    fn build_index(&mut self) -> Result<()> {
        for segment_result in self.segment_manager.segments() {
            let mut segment = segment_result.map_err(TransactionError::Segment)?;

            // Store the segment number before iterating
            let segment_number = segment.number();

            for record_result in segment.iter_event_records() {
                let (position, event, offset) = record_result.map_err(TransactionError::Segment)?;

                // Add to position index
                self.position_idx
                    .insert(
                        position,
                        PositionIndexRecord {
                            segment: segment_number,
                            offset,
                            type_hash: hash_type(&event.event_type),
                        },
                    )
                    .map_err(TransactionError::Io)?;

                // Add to tags index
                for tag in &event.tags {
                    self.tags_idx
                        .insert(tag, position)
                        .map_err(TransactionError::Io)?;
                }
            }
        }

        self.position_idx
            .index_pages
            .flush()
            .map_err(TransactionError::Io)?;
        let mut tags_index_pages = self.tags_idx.index_pages.borrow_mut();
        tags_index_pages.flush().map_err(TransactionError::Io)?;

        Ok(())
    }

    /// Read an event at a specific position (backward compatibility wrapper)
    pub fn read_event_at_position(&mut self, position: Position) -> Result<DCBSequencedEvent> {
        self.read_event_at_position_with_record(position, None)
    }

    /// Read an event at a specific position with an optional position index record
    pub fn read_event_at_position_with_record(
        &mut self,
        position: Position,
        position_index_record: Option<PositionIndexRecord>,
    ) -> Result<DCBSequencedEvent> {
        // Check if the event is in the cache
        if let Some(event) = self.cache.get(&position) {
            return Ok(event.clone());
        }

        // Get the segment number and offset from the index if not provided
        let position_index_record = match position_index_record {
            Some(record) => record,
            None => self
                .position_idx
                .lookup(position)
                .map_err(TransactionError::Io)?
                .ok_or_else(|| TransactionError::PositionNotFound(position))?,
        };

        // Get the segment from the segment manager
        let mut segment = self
            .segment_manager
            .get_segment(position_index_record.segment)
            .map_err(|e| TransactionError::Segment(e))?;

        // Get the position and event blob from the segment
        let (recorded_position, event, _) = segment
            .get_event_record(position_index_record.offset)
            .map_err(|e| TransactionError::Segment(e))?;

        // Check the recorded event position is the one we asked for
        if recorded_position != position {
            return Err(TransactionError::InvalidPosition {
                expected: position,
                found: recorded_position,
            });
        }

        // Construct a sequenced event object
        let sequenced_event = DCBSequencedEvent { event, position };

        // Cache the event
        self.cache.put(position, sequenced_event.clone());

        Ok(sequenced_event)
    }

    /// Deserialize a DCB event from a byte array
    fn deserialize_dcb_event(&self, data: &[u8]) -> Result<(Position, DCBEvent)> {
        use crate::wal::DCBEventWithPosition;
        use rmp_serde::decode;

        let event_with_pos: DCBEventWithPosition = decode::from_slice(data).map_err(|e| {
            let err_msg = format!("Failed to deserialize event: {e}");
            TransactionError::Serialization(err_msg)
        })?;

        let position = event_with_pos.position;
        let event = DCBEvent {
            event_type: event_with_pos.type_,
            data: event_with_pos.data,
            tags: event_with_pos.tags,
        };

        Ok((position, event))
    }

    /// Close the transaction manager
    pub fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Lookup positions for a tag after a specific position and return an iterator
    pub fn lookup_positions_for_tag_after_iter<'a>(
        &'a self,
        tag: &str,
        after: Position,
    ) -> Result<impl Iterator<Item = Result<Position>> + 'a> {
        self.tags_idx
            .lookup_with_after_iter(tag, after)
            .map_err(TransactionError::Io)
            .map(|iter| iter.map(|res| res.map_err(TransactionError::Io)))
    }

    /// Scan positions and get their type hashes
    pub fn scan_positions(
        &mut self,
        after: Option<Position>,
    ) -> Result<Vec<(Position, PositionIndexRecord)>> {
        self.position_idx.scan(after).map_err(TransactionError::Io)
    }

    /// Lookup a position index record for a specific position
    pub fn lookup_position_record(
        &mut self,
        position: Position,
    ) -> Result<Option<PositionIndexRecord>> {
        self.position_idx
            .lookup(position)
            .map_err(TransactionError::Io)
    }

    /// Get the last issued position
    pub fn get_last_issued_position(&self) -> Position {
        self.last_issued_position
    }

    /// Get the last committed position
    pub fn get_last_committed_position(&self) -> Position {
        self.last_committed.position
    }
}

impl Drop for TransactionManager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_transaction_manager_initialization() -> std::io::Result<()> {
        let dir = tempdir()?;
        let tm = TransactionManager::new(dir.path(), None, None)?;

        assert_eq!(tm.last_issued_txn_id, 0);
        assert_eq!(tm.last_issued_position, 0);

        Ok(())
    }

    #[test]
    fn test_begin_transaction() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        let txn_id = tm.begin()?;
        assert_eq!(txn_id, 1);

        let txn_id2 = tm.begin()?;
        assert_eq!(txn_id2, 2);

        Ok(())
    }

    #[test]
    fn test_append_event() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        let txn_id = tm.begin()?;

        let event = DCBEvent {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let position = tm.append_event(txn_id, event)?;
        assert_eq!(position, 1);

        Ok(())
    }

    #[test]
    fn test_commit_transaction() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        let txn_id = tm.begin()?;

        let event = DCBEvent {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let position = tm.append_event(txn_id, event)?;
        tm.commit(txn_id)?;

        assert_eq!(tm.last_committed.txn_id, txn_id);
        assert_eq!(tm.last_committed.position, position);

        Ok(())
    }

    #[test]
    fn test_read_event_at_position() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        let txn_id = tm.begin()?;

        let event = DCBEvent {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let position = tm.append_event(txn_id, event.clone())?;
        tm.commit(txn_id)?;

        let read_event = tm.read_event_at_position(position)?;
        assert_eq!(read_event.position, position);
        assert_eq!(read_event.event.event_type, event.event_type);
        assert_eq!(read_event.event.data, event.data);
        assert_eq!(read_event.event.tags, event.tags);

        tm.flush_and_checkpoint()?;

        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        let positions = tm.tags_idx.lookup("tag1")?;
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], position);

        let read_event = tm.read_event_at_position(position)?;
        assert_eq!(read_event.position, position);
        assert_eq!(read_event.event.event_type, event.event_type);
        assert_eq!(read_event.event.data, event.data);
        assert_eq!(read_event.event.tags, event.tags);

        Ok(())
    }

    #[test]
    fn test_multiple_transactions() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        // First transaction
        let txn_id1 = tm.begin()?;

        let event1 = DCBEvent {
            event_type: "event1".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let position1 = tm.append_event(txn_id1, event1.clone())?;
        tm.commit(txn_id1)?;

        // Second transaction
        let txn_id2 = tm.begin()?;

        let event2 = DCBEvent {
            event_type: "event2".to_string(),
            data: vec![4, 5, 6],
            tags: vec!["tag2".to_string(), "tag3".to_string()],
        };

        let position2 = tm.append_event(txn_id2, event2.clone())?;
        tm.commit(txn_id2)?;

        // Read events
        let read_event1 = tm.read_event_at_position(position1)?;
        assert_eq!(read_event1.position, position1);
        assert_eq!(read_event1.event.event_type, event1.event_type);

        let read_event2 = tm.read_event_at_position(position2)?;
        assert_eq!(read_event2.position, position2);
        assert_eq!(read_event2.event.event_type, event2.event_type);

        tm.flush_and_checkpoint()?;

        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        let positions = tm.tags_idx.lookup("tag1")?;
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], position1);

        let read_event = tm.read_event_at_position(position1)?;
        assert_eq!(read_event.position, position1);
        assert_eq!(read_event.event.event_type, event1.event_type);
        assert_eq!(read_event.event.data, event1.data);
        assert_eq!(read_event.event.tags, event1.tags);

        let positions = tm.tags_idx.lookup("tag2")?;
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0], position1);
        assert_eq!(positions[1], position2);

        let positions = tm.tags_idx.lookup("tag3")?;
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], position2);

        Ok(())
    }

    #[test]
    fn test_flush_and_checkpoint() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None)?;

        let txn_id = tm.begin()?;

        let event = DCBEvent {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let position = tm.append_event(txn_id, event.clone())?;
        tm.commit(txn_id)?;

        tm.flush_and_checkpoint()?;

        // Create a new transaction manager to verify checkpoint was written
        let mut tm2 = TransactionManager::new(dir.path(), None, None)?;

        assert_eq!(tm2.checkpoint.txn_id, txn_id);
        assert_eq!(tm2.checkpoint.position, position);

        // Check that tm2's TagIndex correctly returns the position for "tag1" and "tag2"
        let tag1_positions = tm2.tags_idx.lookup("tag1")?;
        let tag2_positions = tm2.tags_idx.lookup("tag2")?;

        assert!(
            tag1_positions.contains(&position),
            "TagIndex should contain position {} for tag1",
            position
        );
        assert!(
            tag2_positions.contains(&position),
            "TagIndex should contain position {} for tag2",
            position
        );

        // Check that we can lookup this position in PositionIndex and get the correct record
        let position_record = tm2
            .position_idx
            .lookup(position)?
            .expect("Position should exist in PositionIndex");
        assert_eq!(
            position_record.type_hash,
            hash_type(&event.event_type),
            "Type hash in PositionIndex should match event type"
        );

        // Check that read_event_at_position returns an event equal to the one that was appended
        let read_event = tm2.read_event_at_position(position)?;
        assert_eq!(
            read_event.position, position,
            "Read event position should match appended position"
        );
        assert_eq!(
            read_event.event.event_type, event.event_type,
            "Read event type should match appended event type"
        );
        assert_eq!(
            read_event.event.data, event.data,
            "Read event data should match appended event data"
        );
        assert_eq!(
            read_event.event.tags, event.tags,
            "Read event tags should match appended event tags"
        );

        Ok(())
    }

    #[test]
    fn test_recovery_from_wal() -> std::io::Result<()> {
        let dir = tempdir()?;

        // Create a transaction manager and commit some events
        {
            let mut tm = TransactionManager::new(dir.path(), None, None)?;

            let txn_id = tm.begin()?;

            let event = DCBEvent {
                event_type: "test_event".to_string(),
                data: vec![1, 2, 3],
                tags: vec!["tag1".to_string(), "tag2".to_string()],
            };

            let _ = tm.append_event(txn_id, event.clone())?;
            tm.commit(txn_id)?;
            tm.flush_and_checkpoint().unwrap();
        }

        // Create a new transaction manager, which should recover from the WAL
        let mut tm2 = TransactionManager::new(dir.path(), None, None)?;

        assert_eq!(tm2.last_issued_txn_id, 1);
        assert_eq!(tm2.last_issued_position, 1);

        assert_eq!(tm2.last_committed.txn_id, 1);
        assert_eq!(tm2.last_committed.position, 1);

        // Read the event to verify it was recovered
        let read_event = tm2.read_event_at_position(1)?;
        assert_eq!(read_event.position, 1);
        assert_eq!(read_event.event.event_type, "test_event");

        Ok(())
    }

    #[test]
    fn test_build_index() -> std::io::Result<()> {
        let dir = tempdir()?;

        // Create a transaction manager and commit some events
        {
            let mut tm = TransactionManager::new(dir.path(), None, None)?;

            let txn_id = tm.begin()?;

            let event1 = DCBEvent {
                event_type: "event1".to_string(),
                data: vec![1, 2, 3],
                tags: vec!["tag1".to_string(), "tag2".to_string()],
            };

            let event2 = DCBEvent {
                event_type: "event2".to_string(),
                data: vec![4, 5, 6],
                tags: vec!["tag2".to_string(), "tag3".to_string()],
            };

            tm.append_event(txn_id, event1)?;
            tm.append_event(txn_id, event2)?;
            tm.commit(txn_id)?;

            tm.flush_and_checkpoint()?;
        }

        // Delete the index files
        fs::remove_file(
            dir.path()
                .join(TransactionManager::POSITION_INDEX_FILE_NAME),
        )?;
        fs::remove_file(dir.path().join(TransactionManager::TAG_INDEX_FILE_NAME))?;

        // Create a new transaction manager, which should rebuild the indexes
        let mut tm2 = TransactionManager::new(dir.path(), None, None)?;

        // Verify the indexes were rebuilt by reading events
        let read_event1 = tm2.read_event_at_position(1)?;
        assert_eq!(read_event1.position, 1);
        assert_eq!(read_event1.event.event_type, "event1");

        let read_event2 = tm2.read_event_at_position(2)?;
        assert_eq!(read_event2.position, 2);
        assert_eq!(read_event2.event.event_type, "event2");

        let positions = tm2.tags_idx.lookup("tag2")?;
        assert_eq!(positions, vec![1, 2]);

        Ok(())
    }
}
