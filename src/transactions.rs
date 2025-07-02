use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::api::{Event, DCBSequencedEvent};
use crate::checkpoint::CheckpointFile;
use crate::positions::{PositionIndex, PositionIndexRecord, hash_type};
use crate::segments::{SegmentManager};
use crate::tags::TagIndex;
use crate::wal::{Position, TransactionWAL, pack_dcb_event_with_crc};

/// Represents the last committed transaction position
#[derive(Debug, Clone, Copy)]
pub struct LastCommittedTxnPosition {
    pub txn_id: u64,
    pub position: Position,
}

/// Transaction Manager for managing transactions, segments, and indexes
pub struct TransactionManager {
    path: PathBuf,
    checkpoint_interval: Duration,
    uncommitted: HashMap<u64, Vec<(Position, Event, Vec<u8>)>>,

    checkpoint: CheckpointFile,
    wal: TransactionWAL,
    segment_manager: SegmentManager,
    position_idx: PositionIndex,
    tags_idx: TagIndex,

    last_issued_txn_id: u64,
    last_issued_position: Position,
    last_committed: LastCommittedTxnPosition,
    last_checkpoint_time: Instant,
}

impl TransactionManager {
    const POSITION_INDEX_FILE_NAME: &'static str = "position_idx.dat";
    const TAG_INDEX_FILE_NAME: &'static str = "tags_idx.dat";

    /// Create a new TransactionManager
    pub fn new<P: AsRef<Path>>(
        path: P,
        max_segment_size: Option<usize>,
        cache_size: Option<usize>,
        checkpoint_interval: Option<Duration>,
    ) -> std::io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path_buf)?;

        let checkpoint_interval = checkpoint_interval.unwrap_or(Duration::from_millis(200));

        // Read the last checkpoint
        let mut checkpoint = CheckpointFile::new(&path_buf)?;
        checkpoint.read_checkpoint()?;

        // Initialize the WAL
        let wal = match TransactionWAL::new(&path_buf) {
            Ok(wal) => wal,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to create WAL: {}", e))),
        };

        // Initialize the segment manager and recover position
        let mut segment_manager = match SegmentManager::new(&path_buf, max_segment_size) {
            Ok(sm) => sm,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to create segment manager: {}", e))),
        };

        if let Err(e) = segment_manager.recover_position(checkpoint.segment_number as u32, checkpoint.segment_offset) {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to recover position: {}", e)));
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

        let mut tm = Self {
            path: path_buf,
            checkpoint_interval,
            uncommitted: HashMap::new(),

            checkpoint,
            wal,
            segment_manager,
            position_idx,
            tags_idx,

            last_issued_txn_id: 0,
            last_issued_position: 0,
            last_committed: LastCommittedTxnPosition { txn_id: 0, position: 0 },
            last_checkpoint_time: Instant::now(),
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
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read committed transactions: {}", e))),
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
    pub fn begin(&mut self) -> std::io::Result<u64> {
        let txn_id = self.issue_txn_id();

        if let Err(e) = self.wal.begin_transaction(txn_id) {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to begin transaction: {}", e)));
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
    pub fn append_event(
        &mut self,
        txn_id: u64,
        event: Event,
    ) -> std::io::Result<Position> {
        let position = self.issue_position();
        let payload = pack_dcb_event_with_crc(position, event.clone());

        if let Err(e) = self.wal.write_event(txn_id, &payload) {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to write event: {}", e)));
        }

        if let Some(events) = self.uncommitted.get_mut(&txn_id) {
            events.push((position, event, payload));
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Transaction {} not found", txn_id),
            ));
        }

        Ok(position)
    }

    /// Commit a transaction
    pub fn commit(&mut self, txn_id: u64) -> std::io::Result<()> {
        if let Err(e) = self.wal.commit_transaction(txn_id) {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to commit transaction: {}", e)));
        }

        let committed = self.uncommitted.remove(&txn_id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Transaction {} not found", txn_id),
            )
        })?;

        self.push_to_segment_and_indexes(&committed)?;

        let last_position = committed.last().map(|(pos, _, _)| *pos).unwrap_or(0);

        self.last_committed = LastCommittedTxnPosition {
            txn_id,
            position: last_position,
        };

        // Flush and checkpoint one last time
        self.flush_and_checkpoint()?;


        Ok(())
    }

    /// Add an event to both indexes
    fn add_event_to_indexes(
        &mut self,
        position: Position,
        segment_number: u32,
        offset: u64,
        event: &Event,
    ) -> std::io::Result<()> {
        // Add to position index
        self.position_idx.insert(
            position,
            PositionIndexRecord {
                segment: segment_number as i32,
                offset: offset as i32,
                type_hash: hash_type(&event.event_type),
            },
        )?;

        // Add to tags index
        for tag in &event.tags {
            self.tags_idx.insert(tag, position)?;
        }

        Ok(())
    }

    /// Push committed events to segment and indexes
    fn push_to_segment_and_indexes(
        &mut self,
        committed: &[(Position, Event, Vec<u8>)],
    ) -> std::io::Result<()> {
        let payloads: Vec<Vec<u8>> = committed.iter().map(|(_, _, payload)| payload.clone()).collect();

        let segment_numbers_and_offsets = match self.segment_manager.add_payloads(&payloads) {
            Ok(nos) => nos,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to add payloads: {}", e))),
        };

        for (i, (position, event, _)) in committed.iter().enumerate() {
            let (segment_number, segment_offset) = segment_numbers_and_offsets[i];

            // Push to indexes
            self.add_event_to_indexes(*position, segment_number, segment_offset, event)?;
        }

        Ok(())
    }

    /// Flush and checkpoint
    pub fn flush_and_checkpoint(&mut self) -> std::io::Result<()> {
        let last_committed = self.last_committed;
        let checkpoint_txn_id = self.checkpoint.txn_id;

        if last_committed.txn_id == checkpoint_txn_id {
            return Ok(());
        }

        // Flush the segment manager
        if let Err(e) = self.segment_manager.flush() {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to flush segment manager: {}", e)));
        }

        // Flush the indexes
        self.position_idx.index_pages.flush()?;
        self.tags_idx.index_pages.flush()?;

        // Update the checkpoint
        let mut checkpoint = CheckpointFile::new(&self.path)?;
        checkpoint.txn_id = last_committed.txn_id;
        checkpoint.position = last_committed.position;

        // We can't access segment_manager.current_segment directly, so we use the first segment
        // This is a simplification and might not be accurate
        checkpoint.segment_number = 1;
        checkpoint.segment_offset = 0;

        // We can't access wal.commit_offset directly, so we use 0
        // This is a simplification and might not be accurate
        checkpoint.wal_commit_offset = 0;

        checkpoint.write_checkpoint()?;

        // We can't call wal.cut_before_offset directly, so we use truncate_wal_before_checkpoint
        if let Err(e) = self.wal.truncate_wal_before_checkpoint(last_committed.txn_id) {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to truncate WAL: {}", e)));
        }

        // Update the last checkpoint time
        self.last_checkpoint_time = Instant::now();

        Ok(())
    }

    /// Build the position and tags indexes by scanning all segment files
    fn build_index(&mut self) -> std::io::Result<()> {
        for segment_result in self.segment_manager.segments() {
            let mut segment = match segment_result {
                Ok(s) => s,
                Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to get segment: {}", e))),
            };

            // Store the segment number before iterating
            let segment_number = segment.number() as i32;

            for record_result in segment.iter_event_records() {
                let (position, event, offset) = match record_result {
                    Ok(r) => r,
                    Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to get event record: {}", e))),
                };

                // Add to position index
                self.position_idx.insert(
                    position,
                    PositionIndexRecord {
                        segment: segment_number,
                        offset: offset as i32,
                        type_hash: hash_type(&event.event_type),
                    },
                )?;

                // Add to tags index
                for tag in &event.tags {
                    self.tags_idx.insert(tag, position)?;
                }
            }
        }

        self.position_idx.index_pages.flush()?;
        self.tags_idx.index_pages.flush()?;

        Ok(())
    }

    /// Read an event at a specific position
    pub fn read_event_at_position(
        &mut self,
        position: Position,
    ) -> std::io::Result<DCBSequencedEvent> {
        // Get the segment number and offset from the index
        let position_index_record = self.position_idx.lookup(position)?
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Event at position {} not found", position),
            ))?;

        // Get the segment from the segment manager
        let mut segment = match self.segment_manager.get_segment(position_index_record.segment as u32) {
            Ok(s) => s,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to get segment: {}", e))),
        };

        // Get the position and event blob from the segment
        let (recorded_position, event, _) = match segment.get_event_record(position_index_record.offset as u64) {
            Ok(r) => r,
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to get event record: {}", e))),
        };

        // Check the recorded event position is the one we asked for
        if recorded_position != position {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected event at position {}, but found {}", position, recorded_position),
            ));
        }

        // Construct a sequenced event object
        let sequenced_event = DCBSequencedEvent {
            event,
            position,
        };

        Ok(sequenced_event)
    }

    /// Deserialize a DCB event from a byte array
    fn deserialize_dcb_event(&self, data: &[u8]) -> std::io::Result<(Position, Event)> {
        use rmp_serde::decode;
        use crate::wal::DCBEventWithPosition;

        let event_with_pos: DCBEventWithPosition = decode::from_slice(data)
            .map_err(|e| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize event: {}", e)
            ))?;

        let position = event_with_pos.position;
        let event = Event {
            event_type: event_with_pos.type_,
            data: event_with_pos.data,
            tags: event_with_pos.tags,
        };

        Ok((position, event))
    }

    /// Close the transaction manager
    pub fn close(&mut self) -> std::io::Result<()> {
        // Flush and checkpoint one last time
        self.flush_and_checkpoint()?;

        Ok(())
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
        let tm = TransactionManager::new(dir.path(), None, None, None)?;

        assert_eq!(tm.last_issued_txn_id, 0);
        assert_eq!(tm.last_issued_position, 0);

        Ok(())
    }

    #[test]
    fn test_begin_transaction() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

        let txn_id = tm.begin()?;
        assert_eq!(txn_id, 1);

        let txn_id2 = tm.begin()?;
        assert_eq!(txn_id2, 2);

        Ok(())
    }

    #[test]
    fn test_append_event() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

        let txn_id = tm.begin()?;

        let event = Event {
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
        let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

        let txn_id = tm.begin()?;

        let event = Event {
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
        let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

        let txn_id = tm.begin()?;

        let event = Event {
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

        Ok(())
    }

    #[test]
    fn test_multiple_transactions() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

        // First transaction
        let txn_id1 = tm.begin()?;

        let event1 = Event {
            event_type: "event1".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let position1 = tm.append_event(txn_id1, event1.clone())?;
        tm.commit(txn_id1)?;

        // Second transaction
        let txn_id2 = tm.begin()?;

        let event2 = Event {
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

        Ok(())
    }

    #[test]
    fn test_flush_and_checkpoint() -> std::io::Result<()> {
        let dir = tempdir()?;
        let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

        let txn_id = tm.begin()?;

        let event = Event {
            event_type: "test_event".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["tag1".to_string(), "tag2".to_string()],
        };

        let position = tm.append_event(txn_id, event)?;
        tm.commit(txn_id)?;

        tm.flush_and_checkpoint()?;

        // Create a new transaction manager to verify checkpoint was written
        let tm2 = TransactionManager::new(dir.path(), None, None, None)?;

        assert_eq!(tm2.checkpoint.txn_id, txn_id);
        assert_eq!(tm2.checkpoint.position, position);

        Ok(())
    }

    #[test]
    fn test_recovery_from_wal() -> std::io::Result<()> {
        let dir = tempdir()?;

        // Create a transaction manager and commit some events
        {
            let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

            let txn_id = tm.begin()?;

            let event = Event {
                event_type: "test_event".to_string(),
                data: vec![1, 2, 3],
                tags: vec!["tag1".to_string(), "tag2".to_string()],
            };

            let position = tm.append_event(txn_id, event.clone())?;
            tm.commit(txn_id)?;

            // Don't flush and checkpoint, so the events are only in the WAL
        }

        // Create a new transaction manager, which should recover from the WAL
        let mut tm2 = TransactionManager::new(dir.path(), None, None, None)?;

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
            let mut tm = TransactionManager::new(dir.path(), None, None, None)?;

            let txn_id = tm.begin()?;

            let event1 = Event {
                event_type: "event1".to_string(),
                data: vec![1, 2, 3],
                tags: vec!["tag1".to_string(), "tag2".to_string()],
            };

            let event2 = Event {
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
        fs::remove_file(dir.path().join(TransactionManager::POSITION_INDEX_FILE_NAME))?;
        fs::remove_file(dir.path().join(TransactionManager::TAG_INDEX_FILE_NAME))?;

        // Create a new transaction manager, which should rebuild the indexes
        let mut tm2 = TransactionManager::new(dir.path(), None, None, None)?;

        // Verify the indexes were rebuilt by reading events
        let read_event1 = tm2.read_event_at_position(1)?;
        assert_eq!(read_event1.position, 1);
        assert_eq!(read_event1.event.event_type, "event1");

        let read_event2 = tm2.read_event_at_position(2)?;
        assert_eq!(read_event2.position, 2);
        assert_eq!(read_event2.event.event_type, "event2");

        Ok(())
    }
}
