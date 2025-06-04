use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use bincode::{deserialize, serialize};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// Constants
const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

// Core data structures
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub event_type: String,
    pub tags: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SequencedEvent {
    pub position: u64,
    pub event: Event,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryItem {
    pub types: Vec<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Query {
    pub items: Vec<QueryItem>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AppendCondition {
    pub fail_if_events_match: Query,
    pub after: Option<u64>,
}

// Error types
#[derive(Error, Debug)]
pub enum EventStoreError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("Integrity error: condition failed")]
    IntegrityError,
    #[error("Corruption detected: {0}")]
    Corruption(String),
}

pub type Result<T> = std::result::Result<T, EventStoreError>;

// Event record format in WAL is:
// - 4 bytes: length of encoded event
// - N bytes: serialized event payload
// - 4 bytes: CRC32 checksum

// EventStore implementation
pub struct EventStore {
    wal_file: Mutex<File>,
    // path is kept for future extensions (e.g., index files, snapshots)
    path: PathBuf,
}

impl EventStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let wal_path = path.join("eventstore.wal");

        // Create directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        // Remove existing WAL file if it exists
        if wal_path.exists() {
            std::fs::remove_file(&wal_path)?;
        }

        // Open or create WAL file
        let wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        Ok(EventStore {
            wal_file: Mutex::new(wal_file),
            path,
        })
    }

    // Helper method to get a lock on the WAL file
    fn lock_wal(&self) -> Result<MutexGuard<File>> {
        self.wal_file.lock().map_err(|_| {
            EventStoreError::Io(io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on WAL file",
            ))
        })
    }

    // Helper method to write an event record to the WAL
    fn write_event_record(file: &mut File, event: &Event) -> Result<()> {
        let payload = serialize(event)?;
        let length = payload.len() as u32;
        let checksum = CRC.checksum(&payload);

        // Write length
        file.write_all(&length.to_le_bytes())?;

        // Write payload
        file.write_all(&payload)?;

        // Write checksum
        file.write_all(&checksum.to_le_bytes())?;

        Ok(())
    }

    // Helper method to read an event record from the WAL
    fn read_event_record(file: &mut File) -> Result<Option<(Event, u64)>> {
        let position = file.stream_position()?;

        // Read length
        let mut length_bytes = [0u8; 4];
        match file.read_exact(&mut length_bytes) {
            Ok(_) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(EventStoreError::Io(e)),
        }
        let length = u32::from_le_bytes(length_bytes) as usize;

        // Read payload
        let mut payload = vec![0u8; length];
        match file.read_exact(&mut payload) {
            Ok(_) => {},
            Err(e) => {
                // If we can't read the full payload, the file might be corrupted
                // Seek back to the original position
                file.seek(SeekFrom::Start(position))?;
                return Err(EventStoreError::Corruption(format!("Failed to read payload: {}", e)));
            }
        }

        // Read checksum
        let mut checksum_bytes = [0u8; 4];
        match file.read_exact(&mut checksum_bytes) {
            Ok(_) => {},
            Err(e) => {
                // If we can't read the checksum, the file might be corrupted
                // Seek back to the original position
                file.seek(SeekFrom::Start(position))?;
                return Err(EventStoreError::Corruption(format!("Failed to read checksum: {}", e)));
            }
        }
        let checksum = u32::from_le_bytes(checksum_bytes);

        // Verify checksum
        let calculated_checksum = CRC.checksum(&payload);
        if calculated_checksum != checksum {
            // Checksum mismatch, the file might be corrupted
            // Seek back to the original position
            file.seek(SeekFrom::Start(position))?;
            return Err(EventStoreError::Corruption(format!("Checksum mismatch: expected {}, got {}", checksum, calculated_checksum)));
        }

        // Deserialize event
        let event: Event = deserialize(&payload)?;

        Ok(Some((event, position)))
    }

    // Public API methods
    pub fn read(
        &self,
        query: Option<Query>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> Result<(Vec<SequencedEvent>, Option<u64>)> {
        let mut wal = self.lock_wal()?;
        wal.seek(SeekFrom::Start(0))?;

        let mut events = Vec::new();
        let mut event_positions = Vec::new();
        let mut filtered_positions = Vec::new();
        let limit_option = limit;
        let limit = limit.unwrap_or(usize::MAX);

        if limit == 0 {
            return Ok((Vec::new(), None));
        }

        // First pass: read all events and their positions
        while let Some((event, pos)) = EventStore::read_event_record(&mut wal)? {
            event_positions.push((event, pos));
        }

        // Sort events by position (should already be in order, but just to be safe)
        event_positions.sort_by_key(|(_, pos)| *pos);

        // Second pass: filter events
        for (i, (event, _)) in event_positions.iter().enumerate() {
            let position = i as u64 + 1; // 1-based position

            // Skip events up to the 'after' position
            if let Some(after_pos) = after {
                if position <= after_pos {
                    continue;
                }
            }

            // Check if the event matches the query
            if let Some(ref query) = query {
                if !Self::event_matches_query(event, query) {
                    continue;
                }
            }

            // Add the position to the filtered positions
            filtered_positions.push(position);
        }

        // Apply limit
        let filtered_positions = if limit < filtered_positions.len() {
            filtered_positions[0..limit].to_vec()
        } else {
            filtered_positions
        };

        // Add events to the result
        for position in &filtered_positions {
            let index = (*position - 1) as usize;
            let (event, _) = &event_positions[index];
            events.push(SequencedEvent {
                position: *position,
                event: event.clone(),
            });
        }

        // Calculate the head
        let head = if !event_positions.is_empty() {
            // If a limit was applied and we have filtered positions, return the last position in the result
            if limit_option.is_some() && !filtered_positions.is_empty() && filtered_positions.len() == limit {
                Some(filtered_positions[filtered_positions.len() - 1])
            } else {
                // Otherwise, return the total number of events
                Some(event_positions.len() as u64)
            }
        } else {
            None
        };

        Ok((events, head))
    }

    pub fn append(
        &self,
        events: Vec<Event>,
        condition: Option<AppendCondition>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }

        // Check condition if provided
        if let Some(condition) = condition {
            let (matching_events, _) = self.read(
                Some(condition.fail_if_events_match),
                condition.after,
                None,
            )?;

            if !matching_events.is_empty() {
                return Err(EventStoreError::IntegrityError);
            }
        }

        // Append events
        let mut wal = self.lock_wal()?;

        // Count existing events
        wal.seek(SeekFrom::Start(0))?;
        let mut event_count = 0;
        while let Some(_) = EventStore::read_event_record(&mut wal)? {
            event_count += 1;
        }

        // Write events
        for event in &events {
            Self::write_event_record(&mut wal, event)?;
        }

        // Ensure durability
        wal.sync_all()?;

        // Return the position of the last event (1-based)
        Ok(event_count + events.len() as u64)
    }

    // Helper method to check if an event matches a query
    fn event_matches_query(event: &Event, query: &Query) -> bool {
        if query.items.is_empty() {
            return true;
        }

        for item in &query.items {
            if Self::event_matches_query_item(event, item) {
                return true;
            }
        }

        false
    }

    // Helper method to check if an event matches a query item
    fn event_matches_query_item(event: &Event, item: &QueryItem) -> bool {
        // If types are specified, check if the event type matches any of them
        if !item.types.is_empty() {
            if !item.types.contains(&event.event_type) {
                return false;
            }
        }

        // Check if all tags in the query item are present in the event
        let event_tags: HashSet<_> = event.tags.iter().collect();
        for tag in &item.tags {
            if !event_tags.contains(tag) {
                return false;
            }
        }

        true
    }
}
