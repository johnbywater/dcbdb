//! Write-ahead log (WAL) for the event store.
//!
//! This module provides a transaction-based write-ahead log implementation
//! for ensuring durability of event store operations.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use crc32fast::Hasher;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::api::DCBEvent;

// Constants
const MAGIC: &[u8; 4] = b"DCA5";
const HEADER_SIZE: usize = 20; // 4 (magic) + 4 (record_type) + 8 (txn_id) + 4 (length)
const EVENT_CRC_LEN_SIZE: usize = 8; // 4 (crc) + 4 (length)
const MAX_RECORD_SIZE: usize = 1024 * 1024; // 1MB max per event

// Record types
const RECORD_BEGIN: u32 = 1;
const RECORD_EVENT: u32 = 2;
const RECORD_COMMIT: u32 = 3;

/// Position type for event positions
pub type Position = u64;
pub const POSITION_SIZE: usize = 8;

/// Error types for WAL operations
#[derive(Debug, Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Event too large")]
    EventTooLarge,

    #[error("Invalid record: {0}")]
    InvalidRecord(String),

    #[error("Invalid magic number")]
    InvalidMagic,

    #[error("Invalid checksum")]
    InvalidChecksum,

    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Result type for WAL operations
pub type WalResult<T> = Result<T, WalError>;

/// Implement From<WalError> for std::io::Error
impl From<WalError> for std::io::Error {
    fn from(error: WalError) -> Self {
        match error {
            WalError::Io(io_error) => io_error,
            WalError::EventTooLarge => {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "Event too large")
            }
            WalError::InvalidRecord(msg) => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid record: {msg}"),
            ),
            WalError::InvalidMagic => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid magic number")
            }
            WalError::InvalidChecksum => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid checksum")
            }
            WalError::Serialization(msg) => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Serialization error: {msg}"),
            ),
        }
    }
}

/// Record header structure
#[repr(C)]
struct RecordHeader {
    magic: [u8; 4],
    record_type: u32,
    txn_id: u64,
    length: u32,
}

impl RecordHeader {
    fn new(record_type: u32, txn_id: u64, length: u32) -> Self {
        Self {
            magic: [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3]],
            record_type,
            txn_id,
            length,
        }
    }

    fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.record_type.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.txn_id.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.length.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> WalResult<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(WalError::InvalidRecord("Header too short".to_string()));
        }

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);

        if magic != [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3]] {
            return Err(WalError::InvalidMagic);
        }

        let record_type = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let txn_id = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let length = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);

        Ok(Self {
            magic,
            record_type,
            txn_id,
            length,
        })
    }
}

/// Event with position for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DCBEventWithPosition {
    pub position: Position,
    pub type_: String,
    pub data: Vec<u8>,
    pub tags: Vec<String>,
}

impl From<(Position, DCBEvent)> for DCBEventWithPosition {
    fn from((position, event): (Position, DCBEvent)) -> Self {
        Self {
            position,
            type_: event.event_type,
            data: event.data,
            tags: event.tags,
        }
    }
}

/// Transaction Write-Ahead Log
pub struct TransactionWAL {
    file: File,
    buffered: Mutex<Vec<Vec<u8>>>,
    commit_offset: u64,
}

impl TransactionWAL {
    pub fn file_size(&self) -> WalResult<u64> {
        Ok(self.file.metadata()?.len())
    }
}
impl TransactionWAL {
    const WAL_FILE_NAME: &'static str = "wal.dat";

    /// Create a new TransactionWAL at the specified directory
    pub fn new<P: AsRef<Path>>(path: P) -> WalResult<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let wal_path = path_buf.join(Self::WAL_FILE_NAME);

        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&wal_path)?;

        Ok(Self {
            file,
            buffered: Mutex::new(Vec::new()),
            commit_offset: 0,
        })
    }

    /// Encode a record with the given type, transaction ID, and payload
    pub fn commit_offset(&self) -> u64 {
        self.commit_offset
    }

    /// Encode a record with the given type, transaction ID, and payload
    pub fn encode_record(&self, record_type: u32, txn_id: u64, payload: &[u8]) -> Vec<u8> {
        let header = RecordHeader::new(record_type, txn_id, payload.len() as u32);
        let mut record = Vec::with_capacity(HEADER_SIZE + payload.len());
        record.extend_from_slice(&header.to_bytes());
        record.extend_from_slice(payload);
        record
    }

    /// Begin a transaction with the given ID
    pub fn begin_transaction(&self, txn_id: u64) -> WalResult<()> {
        let record = self.encode_record(RECORD_BEGIN, txn_id, &[]);
        let mut buffered = self.buffered.lock().unwrap();
        buffered.push(record);
        Ok(())
    }

    /// Write an event to the transaction
    pub fn write_event(&self, txn_id: u64, payload: &[u8]) -> WalResult<()> {
        if payload.len() > MAX_RECORD_SIZE {
            return Err(WalError::EventTooLarge);
        }

        let record = self.encode_record(RECORD_EVENT, txn_id, payload);
        let mut buffered = self.buffered.lock().unwrap();
        buffered.push(record);
        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&mut self, txn_id: u64) -> WalResult<()> {
        let record = self.encode_record(RECORD_COMMIT, txn_id, &[]);
        {
            let mut buffered = self.buffered.lock().unwrap();
            buffered.push(record);
        }

        // self.write_flush_and_sync()?;
        Ok(())
    }

    /// Write, flush, and sync the buffered records
    pub fn write_flush_and_sync(&mut self) -> WalResult<()> {
        let buffered = {
            let mut buffered_lock = self.buffered.lock().unwrap();
            let buffered_copy = buffered_lock.clone();
            buffered_lock.clear();
            buffered_copy
        };

        {
            self.file.seek(SeekFrom::End(0))?;
        }

        for record in buffered {
            self.file.write_all(&record)?;
        }

        self.flush_and_sync()?;
        self.commit_offset = self.file.stream_position()?;

        Ok(())
    }

    /// Flush and sync the file
    fn flush_and_sync(&mut self) -> WalResult<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Read committed transactions from the WAL
    pub fn read_committed_transactions(&mut self) -> WalResult<Vec<(u64, Vec<Vec<u8>>)>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut txn_events: std::collections::HashMap<u64, Vec<Vec<u8>>> =
            std::collections::HashMap::new();
        let mut committed_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut last_good_offset: u64 = 0;

        loop {
            match self.read_next_record() {
                Ok(Some((record_type, txn_id, payload))) => {
                    last_good_offset = self.file.stream_position()?;

                    match record_type {
                        RECORD_BEGIN => {
                            txn_events.insert(txn_id, Vec::new());
                        }
                        RECORD_EVENT => {
                            if let Some(events) = txn_events.get_mut(&txn_id) {
                                events.push(payload.to_vec());
                            }
                        }
                        RECORD_COMMIT => {
                            if txn_events.contains_key(&txn_id) {
                                committed_txns.insert(txn_id);
                                self.commit_offset = self.file.stream_position()?;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    // Truncate file at last good record
                    self.file.set_len(last_good_offset)?;
                    self.file.seek(SeekFrom::Start(last_good_offset))?;
                    self.flush_and_sync()?;
                    break;
                }
            }
        }

        let result: Vec<(u64, Vec<Vec<u8>>)> = committed_txns
            .into_iter()
            .filter_map(|txn_id| {
                txn_events
                    .get(&txn_id)
                    .map(|events| (txn_id, events.clone()))
            })
            .collect();

        Ok(result)
    }

    /// Read the next record from the file
    fn read_next_record(&mut self) -> WalResult<Option<(u32, u64, Vec<u8>)>> {
        // Check if there's any data left to read
        let current_pos = self.file.stream_position()?;
        let file_size = self.file.metadata()?.len();

        if current_pos >= file_size {
            return Ok(None);
        }

        // If there's less than HEADER_SIZE bytes left, it's corrupted
        if file_size - current_pos < HEADER_SIZE as u64 {
            return Err(WalError::InvalidRecord("Incomplete header".to_string()));
        }

        let mut header_bytes = [0u8; HEADER_SIZE];
        match self.file.read_exact(&mut header_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalError::InvalidRecord(
                    "Unexpected EOF during header read".to_string(),
                ));
            }
            Err(e) => return Err(WalError::Io(e)),
        }

        let header = RecordHeader::from_bytes(&header_bytes)?;

        if header.length > MAX_RECORD_SIZE as u32 {
            return Err(WalError::InvalidRecord("Payload too large".to_string()));
        }

        // Check if there's enough data left for the payload
        let current_pos = self.file.stream_position()?;
        if file_size - current_pos < header.length as u64 {
            return Err(WalError::InvalidRecord("Incomplete payload".to_string()));
        }

        let mut payload = vec![0u8; header.length as usize];
        match self.file.read_exact(&mut payload) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(WalError::InvalidRecord("Incomplete payload".to_string()));
            }
            Err(e) => return Err(WalError::Io(e)),
        }

        // Check CRC for event records
        if header.record_type == RECORD_EVENT && payload.len() >= EVENT_CRC_LEN_SIZE {
            let crc = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let data = &payload[EVENT_CRC_LEN_SIZE..];

            if calc_crc(data) != crc {
                return Err(WalError::InvalidChecksum);
            }
        }

        Ok(Some((header.record_type, header.txn_id, payload)))
    }

    /// Truncate the WAL before the given checkpoint transaction ID
    pub fn truncate_wal_before_checkpoint(&mut self, txn_id: u64) -> WalResult<()> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut cut_offset: Option<u64> = None;

        loop {
            match self.read_next_record() {
                Ok(Some((record_type, record_txn_id, _))) => {
                    if record_type == RECORD_COMMIT && record_txn_id == txn_id {
                        cut_offset = Some(self.file.stream_position()?);
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        if let Some(offset) = cut_offset {
            self.cut_before_offset(offset)?;
        }

        Ok(())
    }

    /// Cut the WAL file before the given offset
    pub fn cut_before_offset(&mut self, cut_offset: u64) -> WalResult<()> {
        self.file.seek(SeekFrom::Start(cut_offset))?;

        let mut keep_data = Vec::new();
        self.file.read_to_end(&mut keep_data)?;

        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&keep_data)?;

        self.flush_and_sync()?;
        self.commit_offset = self.file.stream_position()?;

        Ok(())
    }

    /// Close the WAL file
    pub fn close(&self) -> WalResult<()> {
        // Nothing special to do here as the file is automatically closed when dropped
        Ok(())
    }
}

impl Drop for TransactionWAL {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Calculate CRC32 checksum for data
pub fn calc_crc(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Pack a DCB event with CRC
pub fn pack_dcb_event_with_crc(position: Position, dcb_event: DCBEvent) -> Vec<u8> {
    let event_with_pos = DCBEventWithPosition::from((position, dcb_event));
    let blob = serialize_dcb_event(&event_with_pos).unwrap_or_default();

    let crc = calc_crc(&blob);
    let len = blob.len() as u32;

    let mut result = Vec::with_capacity(EVENT_CRC_LEN_SIZE + blob.len());
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&len.to_le_bytes());
    result.extend_from_slice(&blob);

    result
}

/// Serialize a DCB event with position
fn serialize_dcb_event(event: &DCBEventWithPosition) -> WalResult<Vec<u8>> {
    let mut buf = Vec::new();
    event
        .serialize(&mut Serializer::new(&mut buf))
        .map_err(|e| WalError::Serialization(e.to_string()))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let wal = TransactionWAL::new(temp_dir.path()).unwrap();

        let wal_path = temp_dir.path().join(TransactionWAL::WAL_FILE_NAME);
        assert!(wal_path.exists());
        assert!(wal.buffered.lock().unwrap().is_empty());
    }

    #[test]
    fn test_encode_record() {
        let temp_dir = TempDir::new().unwrap();
        let wal = TransactionWAL::new(temp_dir.path()).unwrap();

        let record_type: u32 = RECORD_EVENT;
        let txn_id: u64 = 123;
        let payload: &[u8] = b"test payload";

        let encoded = wal.encode_record(record_type, txn_id, payload);

        // Check header
        let header = RecordHeader::from_bytes(&encoded[..HEADER_SIZE]).unwrap();
        assert_eq!(header.magic, [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3]]);
        assert_eq!(header.record_type, record_type);
        assert_eq!(header.txn_id, txn_id);
        assert_eq!(header.length as usize, payload.len());

        // Check payload
        let actual_payload = &encoded[HEADER_SIZE..HEADER_SIZE + payload.len()];
        assert_eq!(actual_payload, payload);
    }

    #[test]
    fn test_begin_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let wal = TransactionWAL::new(temp_dir.path()).unwrap();

        let txn_id = 42;
        wal.begin_transaction(txn_id).unwrap();

        // Check 'buffered' has a BEGIN record
        let buffered = wal.buffered.lock().unwrap();
        assert_eq!(buffered.len(), 1);

        let encoded = &buffered[0];
        let header = RecordHeader::from_bytes(&encoded[..HEADER_SIZE]).unwrap();

        assert_eq!(header.magic, [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3]]);
        assert_eq!(header.record_type, RECORD_BEGIN);
        assert_eq!(header.txn_id, txn_id);
        assert_eq!(header.length, 0);
    }

    #[test]
    fn test_write_event() {
        let temp_dir = TempDir::new().unwrap();
        let wal = TransactionWAL::new(temp_dir.path()).unwrap();

        let txn_id = 42;
        let payload = br#"{"event_type": "test_event", "data": "test_data"}"#;

        wal.write_event(txn_id, payload).unwrap();

        let buffered = wal.buffered.lock().unwrap();
        assert_eq!(buffered.len(), 1);

        // Verify the record is correctly encoded
        let encoded = &buffered[0];
        let header = RecordHeader::from_bytes(&encoded[..HEADER_SIZE]).unwrap();

        assert_eq!(header.magic, [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3]]);
        assert_eq!(header.record_type, RECORD_EVENT);
        assert_eq!(header.txn_id, txn_id);
        assert_eq!(header.length as usize, payload.len());

        // Check payload
        let payload_end = HEADER_SIZE + payload.len();
        let payload_copy = &encoded[HEADER_SIZE..payload_end];
        assert_eq!(payload_copy, payload);
    }

    #[test]
    fn test_commit_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = TransactionWAL::new(temp_dir.path()).unwrap();

        // First add some events
        let txn_id = 42;
        wal.write_event(txn_id, br#"{"event_type": "test_event"}"#)
            .unwrap();

        wal.commit_transaction(txn_id).unwrap();

        // Commit should clear the buffer
        wal.write_flush_and_sync().unwrap();

        assert!(wal.buffered.lock().unwrap().is_empty());

        // Verify file contains the records
        let file_size = wal.file_size().unwrap();
        assert!(file_size > 0);
    }

    #[test]
    fn test_event_too_large() {
        let temp_dir = TempDir::new().unwrap();
        let wal = TransactionWAL::new(temp_dir.path()).unwrap();

        let txn_id = 42;
        let large_event = vec![b'x'; MAX_RECORD_SIZE + 1];

        let result = wal.write_event(txn_id, &large_event);
        assert!(matches!(result, Err(WalError::EventTooLarge)));
    }

    #[test]
    fn test_flush_and_sync() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = TransactionWAL::new(temp_dir.path()).unwrap();

        let txn_id = 42;
        wal.begin_transaction(txn_id).unwrap();
        wal.write_event(txn_id, br#"{"event_type": "test_event"}"#)
            .unwrap();

        // Buffer should have records
        assert_eq!(wal.buffered.lock().unwrap().len(), 2);

        // Manually call write_flush_and_sync
        wal.write_flush_and_sync().unwrap();

        // Buffer should be empty
        assert!(wal.buffered.lock().unwrap().is_empty());

        // File should have data
        let file_size = wal.file_size().unwrap();
        assert!(file_size > 0);
    }

    #[test]
    fn test_complete_transaction_flow() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = TransactionWAL::new(temp_dir.path()).unwrap();

        // Begin transaction
        let txn_id = 42;
        wal.begin_transaction(txn_id).unwrap();

        // Write multiple events
        let payloads = [
            br#"{"event_type": "event1", "data": "data1"}"#,
            br#"{"event_type": "event2", "data": "data2"}"#,
            br#"{"event_type": "event3", "data": "data3"}"#,
        ];

        for payload in &payloads {
            wal.write_event(txn_id, *payload).unwrap();
        }

        // Commit transaction
        wal.commit_transaction(txn_id).unwrap();
        wal.write_flush_and_sync().unwrap();

        // Buffer should be empty after commit
        assert!(wal.buffered.lock().unwrap().is_empty());

        // File should have data
        let file_size = wal.file_size().unwrap();
        assert!(file_size > 0);
    }

    #[test]
    fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = TransactionWAL::new(temp_dir.path()).unwrap();

        // Create test events
        let event1 = DCBEvent {
            event_type: "type1".to_string(),
            data: br#"{"msg": "hello"}"#.to_vec(),
            tags: vec![],
        };

        let event2 = DCBEvent {
            event_type: "type2".to_string(),
            data: br#"{"msg": "world"}"#.to_vec(),
            tags: vec![],
        };

        // Committed transaction
        let txn_id_1 = 42;
        wal.begin_transaction(txn_id_1).unwrap();

        let payload1 = pack_dcb_event_with_crc(1, event1.clone());
        wal.write_event(txn_id_1, &payload1).unwrap();
        wal.write_event(txn_id_1, &payload1).unwrap();
        wal.commit_transaction(txn_id_1).unwrap();
        wal.write_flush_and_sync().unwrap();

        // Uncommitted transaction
        let txn_id_2 = 43;
        wal.begin_transaction(txn_id_2).unwrap();

        let payload2 = pack_dcb_event_with_crc(2, event2);
        wal.write_event(txn_id_2, &payload2).unwrap();

        // Read committed transactions
        let committed = wal.read_committed_transactions().unwrap();

        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0, txn_id_1);
        assert_eq!(committed[0].1.len(), 2);
        assert_eq!(committed[0].1[0], payload1);
        assert_eq!(committed[0].1[1], payload1);
    }

    #[test]
    fn test_wal_truncation_on_corruption() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = TransactionWAL::new(temp_dir.path()).unwrap();

        // Write a good transaction
        let txn_id = 42;
        wal.begin_transaction(txn_id).unwrap();

        let event = DCBEvent {
            event_type: "type1".to_string(),
            data: br#"{"msg": "hello"}"#.to_vec(),
            tags: vec![],
        };

        let payload = pack_dcb_event_with_crc(1, event);
        wal.write_event(txn_id, &payload).unwrap();
        wal.commit_transaction(txn_id).unwrap();
        wal.write_flush_and_sync().unwrap();

        // Write a partial record manually (simulate crash)
        {
            let wal_path = temp_dir.path().join(TransactionWAL::WAL_FILE_NAME);

            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .append(true)
                .open(wal_path)
                .unwrap();

            file.write_all(b"CRASH").unwrap();
        }

        // Recover and truncate
        let recovered = wal.read_committed_transactions().unwrap();

        // File should now be truncated to the valid commit
        let wal_path = temp_dir.path().join(TransactionWAL::WAL_FILE_NAME);
        let file_content = std::fs::read(wal_path).unwrap();
        assert!(!file_content.windows(5).any(|window| window == b"CRASH"));

        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].0, txn_id);
    }

    #[test]
    fn test_truncate_wal_before_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = TransactionWAL::new(temp_dir.path()).unwrap();

        // Create test events
        let event1 = DCBEvent {
            event_type: "type1".to_string(),
            data: br#"{"msg": "hello"}"#.to_vec(),
            tags: vec![],
        };

        let event2 = DCBEvent {
            event_type: "type2".to_string(),
            data: br#"{"msg": "world"}"#.to_vec(),
            tags: vec![],
        };

        let event3 = DCBEvent {
            event_type: "type3".to_string(),
            data: br#"{"msg": "world"}"#.to_vec(),
            tags: vec![],
        };

        let payload1 = pack_dcb_event_with_crc(1, event1);
        let payload2 = pack_dcb_event_with_crc(2, event2);
        let payload3 = pack_dcb_event_with_crc(3, event3);

        // Transaction 1
        let txn_id_1 = 42;
        wal.begin_transaction(txn_id_1).unwrap();
        wal.write_event(txn_id_1, &payload1).unwrap();
        wal.commit_transaction(txn_id_1).unwrap();

        // Transaction 2
        let txn_id_2 = 43;
        wal.begin_transaction(txn_id_2).unwrap();
        wal.write_event(txn_id_2, &payload2).unwrap();
        wal.commit_transaction(txn_id_2).unwrap();

        // Transaction 3
        let txn_id_3 = 44;
        wal.begin_transaction(txn_id_3).unwrap();
        wal.write_event(txn_id_3, &payload3).unwrap();
        wal.commit_transaction(txn_id_3).unwrap();

        wal.write_flush_and_sync().unwrap();

        // Truncate before checkpoint
        wal.truncate_wal_before_checkpoint(txn_id_2).unwrap();

        // Read committed transactions
        let recovered = wal.read_committed_transactions().unwrap();

        // Only transaction 3 should remain
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].0, txn_id_3);
    }

    #[test]
    fn test_cut_before_offset() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = TransactionWAL::new(temp_dir.path()).unwrap();

        // Create test events
        let event1 = DCBEvent {
            event_type: "type1".to_string(),
            data: br#"{"msg": "hello"}"#.to_vec(),
            tags: vec![],
        };

        let event2 = DCBEvent {
            event_type: "type2".to_string(),
            data: br#"{"msg": "world"}"#.to_vec(),
            tags: vec![],
        };

        let payload1 = pack_dcb_event_with_crc(1, event1);
        let payload2 = pack_dcb_event_with_crc(2, event2);

        // Transaction 1
        let txn_id_1 = 42;
        wal.begin_transaction(txn_id_1).unwrap();
        wal.write_event(txn_id_1, &payload1).unwrap();
        wal.commit_transaction(txn_id_1).unwrap();
        wal.write_flush_and_sync().unwrap();

        // Get the file size after first transaction
        let file_size_after_txn1 = wal.file_size().unwrap();

        // Transaction 2
        let txn_id_2 = 43;
        wal.begin_transaction(txn_id_2).unwrap();
        wal.write_event(txn_id_2, &payload2).unwrap();
        wal.commit_transaction(txn_id_2).unwrap();
        wal.write_flush_and_sync().unwrap();

        // Get the file size after second transaction
        let file_size_after_txn2 = wal.file_size().unwrap();

        // Directly call cut_before_offset with the file size after first transaction
        wal.cut_before_offset(file_size_after_txn1).unwrap();

        // Verify file size is now the difference between the two sizes
        let new_file_size = wal.file_size().unwrap();
        assert_eq!(new_file_size, file_size_after_txn2 - file_size_after_txn1);

        // Read committed transactions
        let recovered = wal.read_committed_transactions().unwrap();

        // Only transaction 2 should remain
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].0, txn_id_2);
        assert_eq!(recovered[0].1.len(), 1);
        assert_eq!(recovered[0].1[0], payload2);
    }
}
