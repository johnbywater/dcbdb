// //! Segment files for the event store.
// //!
// //! This module provides a file-based segment implementation for storing events.
// 
// use std::collections::BTreeMap;
// use std::fs::{File, OpenOptions};
// use std::io::{self, Read, Seek, SeekFrom, Write};
// use std::path::{Path, PathBuf};
// use std::sync::Mutex;
// 
// use rmp_serde::decode;
// use serde::{Deserialize, Serialize};
// use thiserror::Error;
// 
// use crate::api::DCBEvent;
// use crate::crc::calc_crc;
// use crate::wal::Position;
// 
// /// Event with position for serialization
// #[derive(Debug, Clone, Serialize, Deserialize)]
// struct DCBEventWithPosition {
//     position: Position,
//     type_: String,
//     data: Vec<u8>,
//     tags: Vec<String>,
// }
// 
// impl From<(Position, DCBEvent)> for DCBEventWithPosition {
//     fn from((position, event): (Position, DCBEvent)) -> Self {
//         Self {
//             position,
//             type_: event.event_type,
//             data: event.data,
//             tags: event.tags,
//         }
//     }
// }
// 
// // Constants
// pub const EVENT_CRC_LEN_SIZE: usize = 8; // 4 (crc) + 4 (length)
// pub const DEFAULT_MAX_SEGMENT_SIZE: usize = 256 * 1024 * 1024; // 256 MB
// 
// /// Error types for segment operations
// #[derive(Debug, Error)]
// pub enum SegmentError {
//     #[error("IO error: {0}")]
//     Io(#[from] io::Error),
// 
//     #[error("Not enough data: {0}")]
//     NotEnoughData(String),
// 
//     #[error("Segment not found: {0}")]
//     SegmentNotFound(PathBuf),
// 
//     #[error("Database corrupted: {0}")]
//     DatabaseCorrupted(String),
// 
//     #[error("Serialization error: {0}")]
//     Serialization(String),
// }
// 
// /// Result type for segment operations
// pub type SegmentResult<T> = Result<T, SegmentError>;
// 
// /// Implement From<SegmentError> for std::io::Error
// impl From<SegmentError> for std::io::Error {
//     fn from(error: SegmentError) -> Self {
//         match error {
//             SegmentError::Io(io_error) => io_error,
//             SegmentError::NotEnoughData(msg) => {
//                 std::io::Error::new(std::io::ErrorKind::UnexpectedEof, msg)
//             }
//             SegmentError::SegmentNotFound(path) => std::io::Error::new(
//                 std::io::ErrorKind::NotFound,
//                 format!("Segment not found: {path:?}"),
//             ),
//             SegmentError::DatabaseCorrupted(msg) => std::io::Error::new(
//                 std::io::ErrorKind::InvalidData,
//                 format!("Database corrupted: {msg}"),
//             ),
//             SegmentError::Serialization(msg) => std::io::Error::new(
//                 std::io::ErrorKind::InvalidData,
//                 format!("Serialization error: {msg}"),
//             ),
//         }
//     }
// }
// 
// /// File-based implementation of Segment
// pub struct Segment {
//     number: u64,
//     path: PathBuf,
//     file: File,
//     write_offset: u64,
//     buffer: Vec<Vec<u8>>,
//     buffer_size: usize,
// }
// 
// impl Segment {
//     /// Create a new segment with the given number and path
//     pub fn new(number: u64, path: impl AsRef<Path>) -> SegmentResult<Self> {
//         let path_buf = path.as_ref().to_path_buf();
//         let file = OpenOptions::new()
//             .read(true)
//             .write(true)
//             .create(true)
//             .truncate(false)
//             .open(&path_buf)?;
// 
//         file.sync_all()?;
// 
//         let write_offset = file.metadata()?.len();
// 
//         Ok(Self {
//             number,
//             path: path_buf,
//             file,
//             write_offset,
//             buffer: Vec::new(),
//             buffer_size: 0,
//         })
//     }
// 
//     /// Get the segment number
//     pub fn number(&self) -> u64 {
//         self.number
//     }
// 
//     /// Get the segment number
//     pub fn write_offset(&self) -> u64 {
//         self.write_offset
//     }
// 
//     /// Get the segment path
//     pub fn path(&self) -> &Path {
//         &self.path
//     }
// 
//     /// Get the total size of the segment (file + buffer)
//     pub fn size(&self) -> u64 {
//         self.file_size() + self.buffer_size as u64
//     }
// 
//     /// Get the file size
//     pub fn file_size(&self) -> u64 {
//         self.write_offset
//     }
// 
//     /// Get the buffer size
//     pub fn buffer_size(&self) -> usize {
//         self.buffer_size
//     }
// 
//     /// Set the write offset
//     pub fn set_write_offset(&mut self, offset: u64) {
//         self.write_offset = offset;
//     }
// 
//     /// Add a payload to the segment
//     pub fn add(&mut self, payload: Vec<u8>) -> u64 {
//         let offset = self.write_offset + self.buffer_size as u64;
//         self.buffer.push(payload.clone());
//         self.buffer_size += payload.len();
//         offset
//     }
// 
//     /// Flush the buffer to disk
//     pub fn flush(&mut self) -> SegmentResult<()> {
//         self.file.seek(SeekFrom::End(0))?;
// 
//         for payload in &self.buffer {
//             self.file.write_all(payload)?;
//         }
// 
//         self.write_offset += self.buffer_size as u64;
//         self.file.flush()?;
//         self.buffer.clear();
//         self.buffer_size = 0;
// 
//         // Ensure data is written to disk
//         self.file.sync_all()?;
// 
//         Ok(())
//     }
// 
//     /// Iterate over event records in the segment
//     pub fn iter_event_records(
//         &mut self,
//     ) -> impl Iterator<Item = SegmentResult<(Position, DCBEvent, u64)>> + '_ {
//         EventRecordIterator {
//             segment: self,
//             offset: 0,
//         }
//     }
// 
//     /// Get an event record at the given offset
//     pub fn get_event_record(&mut self, offset: u64) -> SegmentResult<(Position, DCBEvent, usize)> {
//         read_event_record(&mut self.file, offset)
//     }
// 
//     /// Close the segment
//     pub fn close(&mut self) -> SegmentResult<()> {
//         if self.buffer_size > 0 {
//             self.flush()?;
//         }
//         Ok(())
//     }
// }
// 
// impl Drop for Segment {
//     fn drop(&mut self) {
//         let _ = self.close();
//     }
// }
// 
// /// Iterator over event records in a segment
// struct EventRecordIterator<'a> {
//     segment: &'a mut Segment,
//     offset: u64,
// }
// 
// impl<'a> Iterator for EventRecordIterator<'a> {
//     type Item = SegmentResult<(Position, DCBEvent, u64)>;
// 
//     fn next(&mut self) -> Option<Self::Item> {
//         match self.segment.get_event_record(self.offset) {
//             Ok((position, dcb_event, length)) => {
//                 let current_offset = self.offset;
//                 self.offset += length as u64;
//                 Some(Ok((position, dcb_event, current_offset)))
//             }
//             Err(SegmentError::NotEnoughData(_)) => None,
//             Err(e) => Some(Err(e)),
//         }
//     }
// }
// 
// /// File-based implementation of SegmentManager
// pub struct SegmentManager {
//     current_segment_number: u64,
//     path: PathBuf,
//     max_segment_size: usize,
//     pub current_segment: Segment,
//     segment_cache: Mutex<BTreeMap<u32, Box<Segment>>>,
//     segment_cache_size: usize,
// }
// 
// impl SegmentManager {
//     /// Create a new SegmentManager with the given path and max segment size
//     pub fn new(path: impl AsRef<Path>, max_segment_size: Option<usize>) -> SegmentResult<Self> {
//         let path_buf = path.as_ref().to_path_buf();
//         std::fs::create_dir_all(&path_buf)?;
// 
//         let max_size = max_segment_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE);
// 
//         // Find the current segment number
//         let mut current_segment_number: u64 = 0;
// 
//         for entry in std::fs::read_dir(&path_buf)? {
//             let entry = entry?;
//             let file_name = entry.file_name();
//             let file_name_str = file_name.to_string_lossy();
// 
//             if file_name_str.starts_with("segment-") && file_name_str.ends_with(".dat") {
//                 if let Some(num_str) = file_name_str
//                     .strip_prefix("segment-")
//                     .and_then(|s| s.strip_suffix(".dat"))
//                 {
//                     if let Ok(num) = num_str.parse::<u64>() {
//                         current_segment_number = current_segment_number.max(num);
//                     }
//                 }
//             }
//         }
// 
//         let current_segment = if current_segment_number > 0 {
//             // Open existing segment
//             let segment_path = Self::make_segment_path(&path_buf, current_segment_number);
//             Segment::new(current_segment_number, segment_path)?
//         } else {
//             // Create the first segment
//             current_segment_number = 1;
//             let segment_path = Self::make_segment_path(&path_buf, current_segment_number);
//             Segment::new(current_segment_number, segment_path)?
//         };
// 
//         Ok(Self {
//             current_segment_number,
//             path: path_buf,
//             max_segment_size: max_size,
//             current_segment,
//             segment_cache: Mutex::new(BTreeMap::new()),
//             segment_cache_size: 16,
//         })
//     }
// 
//     /// Add payloads to the segment manager
//     pub fn add_payloads(&mut self, payloads: &[Vec<u8>]) -> SegmentResult<Vec<(u64, u64)>> {
//         let mut segment_numbers_and_offsets = Vec::with_capacity(payloads.len());
//         let payloads_size: usize = payloads.iter().map(|p| p.len()).sum();
// 
//         // Check if we need to rotate to a new segment
//         if self.current_segment.size() + payloads_size as u64 > self.max_segment_size as u64 {
//             self.current_segment.flush()?;
// 
//             // Create a new segment
//             self.current_segment_number += 1;
//             let segment_path = Self::make_segment_path(&self.path, self.current_segment_number);
//             let new_segment = Segment::new(self.current_segment_number, segment_path)?;
// 
//             // Update the current segment
//             self.current_segment = new_segment;
//         }
// 
//         // Add payloads to the current segment
//         for payload in payloads {
//             let offset = self.current_segment.add(payload.clone());
//             segment_numbers_and_offsets.push((self.current_segment.number(), offset));
//         }
// 
//         Ok(segment_numbers_and_offsets)
//     }
// 
//     /// Get an iterator over all segments
//     pub fn segments(&self) -> impl Iterator<Item = SegmentResult<Segment>> + '_ {
//         SegmentIterator {
//             manager: self,
//             current_number: 1,
//             stop_after_current: self.current_segment_number > 0,
//         }
//     }
// 
//     /// Recover from a specific position
//     pub fn recover_position(&mut self, segment_number: u64, offset: u64) -> SegmentResult<()> {
//         if segment_number == 0 && offset == 0 {
//             // Create first segment
//             self.current_segment_number = 1;
//             let segment_path = Self::make_segment_path(&self.path, 1);
//             self.current_segment = Segment::new(1, segment_path)?;
//         } else {
//             // Get the specified segment
//             let segment_path = Self::make_segment_path(&self.path, segment_number);
//             let mut segment = Segment::new(segment_number, segment_path)?;
//             segment.set_write_offset(offset);
// 
//             // Update the current segment
//             self.current_segment_number = segment_number;
//             self.current_segment = segment;
//         }
// 
//         Ok(())
//     }
// 
//     /// Get a segment by number
//     pub fn get_segment(&self, segment_number: u64) -> SegmentResult<Segment> {
//         // Check if the segment path exists
//         let segment_path = Self::make_segment_path(&self.path, segment_number);
// 
//         if !segment_path.exists() {
//             return Err(SegmentError::SegmentNotFound(segment_path));
//         }
// 
//         // Create a new segment
//         let segment = Segment::new(segment_number, segment_path)?;
// 
//         Ok(segment)
//     }
// 
//     /// Make a segment path
//     fn make_segment_path(base_path: &Path, segment_number: u64) -> PathBuf {
//         base_path.join(format!("segment-{segment_number:08}.dat"))
//     }
// 
//     /// Flush all segments
//     pub fn flush(&mut self) -> SegmentResult<()> {
//         // Flush the current segment
//         self.current_segment.flush()?;
// 
//         // Trim cache if needed
//         let mut cache = self.segment_cache.lock().unwrap();
// 
//         while cache.len() > self.segment_cache_size {
//             if let Some((&key, _)) = cache.first_key_value() {
//                 if let Some(mut segment) = cache.remove(&key) {
//                     segment.close()?;
//                 }
//             }
//         }
// 
//         Ok(())
//     }
// 
//     /// Close all segments
//     pub fn close(&mut self) -> SegmentResult<()> {
//         // Flush and close the current segment
//         self.current_segment.close()?;
// 
//         // Close all segments in the cache
//         let mut cache = self.segment_cache.lock().unwrap();
// 
//         while !cache.is_empty() {
//             if let Some((&key, _)) = cache.first_key_value() {
//                 if let Some(mut segment) = cache.remove(&key) {
//                     segment.close()?;
//                 }
//             }
//         }
// 
//         Ok(())
//     }
// }
// 
// impl Drop for SegmentManager {
//     fn drop(&mut self) {
//         let _ = self.close();
//     }
// }
// 
// /// Iterator over segments
// struct SegmentIterator<'a> {
//     manager: &'a SegmentManager,
//     current_number: u64,
//     stop_after_current: bool,
// }
// 
// impl<'a> Iterator for SegmentIterator<'a> {
//     type Item = SegmentResult<Segment>;
// 
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.stop_after_current && self.current_number > self.manager.current_segment_number {
//             return None;
//         }
// 
//         match self.manager.get_segment(self.current_number) {
//             Ok(segment) => {
//                 self.current_number += 1;
//                 Some(Ok(segment))
//             }
//             Err(SegmentError::SegmentNotFound(_)) => None,
//             Err(e) => Some(Err(e)),
//         }
//     }
// }
// 
// /// Deserialize a DCB event from bytes
// pub fn deserialize_dcb_event(data: &[u8]) -> SegmentResult<(Position, DCBEvent)> {
//     let event_with_pos: DCBEventWithPosition =
//         decode::from_slice(data).map_err(|e| SegmentError::Serialization(e.to_string()))?;
// 
//     let position = event_with_pos.position;
//     let event = DCBEvent {
//         event_type: event_with_pos.type_,
//         data: event_with_pos.data,
//         tags: event_with_pos.tags,
//     };
// 
//     Ok((position, event))
// }
// 
// /// Read an event record from a file
// pub fn read_event_record(
//     file: &mut File,
//     offset: u64,
// ) -> SegmentResult<(Position, DCBEvent, usize)> {
//     // Get file path for better error messages
//     let file_path = match file.metadata() {
//         Ok(metadata) => {
//             if let Ok(path) = std::env::current_dir() {
//                 path.join(format!("unknown_file_{}", metadata.len()))
//             } else {
//                 PathBuf::from(format!("unknown_file_{}", metadata.len()))
//             }
//         }
//         Err(_) => PathBuf::from("unknown_file"),
//     };
// 
//     // Seek to the offset
//     if let Err(e) = file.seek(SeekFrom::Start(offset)) {
//         return Err(SegmentError::Io(io::Error::new(
//             e.kind(),
//             format!("Failed to seek to offset {offset} in file {file_path:?}: {e}",),
//         )));
//     }
// 
//     // Read CRC and length
//     let mut crc_len_bytes = [0u8; EVENT_CRC_LEN_SIZE];
//     match file.read_exact(&mut crc_len_bytes) {
//         Ok(_) => {}
//         Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
//             let msg = format!(
//                 "Not enough data for event crc and blob length at offset {offset} in file {file_path:?}",
//             );
//             return Err(SegmentError::NotEnoughData(msg));
//         }
//         Err(e) => {
//             let msg = format!(
//                 "Failed to read crc and length at offset {offset} in file {file_path:?}: {e}",
//             );
//             return Err(SegmentError::Io(io::Error::new(e.kind(), msg)));
//         }
//     }
// 
//     let crc = u32::from_le_bytes([
//         crc_len_bytes[0],
//         crc_len_bytes[1],
//         crc_len_bytes[2],
//         crc_len_bytes[3],
//     ]);
//     let blob_len = u32::from_le_bytes([
//         crc_len_bytes[4],
//         crc_len_bytes[5],
//         crc_len_bytes[6],
//         crc_len_bytes[7],
//     ]);
// 
//     // Read blob
//     let mut blob = vec![0u8; blob_len as usize];
//     match file.read_exact(&mut blob) {
//         Ok(_) => {}
//         Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
//             let msg = format!(
//                 "Not enough data for blob (expected {} bytes) at offset {} in file {:?}",
//                 blob_len,
//                 offset + EVENT_CRC_LEN_SIZE as u64,
//                 file_path
//             );
//             return Err(SegmentError::NotEnoughData(msg));
//         }
//         Err(e) => {
//             let msg = format!(
//                 "Failed to read blob (size {}) at offset {} in file {:?}: {}",
//                 blob_len,
//                 offset + EVENT_CRC_LEN_SIZE as u64,
//                 file_path,
//                 e
//             );
//             return Err(SegmentError::Io(io::Error::new(e.kind(), msg)));
//         }
//     }
// 
//     // Check CRC
//     check_crc(&blob, crc)?;
// 
//     // Deserialize event
//     let (position, dcb_event) = deserialize_dcb_event(&blob)?;
// 
//     Ok((position, dcb_event, EVENT_CRC_LEN_SIZE + blob_len as usize))
// }
// 
// /// Check CRC of data
// pub fn check_crc(blob: &[u8], crc: u32) -> SegmentResult<()> {
//     if crc != calc_crc(blob) {
//         return Err(SegmentError::DatabaseCorrupted("CRC mismatch".to_string()));
//     }
//     Ok(())
// }
// 
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::api::DCBEvent;
//     use crate::wal::pack_dcb_event_with_crc;
//     use tempfile::TempDir;
// 
//     #[test]
//     fn test_segment_initialization() {
//         let temp_dir = TempDir::new().unwrap();
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
// 
//         let segment = Segment::new(1, &segment_path).unwrap();
// 
//         assert_eq!(segment.number(), 1);
//         assert_eq!(segment.path(), segment_path.as_path());
//         assert_eq!(segment.size(), 0);
//         assert_eq!(segment.file_size(), 0);
//         assert_eq!(segment.buffer_size(), 0);
//         assert!(segment_path.exists());
//     }
// 
//     #[test]
//     fn test_segment_add_and_flush() {
//         let temp_dir = TempDir::new().unwrap();
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
// 
//         let mut segment = Segment::new(1, &segment_path).unwrap();
// 
//         // Create test payload
//         let payload = b"test payload".to_vec();
// 
//         // Add payload to segment
//         let offset = segment.add(payload.clone());
// 
//         // Check offset and buffer size
//         assert_eq!(offset, 0);
//         assert_eq!(segment.buffer_size(), payload.len());
//         assert_eq!(segment.file_size(), 0);
//         assert_eq!(segment.size(), payload.len() as u64);
// 
//         // Flush to disk
//         segment.flush().unwrap();
// 
//         // Check file size and buffer size after flush
//         assert_eq!(segment.buffer_size(), 0);
//         assert_eq!(segment.file_size(), payload.len() as u64);
//         assert_eq!(segment.size(), payload.len() as u64);
// 
//         // Verify file content
//         let content = std::fs::read(&segment_path).unwrap();
//         assert_eq!(content, payload);
//     }
// 
//     #[test]
//     fn test_segment_multiple_adds() {
//         let temp_dir = TempDir::new().unwrap();
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
// 
//         let mut segment = Segment::new(1, &segment_path).unwrap();
// 
//         let payloads = vec![
//             b"payload1".to_vec(),
//             b"payload2".to_vec(),
//             b"payload3".to_vec(),
//         ];
// 
//         let mut offsets = Vec::new();
// 
//         // Add multiple payloads
//         for payload in &payloads {
//             let offset = segment.add(payload.clone());
//             offsets.push(offset);
//         }
// 
//         // Check offsets and buffer size
//         let expected_offsets = vec![
//             0,
//             payloads[0].len() as u64,
//             (payloads[0].len() + payloads[1].len()) as u64,
//         ];
// 
//         assert_eq!(offsets, expected_offsets);
//         assert_eq!(
//             segment.buffer_size(),
//             payloads.iter().map(|p| p.len()).sum()
//         );
// 
//         // Flush to disk
//         segment.flush().unwrap();
// 
//         // Check file size after flush
//         assert_eq!(
//             segment.file_size(),
//             payloads.iter().map(|p| p.len()).sum::<usize>() as u64
//         );
// 
//         // Verify file content
//         let content = std::fs::read(&segment_path).unwrap();
//         let expected_content: Vec<u8> = payloads.iter().flat_map(|p| p.clone()).collect();
//         assert_eq!(content, expected_content);
//     }
// 
//     #[test]
//     fn test_segment_set_write_offset() {
//         let temp_dir = TempDir::new().unwrap();
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
// 
//         let mut segment = Segment::new(1, &segment_path).unwrap();
// 
//         // Add and flush some data
//         segment.add(b"initial data".to_vec());
//         segment.flush().unwrap();
// 
//         // Set write offset to a new value
//         let new_offset = 5;
//         segment.set_write_offset(new_offset);
// 
//         // Check that the offset was updated
//         assert_eq!(segment.file_size(), new_offset);
//     }
// 
//     #[test]
//     fn test_segment_get_event_record() {
//         let temp_dir = TempDir::new().unwrap();
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
// 
//         let mut segment = Segment::new(1, &segment_path).unwrap();
// 
//         // Create a test event
//         let position = 42;
//         let event = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: br#"{"msg": "hello"}"#.to_vec(),
//             tags: Vec::new(),
//         };
// 
//         // Pack the event with CRC
//         let payload = pack_dcb_event_with_crc(position, event.clone());
// 
//         // Add and flush the payload
//         let offset = segment.add(payload);
//         segment.flush().unwrap();
// 
//         // Retrieve the event record
//         let (retrieved_position, retrieved_event, _length) =
//             segment.get_event_record(offset).unwrap();
// 
//         // Check the retrieved data
//         assert_eq!(retrieved_position, position);
//         assert_eq!(retrieved_event.event_type, event.event_type);
//         assert_eq!(retrieved_event.data, event.data);
//         assert_eq!(retrieved_event.tags, event.tags);
//     }
// 
//     #[test]
//     fn test_segment_iter_event_records() {
//         let temp_dir = TempDir::new().unwrap();
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
// 
//         let mut segment = Segment::new(1, &segment_path).unwrap();
// 
//         // Create test events
//         let events = vec![
//             (
//                 1,
//                 DCBEvent {
//                     event_type: "event1".to_string(),
//                     data: br#"{"msg": "hello"}"#.to_vec(),
//                     tags: Vec::new(),
//                 },
//             ),
//             (
//                 2,
//                 DCBEvent {
//                     event_type: "event2".to_string(),
//                     data: br#"{"msg": "world"}"#.to_vec(),
//                     tags: Vec::new(),
//                 },
//             ),
//             (
//                 3,
//                 DCBEvent {
//                     event_type: "event3".to_string(),
//                     data: br#"{"msg": "!"}"#.to_vec(),
//                     tags: Vec::new(),
//                 },
//             ),
//         ];
// 
//         // Pack and add each event
//         for (position, event) in &events {
//             let payload = pack_dcb_event_with_crc(*position, event.clone());
//             segment.add(payload);
//         }
// 
//         segment.flush().unwrap();
// 
//         // Iterate through records
//         let retrieved_records: Vec<(Position, DCBEvent, u64)> = segment
//             .iter_event_records()
//             .collect::<Result<Vec<_>, _>>()
//             .unwrap();
// 
//         // Check the number of records
//         assert_eq!(retrieved_records.len(), events.len());
// 
//         // Check each record
//         for (i, (position, event, _offset)) in retrieved_records.iter().enumerate() {
//             assert_eq!(*position, events[i].0);
//             assert_eq!(event.event_type, events[i].1.event_type);
//             assert_eq!(event.data, events[i].1.data);
//             assert_eq!(event.tags, events[i].1.tags);
//         }
//     }
// 
//     #[test]
//     fn test_segment_manager_initialization() {
//         let temp_dir = TempDir::new().unwrap();
// 
//         let manager = SegmentManager::new(temp_dir.path(), None).unwrap();
// 
//         assert_eq!(manager.path, temp_dir.path());
//         assert_eq!(manager.max_segment_size, DEFAULT_MAX_SEGMENT_SIZE);
//         assert_eq!(manager.current_segment_number, 1);
// 
//         // Check that the first segment was created
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
//         assert!(segment_path.exists());
//     }
// 
//     #[test]
//     fn test_segment_manager_add_payloads() {
//         let temp_dir = TempDir::new().unwrap();
// 
//         let mut manager = SegmentManager::new(temp_dir.path(), None).unwrap();
// 
//         // Create test payloads
//         let payloads = vec![
//             b"payload1".to_vec(),
//             b"payload2".to_vec(),
//             b"payload3".to_vec(),
//         ];
// 
//         // Add payloads
//         let segment_numbers_and_offsets = manager.add_payloads(&payloads).unwrap();
// 
//         // Check the returned segment numbers and offsets
//         assert_eq!(segment_numbers_and_offsets.len(), payloads.len());
// 
//         for (i, (segment_number, offset)) in segment_numbers_and_offsets.iter().enumerate() {
//             assert_eq!(*segment_number, 1); // All should be in the first segment
// 
//             if i == 0 {
//                 assert_eq!(*offset, 0);
//             } else {
//                 let expected_offset = payloads[..i].iter().map(|p| p.len()).sum::<usize>() as u64;
//                 assert_eq!(*offset, expected_offset);
//             }
//         }
// 
//         // Flush to disk
//         manager.flush().unwrap();
// 
//         // Check the segment file
//         let segment_path = temp_dir.path().join("segment-00000001.dat");
//         let content = std::fs::read(segment_path).unwrap();
//         let expected_content: Vec<u8> = payloads.iter().flat_map(|p| p.clone()).collect();
//         assert_eq!(content, expected_content);
//     }
// 
//     #[test]
//     fn test_segment_manager_rotation() {
//         let temp_dir = TempDir::new().unwrap();
// 
//         // Create a segment manager with a small max size
//         let mut manager = SegmentManager::new(temp_dir.path(), Some(100)).unwrap();
// 
//         // Create payloads that will cause rotation
//         let small_payload = b"small".to_vec();
//         let large_payload = vec![b'x'; 90]; // Close to the max segment size
// 
//         // Add small payload to first segment
//         let result1 = manager.add_payloads(&[small_payload.clone()]).unwrap();
//         assert_eq!(result1[0].0, 1); // First segment
// 
//         // Add large payload to trigger rotation
//         let result2 = manager.add_payloads(&[large_payload.clone()]).unwrap();
//         assert_eq!(result2[0].0, 1); // Still first segment
// 
//         // Add another large payload to trigger rotation
//         let result3 = manager.add_payloads(&[large_payload.clone()]).unwrap();
//         assert_eq!(result3[0].0, 2); // Second segment
// 
//         // Check that both segment files exist
//         let segment1_path = temp_dir.path().join("segment-00000001.dat");
//         let segment2_path = temp_dir.path().join("segment-00000002.dat");
//         assert!(segment1_path.exists());
//         assert!(segment2_path.exists());
// 
//         // Check current segment number
//         assert_eq!(manager.current_segment_number, 2);
//     }
// 
//     #[test]
//     fn test_segment_manager_get_segment() {
//         let temp_dir = TempDir::new().unwrap();
// 
//         let manager = SegmentManager::new(temp_dir.path(), None).unwrap();
// 
//         // Get the current segment
//         let segment = manager.get_segment(1).unwrap();
//         assert_eq!(segment.number(), 1);
// 
//         // Try to get a non-existent segment
//         let result = manager.get_segment(999);
//         assert!(matches!(result, Err(SegmentError::SegmentNotFound(_))));
//     }
// 
//     #[test]
//     fn test_segment_manager_recover_position() {
//         let temp_dir = TempDir::new().unwrap();
// 
//         let mut manager = SegmentManager::new(temp_dir.path(), None).unwrap();
// 
//         // Add some payloads to create segments
//         let large_payload = vec![b'x'; 1000];
//         manager.add_payloads(&[large_payload.clone()]).unwrap();
//         manager.add_payloads(&[large_payload.clone()]).unwrap(); // Creates segment 2
// 
//         // Recover to segment 1, offset 500
//         manager.recover_position(1, 500).unwrap();
// 
//         // Check that the current segment is segment 1
//         assert_eq!(manager.current_segment_number, 1);
// 
//         // Check that the write offset is set correctly
//         assert_eq!(manager.current_segment.file_size(), 500);
//     }
// 
//     #[test]
//     fn test_deserialize_dcb_event() {
//         // Create a test event
//         let position = 42;
//         let event = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: br#"{"msg": "hello"}"#.to_vec(),
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         // Create an event with position
//         let event_with_pos = DCBEventWithPosition {
//             position,
//             type_: event.event_type.clone(),
//             data: event.data.clone(),
//             tags: event.tags.clone(),
//         };
// 
//         // Serialize the event
//         let serialized = rmp_serde::to_vec(&event_with_pos).unwrap();
// 
//         // Deserialize the event
//         let (deserialized_position, deserialized_event) =
//             deserialize_dcb_event(&serialized).unwrap();
// 
//         // Check the deserialized data
//         assert_eq!(deserialized_position, position);
//         assert_eq!(deserialized_event.event_type, event.event_type);
//         assert_eq!(deserialized_event.data, event.data);
//         assert_eq!(deserialized_event.tags, event.tags);
//     }
// 
//     #[test]
//     fn test_read_event_record() {
//         let temp_dir = TempDir::new().unwrap();
//         let file_path = temp_dir.path().join("test_record.dat");
// 
//         // Create a test event
//         let position = 42;
//         let event = DCBEvent {
//             event_type: "test_event".to_string(),
//             data: br#"{"msg": "hello"}"#.to_vec(),
//             tags: vec!["tag1".to_string(), "tag2".to_string()],
//         };
// 
//         // Pack the event with CRC
//         let payload = pack_dcb_event_with_crc(position, event.clone());
// 
//         // Write the payload to a file
//         std::fs::write(&file_path, &payload).unwrap();
// 
//         // Open the file and read the event record
//         let mut file = File::open(&file_path).unwrap();
//         let (retrieved_position, retrieved_event, length) =
//             read_event_record(&mut file, 0).unwrap();
// 
//         // Check the retrieved data
//         assert_eq!(retrieved_position, position);
//         assert_eq!(retrieved_event.event_type, event.event_type);
//         assert_eq!(retrieved_event.data, event.data);
//         assert_eq!(retrieved_event.tags, event.tags);
//         assert_eq!(length, payload.len());
//     }
// 
//     #[test]
//     fn test_read_event_record_not_enough_data() {
//         let temp_dir = TempDir::new().unwrap();
//         let file_path = temp_dir.path().join("test_record.dat");
// 
//         // Write insufficient data to the file
//         std::fs::write(&file_path, b"not enough data").unwrap();
// 
//         // Try to read the event record
//         let mut file = File::open(&file_path).unwrap();
//         let result = read_event_record(&mut file, 0);
// 
//         // Check that the error is NotEnoughData
//         assert!(matches!(result, Err(SegmentError::NotEnoughData(_))));
//     }
// 
//     #[test]
//     fn test_check_crc() {
//         // Create test data
//         let blob = b"test data".to_vec();
//         let crc = calc_crc(&blob);
// 
//         // Check with correct CRC
//         let result = check_crc(&blob, crc);
//         assert!(result.is_ok());
// 
//         // Check with incorrect CRC
//         let result = check_crc(&blob, crc + 1);
//         assert!(matches!(result, Err(SegmentError::DatabaseCorrupted(_))));
//     }
// }
