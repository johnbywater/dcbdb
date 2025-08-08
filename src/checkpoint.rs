//! Checkpoint file for the event store.
//!
//! This module provides a checkpoint file implementation for storing
//! transaction and position information for recovery.

use crate::crc::calc_crc;
use crate::wal::Position;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// Constants
const CHECKPOINT_FILE_NAME: &str = "checkpoint.dat";
const CHECKPOINT_STRUCT_SIZE: usize = 40; // 5 * 8 bytes (u64)
const CRC_SIZE: usize = 4; // 4 bytes (u32)

/// Checkpoint file for storing transaction and position information
pub struct CheckpointFile {
    file_path: PathBuf,
    file: File,
    pub txn_id: u64,
    pub position: Position,
    pub segment_number: u64,
    pub segment_offset: u64,
    pub wal_commit_offset: u64,
}

impl CheckpointFile {
    /// Creates a new CheckpointFile with the given path
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file_path = path_buf.join(CHECKPOINT_FILE_NAME);

        let file_exists = file_path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&file_path)?;

        let mut checkpoint = Self {
            file_path,
            file,
            txn_id: 0,
            position: 0,
            segment_number: 0,
            segment_offset: 0,
            wal_commit_offset: 0,
        };

        // If the file exists and has data, read the checkpoint
        if file_exists {
            let _ = checkpoint.read_checkpoint();
        }

        Ok(checkpoint)
    }

    /// Writes the checkpoint data to the file
    pub fn write_checkpoint(&mut self) -> io::Result<()> {
        // Create the binary blob with all fields
        let mut blob = Vec::with_capacity(CHECKPOINT_STRUCT_SIZE);
        blob.extend_from_slice(&self.txn_id.to_le_bytes());
        blob.extend_from_slice(&self.position.to_le_bytes());
        blob.extend_from_slice(&self.segment_number.to_le_bytes());
        blob.extend_from_slice(&self.segment_offset.to_le_bytes());
        blob.extend_from_slice(&self.wal_commit_offset.to_le_bytes());

        // Calculate CRC
        let crc = calc_crc(&blob);

        // Seek to the beginning of the file
        self.file.seek(SeekFrom::Start(0))?;

        // Write the blob and CRC
        self.file.write_all(&blob)?;
        self.file.write_all(&crc.to_le_bytes())?;

        // Flush and sync to ensure data is written to disk
        self.file.flush()?;
        let _ = self.file.sync_all();

        Ok(())
    }

    /// Returns the path to the checkpoint file
    pub fn path(&self) -> &Path {
        &self.file_path
    }

    /// Reads the checkpoint data from the file
    pub fn read_checkpoint(&mut self) -> io::Result<()> {
        // Seek to the beginning of the file
        self.file.seek(SeekFrom::Start(0))?;

        // Read the data
        let mut data = vec![0; CHECKPOINT_STRUCT_SIZE + CRC_SIZE];
        let bytes_read = self.file.read(&mut data)?;

        // If no data was read, return without changing the checkpoint
        if bytes_read == 0 {
            return Ok(());
        }

        // If we didn't read enough data, return an error
        if bytes_read < CHECKPOINT_STRUCT_SIZE + CRC_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Checkpoint file is too small",
            ));
        }

        // Split the data into blob and CRC
        let blob = &data[0..CHECKPOINT_STRUCT_SIZE];
        let file_crc = u32::from_le_bytes([
            data[CHECKPOINT_STRUCT_SIZE],
            data[CHECKPOINT_STRUCT_SIZE + 1],
            data[CHECKPOINT_STRUCT_SIZE + 2],
            data[CHECKPOINT_STRUCT_SIZE + 3],
        ]);

        // Calculate CRC and verify
        let calculated_crc = calc_crc(blob);
        if calculated_crc != file_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid checkpoint: CRC mismatch",
            ));
        }

        // Parse the fields
        self.txn_id = u64::from_le_bytes([
            blob[0], blob[1], blob[2], blob[3], blob[4], blob[5], blob[6], blob[7],
        ]);

        self.position = u64::from_le_bytes([
            blob[8], blob[9], blob[10], blob[11], blob[12], blob[13], blob[14], blob[15],
        ]);

        self.segment_number = u64::from_le_bytes([
            blob[16], blob[17], blob[18], blob[19], blob[20], blob[21], blob[22], blob[23],
        ]);

        self.segment_offset = u64::from_le_bytes([
            blob[24], blob[25], blob[26], blob[27], blob[28], blob[29], blob[30], blob[31],
        ]);

        self.wal_commit_offset = u64::from_le_bytes([
            blob[32], blob[33], blob[34], blob[35], blob[36], blob[37], blob[38], blob[39],
        ]);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_new_checkpoint_file() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint = CheckpointFile::new(temp_dir.path()).unwrap();

        assert_eq!(checkpoint.txn_id, 0);
        assert_eq!(checkpoint.position, 0);
        assert_eq!(checkpoint.segment_number, 0);
        assert_eq!(checkpoint.segment_offset, 0);
        assert_eq!(checkpoint.wal_commit_offset, 0);

        let checkpoint_path = temp_dir.path().join(CHECKPOINT_FILE_NAME);
        assert!(checkpoint_path.exists());
    }

    #[test]
    fn test_write_and_read_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let mut checkpoint = CheckpointFile::new(temp_dir.path()).unwrap();

        // Set some values
        checkpoint.txn_id = 42;
        checkpoint.position = 100;
        checkpoint.segment_number = 5;
        checkpoint.segment_offset = 1000;
        checkpoint.wal_commit_offset = 2000;

        // Write the checkpoint
        checkpoint.write_checkpoint().unwrap();

        // Create a new checkpoint file instance to read the data
        let checkpoint2 = CheckpointFile::new(temp_dir.path()).unwrap();

        // Verify the values were read correctly
        assert_eq!(checkpoint2.txn_id, 42);
        assert_eq!(checkpoint2.position, 100);
        assert_eq!(checkpoint2.segment_number, 5);
        assert_eq!(checkpoint2.segment_offset, 1000);
        assert_eq!(checkpoint2.wal_commit_offset, 2000);
    }

    #[test]
    fn test_update_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let mut checkpoint = CheckpointFile::new(temp_dir.path()).unwrap();

        // Set initial values
        checkpoint.txn_id = 42;
        checkpoint.position = 100;
        checkpoint.segment_number = 5;
        checkpoint.segment_offset = 1000;
        checkpoint.wal_commit_offset = 2000;

        // Write the checkpoint
        checkpoint.write_checkpoint().unwrap();

        // Update values
        checkpoint.txn_id = 43;
        checkpoint.position = 200;
        checkpoint.segment_number = 6;
        checkpoint.segment_offset = 1500;
        checkpoint.wal_commit_offset = 2500;

        // Write the updated checkpoint
        checkpoint.write_checkpoint().unwrap();

        // Create a new checkpoint file instance to read the data
        let checkpoint2 = CheckpointFile::new(temp_dir.path()).unwrap();

        // Verify the updated values were read correctly
        assert_eq!(checkpoint2.txn_id, 43);
        assert_eq!(checkpoint2.position, 200);
        assert_eq!(checkpoint2.segment_number, 6);
        assert_eq!(checkpoint2.segment_offset, 1500);
        assert_eq!(checkpoint2.wal_commit_offset, 2500);
    }

    #[test]
    fn test_invalid_crc() {
        let temp_dir = TempDir::new().unwrap();
        let mut checkpoint = CheckpointFile::new(temp_dir.path()).unwrap();

        // Set some values
        checkpoint.txn_id = 42;
        checkpoint.position = 100;
        checkpoint.segment_number = 5;
        checkpoint.segment_offset = 1000;
        checkpoint.wal_commit_offset = 2000;

        // Write the checkpoint
        checkpoint.write_checkpoint().unwrap();

        // Corrupt the file by writing invalid data at the CRC position
        let mut file = OpenOptions::new()
            .write(true)
            .open(temp_dir.path().join(CHECKPOINT_FILE_NAME))
            .unwrap();

        file.seek(SeekFrom::Start(CHECKPOINT_STRUCT_SIZE as u64))
            .unwrap();
        file.write_all(&[0, 0, 0, 0]).unwrap();
        file.flush().unwrap();

        // Create a new checkpoint file instance to read the data
        let mut checkpoint2 = CheckpointFile::new(temp_dir.path()).unwrap();

        // Reading should fail with an error
        let result = checkpoint2.read_checkpoint();
        assert!(result.is_err());

        // The error should be about invalid CRC
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("CRC mismatch"));
    }

    #[test]
    fn test_empty_file() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_path = temp_dir.path().join(CHECKPOINT_FILE_NAME);

        // Create an empty file
        File::create(&checkpoint_path).unwrap();

        // Create a checkpoint file instance
        let mut checkpoint = CheckpointFile::new(temp_dir.path()).unwrap();

        // Reading should succeed but not change any values
        let result = checkpoint.read_checkpoint();
        assert!(result.is_ok());

        // Values should still be default
        assert_eq!(checkpoint.txn_id, 0);
        assert_eq!(checkpoint.position, 0);
        assert_eq!(checkpoint.segment_number, 0);
        assert_eq!(checkpoint.segment_offset, 0);
        assert_eq!(checkpoint.wal_commit_offset, 0);
    }
}
