// Paged Index File module
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fmt;
use thiserror::Error;
use serde::{Serialize, Deserialize};

// Constants
pub const PAGE_SIZE: usize = 4096;

// Page ID type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct PageID(pub u32);
pub const PAGE_ID_SIZE: usize = 4; 

impl fmt::Display for PageID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Error)]
pub enum PagedFileError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Data too large: size {0} exceeds page size {1}")]
    DataTooLarge(usize, usize),
}

/// A paged file structure that manages file access
pub struct PagedFile {
    path: PathBuf,
    pub new: bool,
    file: File,
    pub page_size: usize,
}

impl PagedFile {
    /// Creates a new PagedFile with the given path and page size
    /// 
    /// If the file does not exist, it will be created and 'new' will be set to false.
    /// If the file exists, it will be opened and 'new' will be set to true.
    /// 
    /// The page_size parameter defaults to PAGE_SIZE if not specified.
    pub fn new<P: AsRef<Path>>(path: P, page_size: Option<usize>) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file_exists = path_buf.exists();
        let page_size = page_size.unwrap_or(PAGE_SIZE);

        // Open the file for reading and writing, creating it if it doesn't exist
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path_buf)?;

        Ok(PagedFile {
            path: path_buf,
            new: file_exists,
            file,
            page_size,
        })
    }

    /// Writes data to a specific page in the file
    ///
    /// # Arguments
    /// * `page_id` - The page ID to write to
    /// * `page_data` - The data to write to the page
    ///
    /// # Returns
    /// * `Ok(())` if the write was successful
    /// * `Err(PagedFileError::DataTooLarge)` if the data is too large for a page
    /// * `Err(PagedFileError::Io)` if there was an IO error
    pub fn write_page(&mut self, page_id: PageID, page_data: &[u8]) -> Result<(), PagedFileError> {
        // Check if the data is too large
        if page_data.len() > self.page_size {
            return Err(PagedFileError::DataTooLarge(page_data.len(), self.page_size));
        }

        // Calculate the offset in the file
        let offset = page_id.0 as usize * self.page_size;

        // Seek to the correct position in the file
        self.file.seek(SeekFrom::Start(offset as u64))?;

        // Write the data
        self.file.write_all(page_data)?;

        // If the data is smaller than page_size, pad with zeros
        if page_data.len() < self.page_size {
            let padding = vec![0u8; self.page_size - page_data.len()];
            self.file.write_all(&padding)?;
        }

        // Flush to ensure data is written to disk
        self.file.flush()?;

        Ok(())
    }

    /// Reads data from a specific page in the file
    ///
    /// # Arguments
    /// * `page_id` - The page ID to read from
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` containing the page data if the read was successful
    /// * `Err(PagedFileError::Io)` if there was an IO error
    pub fn read_page(&mut self, page_id: PageID) -> Result<Vec<u8>, PagedFileError> {
        // Calculate the offset in the file
        let offset = page_id.0 as usize * self.page_size;

        // Seek to the correct position in the file
        self.file.seek(SeekFrom::Start(offset as u64))?;

        // Create a buffer to hold the data
        let mut buffer = vec![0u8; self.page_size];

        // Read the data
        self.file.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    /// Flushes the file to OS buffers and then fsyncs the data to disk
    ///
    /// # Returns
    /// * `Ok(())` if the operation was successful
    /// * `Err(PagedFileError::Io)` if there was an IO error
    pub fn flush_and_fsync(&mut self) -> Result<(), PagedFileError> {
        // Flush to ensure data is written to OS buffers
        self.file.flush()?;

        // Fsync to ensure data is written to disk
        self.file.sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_paged_file_creation() {
        // Create a temporary directory that will be cleaned up when it goes out of scope
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("position-index.dat");

        // Create a new PagedFile using the constructor - first time, file doesn't exist
        let paged_file = PagedFile::new(test_path.clone(), None)
            .expect("Failed to create PagedFile");

        // Verify the path was correctly stored
        assert_eq!(paged_file.path, test_path);

        // Verify 'new' is false since the file didn't exist before
        assert_eq!(paged_file.new, false);

        // Verify page_size is set to the default PAGE_SIZE
        assert_eq!(paged_file.page_size, PAGE_SIZE);

        // Create another PagedFile with the same path - now the file exists
        let custom_page_size = 8192;
        let second_paged_file = PagedFile::new(test_path.clone(), Some(custom_page_size))
            .expect("Failed to create second PagedFile");

        // Verify 'new' is true since the file now exists
        assert_eq!(second_paged_file.new, true);

        // Verify page_size is set to the custom value
        assert_eq!(second_paged_file.page_size, custom_page_size);

        // The temporary directory will be automatically deleted when temp_dir goes out of scope
    }

    #[test]
    fn test_write_and_read_pages() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("position-index.dat");

        // Create a PagedFile instance
        let mut paged_file = PagedFile::new(test_path.clone(), None)
            .expect("Failed to create PagedFile");

        // Create test data for different pages
        let page0_data = b"This is data for page 0".to_vec();
        let page1_data = b"This is different data for page 1".to_vec();

        // Write data to different pages
        paged_file.write_page(PageID(0), &page0_data)
            .expect("Failed to write to page 0");
        paged_file.write_page(PageID(1), &page1_data)
            .expect("Failed to write to page 1");

        // Read the data back and verify it matches
        let read_page0 = paged_file.read_page(PageID(0))
            .expect("Failed to read page 0");
        let read_page1 = paged_file.read_page(PageID(1))
            .expect("Failed to read page 1");

        // Verify the data matches (note: we only compare the actual data part, not the padding)
        assert_eq!(&read_page0[..page0_data.len()], &page0_data[..]);
        assert_eq!(&read_page1[..page1_data.len()], &page1_data[..]);

        // Verify the rest of the page is padded with zeros
        for i in page0_data.len()..PAGE_SIZE {
            assert_eq!(read_page0[i], 0, "Padding byte at index {} is not zero", i);
        }

        // Create a new PagedFile instance with the same path
        let mut second_paged_file = PagedFile::new(test_path.clone(), None)
            .expect("Failed to create second PagedFile");

        // Read the data again to verify persistence
        let read_page0_again = second_paged_file.read_page(PageID(0))
            .expect("Failed to read page 0 with second instance");
        let read_page1_again = second_paged_file.read_page(PageID(1))
            .expect("Failed to read page 1 with second instance");

        // Verify the data still matches
        assert_eq!(&read_page0_again[..page0_data.len()], &page0_data[..]);
        assert_eq!(&read_page1_again[..page1_data.len()], &page1_data[..]);

        // Write more data with the second instance
        let page2_data = b"This is data written by the second instance to page 2".to_vec();
        second_paged_file.write_page(PageID(2), &page2_data)
            .expect("Failed to write to page 2 with second instance");

        // Read back all pages and verify
        let read_page0_final = second_paged_file.read_page(PageID(0))
            .expect("Failed to read page 0 after writing page 2");
        let read_page1_final = second_paged_file.read_page(PageID(1))
            .expect("Failed to read page 1 after writing page 2");
        let read_page2 = second_paged_file.read_page(PageID(2))
            .expect("Failed to read page 2");

        // Verify all data is correctly stored
        assert_eq!(&read_page0_final[..page0_data.len()], &page0_data[..]);
        assert_eq!(&read_page1_final[..page1_data.len()], &page1_data[..]);
        assert_eq!(&read_page2[..page2_data.len()], &page2_data[..]);
    }

    #[test]
    fn test_flush_and_fsync() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("position-index.dat");

        // Create a PagedFile instance
        let mut paged_file = PagedFile::new(test_path.clone(), None)
            .expect("Failed to create PagedFile");

        // Create test data
        let test_data = b"This is data that will be flushed and fsynced".to_vec();

        // Write data to a page
        paged_file.write_page(PageID(0), &test_data)
            .expect("Failed to write to page");

        // Explicitly call flush_and_fsync
        paged_file.flush_and_fsync()
            .expect("Failed to flush and fsync");

        // Create a new PagedFile instance with the same path to verify data persistence
        let mut second_paged_file = PagedFile::new(test_path.clone(), None)
            .expect("Failed to create second PagedFile");

        // Read the data back
        let read_data = second_paged_file.read_page(PageID(0))
            .expect("Failed to read page");

        // Verify the data matches
        assert_eq!(&read_data[..test_data.len()], &test_data[..]);
    }

    #[test]
    fn test_write_page_data_too_large() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("position-index.dat");

        // Create a PagedFile instance with a small page size for testing
        let page_size = 100; // Small page size for testing
        let mut paged_file = PagedFile::new(test_path.clone(), Some(page_size))
            .expect("Failed to create PagedFile");

        // Create test data that is larger than the page size
        let large_data = vec![1u8; page_size + 50]; // 50 bytes more than page_size

        // Attempt to write data that is too large
        let result = paged_file.write_page(PageID(0), &large_data);

        // Verify that the correct error is returned
        match result {
            Err(PagedFileError::DataTooLarge(actual_size, max_size)) => {
                // Check that the error contains the correct size information
                assert_eq!(actual_size, large_data.len());
                assert_eq!(max_size, page_size);
            },
            _ => panic!("Expected DataTooLarge error, but got: {:?}", result),
        }
    }
}
