// Paged Index File module
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io;

// Constants
pub const PAGE_SIZE: usize = 4096;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn hello_world() {
        println!("Hello, world!");
        assert!(true);
    }

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
}
