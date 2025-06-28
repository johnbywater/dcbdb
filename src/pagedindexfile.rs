// Paged Index File module
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io;

/// A paged file structure that manages file access
pub struct PagedFile {
    path: PathBuf,
    pub new: bool,
    file: File,
}

impl PagedFile {
    /// Creates a new PagedFile with the given path
    /// 
    /// If the file does not exist, it will be created and 'new' will be set to false.
    /// If the file exists, it will be opened and 'new' will be set to true.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file_exists = path_buf.exists();

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
        let paged_file = PagedFile::new(test_path.clone())
            .expect("Failed to create PagedFile");

        // Verify the path was correctly stored
        assert_eq!(paged_file.path, test_path);

        // Verify 'new' is false since the file didn't exist before
        assert_eq!(paged_file.new, false);

        // Create another PagedFile with the same path - now the file exists
        let second_paged_file = PagedFile::new(test_path.clone())
            .expect("Failed to create second PagedFile");

        // Verify 'new' is true since the file now exists
        assert_eq!(second_paged_file.new, true);

        // The temporary directory will be automatically deleted when temp_dir goes out of scope
    }
}
