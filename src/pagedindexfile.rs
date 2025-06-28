// Paged Index File module
use std::path::{Path, PathBuf};

/// A paged file structure that manages file access
pub struct PagedFile {
    path: PathBuf,
}

impl PagedFile {
    /// Creates a new PagedFile with the given path
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        PagedFile {
            path: path.as_ref().to_path_buf(),
        }
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

        // Create a new PagedFile using the constructor
        let paged_file = PagedFile::new(test_path.clone());

        // Verify the path was correctly stored
        assert_eq!(paged_file.path, test_path);

        // The temporary directory will be automatically deleted when temp_dir goes out of scope
    }
}
