use std::path::Path;
use crate::pagedfile::{PagedFile, PAGE_SIZE};

/// A structure that manages index pages
pub struct IndexPages {
    paged_file: PagedFile,
}

impl IndexPages {
    /// Creates a new IndexPages with the given path and page size
    pub fn new<P: AsRef<Path>>(path: P, page_size: usize) -> std::io::Result<Self> {
        let paged_file = PagedFile::new(path, Some(page_size))?;

        Ok(IndexPages {
            paged_file,
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
    fn test_index_pages_creation() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages using the constructor
        let _index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // The temporary directory will be automatically deleted when temp_dir goes out of scope
    }
}
