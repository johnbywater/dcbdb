use std::path::Path;
use std::collections::HashMap;
use crate::pagedfile::{PagedFile, PAGE_SIZE, PageID};

/// A structure that manages index pages
pub struct IndexPages {
    paged_file: PagedFile,
    dirty: HashMap<PageID, bool>,
}

impl IndexPages {
    /// Creates a new IndexPages with the given path and page size
    pub fn new<P: AsRef<Path>>(path: P, page_size: usize) -> std::io::Result<Self> {
        let paged_file = PagedFile::new(path, Some(page_size))?;

        Ok(IndexPages {
            paged_file,
            dirty: HashMap::new(),
        })
    }

    /// Marks a page as dirty
    ///
    /// # Arguments
    /// * `page_id` - The page ID to mark as dirty
    pub fn mark_dirty(&mut self, page_id: PageID) {
        self.dirty.insert(page_id, true);
    }

    /// Clears all entries from the dirty HashMap
    pub fn clear_dirty(&mut self) {
        self.dirty.clear();
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

    #[test]
    fn test_mark_dirty() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Create a few PageID instances
        let page_id1 = PageID(1);
        let page_id2 = PageID(2);
        let page_id3 = PageID(3);

        // Mark some pages as dirty, including duplicates
        index_pages.mark_dirty(page_id1);
        index_pages.mark_dirty(page_id2);
        index_pages.mark_dirty(page_id3);
        index_pages.mark_dirty(page_id1); // Duplicate

        // Create a set of expected PageIDs
        let mut expected_page_ids = HashMap::new();
        expected_page_ids.insert(page_id1, true);
        expected_page_ids.insert(page_id2, true);
        expected_page_ids.insert(page_id3, true);

        // Verify that each unique PageID added exists in the HashMap
        for (page_id, _) in &index_pages.dirty {
            assert!(expected_page_ids.contains_key(page_id), 
                    "Unexpected PageID in dirty HashMap: {:?}", page_id);
        }

        // Verify that all expected PageIDs exist in the HashMap
        for (page_id, _) in &expected_page_ids {
            assert!(index_pages.dirty.contains_key(page_id), 
                    "Expected PageID not found in dirty HashMap: {:?}", page_id);
        }

        // Verify that the number of entries in the dirty HashMap matches the expected count
        assert_eq!(index_pages.dirty.len(), expected_page_ids.len(), 
                   "Number of entries in dirty HashMap does not match expected count");
    }

    #[test]
    fn test_clear_dirty() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Create a few PageID instances
        let page_id1 = PageID(1);
        let page_id2 = PageID(2);
        let page_id3 = PageID(3);

        // Mark some pages as dirty
        index_pages.mark_dirty(page_id1);
        index_pages.mark_dirty(page_id2);
        index_pages.mark_dirty(page_id3);

        // Verify that the dirty HashMap is not empty
        assert!(!index_pages.dirty.is_empty(), "Dirty HashMap should not be empty before clearing");

        // Clear the dirty HashMap
        index_pages.clear_dirty();

        // Verify that the dirty HashMap is now empty
        assert!(index_pages.dirty.is_empty(), "Dirty HashMap should be empty after clearing");
    }
}
