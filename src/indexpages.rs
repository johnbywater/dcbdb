use std::path::Path;
use std::collections::HashMap;
use crate::pagedfile::{PagedFile, PAGE_SIZE, PageID};
use serde::{Serialize, Deserialize};
use rmp_serde::{encode, decode};

/// A structure that manages index pages
pub struct IndexPages {
    paged_file: PagedFile,
    dirty: HashMap<PageID, bool>,
    header_page_id: PageID,
    pub header_node: HeaderNode,
}

/// A structure that represents a header node in the index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderNode {
    pub root_page_id: PageID,
    pub next_page_id: PageID,
}

impl HeaderNode {
    /// Serializes the HeaderNode to msgpack format
    ///
    /// # Returns
    /// * `Result<Vec<u8>, rmp_serde::encode::Error>` - The serialized data or an error
    pub fn to_msgpack(&self) -> Result<Vec<u8>, encode::Error> {
        encode::to_vec(self)
    }
}

impl IndexPages {
    /// Creates a new IndexPages with the given path and page size
    pub fn new<P: AsRef<Path>>(path: P, page_size: usize) -> std::io::Result<Self> {
        let paged_file = PagedFile::new(path, Some(page_size))?;

        Ok(IndexPages {
            paged_file,
            dirty: HashMap::new(),
            header_page_id: PageID(0),
            header_node: HeaderNode {
                root_page_id: PageID(1),
                next_page_id: PageID(2),
            },
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
    fn test_index_pages_creation() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages using the constructor
        let index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Check that header_page_id equals PageID(0)
        assert_eq!(index_pages.header_page_id, PageID(0), 
                   "header_page_id should be initialized to PageID(0)");

        // Check that header_node.root_page_id equals PageID(1)
        assert_eq!(index_pages.header_node.root_page_id, PageID(1),
                   "header_node.root_page_id should be initialized to PageID(1)");

        // Check that header_node.next_page_id equals PageID(2)
        assert_eq!(index_pages.header_node.next_page_id, PageID(2),
                   "header_node.next_page_id should be initialized to PageID(2)");

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

    #[test]
    fn test_header_node() {
        // Create initial PageID values
        let initial_root_page_id = PageID(1);
        let initial_next_page_id = PageID(2);

        // Create a HeaderNode instance
        let mut header_node = HeaderNode {
            root_page_id: initial_root_page_id,
            next_page_id: initial_next_page_id,
        };

        // Verify initial values
        assert_eq!(header_node.root_page_id, initial_root_page_id, 
                   "root_page_id should be initialized to the provided value");
        assert_eq!(header_node.next_page_id, initial_next_page_id, 
                   "next_page_id should be initialized to the provided value");

        // Change the values
        let new_root_page_id = PageID(3);
        let new_next_page_id = PageID(4);

        header_node.root_page_id = new_root_page_id;
        header_node.next_page_id = new_next_page_id;

        // Verify the new values
        assert_eq!(header_node.root_page_id, new_root_page_id, 
                   "root_page_id should be updated to the new value");
        assert_eq!(header_node.next_page_id, new_next_page_id, 
                   "next_page_id should be updated to the new value");
    }

    #[test]
    fn test_header_node_msgpack() {
        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(5),
            next_page_id: PageID(6),
        };

        // Serialize the HeaderNode to msgpack
        let serialized = header_node.to_msgpack().expect("Failed to serialize HeaderNode");

        // Deserialize the msgpack data back to a HeaderNode
        let deserialized: HeaderNode = decode::from_slice(&serialized)
            .expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized HeaderNode matches the original
        assert_eq!(deserialized, header_node, 
                   "Deserialized HeaderNode should match the original");
        assert_eq!(deserialized.root_page_id, header_node.root_page_id, 
                   "root_page_id should match after serialization/deserialization");
        assert_eq!(deserialized.next_page_id, header_node.next_page_id, 
                   "next_page_id should match after serialization/deserialization");
    }
}
