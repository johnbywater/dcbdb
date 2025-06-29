use serde::{Deserialize, Serialize};
use uuid::{uuid, Uuid};
use std::format;
use std::path::Path;
use crate::pagedfile::PageID;
use crate::wal::Position;
use crate::indexpages::IndexPages;

const TYPE_HASH_LEN: usize = 8;

// Position index record
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionIndexRecord {
    pub segment: i32,
    pub offset: i32,
    pub type_hash: Vec<u8>,
}

// Leaf node for the B+tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode {
    pub keys: Vec<Position>,
    pub values: Vec<PositionIndexRecord>,
    pub next_leaf_id: Option<PageID>,
}

// Internal node for the B+tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

// Hash a type string to a fixed-length byte array
pub fn hash_type(type_str: &str) -> Vec<u8> {
    let namespace = uuid!("6ba7b810-9dad-11d1-80b4-00c04fd430c8"); // NAMESPACE_URL
    let uuid = Uuid::new_v5(&namespace, format!("/type/{}", type_str).as_bytes());
    uuid.as_bytes()[..TYPE_HASH_LEN].to_vec()
}

/// A structure that manages position indexing using IndexPages
pub struct PositionIndex {
    pub index_pages: IndexPages,
}

impl PositionIndex {
    /// Creates a new PositionIndex with the given path and page size
    pub fn new<P: AsRef<Path>>(path: P, page_size: usize) -> std::io::Result<Self> {
        let index_pages = IndexPages::new(path, page_size)?;
        Ok(Self { index_pages })
    }

    /// Creates a new PositionIndex with the given path, page size, and cache capacity
    pub fn new_with_cache_capacity<P: AsRef<Path>>(path: P, page_size: usize, cache_capacity: Option<usize>) -> std::io::Result<Self> {
        let index_pages = IndexPages::new_with_cache_capacity(path, page_size, cache_capacity)?;
        Ok(Self { index_pages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_position_index_construction() {
        // Create a temporary file path for testing
        let path = Path::new("test_position_index.db");

        // Create a new PositionIndex instance using the constructor
        let position_index = PositionIndex::new(path, 4096).unwrap();

        // Verify that the PositionIndex was created successfully
        assert!(path.exists());

        // Clean up the test file
        std::fs::remove_file(path).unwrap_or(());
    }
}
