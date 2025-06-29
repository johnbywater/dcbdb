use serde::{Deserialize, Serialize};
use uuid::{uuid, Uuid};
use std::format;
use std::path::Path;
use std::any::Any;
use crate::pagedfile::PageID;
use crate::wal::Position;
use crate::indexpages::{IndexPages, Node};
use rmp_serde::{encode, decode};

const TYPE_HASH_LEN: usize = 8;

/// Constant for the internal node type
pub const INTERNAL_NODE_TYPE: u8 = 2;

/// Constant for the leaf node type
pub const LEAF_NODE_TYPE: u8 = 3;

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

impl Node for LeafNode {
    fn to_msgpack(&self) -> Result<Vec<u8>, encode::Error> {
        encode::to_vec(self)
    }

    fn node_type_byte(&self) -> u8 {
        LEAF_NODE_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// Internal node for the B+tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl Node for InternalNode {
    fn to_msgpack(&self) -> Result<Vec<u8>, encode::Error> {
        encode::to_vec(self)
    }

    fn node_type_byte(&self) -> u8 {
        INTERNAL_NODE_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
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

    #[test]
    fn test_leaf_node_serialization() {
        // Create a non-trivial LeafNode instance
        let leaf_node = LeafNode {
            keys: vec![
                1000, // Position is just an i64
                2000,
                3000,
            ],
            values: vec![
                PositionIndexRecord { segment: 1, offset: 100, type_hash: hash_type("User") },
                PositionIndexRecord { segment: 2, offset: 200, type_hash: hash_type("Product") },
                PositionIndexRecord { segment: 3, offset: 300, type_hash: hash_type("Order") },
            ],
            next_leaf_id: Some(PageID(42)),
        };

        // Serialize the node
        let serialized = leaf_node.to_msgpack().unwrap();

        // Deserialize the node
        let deserialized: LeafNode = decode::from_slice(&serialized).unwrap();

        // Verify that the deserialized node matches the original
        assert_eq!(deserialized.keys.len(), leaf_node.keys.len());
        assert_eq!(deserialized.values.len(), leaf_node.values.len());
        assert_eq!(deserialized.next_leaf_id, leaf_node.next_leaf_id);

        for i in 0..leaf_node.keys.len() {
            assert_eq!(deserialized.keys[i], leaf_node.keys[i]);
            assert_eq!(deserialized.values[i], leaf_node.values[i]);
        }

        // Verify the node type byte
        assert_eq!(leaf_node.node_type_byte(), LEAF_NODE_TYPE);
    }

    #[test]
    fn test_internal_node_serialization() {
        // Create a non-trivial InternalNode instance
        let internal_node = InternalNode {
            keys: vec![
                1000, // Position is just an i64
                2000,
                3000,
            ],
            child_ids: vec![
                PageID(10),
                PageID(20),
                PageID(30),
                PageID(40),
            ],
        };

        // Serialize the node
        let serialized = internal_node.to_msgpack().unwrap();

        // Deserialize the node
        let deserialized: InternalNode = decode::from_slice(&serialized).unwrap();

        // Verify that the deserialized node matches the original
        assert_eq!(deserialized.keys.len(), internal_node.keys.len());
        assert_eq!(deserialized.child_ids.len(), internal_node.child_ids.len());

        for i in 0..internal_node.keys.len() {
            assert_eq!(deserialized.keys[i], internal_node.keys[i]);
        }

        for i in 0..internal_node.child_ids.len() {
            assert_eq!(deserialized.child_ids[i], internal_node.child_ids[i]);
        }

        // Verify the node type byte
        assert_eq!(internal_node.node_type_byte(), INTERNAL_NODE_TYPE);
    }
}
