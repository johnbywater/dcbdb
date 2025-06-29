use serde::{Deserialize, Serialize};
use uuid::{uuid, Uuid};
use std::format;
use std::path::Path;
use std::any::Any;
use crate::pagedfile::PageID;
use crate::wal::Position;
use crate::indexpages::{IndexPages, Node, IndexPage, HeaderNode, HEADER_NODE_TYPE};
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
        Self::new_with_cache_capacity(path, page_size, None)
    }

    /// Creates a new PositionIndex with the given path, page size, and cache capacity
    pub fn new_with_cache_capacity<P: AsRef<Path>>(path: P, page_size: usize, cache_capacity: Option<usize>) -> std::io::Result<Self> {
        let mut index_pages = IndexPages::new_with_cache_capacity(path, page_size, cache_capacity)?;

        // Register deserializers for LeafNode and InternalNode
        index_pages.deserializer.register(LEAF_NODE_TYPE, |data| {
            let leaf_node: LeafNode = decode::from_slice(data)?;
            Ok(Box::new(leaf_node) as Box<dyn Node>)
        });

        index_pages.deserializer.register(INTERNAL_NODE_TYPE, |data| {
            let internal_node: InternalNode = decode::from_slice(data)?;
            Ok(Box::new(internal_node) as Box<dyn Node>)
        });

        // Get the root page ID from the header page
        let header_node = index_pages.header_page.node.as_any().downcast_ref::<HeaderNode>().unwrap();
        let root_page_id = header_node.root_page_id;

        // Try to get the root page
        let root_page_result = index_pages.get_page(root_page_id);

        // If the root page doesn't exist, create it
        if root_page_result.is_err() {
            // Create an empty LeafNode
            let leaf_node = LeafNode {
                keys: Vec::new(),
                values: Vec::new(),
                next_leaf_id: None,
            };

            // Create an IndexPage with the root page ID and the empty LeafNode
            let root_page = IndexPage {
                page_id: root_page_id,
                node: Box::new(leaf_node),
                serialized: Vec::new(),
            };

            // Add the page to the cache and mark it as dirty
            index_pages.add_page(root_page);

            // Flush changes to disk
            index_pages.flush()?;
        }

        Ok(Self { index_pages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use crate::indexpages::IndexPage;

    #[test]
    fn test_position_index_construction() {
        // Create a temporary file path for testing
        let path = Path::new("test_position_index.db");

        // Create a new PositionIndex instance using the constructor
        let mut position_index = PositionIndex::new(path, 4096).unwrap();

        // Verify that the PositionIndex was created successfully
        assert!(path.exists());

        // Get the root page ID from the header page
        let header_node = position_index.index_pages.header_page.node.as_any().downcast_ref::<HeaderNode>().unwrap();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let root_page = position_index.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page contains a LeafNode
        assert_eq!(root_page.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast the node to a LeafNode
        let leaf_node = root_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode is empty
        assert!(leaf_node.keys.is_empty());
        assert!(leaf_node.values.is_empty());
        assert_eq!(leaf_node.next_leaf_id, None);

        // Create a second instance of PositionIndex
        let mut position_index2 = PositionIndex::new(path, 4096).unwrap();

        // Get the root page from the second instance
        let root_page2 = position_index2.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page contains a LeafNode
        assert_eq!(root_page2.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast the node to a LeafNode
        let leaf_node2 = root_page2.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode is empty
        assert!(leaf_node2.keys.is_empty());
        assert!(leaf_node2.values.is_empty());
        assert_eq!(leaf_node2.next_leaf_id, None);

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

    #[test]
    fn test_deserializer_registration() {
        // Create a temporary file path for testing
        let path = Path::new("test_deserializer_registration.db");

        // Create a new PositionIndex instance
        let position_index = PositionIndex::new(path, 4096).unwrap();

        // Create a LeafNode and an InternalNode
        let leaf_node = LeafNode {
            keys: vec![1000, 2000],
            values: vec![
                PositionIndexRecord { segment: 1, offset: 100, type_hash: hash_type("User") },
                PositionIndexRecord { segment: 2, offset: 200, type_hash: hash_type("Product") },
            ],
            next_leaf_id: Some(PageID(42)),
        };

        let internal_node = InternalNode {
            keys: vec![1000, 2000],
            child_ids: vec![PageID(10), PageID(20), PageID(30)],
        };

        // Create IndexPage instances with the nodes
        let leaf_page = IndexPage {
            page_id: PageID(1),
            node: Box::new(leaf_node.clone()),
            serialized: Vec::new(),
        };

        let internal_page = IndexPage {
            page_id: PageID(2),
            node: Box::new(internal_node.clone()),
            serialized: Vec::new(),
        };

        // Serialize the pages
        let leaf_serialized = leaf_page.serialize_page().unwrap();
        let internal_serialized = internal_page.serialize_page().unwrap();

        // Deserialize the pages using the registered deserializers
        let deserialized_leaf_page = position_index.index_pages.deserializer.deserialize_page(&leaf_serialized, PageID(1)).unwrap();
        let deserialized_internal_page = position_index.index_pages.deserializer.deserialize_page(&internal_serialized, PageID(2)).unwrap();

        // Verify that the deserialized nodes have the correct type
        assert_eq!(deserialized_leaf_page.node.node_type_byte(), LEAF_NODE_TYPE);
        assert_eq!(deserialized_internal_page.node.node_type_byte(), INTERNAL_NODE_TYPE);

        // Downcast and verify the leaf node
        let deserialized_leaf = deserialized_leaf_page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(deserialized_leaf.keys.len(), leaf_node.keys.len());
        assert_eq!(deserialized_leaf.values.len(), leaf_node.values.len());

        // Downcast and verify the internal node
        let deserialized_internal = deserialized_internal_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(deserialized_internal.keys.len(), internal_node.keys.len());
        assert_eq!(deserialized_internal.child_ids.len(), internal_node.child_ids.len());

        // Clean up the test file
        std::fs::remove_file(path).unwrap_or(());
    }
}
