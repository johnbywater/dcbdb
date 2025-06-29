use serde::{Deserialize, Serialize};
use uuid::{uuid, Uuid};
use std::format;
use std::path::Path;
use std::any::Any;
use crate::pagedfile::PageID;
use crate::wal::Position;
use crate::indexpages::{IndexPages, Node, IndexPage, HeaderNode};
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

    /// Adds a key and value to a leaf node, splitting it if necessary
    ///
    /// # Arguments
    /// * `page_id` - The PageID of the page to add the key and value to
    /// * `key` - The key to add
    /// * `value` - The value to add
    ///
    /// # Returns
    /// * `Option<(Position, PageID)>` - If the leaf node was split, returns the first key of the new leaf node and the new page ID
    pub fn append_leaf_key_and_value(&mut self, page_id: PageID, key: Position, value: PositionIndexRecord) -> Option<(Position, PageID)> {
        // Add the key and value to the leaf node
        {
            // Get a mutable reference to the page
            let page = self.index_pages.get_page_mut(page_id).unwrap();

            // Downcast the node to a LeafNode
            let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();

            // Optimization for append-only event store:
            // Since positions are always added in ascending order, we only need to check
            // if the leaf node is empty or if the key is greater than the last key
            if !leaf_node.keys.is_empty() {
                let last_index = leaf_node.keys.len() - 1;
                let last_key = leaf_node.keys[last_index];

                // Only push the new key and value if it's greater than the last key
                if key <= last_key {
                    return None;
                }
                // If key > last_key, we'll append it below
            }

            // Add the key and value
            leaf_node.keys.push(key);
            leaf_node.values.push(value);
        }

        // Check if the page needs splitting by serializing it and checking if it's too large
        let needs_splitting = {
            let page = self.index_pages.get_page_mut(page_id).unwrap();
            let serialized = page.serialize_page().unwrap();
            serialized.len() + 10 > self.index_pages.paged_file.page_size
        };

        if !needs_splitting {
            // If the leaf node doesn't need splitting, just mark the page as dirty
            self.index_pages.mark_dirty(page_id);
            None
        } else {
            // If the leaf node needs splitting, split it
            // Extract the last key and value from the leaf node
            let (last_key, last_value) = {
                // Get a mutable reference to the page
                let page = self.index_pages.get_page_mut(page_id).unwrap();

                // Downcast the node to a LeafNode
                let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();

                // Get the last key and value
                let key = leaf_node.keys.pop().unwrap();
                let value = leaf_node.values.pop().unwrap();

                (key, value)
            };

            // Allocate a new page ID
            let new_page_id = self.index_pages.alloc_page_id();

            // Create a new LeafNode with the last key and value
            let new_leaf_node = LeafNode {
                keys: vec![last_key],
                values: vec![last_value],
                next_leaf_id: None,
            };

            // Create a new IndexPage with the new LeafNode
            let new_page = IndexPage {
                page_id: new_page_id,
                node: Box::new(new_leaf_node),
                serialized: Vec::new(),
            };

            // Set the next_leaf_id of the original leaf node to the new page ID
            {
                // Get a mutable reference to the page again
                let page = self.index_pages.get_page_mut(page_id).unwrap();

                // Downcast the node to a LeafNode
                let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();

                // Set the next_leaf_id
                leaf_node.next_leaf_id = Some(new_page_id);
            }

            // Mark the original page as dirty
            self.index_pages.mark_dirty(page_id);

            // Add the new page to the collection
            self.index_pages.add_page(new_page);

            // Return the first key of the new leaf node and the new page ID
            Some((last_key, new_page_id))
        }
    }

    /// Adds a key and value to an internal node, splitting it if necessary
    ///
    /// # Arguments
    /// * `page_id` - The PageID of the page to add the key and value to
    /// * `key` - The key to add
    /// * `child_id` - The child ID to add
    ///
    /// # Returns
    /// * `Option<(Position, PageID)>` - If the internal node was split, returns the promoted key and the new page ID
    pub fn append_internal_key_and_value(&mut self, page_id: PageID, key: Position, child_id: PageID) -> Option<(Position, PageID)> {
        // Add the key and child_id to the internal node
        {
            // Get a mutable reference to the page
            let page = self.index_pages.get_page_mut(page_id).unwrap();

            // Downcast the node to an InternalNode
            let internal_node = page.node.as_any_mut().downcast_mut::<InternalNode>().unwrap();

            // Add the key and child_id
            internal_node.keys.push(key);
            internal_node.child_ids.push(child_id);
        }

        // Check if the page needs splitting by serializing it and checking if it's too large
        let needs_splitting = {
            let page = self.index_pages.get_page_mut(page_id).unwrap();
            let serialized = page.serialize_page().unwrap();
            serialized.len() > self.index_pages.paged_file.page_size
        };

        if !needs_splitting {
            // If the internal node doesn't need splitting, just mark the page as dirty
            self.index_pages.mark_dirty(page_id);
            None
        } else {
            // If the internal node needs splitting, split it according to the specified logic
            // Extract the promote_key, last key, and last two child_ids from the internal node
            let (promote_key, last_key, last_two_child_ids) = {
                // Get a mutable reference to the page
                let page = self.index_pages.get_page_mut(page_id).unwrap();

                // Downcast the node to an InternalNode
                let internal_node = page.node.as_any_mut().downcast_mut::<InternalNode>().unwrap();


                // Get the last key
                let last_key = internal_node.keys.pop().unwrap();

                // Get the promote_key (second-to-last key)
                let promote_key = internal_node.keys.pop().unwrap();

                // Get the last two child_ids
                let last_child_id = internal_node.child_ids.pop().unwrap();
                let second_last_child_id = internal_node.child_ids.pop().unwrap();

                (promote_key, last_key, vec![second_last_child_id, last_child_id])
            };

            // Allocate a new page ID
            let new_page_id = self.index_pages.alloc_page_id();

            // Create a new InternalNode with the last key and last two child_ids
            let new_internal_node = InternalNode {
                keys: vec![last_key],
                child_ids: last_two_child_ids,
            };

            // Create a new IndexPage with the new InternalNode
            let new_page = IndexPage {
                page_id: new_page_id,
                node: Box::new(new_internal_node),
                serialized: Vec::new(),
            };

            // Mark the original page as dirty
            self.index_pages.mark_dirty(page_id);

            // Add the new page to the collection
            self.index_pages.add_page(new_page);

            // Return the promote_key and the new page ID
            Some((promote_key, new_page_id))
        }
    }

    /// Inserts a key and value into the position index
    ///
    /// # Arguments
    /// * `key` - The key to insert
    /// * `value` - The value to insert
    ///
    /// # Returns
    /// * `std::io::Result<()>` - Ok if the insertion was successful, Err otherwise
    pub fn insert(&mut self, key: Position, value: PositionIndexRecord) -> std::io::Result<()> {
        // Get the root page ID from the header page
        let header_node = self.index_pages.header_node();
        let root_page_id = header_node.root_page_id;

        // Stack to keep track of the path from root to leaf
        // Each entry is a page ID and the index of the child to follow
        let mut stack: Vec<(PageID, usize)> = Vec::new();
        let mut current_page_id = root_page_id;

        // First phase: traverse down to the leaf node
        loop {
            let page = self.index_pages.get_page(current_page_id)?;

            if page.node.node_type_byte() == LEAF_NODE_TYPE {
                // Found the leaf node, break out of the loop
                break;
            } else if page.node.node_type_byte() == INTERNAL_NODE_TYPE {
                // Internal node, always follow the last child since positions are always added in ascending order
                let internal_node = page.node.as_any().downcast_ref::<InternalNode>().unwrap();

                // Get the index of the last child
                let index = internal_node.child_ids.len() - 1;

                // Push the current page and child index onto the stack
                stack.push((current_page_id, index));

                // Move to the child
                current_page_id = internal_node.child_ids[index];
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid node type",
                ));
            }
        }

        // Second phase: insert into the leaf node
        let mut split_info = self.append_leaf_key_and_value(current_page_id, key, value);

        // Third phase: propagate splits up the tree
        while let Some((promoted_key, new_page_id)) = split_info {
            if stack.is_empty() {
                // We've reached the root, create a new root
                let new_root_page_id = self.index_pages.alloc_page_id();

                // Create a new internal node with the promoted key and two child IDs
                let internal_node = InternalNode {
                    keys: vec![promoted_key],
                    child_ids: vec![root_page_id, new_page_id],
                };

                // Create a new page with the internal node
                let new_root_page = IndexPage {
                    page_id: new_root_page_id,
                    node: Box::new(internal_node),
                    serialized: Vec::new(),
                };

                // Add the new page to the collection
                self.index_pages.add_page(new_root_page);

                // Set the new page as the root page
                self.index_pages.set_root_page_id(new_root_page_id);

                break;
            }

            // Pop the parent from the stack
            let (parent_id, _child_index) = stack.pop().unwrap();

            // Insert the promoted key and new page ID into the parent
            split_info = self.append_internal_key_and_value(parent_id, promoted_key, new_page_id);
        }

        Ok(())
    }

    /// Looks up a key in the position index
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `std::io::Result<Option<PositionIndexRecord>>` - Ok(Some(record)) if the key was found, Ok(None) if not found, Err if an error occurred
    pub fn lookup(&mut self, key: Position) -> std::io::Result<Option<PositionIndexRecord>> {
        // Get the root page ID from the header page
        let header_node = self.index_pages.header_page.node.as_any().downcast_ref::<HeaderNode>().unwrap();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let page = self.index_pages.get_page(root_page_id)?;

        // Check if the root page is a leaf node
        if page.node.node_type_byte() == LEAF_NODE_TYPE {
            // Downcast the node to a LeafNode
            let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();

            // Use binary search to find the key in the leaf node's keys
            match leaf_node.keys.binary_search(&key) {
                Ok(index) => {
                    // Key found, return the corresponding value
                    return Ok(Some(leaf_node.values[index].clone()));
                }
                Err(_) => {
                    // Key not found
                }
            }

            // Key not found
            Ok(None)
        } else if page.node.node_type_byte() == INTERNAL_NODE_TYPE {
            // If the root is an internal node, we need to traverse the tree to find the leaf node
            let mut current_page_id = root_page_id;

            // Traverse the tree until we find a leaf node
            loop {
                let page = self.index_pages.get_page(current_page_id)?;

                if page.node.node_type_byte() == LEAF_NODE_TYPE {
                    // Found a leaf node, search for the key
                    let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();

                    // Use binary search to find the key in the leaf node's keys
                    match leaf_node.keys.binary_search(&key) {
                        Ok(index) => {
                            // Key found, return the corresponding value
                            return Ok(Some(leaf_node.values[index].clone()));
                        }
                        Err(_) => {
                            // Key not found
                        }
                    }

                    // Key not found in this leaf node
                    // If there's a next leaf node, we could check it, but for now we'll just return None
                    return Ok(None);
                } else if page.node.node_type_byte() == INTERNAL_NODE_TYPE {
                    // Internal node, find the child to follow
                    let internal_node = page.node.as_any().downcast_ref::<InternalNode>().unwrap();

                    // Find the index of the first key greater than the search key using binary search
                    let index = match internal_node.keys.binary_search_by(|probe| probe.cmp(&key)) {
                        // If key is found, use the child at that index + 1
                        Ok(idx) => idx + 1,
                        // If key is not found, Err(idx) gives the index where it would be inserted
                        // This is the index of the first key greater than the search key
                        Err(idx) => idx,
                    };

                    // Use the child at the found index
                    current_page_id = internal_node.child_ids[index];
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid node type",
                    ));
                }
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid node type",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexpages::IndexPage;
    use tempfile::TempDir;

    #[test]
    fn test_position_index_construction() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance using the constructor
        let mut position_index = PositionIndex::new(&test_path, 4096).unwrap();

        // Verify that the PositionIndex was created successfully
        assert!(test_path.exists());

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
        let mut position_index2 = PositionIndex::new(&test_path, 4096).unwrap();

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

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
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
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance
        let position_index = PositionIndex::new(&test_path, 4096).unwrap();

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

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_leaf_node_add_modify() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance
        let mut position_index = PositionIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(10);

        // Create a LeafNode with initial keys and values
        let leaf_node = LeafNode {
            keys: vec![1000, 2000],
            values: vec![
                PositionIndexRecord { segment: 1, offset: 100, type_hash: hash_type("User") },
                PositionIndexRecord { segment: 2, offset: 200, type_hash: hash_type("Product") },
            ],
            next_leaf_id: None,
        };

        // Create an IndexPage with the LeafNode
        let leaf_page = IndexPage {
            page_id,
            node: Box::new(leaf_node),
            serialized: Vec::new(),
        };

        // Add the page to the index
        position_index.index_pages.add_page(leaf_page);

        // Get the page and check keys and values
        let retrieved_page = position_index.index_pages.get_page(page_id).unwrap();
        assert_eq!(retrieved_page.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast to LeafNode and check contents
        let leaf_node = retrieved_page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf_node.keys.len(), 2);
        assert_eq!(leaf_node.values.len(), 2);
        assert_eq!(leaf_node.keys[0], 1000);
        assert_eq!(leaf_node.keys[1], 2000);
        assert_eq!(leaf_node.values[0].segment, 1);
        assert_eq!(leaf_node.values[0].offset, 100);
        assert_eq!(leaf_node.values[1].segment, 2);
        assert_eq!(leaf_node.values[1].offset, 200);

        // Get a mutable reference to the page and append a key and value
        let mut_page = position_index.index_pages.get_page_mut(page_id).unwrap();
        let leaf_node_mut = mut_page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();
        leaf_node_mut.keys.push(3000);
        leaf_node_mut.values.push(PositionIndexRecord { 
            segment: 3, 
            offset: 300, 
            type_hash: hash_type("Order") 
        });

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it's a LeafNode with the correct keys and values
        let retrieved_page2 = position_index2.index_pages.get_page(page_id).unwrap();
        assert_eq!(retrieved_page2.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast to LeafNode and check contents
        let leaf_node2 = retrieved_page2.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf_node2.keys.len(), 3);
        assert_eq!(leaf_node2.values.len(), 3);
        assert_eq!(leaf_node2.keys[0], 1000);
        assert_eq!(leaf_node2.keys[1], 2000);
        assert_eq!(leaf_node2.keys[2], 3000);
        assert_eq!(leaf_node2.values[0].segment, 1);
        assert_eq!(leaf_node2.values[0].offset, 100);
        assert_eq!(leaf_node2.values[1].segment, 2);
        assert_eq!(leaf_node2.values[1].offset, 200);
        assert_eq!(leaf_node2.values[2].segment, 3);
        assert_eq!(leaf_node2.values[2].offset, 300);

        // Get a mutable reference to the page and add another key and value
        let mut_page2 = position_index2.index_pages.get_page_mut(page_id).unwrap();
        let leaf_node_mut2 = mut_page2.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();
        leaf_node_mut2.keys.push(4000);
        leaf_node_mut2.values.push(PositionIndexRecord { 
            segment: 4, 
            offset: 400, 
            type_hash: hash_type("Category") 
        });

        // Mark the page as dirty
        position_index2.index_pages.mark_dirty(page_id);

        // Flush changes to disk
        position_index2.index_pages.flush().unwrap();

        // Create a third instance of PositionIndex
        let mut position_index3 = PositionIndex::new(&test_path, 4096).unwrap();

        // Get the page and check keys and values
        let retrieved_page3 = position_index3.index_pages.get_page(page_id).unwrap();
        assert_eq!(retrieved_page3.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast to LeafNode and check contents
        let leaf_node3 = retrieved_page3.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf_node3.keys.len(), 4);
        assert_eq!(leaf_node3.values.len(), 4);
        assert_eq!(leaf_node3.keys[0], 1000);
        assert_eq!(leaf_node3.keys[1], 2000);
        assert_eq!(leaf_node3.keys[2], 3000);
        assert_eq!(leaf_node3.keys[3], 4000);
        assert_eq!(leaf_node3.values[0].segment, 1);
        assert_eq!(leaf_node3.values[0].offset, 100);
        assert_eq!(leaf_node3.values[1].segment, 2);
        assert_eq!(leaf_node3.values[1].offset, 200);
        assert_eq!(leaf_node3.values[2].segment, 3);
        assert_eq!(leaf_node3.values[2].offset, 300);
        assert_eq!(leaf_node3.values[3].segment, 4);
        assert_eq!(leaf_node3.values[3].offset, 400);

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_internal_node_add_modify() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance
        let mut position_index = PositionIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(20);

        // Create an InternalNode with initial keys and child_ids
        let internal_node = InternalNode {
            keys: vec![1000, 2000],
            child_ids: vec![PageID(100), PageID(200), PageID(300)],
        };

        // Create an IndexPage with the InternalNode
        let internal_page = IndexPage {
            page_id,
            node: Box::new(internal_node),
            serialized: Vec::new(),
        };

        // Add the page to the index
        position_index.index_pages.add_page(internal_page);

        // Get the page and check keys and child_ids
        let retrieved_page = position_index.index_pages.get_page(page_id).unwrap();
        assert_eq!(retrieved_page.node.node_type_byte(), INTERNAL_NODE_TYPE);

        // Downcast to InternalNode and check contents
        let internal_node = retrieved_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(internal_node.keys.len(), 2);
        assert_eq!(internal_node.child_ids.len(), 3);
        assert_eq!(internal_node.keys[0], 1000);
        assert_eq!(internal_node.keys[1], 2000);
        assert_eq!(internal_node.child_ids[0], PageID(100));
        assert_eq!(internal_node.child_ids[1], PageID(200));
        assert_eq!(internal_node.child_ids[2], PageID(300));

        // Get a mutable reference to the page and append a key and child_id
        let mut_page = position_index.index_pages.get_page_mut(page_id).unwrap();
        let internal_node_mut = mut_page.node.as_any_mut().downcast_mut::<InternalNode>().unwrap();
        internal_node_mut.keys.push(3000);
        internal_node_mut.child_ids.push(PageID(400));

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it's an InternalNode with the correct keys and child_ids
        let retrieved_page2 = position_index2.index_pages.get_page(page_id).unwrap();
        assert_eq!(retrieved_page2.node.node_type_byte(), INTERNAL_NODE_TYPE);

        // Downcast to InternalNode and check contents
        let internal_node2 = retrieved_page2.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(internal_node2.keys.len(), 3);
        assert_eq!(internal_node2.child_ids.len(), 4);
        assert_eq!(internal_node2.keys[0], 1000);
        assert_eq!(internal_node2.keys[1], 2000);
        assert_eq!(internal_node2.keys[2], 3000);
        assert_eq!(internal_node2.child_ids[0], PageID(100));
        assert_eq!(internal_node2.child_ids[1], PageID(200));
        assert_eq!(internal_node2.child_ids[2], PageID(300));
        assert_eq!(internal_node2.child_ids[3], PageID(400));

        // Get a mutable reference to the page and add another key and child_id
        let mut_page2 = position_index2.index_pages.get_page_mut(page_id).unwrap();
        let internal_node_mut2 = mut_page2.node.as_any_mut().downcast_mut::<InternalNode>().unwrap();
        internal_node_mut2.keys.push(4000);
        internal_node_mut2.child_ids.push(PageID(500));

        // Mark the page as dirty
        position_index2.index_pages.mark_dirty(page_id);

        // Flush changes to disk
        position_index2.index_pages.flush().unwrap();

        // Create a third instance of PositionIndex
        let mut position_index3 = PositionIndex::new(&test_path, 4096).unwrap();

        // Get the page and check keys and child_ids
        let retrieved_page3 = position_index3.index_pages.get_page(page_id).unwrap();
        assert_eq!(retrieved_page3.node.node_type_byte(), INTERNAL_NODE_TYPE);

        // Downcast to InternalNode and check contents
        let internal_node3 = retrieved_page3.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(internal_node3.keys.len(), 4);
        assert_eq!(internal_node3.child_ids.len(), 5);
        assert_eq!(internal_node3.keys[0], 1000);
        assert_eq!(internal_node3.keys[1], 2000);
        assert_eq!(internal_node3.keys[2], 3000);
        assert_eq!(internal_node3.keys[3], 4000);
        assert_eq!(internal_node3.child_ids[0], PageID(100));
        assert_eq!(internal_node3.child_ids[1], PageID(200));
        assert_eq!(internal_node3.child_ids[2], PageID(300));
        assert_eq!(internal_node3.child_ids[3], PageID(400));
        assert_eq!(internal_node3.child_ids[4], PageID(500));

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_append_leaf_key_and_value_with_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Construct a PageID
        let page_id = PageID(30);

        // Create a LeafNode with 11 keys and 11 values
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for i in 0..11 {
            keys.push(i * 1000);
            values.push(PositionIndexRecord {
                segment: i as i32,
                offset: i as i32 * 100,
                type_hash: hash_type(&format!("Type{}", i)),
            });
        }

        let mut leaf_node = LeafNode {
            keys,
            values,
            next_leaf_id: None,
        };

        // Create an IndexPage with the LeafNode
        let mut leaf_page = IndexPage {
            page_id,
            node: Box::new(leaf_node),
            serialized: Vec::new(),
        };

        // Serialize the page to get its length
        let serialized = leaf_page.serialize_page().unwrap();
        let serialized_length = serialized.len();

        // Create a new PositionIndex instance with a page size that is the serialized length + 9
        // This will ensure that the page needs splitting when we add another key
        let mut position_index = PositionIndex::new(&test_path, serialized_length + 9).unwrap();

        // Remove the last key and value from the leaf node
        let leaf_node = leaf_page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();
        leaf_node.keys.pop();
        leaf_node.values.pop();

        // Serialize the page again to check if serialized length + 10 is less than page_size
        let serialized = leaf_page.serialize_page().unwrap();
        assert!(serialized.len() + 10 <= serialized_length + 9, 
                "Serialized length + 10 should be less than or equal to page_size");

        // Add the page to the index
        position_index.index_pages.add_page(leaf_page);

        // Add a new key and value that will cause the leaf to split
        let new_key = 10000;
        let new_value = PositionIndexRecord {
            segment: 10,
            offset: 1000,
            type_hash: hash_type("Type10"),
        };

        // Call append_leaf_key_and_value with the page_id, key, and value
        let split_result = position_index.append_leaf_key_and_value(page_id, new_key, new_value);

        // Verify that the leaf was split
        assert!(split_result.is_some());
        let (first_key, new_page_id) = split_result.unwrap();

        // Verify the first key of the new leaf node
        assert_eq!(first_key, 10000);

        // Get the original page and verify it has 10 keys and values
        let original_page = position_index.index_pages.get_page(page_id).unwrap();
        let original_leaf = original_page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(original_leaf.keys.len(), 10);
        assert_eq!(original_leaf.values.len(), 10);
        assert_eq!(original_leaf.next_leaf_id, Some(new_page_id));

        // Get the new page and verify it has 1 key and value
        let new_page = position_index.index_pages.get_page(new_page_id).unwrap();
        let new_leaf = new_page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(new_leaf.keys.len(), 1);
        assert_eq!(new_leaf.values.len(), 1);
        assert_eq!(new_leaf.keys[0], 10000);
        assert_eq!(new_leaf.values[0].segment, 10);
        assert_eq!(new_leaf.values[0].offset, 1000);
        assert_eq!(new_leaf.next_leaf_id, None);

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_append_leaf_key_and_value_without_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance with a large page size to avoid splitting
        let mut position_index = PositionIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(30);

        // Create a LeafNode with 10 keys and 10 values
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for i in 0..10 {
            keys.push(i * 1000);
            values.push(PositionIndexRecord {
                segment: i as i32,
                offset: i as i32 * 100,
                type_hash: hash_type(&format!("Type{}", i)),
            });
        }

        let leaf_node = LeafNode {
            keys,
            values,
            next_leaf_id: None,
        };

        // Create an IndexPage with the LeafNode
        let leaf_page = IndexPage {
            page_id,
            node: Box::new(leaf_node),
            serialized: Vec::new(),
        };

        // Add the page to the index
        position_index.index_pages.add_page(leaf_page);

        // Add a new key and value that will not cause the leaf to split
        let new_key = 9999;
        let new_value = PositionIndexRecord {
            segment: 9,
            offset: 999,
            type_hash: hash_type("Type9"),
        };

        // Call append_leaf_key_and_value with the page_id, key, and value
        let split_result = position_index.append_leaf_key_and_value(page_id, new_key, new_value);

        // Verify that the leaf was not split
        assert!(split_result.is_none());

        // Get the page and verify it has 11 keys and values
        let page = position_index.index_pages.get_page(page_id).unwrap();
        let leaf = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf.keys.len(), 11);
        assert_eq!(leaf.values.len(), 11);
        assert_eq!(leaf.next_leaf_id, None);
        assert_eq!(leaf.keys[10], 9999);
        assert_eq!(leaf.values[10].segment, 9);
        assert_eq!(leaf.values[10].offset, 999);

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it has 11 keys and values
        let page2 = position_index2.index_pages.get_page(page_id).unwrap();
        let leaf2 = page2.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf2.keys.len(), 11);
        assert_eq!(leaf2.values.len(), 11);
        assert_eq!(leaf2.next_leaf_id, None);
        assert_eq!(leaf2.keys[10], 9999);
        assert_eq!(leaf2.values[10].segment, 9);
        assert_eq!(leaf2.values[10].offset, 999);

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_append_internal_key_and_value_with_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Construct a PageID
        let page_id = PageID(40);

        // Create an InternalNode with 5 keys and 6 child_ids
        let mut keys = Vec::new();
        let mut child_ids = Vec::new();
        for i in 0..5 {
            keys.push((i * 1000) as i64);
            child_ids.push(PageID(i * 100));
        }
        child_ids.push(PageID(500)); // One more child_id than keys

        let mut internal_node = InternalNode {
            keys,
            child_ids,
        };

        // Create an IndexPage with the InternalNode
        let mut internal_page = IndexPage {
            page_id,
            node: Box::new(internal_node),
            serialized: Vec::new(),
        };

        // Serialize the page to get its length
        let serialized = internal_page.serialize_page().unwrap();
        let page_size = serialized.len();

        // Create a new PositionIndex instance with a page size that is exactly the serialized length
        // This will ensure that the page needs splitting when we add another key and child_id
        let mut position_index = PositionIndex::new(&test_path, page_size).unwrap();

        // Add the page to the index
        position_index.index_pages.add_page(internal_page);

        // Add a new key and child_id that will cause the internal node to split
        let new_key = 5000i64;
        let new_child_id = PageID(600);

        // Call append_internal_key_and_value with the page_id, key, and child_id
        let split_result = position_index.append_internal_key_and_value(page_id, new_key, new_child_id);

        // Verify that the internal node was split
        assert!(split_result.is_some());
        let (promote_key, new_page_id) = split_result.unwrap();

        // Verify the promote_key
        assert_eq!(promote_key, 4000i64);

        // Get the original page and verify it has 4 keys and 5 child_ids
        let original_page = position_index.index_pages.get_page(page_id).unwrap();
        let original_internal = original_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(original_internal.keys.len(), 4);
        assert_eq!(original_internal.child_ids.len(), 5);
        assert_eq!(original_internal.keys[0], 0i64);
        assert_eq!(original_internal.keys[1], 1000i64);
        assert_eq!(original_internal.keys[2], 2000i64);
        assert_eq!(original_internal.keys[3], 3000i64);
        assert_eq!(original_internal.child_ids[0], PageID(0));
        assert_eq!(original_internal.child_ids[1], PageID(100));
        assert_eq!(original_internal.child_ids[2], PageID(200));
        assert_eq!(original_internal.child_ids[3], PageID(300));
        assert_eq!(original_internal.child_ids[4], PageID(400));

        // Get the new page and verify it has 1 key and 2 child_ids
        let new_page = position_index.index_pages.get_page(new_page_id).unwrap();
        let new_internal = new_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(new_internal.keys.len(), 1);
        assert_eq!(new_internal.child_ids.len(), 2);
        assert_eq!(new_internal.keys[0], 5000i64);
        assert_eq!(new_internal.child_ids[0], PageID(500));
        assert_eq!(new_internal.child_ids[1], PageID(600));

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, page_size).unwrap();

        // Get the original page and verify it has 4 keys and 5 child_ids
        let original_page = position_index2.index_pages.get_page(page_id).unwrap();
        let original_internal = original_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(original_internal.keys.len(), 4);
        assert_eq!(original_internal.child_ids.len(), 5);
        assert_eq!(original_internal.keys[0], 0i64);
        assert_eq!(original_internal.keys[1], 1000i64);
        assert_eq!(original_internal.keys[2], 2000i64);
        assert_eq!(original_internal.keys[3], 3000i64);
        assert_eq!(original_internal.child_ids[0], PageID(0));
        assert_eq!(original_internal.child_ids[1], PageID(100));
        assert_eq!(original_internal.child_ids[2], PageID(200));
        assert_eq!(original_internal.child_ids[3], PageID(300));
        assert_eq!(original_internal.child_ids[4], PageID(400));

        // Get the new page and verify it has 1 key and 2 child_ids
        let new_page = position_index2.index_pages.get_page(new_page_id).unwrap();
        let new_internal = new_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(new_internal.keys.len(), 1);
        assert_eq!(new_internal.child_ids.len(), 2);
        assert_eq!(new_internal.keys[0], 5000i64);
        assert_eq!(new_internal.child_ids[0], PageID(500));
        assert_eq!(new_internal.child_ids[1], PageID(600));
        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_append_internal_key_and_value_without_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance with a large page size to avoid splitting
        let mut position_index = PositionIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(40);

        // Create an InternalNode with 4 keys and 5 child_ids
        let mut keys = Vec::new();
        let mut child_ids = Vec::new();
        for i in 0..4 {
            keys.push((i * 1000) as i64);
            child_ids.push(PageID(i * 100));
        }
        child_ids.push(PageID(400)); // One more child_id than keys

        let internal_node = InternalNode {
            keys,
            child_ids,
        };

        // Create an IndexPage with the InternalNode
        let internal_page = IndexPage {
            page_id,
            node: Box::new(internal_node),
            serialized: Vec::new(),
        };

        // Add the page to the index
        position_index.index_pages.add_page(internal_page);

        // Add a new key and child_id that will not cause the internal node to split
        let new_key = 4000i64;
        let new_child_id = PageID(500);

        // Call append_internal_key_and_value with the page_id, key, and child_id
        let split_result = position_index.append_internal_key_and_value(page_id, new_key, new_child_id);

        // Verify that the internal node was not split
        assert!(split_result.is_none());

        // Get the page and verify it has 5 keys and 6 child_ids
        let page = position_index.index_pages.get_page(page_id).unwrap();
        let internal = page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(internal.keys.len(), 5);
        assert_eq!(internal.child_ids.len(), 6);
        assert_eq!(internal.keys[4], 4000i64);
        assert_eq!(internal.child_ids[5], PageID(500));

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it has 5 keys and 6 child_ids
        let page2 = position_index2.index_pages.get_page(page_id).unwrap();
        let internal2 = page2.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(internal2.keys.len(), 5);
        assert_eq!(internal2.child_ids.len(), 6);
        assert_eq!(internal2.keys[4], 4000i64);
        assert_eq!(internal2.child_ids[5], PageID(500));

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_insert_lookup_no_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance with a large page size to avoid splitting
        let mut position_index = PositionIndex::new(&test_path, 4096).unwrap();


        // Insert a few records
        let mut inserted: Vec<(Position, PositionIndexRecord)> = Vec::new();
        for i in 0..5 {
            let position = (i + 1).into();
            let record = PositionIndexRecord {
                segment: i,
                offset: i * 10,
                type_hash: hash_type(&format!("test-{}", i)),
            };

            position_index.insert(position, record.clone()).unwrap();
            inserted.push((position, record));
        }

        // Check lookup before flush
        for (position, record) in &inserted {
            let result = position_index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, 4096).unwrap();


        // Check lookup after flush
        for (position, record) in &inserted {
            let result = position_index2.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }
    }    

    #[test]
    fn test_insert_lookup_split_leaf_node() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance with a large page size to avoid splitting
        let page_size = 250;
        let mut position_index = PositionIndex::new(&test_path, page_size).unwrap();


        // Insert a few records
        let mut inserted: Vec<(Position, PositionIndexRecord)> = Vec::new();
        for i in 0..20 {
            let position = (i + 1).into();
            let record = PositionIndexRecord {
                segment: i,
                offset: i * 10,
                type_hash: hash_type(&format!("test-{}", i)),
            };

            position_index.insert(position, record.clone()).unwrap();
            inserted.push((position, record));
        }

        // Check lookup before flush
        for (position, record) in &inserted {
            let result = position_index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, page_size).unwrap();


        // Check lookup after flush
        for (position, record) in &inserted {
            let result = position_index2.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }
    }

    #[test]
    fn test_insert_lookup_split_internal_node_small_page_size() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance with a large page size to avoid splitting
        let page_size = 4096;
        let mut position_index = PositionIndex::new(&test_path, page_size).unwrap();

        // Insert a few records
        let mut inserted: Vec<(Position, PositionIndexRecord)> = Vec::new();
        for i in 0..150000 {
            let position = (i + 1).into();
            let record = PositionIndexRecord {
                segment: i,
                offset: i * 10,
                type_hash: hash_type(&format!("test-{}", i)),
            };

            position_index.insert(position, record.clone()).unwrap();
            inserted.push((position, record));
        }

        // Check lookup before flush
        for (position, record) in &inserted {
            let result = position_index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();
        
        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, page_size).unwrap();
        
        
        // Check lookup after flush
        for (position, record) in &inserted {
            let result = position_index2.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }
    }

    #[test]
    fn test_insert_lookup_split_internal_node_normal_page_size() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance with a large page size to avoid splitting
        let page_size = 4096;
        let mut position_index = PositionIndex::new(&test_path, page_size).unwrap();


        // Insert a few records
        let mut inserted: Vec<(Position, PositionIndexRecord)> = Vec::new();
        for i in 0..70000 {
            let position = (i + 1).into();
            let record = PositionIndexRecord {
                segment: i,
                offset: i * 10,
                type_hash: hash_type(&format!("test-{}", i)),
            };

            position_index.insert(position, record.clone()).unwrap();
            inserted.push((position, record));
        }

        // Check lookup before flush
        for (position, record) in &inserted {
            let result = position_index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }

        // Flush changes to disk
        position_index.index_pages.flush().unwrap();

        // Create another instance of PositionIndex
        let mut position_index2 = PositionIndex::new(&test_path, page_size).unwrap();


        // Check lookup after flush
        for (position, record) in &inserted {
            let result = position_index2.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }
    }

    #[test]
    fn test_no_duplicate_keys_in_leaf_node() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new PositionIndex instance
        let page_size = 4096;
        let mut position_index = PositionIndex::new(&test_path, page_size).unwrap();

        // Create a position and two different records
        let position: Position = 42.into();
        let record1 = PositionIndexRecord {
            segment: 1,
            offset: 100,
            type_hash: hash_type("test-1"),
        };
        let record2 = PositionIndexRecord {
            segment: 2,
            offset: 200,
            type_hash: hash_type("test-2"),
        };

        // Insert the first record
        position_index.insert(position, record1.clone()).unwrap();

        // Try to insert a second record with the same key
        position_index.insert(position, record2.clone()).unwrap();

        // Lookup the key and verify only one record exists and it's the latest one
        let result = position_index.lookup(position).unwrap();
        assert!(result.is_some());
        assert_eq!(&result.unwrap(), &record1);

        // Get the root page ID
        let header_node = position_index.index_pages.header_node();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let page = position_index.index_pages.get_page(root_page_id).unwrap();

        // Verify it's a leaf node
        assert_eq!(page.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast to a LeafNode
        let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Count occurrences of our position in the keys
        let key_count = leaf_node.keys.iter().filter(|&k| *k == position).count();

        // Verify there's only one occurrence of the key
        assert_eq!(key_count, 1, "Duplicate keys should not be added to leaf nodes");
    }
}
