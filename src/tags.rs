use uuid::{uuid, Uuid};
use std::format;
use std::path::Path;
use std::any::Any;
use crate::pagedfile::{PageID, PAGE_ID_SIZE};
use crate::wal::{Position, POSITION_SIZE};
use crate::indexpages::{IndexPages, Node, IndexPage, HeaderNode};


// Hash a tag string to a fixed-length byte array
const NAMESPACE_URL: Uuid = uuid!("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
const TAG_HASH_LEN: usize = 8;
// TODO: Maybe this should be 16?

pub fn hash_tag(type_str: &str) -> Vec<u8> {
    let uuid = Uuid::new_v5(&NAMESPACE_URL, format!("/tag/{}", type_str).as_bytes());
    uuid.as_bytes()[..TAG_HASH_LEN].to_vec()
}

/// Constant for the internal node type
pub const INTERNAL_NODE_TYPE: u8 = 2;

/// Constant for the leaf node type
pub const LEAF_NODE_TYPE: u8 = 3;

/// Constant for the tag leaf node type
pub const TAG_LEAF_NODE_TYPE: u8 = 4;

/// Constant for the tag internal node type
pub const TAG_INTERNAL_NODE_TYPE: u8 = 5;

// Leaf node for the B+tree
#[derive(Debug, Clone)]
pub struct LeafNode {
    pub keys: Vec<[u8; TAG_HASH_LEN]>,
    pub values: Vec<Vec<Position>>,
}

impl LeafNode {
    fn calc_serialized_size(&self) -> usize {
        // This is a simplified implementation for now
        // 2 bytes for keys_len + keys * (8 bytes per key) + values size
        let keys_len = self.keys.len();
        let mut total_size = 2 + (keys_len * TAG_HASH_LEN);

        // Add size for values (simplified for now)
        for value in &self.values {
            total_size += 2; // 2 bytes for length
            total_size += value.len() * POSITION_SIZE; // Each Position is 8 bytes
        }

        total_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        // This is a simplified implementation for now
        let total_size = self.calc_serialized_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(key);
        }

        // Serialize each value (simplified for now)
        for value in &self.values {
            // Serialize the length of the value (2 bytes)
            result.extend_from_slice(&(value.len() as u16).to_le_bytes());

            // Serialize each Position (8 bytes each)
            for pos in value {
                result.extend_from_slice(&pos.to_le_bytes());
            }
        }

        result
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, std::io::Error> {
        // This is a simplified implementation for now
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected at least 2 bytes, got {}", slice.len()),
            ));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * TAG_HASH_LEN);
        if slice.len() < min_expected_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected at least {} bytes for keys, got {}", min_expected_size, slice.len()),
            ));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * TAG_HASH_LEN);
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&slice[start..start + TAG_HASH_LEN]);
            keys.push(key);
        }

        // Extract the values (simplified for now)
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * TAG_HASH_LEN);

        for _ in 0..keys_len {
            if offset + 2 > slice.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unexpected end of data while reading value length",
                ));
            }

            // Extract the length of the value (2 bytes)
            let value_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
            offset += 2;

            if offset + (value_len * POSITION_SIZE) > slice.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unexpected end of data while reading value",
                ));
            }

            // Extract the Positions (8 bytes each)
            let mut value = Vec::with_capacity(value_len);
            for j in 0..value_len {
                let pos_start = offset + (j * POSITION_SIZE);
                let pos = i64::from_le_bytes([
                    slice[pos_start], slice[pos_start + 1], slice[pos_start + 2], slice[pos_start + 3],
                    slice[pos_start + 4], slice[pos_start + 5], slice[pos_start + 6], slice[pos_start + 7],
                ]);
                value.push(pos);
            }

            values.push(value);
            offset += value_len * POSITION_SIZE;
        }

        Ok(LeafNode {
            keys,
            values,
        })
    }
}

impl Node for LeafNode {
    fn node_type_byte(&self) -> u8 {
        LEAF_NODE_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize()
    }

    fn calc_serialized_size(&self) -> usize {
        self.calc_serialized_size()
    }
}

// Internal node for the B+tree
#[derive(Debug, Clone)]
pub struct InternalNode {
    pub keys: Vec<[u8; TAG_HASH_LEN]>,
    pub child_ids: Vec<PageID>,
}

impl InternalNode {
    fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + keys * (8 bytes per key) + child_ids * (4 bytes per child_id)
        let keys_len = self.keys.len();
        let total_size = 2 + (keys_len * TAG_HASH_LEN) + (self.child_ids.len() * PAGE_ID_SIZE);
        total_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total_size = self.calc_serialized_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(key);
        }

        // Serialize each PageID value (4 bytes each)
        for child_id in &self.child_ids {
            result.extend_from_slice(&child_id.0.to_le_bytes());
        }

        result
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, std::io::Error> {
        // Check if the slice has at least 2 bytes for the keys_len
        if slice.len() < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected at least 2 bytes, got {}", slice.len()),
            ));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the expected size of the slice
        // 2 bytes for keys_len + 8 bytes per key + 4 bytes per child_id (keys_len + 1 child_ids)
        let expected_size = 2 + (keys_len * TAG_HASH_LEN) + ((keys_len + 1) * PAGE_ID_SIZE);
        if slice.len() < expected_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected {} bytes, got {}", expected_size, slice.len()),
            ));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * TAG_HASH_LEN);
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&slice[start..start + TAG_HASH_LEN]);
            keys.push(key);
        }

        // Extract the PageID values (4 bytes each)
        let mut child_ids = Vec::with_capacity(keys_len + 1);
        for i in 0..(keys_len + 1) {
            let start = 2 + (keys_len * TAG_HASH_LEN) + (i * PAGE_ID_SIZE);
            let page_id = u32::from_le_bytes([
                slice[start], slice[start + 1], slice[start + 2], slice[start + 3],
            ]);
            child_ids.push(PageID(page_id));
        }

        Ok(InternalNode {
            keys,
            child_ids,
        })
    }
}

impl Node for InternalNode {
    fn node_type_byte(&self) -> u8 {
        INTERNAL_NODE_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize()
    }

    fn calc_serialized_size(&self) -> usize {
        self.calc_serialized_size()
    }
}

// Tag leaf node for the B+tree
#[derive(Debug, Clone)]
pub struct TagLeafNode {
    pub positions: Vec<Position>,
    pub next_leaf_id: Option<PageID>,
}

impl TagLeafNode {
    fn calc_serialized_size(&self) -> usize {
        // 4 bytes for next_leaf_id + 2 bytes for positions_len + positions * (8 bytes per position)
        let positions_len = self.positions.len();
        let total_size = 4 + 2 + (positions_len * POSITION_SIZE);
        total_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total_size = self.calc_serialized_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the next_leaf_id (4 bytes)
        let next_leaf_id = match self.next_leaf_id {
            Some(id) => id.0,
            None => 0, // Use 0 to represent None
        };
        result.extend_from_slice(&next_leaf_id.to_le_bytes());

        // Serialize the length of the positions (2 bytes)
        result.extend_from_slice(&(self.positions.len() as u16).to_le_bytes());

        // Serialize each Position (8 bytes each)
        for pos in &self.positions {
            result.extend_from_slice(&pos.to_le_bytes());
        }

        result
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, std::io::Error> {
        // Check if the slice has at least 6 bytes (4 for next_leaf_id, 2 for positions_len)
        if slice.len() < 6 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected at least 6 bytes, got {}", slice.len()),
            ));
        }

        // Extract the next_leaf_id (first 4 bytes)
        let next_leaf_id = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]);
        let next_leaf_id = if next_leaf_id == 0 { None } else { Some(PageID(next_leaf_id)) };

        // Extract the length of the positions (next 2 bytes)
        let positions_len = u16::from_le_bytes([slice[4], slice[5]]) as usize;

        // Calculate the expected size of the slice
        let expected_size = 6 + (positions_len * POSITION_SIZE);
        if slice.len() < expected_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected {} bytes, got {}", expected_size, slice.len()),
            ));
        }

        // Extract the Position values (8 bytes each)
        let mut positions = Vec::with_capacity(positions_len);
        for i in 0..positions_len {
            let start = 6 + (i * POSITION_SIZE);
            let pos = i64::from_le_bytes([
                slice[start], slice[start + 1], slice[start + 2], slice[start + 3],
                slice[start + 4], slice[start + 5], slice[start + 6], slice[start + 7],
            ]);
            positions.push(pos);
        }

        Ok(TagLeafNode {
            positions,
            next_leaf_id,
        })
    }
}

impl Node for TagLeafNode {
    fn node_type_byte(&self) -> u8 {
        TAG_LEAF_NODE_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize()
    }

    fn calc_serialized_size(&self) -> usize {
        self.calc_serialized_size()
    }
}

// Tag internal node for the B+tree
#[derive(Debug, Clone)]
pub struct TagInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl TagInternalNode {
    fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len + keys * (8 bytes per key) + child_ids * (4 bytes per child_id)
        let keys_len = self.keys.len();
        let total_size = 2 + (keys_len * POSITION_SIZE) + (self.child_ids.len() * PAGE_ID_SIZE);
        total_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total_size = self.calc_serialized_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each Position key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.to_le_bytes());
        }

        // Serialize each PageID value (4 bytes each)
        for child_id in &self.child_ids {
            result.extend_from_slice(&child_id.0.to_le_bytes());
        }

        result
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, std::io::Error> {
        // Check if the slice has at least 2 bytes for the keys_len
        if slice.len() < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected at least 2 bytes, got {}", slice.len()),
            ));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the expected size of the slice
        // 2 bytes for keys_len + 8 bytes per key + 4 bytes per child_id (keys_len + 1 child_ids)
        let expected_size = 2 + (keys_len * POSITION_SIZE) + ((keys_len + 1) * PAGE_ID_SIZE);
        if slice.len() < expected_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected {} bytes, got {}", expected_size, slice.len()),
            ));
        }

        // Extract the Position keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * POSITION_SIZE);
            let key = i64::from_le_bytes([
                slice[start], slice[start + 1], slice[start + 2], slice[start + 3],
                slice[start + 4], slice[start + 5], slice[start + 6], slice[start + 7],
            ]);
            keys.push(key);
        }

        // Extract the PageID values (4 bytes each)
        let mut child_ids = Vec::with_capacity(keys_len + 1);
        for i in 0..(keys_len + 1) {
            let start = 2 + (keys_len * POSITION_SIZE) + (i * PAGE_ID_SIZE);
            let page_id = u32::from_le_bytes([
                slice[start], slice[start + 1], slice[start + 2], slice[start + 3],
            ]);
            child_ids.push(PageID(page_id));
        }

        Ok(TagInternalNode {
            keys,
            child_ids,
        })
    }
}

impl Node for TagInternalNode {
    fn node_type_byte(&self) -> u8 {
        TAG_INTERNAL_NODE_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize()
    }

    fn calc_serialized_size(&self) -> usize {
        self.calc_serialized_size()
    }
}

/// A structure that manages tag indexing using IndexPages
pub struct TagIndex {
    pub index_pages: IndexPages,
}

impl TagIndex {
    /// Creates a new TagIndex with the given path and page size
    pub fn new<P: AsRef<Path>>(path: P, page_size: usize) -> std::io::Result<Self> {
        Self::new_with_cache_capacity(path, page_size, None)
    }

    /// Creates a new TagIndex with the given path, page size, and cache capacity
    pub fn new_with_cache_capacity<P: AsRef<Path>>(path: P, page_size: usize, cache_capacity: Option<usize>) -> std::io::Result<Self> {
        let mut index_pages = IndexPages::new_with_cache_capacity(path, page_size, cache_capacity)?;

        // Register deserializers for LeafNode and InternalNode
        index_pages.deserializer.register(LEAF_NODE_TYPE, |data| {
            let leaf_node: LeafNode = LeafNode::from_slice(data)?;
            Ok(Box::new(leaf_node) as Box<dyn Node>)
        });

        index_pages.deserializer.register(INTERNAL_NODE_TYPE, |data| {
            let internal_node: InternalNode = InternalNode::from_slice(data)?;
            Ok(Box::new(internal_node) as Box<dyn Node>)
        });

        index_pages.deserializer.register(TAG_LEAF_NODE_TYPE, |data| {
            let tag_leaf_node: TagLeafNode = TagLeafNode::from_slice(data)?;
            Ok(Box::new(tag_leaf_node) as Box<dyn Node>)
        });

        index_pages.deserializer.register(TAG_INTERNAL_NODE_TYPE, |data| {
            let tag_internal_node: TagInternalNode = TagInternalNode::from_slice(data)?;
            Ok(Box::new(tag_internal_node) as Box<dyn Node>)
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
            };

            // Create an IndexPage with the root page ID and the empty LeafNode
            let root_page = IndexPage {
                page_id: root_page_id,
                node: Box::new(leaf_node),
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
    use crate::indexpages::{HeaderNode, IndexPage};
    use tempfile::TempDir;

    #[test]
    fn test_tag_index_construction() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance using the constructor
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Verify that the TagIndex was created successfully
        assert!(test_path.exists());

        // Get the root page ID from the header page
        let header_node = tag_index.index_pages.header_page.node.as_any().downcast_ref::<HeaderNode>().unwrap();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let root_page = tag_index.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page contains a LeafNode
        assert_eq!(root_page.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast the node to a LeafNode
        let leaf_node = root_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode is empty
        assert!(leaf_node.keys.is_empty());
        assert!(leaf_node.values.is_empty());

        // Create a second instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the root page from the second instance
        let root_page2 = tag_index2.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page contains a LeafNode
        assert_eq!(root_page2.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast the node to a LeafNode
        let leaf_node2 = root_page2.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode is empty
        assert!(leaf_node2.keys.is_empty());
        assert!(leaf_node2.values.is_empty());

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_leaf_node_serialization() {
        // Create a non-trivial LeafNode instance
        let mut key1 = [0u8; TAG_HASH_LEN];
        let mut key2 = [0u8; TAG_HASH_LEN];
        let mut key3 = [0u8; TAG_HASH_LEN];

        // Use hash_tag to generate tag hashes
        let tag1_hash = hash_tag("user");
        let tag2_hash = hash_tag("product");
        let tag3_hash = hash_tag("order");

        // Copy the hash values to the fixed-size arrays
        key1.copy_from_slice(&tag1_hash[..TAG_HASH_LEN]);
        key2.copy_from_slice(&tag2_hash[..TAG_HASH_LEN]);
        key3.copy_from_slice(&tag3_hash[..TAG_HASH_LEN]);

        let leaf_node = LeafNode {
            keys: vec![
                key1,
                key2,
                key3,
            ],
            values: vec![
                vec![1000, 1001, 1002], // Positions for tag1
                vec![2000, 2001],       // Positions for tag2
                vec![3000],             // Positions for tag3
            ],
        };

        // Serialize the node
        let serialized = leaf_node.serialize();

        // Deserialize the node
        let deserialized: LeafNode = LeafNode::from_slice(&serialized).unwrap();

        // Verify that the deserialized node matches the original
        assert_eq!(deserialized.keys.len(), leaf_node.keys.len());
        assert_eq!(deserialized.values.len(), leaf_node.values.len());

        for i in 0..leaf_node.keys.len() {
            assert_eq!(deserialized.keys[i], leaf_node.keys[i]);
            assert_eq!(deserialized.values[i], leaf_node.values[i]);
        }

        // Verify the node type byte
        assert_eq!(leaf_node.node_type_byte(), LEAF_NODE_TYPE);

        // Calculate the serialized page size
        let page_size = leaf_node.calc_serialized_page_size();

        // Calculate the expected size: 
        // 2 bytes for keys length + 
        // (3 keys * 8 bytes) + 
        // (2 bytes for value1 length + 3 positions * 8 bytes) +
        // (2 bytes for value2 length + 2 positions * 8 bytes) +
        // (2 bytes for value3 length + 1 position * 8 bytes) +
        // 9 bytes for page overhead
        let expected_size = 2 + (3 * TAG_HASH_LEN) + 
                           (2 + 3 * 8) + (2 + 2 * 8) + (2 + 1 * 8) + 9;

        // Verify that the page size is correct
        assert_eq!(page_size, expected_size, 
                   "Page size should be {} bytes", expected_size);

        // Serialize the LeafNode to a page format
        let page_data = leaf_node.serialize_page();

        // Verify that the page data is not empty
        assert!(!page_data.is_empty(), "Page data should not be empty");

        // Verify that the page data has the correct length
        assert_eq!(page_data.len(), expected_size, "Page data should be {} bytes", expected_size);

        // Verify that the page data starts with the correct node type byte
        assert_eq!(page_data[0], LEAF_NODE_TYPE, "Page data should start with the leaf node type byte");
    }

    #[test]
    fn test_internal_node_serialization() {
        // Create a non-trivial InternalNode instance
        let mut key1 = [0u8; TAG_HASH_LEN];
        let mut key2 = [0u8; TAG_HASH_LEN];
        let mut key3 = [0u8; TAG_HASH_LEN];

        // Use hash_tag to generate tag hashes
        let tag1_hash = hash_tag("user");
        let tag2_hash = hash_tag("product");
        let tag3_hash = hash_tag("order");

        // Copy the hash values to the fixed-size arrays
        key1.copy_from_slice(&tag1_hash[..TAG_HASH_LEN]);
        key2.copy_from_slice(&tag2_hash[..TAG_HASH_LEN]);
        key3.copy_from_slice(&tag3_hash[..TAG_HASH_LEN]);

        let internal_node = InternalNode {
            keys: vec![
                key1,
                key2,
                key3,
            ],
            child_ids: vec![
                PageID(10),
                PageID(20),
                PageID(30),
                PageID(40),
            ],
        };

        // Serialize the node
        let serialized = internal_node.serialize();

        // Deserialize the node
        let deserialized: InternalNode = InternalNode::from_slice(&serialized).unwrap();

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

        // Calculate the serialized page size
        let page_size = internal_node.calc_serialized_page_size();

        // Calculate the expected size: 
        // 2 bytes for keys length + 
        // (3 keys * 8 bytes) + (4 child_ids * 4 bytes) + 9 bytes for page overhead
        let expected_size = 2 + (3 * TAG_HASH_LEN) + (4 * PAGE_ID_SIZE) + 9;

        // Verify that the page size is correct
        assert_eq!(page_size, expected_size, 
                   "Page size should be {} bytes", expected_size);

        // Serialize the InternalNode to a page format
        let page_data = internal_node.serialize_page();

        // Verify that the page data is not empty
        assert!(!page_data.is_empty(), "Page data should not be empty");

        // Verify that the page data has the correct length
        assert_eq!(page_data.len(), expected_size, "Page data should be {} bytes", expected_size);

        // Verify that the page data starts with the correct node type byte
        assert_eq!(page_data[0], INTERNAL_NODE_TYPE, "Page data should start with the internal node type byte");
    }

    #[test]
    fn test_tag_leaf_node_serialization() {
        // Test case 1: TagLeafNode with next_leaf_id as Some
        // Create a non-trivial TagLeafNode instance
        let tag_leaf_node = TagLeafNode {
            positions: vec![
                1000, // Position is just an i64
                2000,
                3000,
                4000,
                5000,
            ],
            next_leaf_id: Some(PageID(42)),
        };

        // Serialize the node
        let serialized = tag_leaf_node.serialize();

        // Deserialize the node
        let deserialized: TagLeafNode = TagLeafNode::from_slice(&serialized).unwrap();

        // Verify that the deserialized node matches the original
        assert_eq!(deserialized.positions.len(), tag_leaf_node.positions.len());
        assert_eq!(deserialized.next_leaf_id, tag_leaf_node.next_leaf_id);

        for i in 0..tag_leaf_node.positions.len() {
            assert_eq!(deserialized.positions[i], tag_leaf_node.positions[i]);
        }

        // Verify the node type byte
        assert_eq!(tag_leaf_node.node_type_byte(), TAG_LEAF_NODE_TYPE);

        // Calculate the serialized page size
        let page_size = tag_leaf_node.calc_serialized_page_size();

        // Calculate the expected size: 
        // 4 bytes for next_leaf_id + 2 bytes for positions length + 
        // (5 positions * 8 bytes) + 9 bytes for page overhead
        let expected_size = 4 + 2 + (5 * POSITION_SIZE) + 9;

        // Verify that the page size is correct
        assert_eq!(page_size, expected_size, 
                   "Page size should be {} bytes", expected_size);

        // Serialize the TagLeafNode to a page format
        let page_data = tag_leaf_node.serialize_page();

        // Verify that the page data is not empty
        assert!(!page_data.is_empty(), "Page data should not be empty");

        // Verify that the page data has the correct length
        assert_eq!(page_data.len(), expected_size, "Page data should be {} bytes", expected_size);

        // Verify that the page data starts with the correct node type byte
        assert_eq!(page_data[0], TAG_LEAF_NODE_TYPE, "Page data should start with the tag leaf node type byte");

        // Test case 2: TagLeafNode with next_leaf_id as None
        // Create a TagLeafNode instance with next_leaf_id as None
        let tag_leaf_node_none = TagLeafNode {
            positions: vec![
                1000, // Position is just an i64
                2000,
                3000,
            ],
            next_leaf_id: None,
        };

        // Serialize the node
        let serialized_none = tag_leaf_node_none.serialize();

        // Deserialize the node
        let deserialized_none: TagLeafNode = TagLeafNode::from_slice(&serialized_none).unwrap();

        // Verify that the deserialized node matches the original
        assert_eq!(deserialized_none.positions.len(), tag_leaf_node_none.positions.len());
        assert_eq!(deserialized_none.next_leaf_id, tag_leaf_node_none.next_leaf_id);
        assert_eq!(deserialized_none.next_leaf_id, None);

        for i in 0..tag_leaf_node_none.positions.len() {
            assert_eq!(deserialized_none.positions[i], tag_leaf_node_none.positions[i]);
        }

        // Verify the node type byte
        assert_eq!(tag_leaf_node_none.node_type_byte(), TAG_LEAF_NODE_TYPE);

        // Calculate the serialized page size
        let page_size_none = tag_leaf_node_none.calc_serialized_page_size();

        // Calculate the expected size: 
        // 4 bytes for next_leaf_id + 2 bytes for positions length + 
        // (3 positions * 8 bytes) + 9 bytes for page overhead
        let expected_size_none = 4 + 2 + (3 * POSITION_SIZE) + 9;

        // Verify that the page size is correct
        assert_eq!(page_size_none, expected_size_none, 
                   "Page size should be {} bytes", expected_size_none);

        // Serialize the TagLeafNode to a page format
        let page_data_none = tag_leaf_node_none.serialize_page();

        // Verify that the page data is not empty
        assert!(!page_data_none.is_empty(), "Page data should not be empty");

        // Verify that the page data has the correct length
        assert_eq!(page_data_none.len(), expected_size_none, "Page data should be {} bytes", expected_size_none);

        // Verify that the page data starts with the correct node type byte
        assert_eq!(page_data_none[0], TAG_LEAF_NODE_TYPE, "Page data should start with the tag leaf node type byte");
    }

    #[test]
    fn test_tag_internal_node_serialization() {
        // Create a non-trivial TagInternalNode instance
        let tag_internal_node = TagInternalNode {
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
        let serialized = tag_internal_node.serialize();

        // Deserialize the node
        let deserialized: TagInternalNode = TagInternalNode::from_slice(&serialized).unwrap();

        // Verify that the deserialized node matches the original
        assert_eq!(deserialized.keys.len(), tag_internal_node.keys.len());
        assert_eq!(deserialized.child_ids.len(), tag_internal_node.child_ids.len());

        for i in 0..tag_internal_node.keys.len() {
            assert_eq!(deserialized.keys[i], tag_internal_node.keys[i]);
        }

        for i in 0..tag_internal_node.child_ids.len() {
            assert_eq!(deserialized.child_ids[i], tag_internal_node.child_ids[i]);
        }

        // Verify the node type byte
        assert_eq!(tag_internal_node.node_type_byte(), TAG_INTERNAL_NODE_TYPE);

        // Calculate the serialized page size
        let page_size = tag_internal_node.calc_serialized_page_size();

        // Calculate the expected size: 
        // 2 bytes for keys length + 
        // (3 keys * 8 bytes) + (4 child_ids * 4 bytes) + 9 bytes for page overhead
        let expected_size = 2 + (3 * POSITION_SIZE) + (4 * PAGE_ID_SIZE) + 9;

        // Verify that the page size is correct
        assert_eq!(page_size, expected_size, 
                   "Page size should be {} bytes", expected_size);

        // Serialize the TagInternalNode to a page format
        let page_data = tag_internal_node.serialize_page();

        // Verify that the page data is not empty
        assert!(!page_data.is_empty(), "Page data should not be empty");

        // Verify that the page data has the correct length
        assert_eq!(page_data.len(), expected_size, "Page data should be {} bytes", expected_size);

        // Verify that the page data starts with the correct node type byte
        assert_eq!(page_data[0], TAG_INTERNAL_NODE_TYPE, "Page data should start with the tag internal node type byte");
    }

    #[test]
    fn test_deserializer_registration() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance
        let tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Create instances of all four node types
        // 1. LeafNode
        let mut key1 = [0u8; TAG_HASH_LEN];
        let mut key2 = [0u8; TAG_HASH_LEN];

        // Use hash_tag to generate tag hashes
        let tag1_hash = hash_tag("user");
        let tag2_hash = hash_tag("product");

        // Copy the hash values to the fixed-size arrays
        key1.copy_from_slice(&tag1_hash[..TAG_HASH_LEN]);
        key2.copy_from_slice(&tag2_hash[..TAG_HASH_LEN]);

        let leaf_node = LeafNode {
            keys: vec![key1, key2],
            values: vec![
                vec![1000, 1001], // Positions for tag1
                vec![2000, 2001], // Positions for tag2
            ],
        };

        // 2. InternalNode
        let internal_node = InternalNode {
            keys: vec![key1, key2],
            child_ids: vec![PageID(10), PageID(20), PageID(30)],
        };

        // 3. TagLeafNode
        let tag_leaf_node = TagLeafNode {
            positions: vec![1000, 2000, 3000],
            next_leaf_id: Some(PageID(42)),
        };

        // 4. TagInternalNode
        let tag_internal_node = TagInternalNode {
            keys: vec![1000, 2000],
            child_ids: vec![PageID(10), PageID(20), PageID(30)],
        };

        // Create IndexPage instances with the nodes
        let leaf_page = IndexPage {
            page_id: PageID(1),
            node: Box::new(leaf_node.clone()),
        };

        let internal_page = IndexPage {
            page_id: PageID(2),
            node: Box::new(internal_node.clone()),
        };

        let tag_leaf_page = IndexPage {
            page_id: PageID(3),
            node: Box::new(tag_leaf_node.clone()),
        };

        let tag_internal_page = IndexPage {
            page_id: PageID(4),
            node: Box::new(tag_internal_node.clone()),
        };

        // Serialize the pages
        let leaf_serialized = leaf_page.node.serialize_page();
        let internal_serialized = internal_page.node.serialize_page();
        let tag_leaf_serialized = tag_leaf_page.node.serialize_page();
        let tag_internal_serialized = tag_internal_page.node.serialize_page();

        // Deserialize the pages using the registered deserializers
        let deserialized_leaf_page = tag_index.index_pages.deserializer.deserialize_page(&leaf_serialized, PageID(1)).unwrap();
        let deserialized_internal_page = tag_index.index_pages.deserializer.deserialize_page(&internal_serialized, PageID(2)).unwrap();
        let deserialized_tag_leaf_page = tag_index.index_pages.deserializer.deserialize_page(&tag_leaf_serialized, PageID(3)).unwrap();
        let deserialized_tag_internal_page = tag_index.index_pages.deserializer.deserialize_page(&tag_internal_serialized, PageID(4)).unwrap();

        // Verify that the deserialized nodes have the correct type
        assert_eq!(deserialized_leaf_page.node.node_type_byte(), LEAF_NODE_TYPE);
        assert_eq!(deserialized_internal_page.node.node_type_byte(), INTERNAL_NODE_TYPE);
        assert_eq!(deserialized_tag_leaf_page.node.node_type_byte(), TAG_LEAF_NODE_TYPE);
        assert_eq!(deserialized_tag_internal_page.node.node_type_byte(), TAG_INTERNAL_NODE_TYPE);

        // Downcast and verify the leaf node
        let deserialized_leaf = deserialized_leaf_page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(deserialized_leaf.keys.len(), leaf_node.keys.len());
        assert_eq!(deserialized_leaf.values.len(), leaf_node.values.len());

        // Downcast and verify the internal node
        let deserialized_internal = deserialized_internal_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(deserialized_internal.keys.len(), internal_node.keys.len());
        assert_eq!(deserialized_internal.child_ids.len(), internal_node.child_ids.len());

        // Downcast and verify the tag leaf node
        let deserialized_tag_leaf = deserialized_tag_leaf_page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(deserialized_tag_leaf.positions.len(), tag_leaf_node.positions.len());
        assert_eq!(deserialized_tag_leaf.next_leaf_id, tag_leaf_node.next_leaf_id);

        // Downcast and verify the tag internal node
        let deserialized_tag_internal = deserialized_tag_internal_page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
        assert_eq!(deserialized_tag_internal.keys.len(), tag_internal_node.keys.len());
        assert_eq!(deserialized_tag_internal.child_ids.len(), tag_internal_node.child_ids.len());

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }
}
