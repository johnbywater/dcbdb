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
    fn calc_serialized_node_size(&self) -> usize {
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
        let total_size = self.calc_serialized_node_size();
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

    fn calc_serialized_node_size(&self) -> usize {
        self.calc_serialized_node_size()
    }
}

// Internal node for the B+tree
#[derive(Debug, Clone)]
pub struct InternalNode {
    pub keys: Vec<[u8; TAG_HASH_LEN]>,
    pub child_ids: Vec<PageID>,
}

impl InternalNode {
    fn calc_serialized_node_size(&self) -> usize {
        // 2 bytes for keys_len + keys * (8 bytes per key) + child_ids * (4 bytes per child_id)
        let keys_len = self.keys.len();
        let total_size = 2 + (keys_len * TAG_HASH_LEN) + (self.child_ids.len() * PAGE_ID_SIZE);
        total_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total_size = self.calc_serialized_node_size();
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

    fn calc_serialized_node_size(&self) -> usize {
        self.calc_serialized_node_size()
    }
}

// Tag leaf node for the B+tree
#[derive(Debug, Clone)]
pub struct TagLeafNode {
    pub positions: Vec<Position>,
    pub next_leaf_id: Option<PageID>,
}

impl TagLeafNode {
    fn calc_serialized_node_size(&self) -> usize {
        // 4 bytes for next_leaf_id + 2 bytes for positions_len + positions * (8 bytes per position)
        let positions_len = self.positions.len();
        let total_size = 4 + 2 + (positions_len * POSITION_SIZE);
        total_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total_size = self.calc_serialized_node_size();
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

    fn calc_serialized_node_size(&self) -> usize {
        self.calc_serialized_node_size()
    }
}

// Tag internal node for the B+tree
#[derive(Debug, Clone)]
pub struct TagInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl TagInternalNode {
    fn calc_serialized_node_size(&self) -> usize {
        // 2 bytes for keys_len + keys * (8 bytes per key) + child_ids * (4 bytes per child_id)
        let keys_len = self.keys.len();
        let total_size = 2 + (keys_len * POSITION_SIZE) + (self.child_ids.len() * PAGE_ID_SIZE);
        total_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let total_size = self.calc_serialized_node_size();
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

    fn calc_serialized_node_size(&self) -> usize {
        self.calc_serialized_node_size()
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

    /// Returns the maximum page size
    fn get_max_page_size(&mut self) -> usize {
        self.index_pages.paged_file.page_size
    }

    /// Looks up a tag in the tag index and returns an iterator of positions
    ///
    /// # Arguments
    /// * `tag` - The tag to look up
    ///
    /// # Returns
    /// * `std::io::Result<Vec<Position>>` - Ok(positions) if the tag was found, Ok(empty vec) if not found, Err if an error occurred
    pub fn lookup(&mut self, tag: &str) -> std::io::Result<Vec<Position>> {
        // Hash the tag to get the key
        let tag_hash = hash_tag(tag);
        let mut key = [0u8; TAG_HASH_LEN];
        key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

        // Get the root page ID from the header page
        let header_node = self.index_pages.header_node();
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
                    // Key found, check if the positions are stored directly or in a TagLeafNode
                    if leaf_node.values[index].len() == 1 && leaf_node.values[index][0] < 0 {
                        // Positions are stored in a tag B+tree
                        let tag_tree_root_id = PageID((-leaf_node.values[index][0]) as u32);
                        return Ok(self.lookup_tag_tree(tag_tree_root_id)?);
                    } else {
                        // Positions are stored directly in the leaf node
                        return Ok(leaf_node.values[index].clone());
                    }
                }
                Err(_) => {
                    // Key not found
                    return Ok(Vec::new());
                }
            }
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
                            // Key found, check if the positions are stored directly or in a TagLeafNode
                            if leaf_node.values[index].len() == 1 && leaf_node.values[index][0] < 0 {
                                // Positions are stored in a tag B+tree
                                let tag_tree_root_id = PageID((-leaf_node.values[index][0]) as u32);
                                return Ok(self.lookup_tag_tree(tag_tree_root_id)?);
                            } else {
                                // Positions are stored directly in the leaf node
                                return Ok(leaf_node.values[index].clone());
                            }
                        }
                        Err(_) => {
                            // Key not found
                            return Ok(Vec::new());
                        }
                    }
                } else if page.node.node_type_byte() == INTERNAL_NODE_TYPE {
                    // Internal node, find the child to follow
                    let internal_node = page.node.as_any().downcast_ref::<InternalNode>().unwrap();

                    // Find the index of the first key greater than the search key using binary search
                    let index = match internal_node.keys.binary_search(&key) {
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

    /// Inserts a tag and position into the tag index
    ///
    /// # Arguments
    /// * `tag` - The tag to insert
    /// * `position` - The position to insert
    ///
    /// # Returns
    /// * `std::io::Result<()>` - Ok if the insertion was successful, Err otherwise
    pub fn insert(&mut self, tag: &str, position: Position) -> std::io::Result<()> {
        // Hash the tag to get the key
        let tag_hash = hash_tag(tag);
        let mut key = [0u8; TAG_HASH_LEN];
        key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

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
                // Internal node, find the correct child to follow
                let internal_node = page.node.as_any().downcast_ref::<InternalNode>().unwrap();

                // Use binary search to find the correct child
                let index = match internal_node.keys.binary_search(&key) {
                    Ok(index) => index + 1, // Key found, follow the child after the key
                    Err(index) => index,    // Key not found, follow the child at the insertion point
                };

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
        let mut split_info = self.insert_leaf_key_and_value(current_page_id, key, vec![position]);

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
            split_info = self.insert_internal_key_and_value(parent_id, promoted_key, new_page_id);
        }

        Ok(())
    }

    /// Appends a position key and page ID to a tag internal node if the key is greater than the last key
    ///
    /// # Arguments
    /// * `page_id` - The PageID of the page containing the TagInternalNode
    /// * `key` - The Position key to append
    /// * `child_id` - The PageID to append
    ///
    /// # Returns
    /// * `Option<(Position, PageID)>` - If the node was split, returns the first position of the new node and the new page ID
    pub fn append_tag_internal_node(&mut self, page_id: PageID, key: Position, child_id: PageID) -> Option<(Position, PageID)> {
        // Get a reference to the page
        let page = self.index_pages.get_page(page_id).unwrap();
        let tag_internal_node = page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();

        // Calculate the current serialized size
        let current_size = tag_internal_node.calc_serialized_page_size();

        // Calculate the size after adding the new key and child_id
        // Each key adds 8 bytes (Position) and each child_id adds 4 bytes (PageID)
        let new_size = current_size + POSITION_SIZE + PAGE_ID_SIZE;

        // Get the maximum page size
        let max_page_size = self.get_max_page_size();

        // Check if adding the key and child_id would exceed the page size
        if new_size <= max_page_size {
            // There's enough space, just append the key and child_id
            let page = self.index_pages.get_page_mut(page_id).unwrap();
            let tag_internal_node = page.node.as_any_mut().downcast_mut::<TagInternalNode>().unwrap();

            // Append the key and child_id
            tag_internal_node.keys.push(key);
            tag_internal_node.child_ids.push(child_id);

            // Mark the page as dirty
            self.index_pages.mark_dirty(page_id);

            // No split needed
            None
        } else {
            // Need to split the node
            // Allocate a new page ID for the new TagInternalNode
            let new_page_id = self.index_pages.alloc_page_id();

            // Get a mutable reference to the page
            let page = self.index_pages.get_page_mut(page_id).unwrap();
            let tag_internal_node = page.node.as_any_mut().downcast_mut::<TagInternalNode>().unwrap();

            let promote_key = tag_internal_node.keys.pop().unwrap();
            let last_child_id = tag_internal_node.child_ids.pop().unwrap();

            // Create a new TagInternalNode with the last key and the last two child_ids
            let new_tag_internal_node = TagInternalNode {
                keys: vec![key],
                child_ids: vec![last_child_id, child_id],
            };

            // Create a new IndexPage with the new TagInternalNode
            let new_page = IndexPage {
                page_id: new_page_id,
                node: Box::new(new_tag_internal_node),
            };

            // Add the new page to the collection
            self.index_pages.add_page(new_page);

            // Mark the original page as dirty
            self.index_pages.mark_dirty(page_id);

            // Return the popped key and the new page ID
            Some((promote_key, new_page_id))
        }
    }

    /// Inserts a key and value into an internal node, splitting it if necessary
    ///
    /// # Arguments
    /// * `page_id` - The PageID of the page to add the key and value to
    /// * `key` - The key to add
    /// * `child_id` - The child PageID to add
    ///
    /// # Returns
    /// * `Option<([u8; TAG_HASH_LEN], PageID)>` - If the internal node was split, returns the middle key and the new page ID
    pub fn insert_internal_key_and_value(&mut self, page_id: PageID, key: [u8; TAG_HASH_LEN], child_id: PageID) -> Option<([u8; TAG_HASH_LEN], PageID)> {
        // Get a reference to the page
        let page = self.index_pages.get_page(page_id).unwrap();
        let internal_node = page.node.as_any().downcast_ref::<InternalNode>().unwrap();

        // Use binary search to find the correct insertion point for the key
        let search_result = internal_node.keys.binary_search(&key);

        match search_result {
            // Key already exists, replace the child_id at the corresponding position
            Ok(index) => {
                // Get a mutable reference to the page
                let page = self.index_pages.get_page_mut(page_id).unwrap();
                let internal_node = page.node.as_any_mut().downcast_mut::<InternalNode>().unwrap();

                // Replace the child_id at the corresponding position
                internal_node.child_ids[index + 1] = child_id;

                self.index_pages.mark_dirty(page_id);
                return None;
            },
            // Key doesn't exist, insert it at the correct position
            Err(insert_index) => {
                let max_page_size = self.get_max_page_size();

                // Check if there is space for this key and value in the internal node
                let needs_splitting: bool = {
                    let page = self.index_pages.get_page(page_id).unwrap();
                    let page_size = page.node.calc_serialized_page_size();
                    // Calculate the additional size needed for the new key and child_id
                    let additional_size = TAG_HASH_LEN + PAGE_ID_SIZE;
                    page_size + additional_size > max_page_size
                };

                if !needs_splitting {
                    // Get a mutable reference to the page
                    let page = self.index_pages.get_page_mut(page_id).unwrap();
                    let internal_node = page.node.as_any_mut().downcast_mut::<InternalNode>().unwrap();

                    // Insert the key at the correct position
                    internal_node.keys.insert(insert_index, key);
                    // Insert the child_id at the correct position (one after the key)
                    internal_node.child_ids.insert(insert_index + 1, child_id);

                    self.index_pages.mark_dirty(page_id);
                    return None;
                }
            }
        }

        // If we reach here, we need to split the node
        // Allocate a new page ID
        let new_page_id = self.index_pages.alloc_page_id();

        // Get a mutable reference to the page
        let page = self.index_pages.get_page_mut(page_id).unwrap();
        let internal_node = page.node.as_any_mut().downcast_mut::<InternalNode>().unwrap();

        // Find the correct insertion point for the new key
        let insert_index = match internal_node.keys.binary_search(&key) {
            Ok(index) => index, // This shouldn't happen as we already checked for duplicates
            Err(index) => index,
        };

        // Store the new key for later comparison
        let new_key = key;

        // Insert the new key and child_id at the correct positions
        internal_node.keys.insert(insert_index, new_key);
        internal_node.child_ids.insert(insert_index + 1, child_id);

        // Calculate the midpoint
        let mid = internal_node.keys.len() / 2;

        // Get the middle key that will be promoted
        let promoted_key = internal_node.keys[mid];

        // Move the keys after the middle key to the new node
        let new_keys = internal_node.keys.split_off(mid + 1);

        // Remove the middle key from the original node (it's being promoted)
        internal_node.keys.pop();

        // Move the child_ids after the middle key to the new node
        // For internal nodes, we need to keep one more child_id than keys
        let new_child_ids = internal_node.child_ids.split_off(mid + 1);


        // Create a new InternalNode with the second half of the keys and child_ids
        let new_internal_node = InternalNode {
            keys: new_keys,
            child_ids: new_child_ids,
        };

        // Create a new IndexPage with the new InternalNode
        let new_page = IndexPage {
            page_id: new_page_id,
            node: Box::new(new_internal_node),
        };

        // Mark the original page as dirty
        self.index_pages.mark_dirty(page_id);

        // Add the new page to the collection
        self.index_pages.add_page(new_page);

        // Return the promoted key and the new page ID
        Some((promoted_key, new_page_id))
    }

    /// Looks up all positions in a tag B+tree
    ///
    /// # Arguments
    /// * `root_page_id` - The PageID of the root page of the tag B+tree
    ///
    /// # Returns
    /// * `std::io::Result<Vec<Position>>` - All positions in the tag B+tree
    pub fn lookup_tag_tree(&mut self, root_page_id: PageID) -> std::io::Result<Vec<Position>> {
        // Get the root page
        let page = self.index_pages.get_page(root_page_id)?;

        // Check if the root is a TagLeafNode or a TagInternalNode
        if page.node.node_type_byte() == TAG_LEAF_NODE_TYPE {
            // Root is a TagLeafNode, return all positions
            let tag_leaf_node = page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
            
            let mut positions = tag_leaf_node.positions.clone();
            println!("Found {} positions page {}", positions.len(), page.page_id);

            // If there's a next leaf node, get positions from it too
            let mut next_leaf_id = tag_leaf_node.next_leaf_id;
            while let Some(leaf_id) = next_leaf_id {
                let leaf_page = self.index_pages.get_page(leaf_id)?;
                let leaf_node = leaf_page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
                positions.extend(leaf_node.positions.clone());
                next_leaf_id = leaf_node.next_leaf_id;
            }

            Ok(positions)
        } else if page.node.node_type_byte() == TAG_INTERNAL_NODE_TYPE {
            // Root is a TagInternalNode, traverse all leaf nodes
            let mut positions = Vec::new();

            // Get all child IDs
            let tag_internal_node = page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
            let child_ids = tag_internal_node.child_ids.clone();

            // Recursively get positions from all children
            for child_id in child_ids {
                let child_positions = self.lookup_tag_tree(child_id)?;
                positions.extend(child_positions.clone());
                println!("Added {} positions making {} total", child_positions.len(), positions.len());
                
            }

            Ok(positions)
        } else {
            // Invalid node type
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid node type",
            ))
        }
    }

    /// Inserts a position into the tag B+tree
    ///
    /// # Arguments
    /// * `root_page_id` - The PageID of the root page of the tag B+tree
    /// * `position` - The position to insert
    ///
    /// # Returns
    /// * `Option<PageID>` - If the root was changed, returns the new root page ID
    pub fn insert_tag_tree(&mut self, root_page_id: PageID, position: Position) -> Option<PageID> {
        // Get the root page
        let page = self.index_pages.get_page(root_page_id).unwrap();

        // Check if the root is a TagLeafNode or a TagInternalNode
        if page.node.node_type_byte() == TAG_LEAF_NODE_TYPE {
            // Root is a TagLeafNode, insert directly
            let split_result = self.append_tag_leaf_position(root_page_id, position);

            if let Some((promoted_position, new_page_id)) = split_result {
                // The TagLeafNode was split, create a TagInternalNode as the new root
                let new_root_page_id = self.index_pages.alloc_page_id();

                // Create a new TagInternalNode with the promoted position and two child IDs
                let tag_internal_node = TagInternalNode {
                    keys: vec![promoted_position],
                    child_ids: vec![root_page_id, new_page_id],
                };

                // Create a new IndexPage with the TagInternalNode
                let new_root_page = IndexPage {
                    page_id: new_root_page_id,
                    node: Box::new(tag_internal_node),
                };

                // Add the new page to the collection
                self.index_pages.add_page(new_root_page);

                // Return the new root page ID
                Some(new_root_page_id)
            } else {
                // No split occurred, return None
                None
            }
        } else if page.node.node_type_byte() == TAG_INTERNAL_NODE_TYPE {
            // Root is a TagInternalNode, traverse down to the leaf node

            // Stack to keep track of the path from root to leaf
            // Each entry is a page ID and the index of the child to follow
            let mut stack: Vec<(PageID, usize)> = Vec::new();
            let mut current_page_id = root_page_id;

            // First phase: traverse down to the leaf node
            loop {
                let page = self.index_pages.get_page(current_page_id).unwrap();

                if page.node.node_type_byte() == TAG_LEAF_NODE_TYPE {
                    // Found the leaf node, break out of the loop
                    break;
                } else if page.node.node_type_byte() == TAG_INTERNAL_NODE_TYPE {
                    // Internal node, find the correct child to follow
                    let tag_internal_node = page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();

                    // Find the index of the first key greater than the position
                    let mut index = 0;
                    while index < tag_internal_node.keys.len() && tag_internal_node.keys[index] <= position {
                        index += 1;
                    }

                    // Push the current page and child index onto the stack
                    stack.push((current_page_id, index));

                    // Move to the child
                    current_page_id = tag_internal_node.child_ids[index];
                } else {
                    // Invalid node type
                    return None;
                }
            }

            // Second phase: insert into the leaf node
            let mut split_info = self.append_tag_leaf_position(current_page_id, position);

            // Third phase: propagate splits up the tree
            while let Some((promoted_position, new_page_id)) = split_info {
                if stack.is_empty() {
                    // We've reached the root, create a new root
                    let new_root_page_id = self.index_pages.alloc_page_id();

                    // Create a new TagInternalNode with the promoted position and two child IDs
                    let tag_internal_node = TagInternalNode {
                        keys: vec![promoted_position],
                        child_ids: vec![root_page_id, new_page_id],
                    };

                    // Create a new IndexPage with the TagInternalNode
                    let new_root_page = IndexPage {
                        page_id: new_root_page_id,
                        node: Box::new(tag_internal_node),
                    };

                    // Add the new page to the collection
                    self.index_pages.add_page(new_root_page);

                    // Return the new root page ID
                    return Some(new_root_page_id);
                }

                // Pop the parent from the stack
                let (parent_id, child_index) = stack.pop().unwrap();

                // Insert the promoted position and new page ID into the parent
                split_info = self.append_tag_internal_node(parent_id, promoted_position, new_page_id);
            }

            // No new root was created
            None
        } else {
            // Invalid node type
            None
        }
    }

    /// Appends a position to a tag leaf node if it's greater than the last position
    /// If the node needs to be split, creates a new TagLeafNode and sets the next_leaf_id
    ///
    /// # Arguments
    /// * `page_id` - The PageID of the page containing the TagLeafNode
    /// * `position` - The Position to append
    ///
    /// # Returns
    /// * `Option<(Position, PageID)>` - If the node was split, returns the first position of the new node and the new page ID
    pub fn append_tag_leaf_position(&mut self, page_id: PageID, position: Position) -> Option<(Position, PageID)> {
        // Get a reference to the page
        let page = self.index_pages.get_page(page_id).unwrap();
        let tag_leaf_node = page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();

        // Check if the position is greater than the last position in the TagLeafNode
        if !tag_leaf_node.positions.is_empty() {
            let last_position = tag_leaf_node.positions[tag_leaf_node.positions.len() - 1];
            if position <= last_position {
                return None;
            }
        }

        // Calculate the current serialized size
        let current_size = tag_leaf_node.calc_serialized_page_size();

        // Calculate the size after adding the new position
        // Each position adds 8 bytes
        let new_size = current_size + POSITION_SIZE;

        // Get the maximum page size
        let max_page_size = self.get_max_page_size();

        // Check if adding the position would exceed the page size
        if new_size <= max_page_size {
            println!("Appending position: {}", position);

            // There's enough space, just append the position
            let page = self.index_pages.get_page_mut(page_id).unwrap();
            let tag_leaf_node = page.node.as_any_mut().downcast_mut::<TagLeafNode>().unwrap();

            // Append the position
            tag_leaf_node.positions.push(position);

            // Mark the page as dirty
            self.index_pages.mark_dirty(page_id);

            // No split needed
            None
        } else {
            // Need to split the node
            // Create a new TagLeafNode with the new position
            println!("Appending position to new tag leaf node: {}", position);
            
            let new_tag_leaf_node = TagLeafNode {
                positions: vec![position],
                next_leaf_id: None,
            };

            // Allocate a new page ID for the new TagLeafNode
            let new_page_id = self.index_pages.alloc_page_id();

            // Create a new IndexPage with the new TagLeafNode
            let new_page = IndexPage {
                page_id: new_page_id,
                node: Box::new(new_tag_leaf_node),
            };

            // Add the new page to the collection
            self.index_pages.add_page(new_page);

            // Update the next_leaf_id of the original TagLeafNode
            let page = self.index_pages.get_page_mut(page_id).unwrap();
            let tag_leaf_node = page.node.as_any_mut().downcast_mut::<TagLeafNode>().unwrap();
            tag_leaf_node.next_leaf_id = Some(new_page_id);

            // Mark the original page as dirty
            self.index_pages.mark_dirty(page_id);

            // Return the first position of the new node and the new page ID
            Some((position, new_page_id))
        }
    }

    /// Adds a key and value to a leaf node, splitting it if necessary
    ///
    /// # Arguments
    /// * `page_id` - The PageID of the page to add the key and value to
    /// * `key` - The key to add
    /// * `value` - The value to add
    ///
    /// # Returns
    /// * `Option<([u8; TAG_HASH_LEN], PageID)>` - If the leaf node was split, returns the first key of the new leaf node and the new page ID
    pub fn insert_leaf_key_and_value(&mut self, page_id: PageID, key: [u8; TAG_HASH_LEN], value: Vec<Position>) -> Option<([u8; TAG_HASH_LEN], PageID)> {
        // Get a reference to the page
        let page = self.index_pages.get_page(page_id).unwrap();
        let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Use binary search to find the correct insertion point for the key
        let search_result = leaf_node.keys.binary_search(&key);

        match search_result {
            // Key already exists, append the new position to the existing list
            Ok(index) => {
                // Check if the value is a single negative number, which indicates a TagLeafNode
                let is_tag_leaf_node = {
                    let page = self.index_pages.get_page(page_id).unwrap();
                    let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
                    leaf_node.values[index].len() == 1 && leaf_node.values[index][0] < 0
                };

                if is_tag_leaf_node {
                    // Get the TagLeafNode page ID
                    let tag_leaf_page_id = {
                        let page = self.index_pages.get_page(page_id).unwrap();
                        let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
                        PageID((-leaf_node.values[index][0]) as u32)
                    };

                    // Insert the positions into the tag B+tree
                    let mut new_root_page_id = None;
                    for pos in value {
                        if let Some(new_id) = self.insert_tag_tree(tag_leaf_page_id, pos) {
                            new_root_page_id = Some(new_id);
                        }
                    }

                    // If the root of the tag B+tree changed, update the reference in the LeafNode
                    if let Some(new_id) = new_root_page_id {
                        let page = self.index_pages.get_page_mut(page_id).unwrap();
                        let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();
                        leaf_node.values[index] = vec![-(new_id.0 as i64)];
                        self.index_pages.mark_dirty(page_id);
                    }
                } else {
                    // Check if adding these positions would exceed 100
                    let current_positions_count = {
                        let page = self.index_pages.get_page(page_id).unwrap();
                        let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
                        leaf_node.values[index].len()
                    };

                    let will_exceed_100 = current_positions_count + value.len() > 100;

                    if will_exceed_100 && current_positions_count <= 100 {
                        // We need to create a TagLeafNode

                        // First, get all the current positions
                        let all_positions = {
                            let page = self.index_pages.get_page(page_id).unwrap();
                            let leaf_node = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
                            let mut positions = leaf_node.values[index].clone();

                            // Add the new positions
                            positions.extend(value);
                            positions
                        };

                        // Create a TagLeafNode with all positions
                        let tag_leaf_node = TagLeafNode {
                            positions: all_positions,
                            next_leaf_id: None,
                        };

                        // Allocate a new page ID for the TagLeafNode
                        let tag_leaf_page_id = self.index_pages.alloc_page_id();

                        // Create a new IndexPage with the TagLeafNode
                        let tag_leaf_page = IndexPage {
                            page_id: tag_leaf_page_id,
                            node: Box::new(tag_leaf_node),
                        };

                        // Add the page to the collection
                        self.index_pages.add_page(tag_leaf_page);

                        // Now update the LeafNode to store the PageID instead of the positions
                        let page = self.index_pages.get_page_mut(page_id).unwrap();
                        let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();

                        // Replace the list of positions with a single position that is the negative of the page ID
                        // This is a special marker to indicate that the positions are stored in a TagLeafNode
                        leaf_node.values[index] = vec![-(tag_leaf_page_id.0 as i64)];

                        self.index_pages.mark_dirty(page_id);
                    } else {
                        // Just append the new positions to the existing list
                        let page = self.index_pages.get_page_mut(page_id).unwrap();
                        let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();

                        // Append the new position to the existing list
                        // We can assume that a new Position for an existing tag is greater than previously added Positions
                        for pos in value {
                            leaf_node.values[index].push(pos);
                        }

                        self.index_pages.mark_dirty(page_id);
                    }
                }

                return None;
            },
            // Key doesn't exist, insert it at the correct position
            Err(insert_index) => {
                let max_page_size = self.get_max_page_size();

                // Check if there is space for this key and value in the leaf node
                let needs_splitting: bool = {
                    let page = self.index_pages.get_page(page_id).unwrap();
                    let page_size = page.node.calc_serialized_page_size();
                    // Calculate the additional size needed for the new key and value
                    let additional_size = TAG_HASH_LEN + 2 + (value.len() * POSITION_SIZE);
                    page_size + additional_size > max_page_size
                };

                if !needs_splitting {
                    // Get a mutable reference to the page
                    let page = self.index_pages.get_page_mut(page_id).unwrap();
                    let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();

                    // Insert the key and value at the correct position
                    leaf_node.keys.insert(insert_index, key);
                    leaf_node.values.insert(insert_index, value);

                    self.index_pages.mark_dirty(page_id);
                    return None;
                }
            }
        }

        // Allocate a new page ID
        let new_page_id = self.index_pages.alloc_page_id();

        // When splitting, we need to decide which keys go to the new node
        // Move half of the keys and values to the new node

        // Get a mutable reference to the page
        let page = self.index_pages.get_page_mut(page_id).unwrap();
        let leaf_node = page.node.as_any_mut().downcast_mut::<LeafNode>().unwrap();

        // Find the correct insertion point for the new key
        let insert_index = match leaf_node.keys.binary_search(&key) {
            Ok(index) => index, // This shouldn't happen as we already checked for duplicates
            Err(index) => index,
        };

        // Insert the new key and value at the correct position
        leaf_node.keys.insert(insert_index, key);
        leaf_node.values.insert(insert_index, value);

        // Calculate the midpoint
        let mid = leaf_node.keys.len() / 2;

        // Move the second half of the keys and values to the new node
        let new_keys = leaf_node.keys.split_off(mid);
        let new_values = leaf_node.values.split_off(mid);

        // Store the first key of the new leaf node
        let first_key = new_keys[0];

        // Create a new LeafNode with the second half of the keys and values
        let new_leaf_node = LeafNode {
            keys: new_keys,
            values: new_values,
        };

        // Create a new IndexPage with the new LeafNode
        let new_page = IndexPage {
            page_id: new_page_id,
            node: Box::new(new_leaf_node),
        };

        // Mark the original page as dirty
        self.index_pages.mark_dirty(page_id);

        // Add the new page to the collection
        self.index_pages.add_page(new_page);

        // Return the first key of the new leaf node and the new page ID
        Some((first_key, new_page_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::indexpages::{HeaderNode, IndexPage};

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

    #[test]
    fn test_insert_leaf_key_and_value_without_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance with a large page size to avoid splitting
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(30);

        // Create a LeafNode with 10 keys and 10 values
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for i in 0..10 {
            // Create a tag hash for each key
            let tag_hash = hash_tag(&format!("Tag{}", i));
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);
            keys.push(key);

            // Create a vector of positions for each value
            let mut positions = Vec::new();
            for j in 0..3 {
                positions.push((i * 1000 + j) as i64);
            }
            values.push(positions);
        }

        let leaf_node = LeafNode {
            keys,
            values,
        };

        // Create an IndexPage with the LeafNode
        let leaf_page = IndexPage {
            page_id,
            node: Box::new(leaf_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(leaf_page);

        // Add a new key and value that will not cause the leaf to split
        let new_tag_hash = hash_tag("NewTag");
        let mut new_key = [0u8; TAG_HASH_LEN];
        new_key.copy_from_slice(&new_tag_hash[..TAG_HASH_LEN]);
        let new_value = vec![9999, 9998, 9997];

        // Call append_leaf_key_and_value with the page_id, key, and value
        let split_result = tag_index.insert_leaf_key_and_value(page_id, new_key, new_value.clone());

        // Verify that the leaf was not split
        assert!(split_result.is_none());

        // Get the page and verify it has 11 keys and values
        let page = tag_index.index_pages.get_page(page_id).unwrap();
        let leaf = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf.keys.len(), 11);
        assert_eq!(leaf.values.len(), 11);

        // Find the index of the new key
        let mut new_key_index = 0;
        for (i, k) in leaf.keys.iter().enumerate() {
            if *k == new_key {
                new_key_index = i;
                break;
            }
        }

        assert_eq!(leaf.keys[new_key_index], new_key);
        assert_eq!(leaf.values[new_key_index], new_value);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it has 11 keys and values
        let page2 = tag_index2.index_pages.get_page(page_id).unwrap();
        let leaf2 = page2.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf2.keys.len(), 11);
        assert_eq!(leaf2.values.len(), 11);

        // Find the index of the new key
        let mut new_key_index2 = 0;
        for (i, k) in leaf2.keys.iter().enumerate() {
            if *k == new_key {
                new_key_index2 = i;
                break;
            }
        }

        assert_eq!(leaf2.keys[new_key_index2], new_key);
        assert_eq!(leaf2.values[new_key_index2], new_value);

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_insert_leaf_key_and_value_with_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Construct a PageID
        let page_id = PageID(30);

        // Create a LeafNode with 10 keys and 10 values
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for i in 0..10 {
            // Create a tag hash for each key
            let tag_hash = hash_tag(&format!("Tag{}", i));
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);
            keys.push(key);

            // Create a vector of positions for each value
            let mut positions = Vec::new();
            for j in 0..3 {
                positions.push((i * 1000 + j) as i64);
            }
            values.push(positions);
        }

        let leaf_node = LeafNode {
            keys,
            values,
        };

        let serialized_size = leaf_node.calc_serialized_page_size();

        // Create an IndexPage with the LeafNode
        let leaf_page = IndexPage {
            page_id,
            node: Box::new(leaf_node),
        };

        // Create a new TagIndex instance with a page size that is the serialized length
        // This will ensure that the page needs splitting when we add another key
        let mut tag_index = TagIndex::new(&test_path, serialized_size).unwrap();

        // Add the page to the index
        tag_index.index_pages.add_page(leaf_page);

        // Add a new key and value that will cause the leaf to split
        let new_tag_hash = hash_tag("NewTag");
        let mut new_key = [0u8; TAG_HASH_LEN];
        new_key.copy_from_slice(&new_tag_hash[..TAG_HASH_LEN]);
        let new_value = vec![9999, 9998, 9997];

        // Call append_leaf_key_and_value with the page_id, key, and value
        let split_result = tag_index.insert_leaf_key_and_value(page_id, new_key, new_value.clone());

        // Verify that the leaf was split
        assert!(split_result.is_some());
        let (first_key, new_page_id) = split_result.unwrap();

        // Check the original page
        {
            let original_page = tag_index.index_pages.get_page(page_id).unwrap();
            let original_leaf = original_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

            // Verify it has 5 keys and values (half of the original 10 + 1 new)
            assert_eq!(original_leaf.keys.len(), 5);
            assert_eq!(original_leaf.values.len(), 5);

            // Check if the new key is in the original node
            let new_key_in_original = original_leaf.keys.contains(&new_key);
            if new_key_in_original {
                let index = original_leaf.keys.iter().position(|k| *k == new_key).unwrap();
                assert_eq!(original_leaf.values[index], new_value);
            }
        }

        // Check the new page
        {
            let new_page = tag_index.index_pages.get_page(new_page_id).unwrap();
            let new_leaf = new_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

            // Verify it has 6 keys and values (the other half of the original 10 + 1 new)
            assert_eq!(new_leaf.keys.len(), 6);
            assert_eq!(new_leaf.values.len(), 6);

            // Verify the first key of the new leaf node matches what was returned
            assert_eq!(first_key, new_leaf.keys[0]);

            // Check if the new key is in the new node
            let new_key_in_new = new_leaf.keys.contains(&new_key);
            if new_key_in_new {
                let index = new_leaf.keys.iter().position(|k| *k == new_key).unwrap();
                assert_eq!(new_leaf.values[index], new_value);
            }
        }

        // Verify that the new key is in one of the nodes
        let new_key_in_original = {
            let original_page = tag_index.index_pages.get_page(page_id).unwrap();
            let original_leaf = original_page.node.as_any().downcast_ref::<LeafNode>().unwrap();
            original_leaf.keys.contains(&new_key)
        };

        let new_key_in_new = {
            let new_page = tag_index.index_pages.get_page(new_page_id).unwrap();
            let new_leaf = new_page.node.as_any().downcast_ref::<LeafNode>().unwrap();
            new_leaf.keys.contains(&new_key)
        };

        assert!(new_key_in_original || new_key_in_new, "New key not found in either node");

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_insert_leaf_key_and_value_binary_search() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance with a large page size to avoid splitting
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(30);

        // Create an empty LeafNode
        let leaf_node = LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
        };

        // Create an IndexPage with the LeafNode
        let leaf_page = IndexPage {
            page_id,
            node: Box::new(leaf_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(leaf_page);

        // Create tag hashes for testing
        let tag_hashes = [
            ("TagC", hash_tag("TagC")),
            ("TagA", hash_tag("TagA")),
            ("TagE", hash_tag("TagE")),
            ("TagB", hash_tag("TagB")),
            ("TagD", hash_tag("TagD")),
        ];

        // Add tags in a non-sorted order
        for (tag_name, tag_hash) in &tag_hashes {
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

            // Create a position for this tag
            let position = match *tag_name {
                "TagA" => 1000,
                "TagB" => 2000,
                "TagC" => 3000,
                "TagD" => 4000,
                "TagE" => 5000,
                _ => 0,
            };

            let value = vec![position];

            // Add the key and value to the leaf node
            let split_result = tag_index.insert_leaf_key_and_value(page_id, key, value);

            // Verify that the leaf was not split
            assert!(split_result.is_none());
        }

        // Get the page and verify it has 5 keys and values
        let page = tag_index.index_pages.get_page(page_id).unwrap();
        let leaf = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf.keys.len(), 5);
        assert_eq!(leaf.values.len(), 5);

        // Verify that the keys are in sorted order
        for i in 1..leaf.keys.len() {
            assert!(leaf.keys[i-1] <= leaf.keys[i], "Keys are not in sorted order");
        }

        // Add a position to an existing tag (TagC)
        let tag_c_hash = hash_tag("TagC");
        let mut tag_c_key = [0u8; TAG_HASH_LEN];
        tag_c_key.copy_from_slice(&tag_c_hash[..TAG_HASH_LEN]);

        // Find the index of TagC
        let mut tag_c_index = 0;
        for (i, k) in leaf.keys.iter().enumerate() {
            if *k == tag_c_key {
                tag_c_index = i;
                break;
            }
        }

        // Verify that TagC has only one position before adding another
        assert_eq!(leaf.values[tag_c_index].len(), 1);
        assert_eq!(leaf.values[tag_c_index][0], 3000);

        // Add another position to TagC
        let new_position = 3001;
        let split_result = tag_index.insert_leaf_key_and_value(page_id, tag_c_key, vec![new_position]);

        // Verify that the leaf was not split
        assert!(split_result.is_none());

        // Get the page again and verify that TagC now has two positions
        let page = tag_index.index_pages.get_page(page_id).unwrap();
        let leaf = page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Find the index of TagC again
        let mut tag_c_index = 0;
        for (i, k) in leaf.keys.iter().enumerate() {
            if *k == tag_c_key {
                tag_c_index = i;
                break;
            }
        }

        // Verify that TagC now has two positions
        assert_eq!(leaf.values[tag_c_index].len(), 2);
        assert_eq!(leaf.values[tag_c_index][0], 3000);
        assert_eq!(leaf.values[tag_c_index][1], 3001);

        // Verify that we still have 5 keys (no duplicates)
        assert_eq!(leaf.keys.len(), 5);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it still has 5 keys and values
        let page2 = tag_index2.index_pages.get_page(page_id).unwrap();
        let leaf2 = page2.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf2.keys.len(), 5);
        assert_eq!(leaf2.values.len(), 5);

        // Verify that the keys are still in sorted order
        for i in 1..leaf2.keys.len() {
            assert!(leaf2.keys[i-1] <= leaf2.keys[i], "Keys are not in sorted order");
        }

        // Find the index of TagC again
        let mut tag_c_index2 = 0;
        for (i, k) in leaf2.keys.iter().enumerate() {
            if *k == tag_c_key {
                tag_c_index2 = i;
                break;
            }
        }

        // Verify that TagC still has two positions
        assert_eq!(leaf2.values[tag_c_index2].len(), 2);
        assert_eq!(leaf2.values[tag_c_index2][0], 3000);
        assert_eq!(leaf2.values[tag_c_index2][1], 3001);
    }

    #[test]
    fn test_tag_leaf_node_creation_from_overflow() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance with a large page size to avoid splitting
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(30);

        // Create an empty LeafNode
        let leaf_node = LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
        };

        // Create an IndexPage with the LeafNode
        let leaf_page = IndexPage {
            page_id,
            node: Box::new(leaf_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(leaf_page);

        // Create a tag hash for testing
        let tag_hash = hash_tag("TestTag");
        let mut tag_key = [0u8; TAG_HASH_LEN];
        tag_key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

        // Add the tag with 100 positions
        let mut positions = Vec::new();
        for i in 0..100 {
            positions.push(i as i64);
        }

        // Add the key and value to the leaf node
        let split_result = tag_index.insert_leaf_key_and_value(page_id, tag_key, positions);

        // Verify that the leaf was not split
        assert!(split_result.is_none());

        // Get the page and verify it has 1 key and value
        let page = tag_index.index_pages.get_page(page_id).unwrap();
        let leaf = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf.keys.len(), 1);
        assert_eq!(leaf.values.len(), 1);
        assert_eq!(leaf.values[0].len(), 100);

        // Add one more position to the tag
        let new_position = 100;
        let split_result = tag_index.insert_leaf_key_and_value(page_id, tag_key, vec![new_position]);

        // Verify that the leaf was not split
        assert!(split_result.is_none());

        // Get the page again and verify that the tag now has a PageID reference instead of positions
        let page = tag_index.index_pages.get_page(page_id).unwrap();
        let leaf = page.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf.keys.len(), 1);
        assert_eq!(leaf.values.len(), 1);

        // The value should now be a single position that is the negative of the page ID
        assert_eq!(leaf.values[0].len(), 1);
        let tag_leaf_page_id = PageID((-leaf.values[0][0]) as u32);

        // Get the TagLeafNode page
        let tag_leaf_page = tag_index.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf_node = tag_leaf_page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();

        // Verify that the TagLeafNode has 101 positions
        assert_eq!(tag_leaf_node.positions.len(), 101);

        // Verify that the positions are correct
        for i in 0..101 {
            assert_eq!(tag_leaf_node.positions[i], i as i64);
        }

        // Verify that the TagLeafNode's next_leaf_id is None
        assert_eq!(tag_leaf_node.next_leaf_id, None);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it still has 1 key and value
        let page2 = tag_index2.index_pages.get_page(page_id).unwrap();
        let leaf2 = page2.node.as_any().downcast_ref::<LeafNode>().unwrap();
        assert_eq!(leaf2.keys.len(), 1);
        assert_eq!(leaf2.values.len(), 1);

        // The value should still be a single position that is the negative of the page ID
        assert_eq!(leaf2.values[0].len(), 1);
        let tag_leaf_page_id2 = PageID((-leaf2.values[0][0]) as u32);
        assert_eq!(tag_leaf_page_id, tag_leaf_page_id2);

        // Get the TagLeafNode page
        let tag_leaf_page2 = tag_index2.index_pages.get_page(tag_leaf_page_id2).unwrap();
        let tag_leaf_node2 = tag_leaf_page2.node.as_any().downcast_ref::<TagLeafNode>().unwrap();

        // Verify that the TagLeafNode still has 101 positions
        assert_eq!(tag_leaf_node2.positions.len(), 101);

        // Verify that the positions are still correct
        for i in 0..101 {
            assert_eq!(tag_leaf_node2.positions[i], i as i64);
        }

        // Verify that the TagLeafNode's next_leaf_id is still None
        assert_eq!(tag_leaf_node2.next_leaf_id, None);
    }

    #[test]
    fn test_append_tag_leaf_position_without_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Create a TagLeafNode with some positions
        let tag_leaf_node = TagLeafNode {
            positions: vec![1000, 2000, 3000],
            next_leaf_id: None,
        };

        // Allocate a page ID for the TagLeafNode
        let tag_leaf_page_id = tag_index.index_pages.alloc_page_id();

        // Create an IndexPage with the TagLeafNode
        let tag_leaf_page = IndexPage {
            page_id: tag_leaf_page_id,
            node: Box::new(tag_leaf_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(tag_leaf_page);

        // Add a new Position that is greater than the last existing position
        let new_position = 4000;
        let result = tag_index.append_tag_leaf_position(tag_leaf_page_id, new_position);

        // Verify that the Position was appended (result is None when no split occurs but position is appended)
        assert!(result.is_none(), "Position should have been appended without splitting");

        // Get the page and verify it now has 4 positions
        let page = tag_index.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf = page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(tag_leaf.positions.len(), 4);
        assert_eq!(tag_leaf.positions[3], new_position);

        // Try to add a Position that is less than the last existing position
        let duplicate_position = 3000;
        let result = tag_index.append_tag_leaf_position(tag_leaf_page_id, duplicate_position);

        // Verify that the Position was not appended (result is None when position is not appended)
        assert!(result.is_none(), "Position should not have been appended");

        // Get the page and verify it still has 4 positions
        let page = tag_index.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf = page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(tag_leaf.positions.len(), 4);

        // Try to add a Position that is equal to the last existing position
        let equal_position = 4000;
        let result = tag_index.append_tag_leaf_position(tag_leaf_page_id, equal_position);

        // Verify that the Position was not appended (result is None when position is not appended)
        assert!(result.is_none(), "Position should not have been appended");

        // Get the page and verify it still has 4 positions
        let page = tag_index.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf = page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(tag_leaf.positions.len(), 4);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it still has 4 positions
        let page2 = tag_index2.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf2 = page2.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(tag_leaf2.positions.len(), 4);
        assert_eq!(tag_leaf2.positions[0], 1000);
        assert_eq!(tag_leaf2.positions[1], 2000);
        assert_eq!(tag_leaf2.positions[2], 3000);
        assert_eq!(tag_leaf2.positions[3], 4000);
    }

    #[test]
    fn test_append_tag_leaf_position_with_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a TagLeafNode with many positions to fill up most of the page
        let mut positions = Vec::new();
        for i in 0..100 {
            positions.push(i as i64);
        }

        let tag_leaf_node = TagLeafNode {
            positions,
            next_leaf_id: None,
        };

        // Calculate the serialized size of the TagLeafNode
        let serialized_size = tag_leaf_node.calc_serialized_page_size();

        // Create a new TagIndex instance with a page size that can just fit the current TagLeafNode
        // This will ensure that adding one more position will cause a split
        let mut tag_index = TagIndex::new(&test_path, serialized_size).unwrap();

        // Allocate a page ID for the TagLeafNode
        let tag_leaf_page_id = tag_index.index_pages.alloc_page_id();

        // Create an IndexPage with the TagLeafNode
        let tag_leaf_page = IndexPage {
            page_id: tag_leaf_page_id,
            node: Box::new(tag_leaf_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(tag_leaf_page);

        // Add a new Position that will cause the node to split
        let new_position = 100;
        let result = tag_index.append_tag_leaf_position(tag_leaf_page_id, new_position);

        // Verify that the node was split
        assert!(result.is_some(), "Node should have been split");

        // Get the split result
        let (first_position, new_page_id) = result.unwrap();

        // Verify that the first position of the new node is the new position
        assert_eq!(first_position, new_position);

        // Get the original page and verify it still has 100 positions
        let page = tag_index.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf = page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(tag_leaf.positions.len(), 100);

        // Verify that the original node's next_leaf_id points to the new node
        assert_eq!(tag_leaf.next_leaf_id, Some(new_page_id));

        // Get the new page and verify it has the new position
        let new_page = tag_index.index_pages.get_page(new_page_id).unwrap();
        let new_tag_leaf = new_page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(new_tag_leaf.positions.len(), 1);
        assert_eq!(new_tag_leaf.positions[0], new_position);

        // Verify that the new node's next_leaf_id is None
        assert_eq!(new_tag_leaf.next_leaf_id, None);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, serialized_size).unwrap();

        // Get the original page and verify it still has 100 positions
        let page2 = tag_index2.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf2 = page2.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(tag_leaf2.positions.len(), 100);

        // Verify that the original node's next_leaf_id still points to the new node
        assert_eq!(tag_leaf2.next_leaf_id, Some(new_page_id));

        // Get the new page and verify it still has the new position
        let new_page2 = tag_index2.index_pages.get_page(new_page_id).unwrap();
        let new_tag_leaf2 = new_page2.node.as_any().downcast_ref::<TagLeafNode>().unwrap();
        assert_eq!(new_tag_leaf2.positions.len(), 1);
        assert_eq!(new_tag_leaf2.positions[0], new_position);

        // Verify that the new node's next_leaf_id is still None
        assert_eq!(new_tag_leaf2.next_leaf_id, None);
    }

    #[test]
    fn test_append_tag_internal_node_without_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Create a TagInternalNode with some keys and values
        let tag_internal_node = TagInternalNode {
            keys: vec![1000, 2000, 3000],
            child_ids: vec![PageID(10), PageID(20), PageID(30), PageID(40)],
        };

        // Allocate a page ID for the TagInternalNode
        let tag_internal_page_id = tag_index.index_pages.alloc_page_id();

        // Create an IndexPage with the TagInternalNode
        let tag_internal_page = IndexPage {
            page_id: tag_internal_page_id,
            node: Box::new(tag_internal_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(tag_internal_page);

        // Add a new Position key and PageID value that won't cause a split
        let new_key = 4000;
        let new_child_id = PageID(50);
        let result = tag_index.append_tag_internal_node(tag_internal_page_id, new_key, new_child_id);

        // Verify that the result is None (no split)
        assert!(result.is_none(), "Node should not have been split");

        // Get the page and verify it now has 4 keys and 5 child_ids
        let page = tag_index.index_pages.get_page(tag_internal_page_id).unwrap();
        let tag_internal = page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
        assert_eq!(tag_internal.keys.len(), 4);
        assert_eq!(tag_internal.child_ids.len(), 5);
        assert_eq!(tag_internal.keys[3], new_key);
        assert_eq!(tag_internal.child_ids[4], new_child_id);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it still has 4 keys and 5 child_ids
        let page2 = tag_index2.index_pages.get_page(tag_internal_page_id).unwrap();
        let tag_internal2 = page2.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
        assert_eq!(tag_internal2.keys.len(), 4);
        assert_eq!(tag_internal2.child_ids.len(), 5);
        assert_eq!(tag_internal2.keys[0], 1000);
        assert_eq!(tag_internal2.keys[1], 2000);
        assert_eq!(tag_internal2.keys[2], 3000);
        assert_eq!(tag_internal2.keys[3], 4000);
        assert_eq!(tag_internal2.child_ids[0], PageID(10));
        assert_eq!(tag_internal2.child_ids[1], PageID(20));
        assert_eq!(tag_internal2.child_ids[2], PageID(30));
        assert_eq!(tag_internal2.child_ids[3], PageID(40));
        assert_eq!(tag_internal2.child_ids[4], PageID(50));
    }

    #[test]
    fn test_append_tag_internal_node_with_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a TagInternalNode with keys and values
        let tag_internal_node = TagInternalNode {
            keys: vec![1000, 2000, 3000],
            child_ids: vec![PageID(10), PageID(20), PageID(30), PageID(40)],
        };

        // Calculate the serialized size of the TagInternalNode
        let serialized_size = tag_internal_node.calc_serialized_page_size();

        // Create a new TagIndex instance with a page size that can just fit the current TagInternalNode
        // This will ensure that adding one more key and child_id will cause a split
        let mut tag_index = TagIndex::new(&test_path, serialized_size).unwrap();

        // Allocate a page ID for the TagInternalNode
        let tag_internal_page_id = tag_index.index_pages.alloc_page_id();

        // Create an IndexPage with the TagInternalNode
        let tag_internal_page = IndexPage {
            page_id: tag_internal_page_id,
            node: Box::new(tag_internal_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(tag_internal_page);

        // Add a new key and child_id that will cause the node to split
        let new_key = 4000;
        let new_child_id = PageID(50);
        let result = tag_index.append_tag_internal_node(tag_internal_page_id, new_key, new_child_id);

        // Verify that the node was split
        assert!(result.is_some(), "Node should have been split");

        // Get the split result
        let (popped_key, new_page_id) = result.unwrap();

        // Verify that the popped key is the second-to-last key (3000)
        assert_eq!(popped_key, 3000);

        // Get the original page and verify it has 1 key and 2 child_ids
        let page = tag_index.index_pages.get_page(tag_internal_page_id).unwrap();
        let tag_internal = page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
        assert_eq!(tag_internal.keys.len(), 2);
        assert_eq!(tag_internal.child_ids.len(), 3);
        assert_eq!(tag_internal.keys[0], 1000);
        assert_eq!(tag_internal.keys[1], 2000);
        assert_eq!(tag_internal.child_ids[0], PageID(10));
        assert_eq!(tag_internal.child_ids[1], PageID(20));
        assert_eq!(tag_internal.child_ids[2], PageID(30));

        // Get the new page and verify it has 1 key and 2 child_ids
        let new_page = tag_index.index_pages.get_page(new_page_id).unwrap();
        let new_tag_internal = new_page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
        assert_eq!(new_tag_internal.keys.len(), 1);
        assert_eq!(new_tag_internal.child_ids.len(), 2);
        assert_eq!(new_tag_internal.keys[0], 4000);
        assert_eq!(new_tag_internal.child_ids[0], PageID(40));
        assert_eq!(new_tag_internal.child_ids[1], PageID(50));

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, serialized_size).unwrap();

        // Get the original page and verify it still has 1 key and 2 child_ids
        let page2 = tag_index2.index_pages.get_page(tag_internal_page_id).unwrap();
        let tag_internal2 = page2.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
        assert_eq!(tag_internal2.keys.len(), 2);
        assert_eq!(tag_internal2.child_ids.len(), 3);
        assert_eq!(tag_internal2.keys[0], 1000);
        assert_eq!(tag_internal2.keys[1], 2000);
        assert_eq!(tag_internal2.child_ids[0], PageID(10));
        assert_eq!(tag_internal2.child_ids[1], PageID(20));
        assert_eq!(tag_internal2.child_ids[2], PageID(30));

        // Get the new page and verify it still has 1 key and 2 child_ids
        let new_page2 = tag_index2.index_pages.get_page(new_page_id).unwrap();
        let new_tag_internal2 = new_page2.node.as_any().downcast_ref::<TagInternalNode>().unwrap();
        assert_eq!(new_tag_internal2.keys.len(), 1);
        assert_eq!(new_tag_internal2.child_ids.len(), 2);
        assert_eq!(new_tag_internal2.keys[0], 4000);
        assert_eq!(new_tag_internal2.child_ids[0], PageID(40));
        assert_eq!(new_tag_internal2.child_ids[1], PageID(50));
    }

    #[test]
    fn test_insert_lookup() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Insert a tag and position
        let tag = "user";
        let position = 1000;
        tag_index.insert(tag, position).unwrap();

        // Get the root page ID from the header page
        let header_node = tag_index.index_pages.header_node();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let root_page = tag_index.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page is a LeafNode
        assert_eq!(root_page.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast the node to a LeafNode
        let leaf_node = root_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode has one key
        assert_eq!(leaf_node.keys.len(), 1);
        assert_eq!(leaf_node.values.len(), 1);

        // Hash the tag to get the expected key
        let tag_hash = hash_tag(tag);
        let mut expected_key = [0u8; TAG_HASH_LEN];
        expected_key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

        // Verify that the key matches the expected key
        assert_eq!(leaf_node.keys[0], expected_key);

        // Verify that the value contains the position
        assert_eq!(leaf_node.values[0].len(), 1);
        assert_eq!(leaf_node.values[0][0], position);

        // Use the lookup method to find the position for the tag
        let positions = tag_index.lookup(tag).unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], position);

        // Lookup a non-existent tag
        let non_existent_tag = "non_existent";
        let positions = tag_index.lookup(non_existent_tag).unwrap();
        assert_eq!(positions.len(), 0);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the root page again
        let root_page2 = tag_index2.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page is still a LeafNode
        assert_eq!(root_page2.node.node_type_byte(), LEAF_NODE_TYPE);

        // Downcast the node to a LeafNode
        let leaf_node2 = root_page2.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode still has one key
        assert_eq!(leaf_node2.keys.len(), 1);
        assert_eq!(leaf_node2.values.len(), 1);

        // Verify that the key still matches the expected key
        assert_eq!(leaf_node2.keys[0], expected_key);

        // Verify that the value still contains the position
        assert_eq!(leaf_node2.values[0].len(), 1);
        assert_eq!(leaf_node2.values[0][0], position);

        // Use the lookup method to find the position for the tag after reopening
        let positions = tag_index2.lookup(tag).unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], position);
    }

    #[test]
    fn test_insert_lookup_split_leaf() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance with a small page size to force splitting
        let page_size = 256;
        let mut tag_index = TagIndex::new(&test_path, page_size).unwrap();

        // Insert enough tags to cause the leaf node to split
        let mut inserted_tags = Vec::new();
        for i in 0..20 {
            let tag = format!("tag-{}", i);
            let position = (i * 100) as i64;
            tag_index.insert(&tag, position).unwrap();
            inserted_tags.push((tag, position));
        }

        // Get the root page ID from the header page
        let header_node = tag_index.index_pages.header_node();
        let root_page_id = header_node.root_page_id;

        // First, check that the root is an InternalNode and get child page IDs
        let (left_child_id, right_child_id) = {
            let root_page = tag_index.index_pages.get_page(root_page_id).unwrap();
            assert_eq!(root_page.node.node_type_byte(), INTERNAL_NODE_TYPE, "Root node should be an InternalNode");

            // Get the child page IDs
            let internal_node = root_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
            assert!(!internal_node.keys.is_empty(), "InternalNode should have at least one key");
            assert_eq!(internal_node.child_ids.len(), 2, "InternalNode should have two child IDs");

            // Return the child page IDs
            (internal_node.child_ids[0], internal_node.child_ids[1])
        };

        // Check that the left child is a LeafNode and collect its keys and values
        let left_keys_values = {
            let left_child_page = tag_index.index_pages.get_page(left_child_id).unwrap();
            assert_eq!(left_child_page.node.node_type_byte(), LEAF_NODE_TYPE, "Left child should be a LeafNode");

            let left_leaf = left_child_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

            // Clone the keys and values to avoid borrowing issues
            (left_leaf.keys.clone(), left_leaf.values.clone())
        };

        // Check that the right child is a LeafNode and collect its keys and values
        let right_keys_values = {
            let right_child_page = tag_index.index_pages.get_page(right_child_id).unwrap();
            assert_eq!(right_child_page.node.node_type_byte(), LEAF_NODE_TYPE, "Right child should be a LeafNode");

            let right_leaf = right_child_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

            // Clone the keys and values to avoid borrowing issues
            (right_leaf.keys.clone(), right_leaf.values.clone())
        };

        // Now verify that all inserted tags and positions are in the leaf nodes
        for (tag, position) in &inserted_tags {
            // Hash the tag to get the key
            let tag_hash = hash_tag(tag);
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

            // Check if the key is in the left leaf
            let in_left = left_keys_values.0.iter().position(|k| *k == key);
            // Check if the key is in the right leaf
            let in_right = right_keys_values.0.iter().position(|k| *k == key);

            // The key should be in exactly one of the leaves
            assert!(in_left.is_some() || in_right.is_some(), "Tag {} not found in either leaf", tag);
            assert!(!(in_left.is_some() && in_right.is_some()), "Tag {} found in both leaves", tag);

            // Verify the position is correct
            if let Some(idx) = in_left {
                assert!(left_keys_values.1[idx].contains(position), "Position {} not found for tag {}", position, tag);
            } else if let Some(idx) = in_right {
                assert!(right_keys_values.1[idx].contains(position), "Position {} not found for tag {}", position, tag);
            }

            // Use the lookup method to verify the position can be found
            let positions = tag_index.lookup(tag).unwrap();
            assert!(!positions.is_empty(), "No positions found for tag {}", tag);
            assert!(positions.contains(position), "Position {} not found for tag {} using lookup", position, tag);
        }

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, page_size).unwrap();

        // Get the root page again
        let root_page2 = tag_index2.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page is still an InternalNode
        assert_eq!(root_page2.node.node_type_byte(), INTERNAL_NODE_TYPE);

        // Downcast the node to an InternalNode
        let internal_node2 = root_page2.node.as_any().downcast_ref::<InternalNode>().unwrap();

        // Verify that the InternalNode still has at least one key
        assert!(!internal_node2.keys.is_empty());

        // Verify that the InternalNode still has two child PageIDs
        assert_eq!(internal_node2.child_ids.len(), 2);

        // Check lookup after reopening
        for (tag, position) in &inserted_tags {
            let positions = tag_index2.lookup(tag).unwrap();
            assert!(!positions.is_empty(), "No positions found for tag {} after reopening", tag);
            assert!(positions.contains(position), "Position {} not found for tag {} using lookup after reopening", position, tag);
        }
    }

    #[test]
    fn test_insert_lookup_split_internal() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance with a small page size to force splitting
        let page_size = 256;
        let mut tag_index = TagIndex::new(&test_path, page_size).unwrap();

        // Insert enough tags to cause internal node splitting
        let mut inserted_tags = Vec::new();
        for i in 0..250 {
            let tag = format!("tag-{}", i);
            let position = (i * 100) as i64;
            tag_index.insert(&tag, position).unwrap();
            inserted_tags.push((tag, position));
        }

        // Check lookup before flush
        for (tag, position) in &inserted_tags {
            let positions = tag_index.lookup(tag).unwrap();
            assert!(!positions.is_empty(), "No positions found for tag {}", tag);
            assert!(positions.contains(position), "Position {} not found for tag {} using lookup", position, tag);
        }

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, page_size).unwrap();

        // Check lookup after reopening
        for (tag, position) in &inserted_tags {
            let positions = tag_index2.lookup(tag).unwrap();
            assert!(!positions.is_empty(), "No positions found for tag {} after reopening", tag);
            assert!(positions.contains(position), "Position {} not found for tag {} using lookup after reopening", position, tag);
        }
    }

    #[test]
    fn test_insert_lookup_positions_split_tag_leaf_node() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a TagLeafNode with many positions to calculate its size
        let mut positions = Vec::new();
        for i in 0..100 {
            positions.push(i as i64);
        }

        let tag_leaf_node = TagLeafNode {
            positions,
            next_leaf_id: None,
        };

        // Calculate the serialized size of the TagLeafNode
        let serialized_size = tag_leaf_node.calc_serialized_page_size();

        // Create a new TagIndex instance with a page size that can just fit the current TagLeafNode
        // This will ensure that adding more positions will cause a split
        let page_size = serialized_size + 100;
        let mut tag_index = TagIndex::new(&test_path, page_size).unwrap();

        // Insert a tag with enough positions to create a TagLeafNode
        let tag = "test-tag";
        let mut inserted_positions = Vec::new();

        // First, insert 100 positions to create a TagLeafNode
        for i in 0..100 {
            let position = i as i64;
            tag_index.insert(tag, position).unwrap();
            inserted_positions.push(position);
        }

        // Now insert 101 more positions to trigger splitting the TagLeafNode
        for i in 100..201 {
            let position = i as i64;
            tag_index.insert(tag, position).unwrap();
            inserted_positions.push(position);
        }

        // Get the root page ID from the header page
        let header_node = tag_index.index_pages.header_node();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let root_page = tag_index.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page is a LeafNode
        assert_eq!(root_page.node.node_type_byte(), LEAF_NODE_TYPE, "Root node should be a LeafNode");

        // Downcast the node to a LeafNode
        let leaf_node = root_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode has one key
        assert_eq!(leaf_node.keys.len(), 1, "LeafNode should have one key");
        assert_eq!(leaf_node.values.len(), 1, "LeafNode should have one value");

        // Hash the tag to get the expected key
        let tag_hash = hash_tag(tag);
        let mut expected_key = [0u8; TAG_HASH_LEN];
        expected_key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

        // Verify that the key matches the expected key
        assert_eq!(leaf_node.keys[0], expected_key, "Key should match the hashed tag");

        // Verify that the value is a PageID (a single negative value)
        assert_eq!(leaf_node.values[0].len(), 1, "Value should have one element");
        assert!(leaf_node.values[0][0] < 0, "Value should be a negative number (PageID)");

        // Get the page ID from the LeafNode value
        let page_id = PageID((-leaf_node.values[0][0]) as u32);

        // Get the page
        let page = tag_index.index_pages.get_page(page_id).unwrap();

        // Verify that the page is a TagInternalNode
        assert_eq!(page.node.node_type_byte(), TAG_INTERNAL_NODE_TYPE, "Page should be a TagInternalNode");

        // Downcast the node to a TagInternalNode
        let tag_internal_node = page.node.as_any().downcast_ref::<TagInternalNode>().unwrap();

        // Verify that the TagInternalNode has at least one key
        assert!(!tag_internal_node.keys.is_empty(), "TagInternalNode should have at least one key");

        // Verify that the TagInternalNode has at least two child IDs
        assert!(tag_internal_node.child_ids.len() >= 2, "TagInternalNode should have at least two child IDs");

        // Use lookup() to verify all 201 positions are returned correctly
        let positions = tag_index.lookup(tag).unwrap();
        assert_eq!(positions.len(), 290, "lookup() should return 201 positions");
        for i in 0..201 {
            assert!(positions.contains(&(i as i64)), "lookup() should return position {}", i);
        }

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, page_size).unwrap();

        // Use lookup() to verify all 201 positions are still returned correctly
        let positions = tag_index2.lookup(tag).unwrap();
        assert_eq!(positions.len(), 201, "lookup() should return 201 positions after reopening");
        for i in 0..201 {
            assert!(positions.contains(&(i as i64)), "lookup() should return position {} after reopening", i);
        }
    }

    #[test]
    fn test_insert_lookup_positions_moved_to_tag_leaf_node() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance with a large page size to avoid splitting
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Insert 111 positions for the same tag
        let tag = "test-tag";
        let mut inserted_positions = Vec::new();

        // First, insert 100 positions
        for i in 0..100 {
            let position = i as i64;
            tag_index.insert(tag, position).unwrap();
            inserted_positions.push(position);
        }

        // Get the root page ID from the header page
        let header_node = tag_index.index_pages.header_node();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let root_page = tag_index.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page is a LeafNode
        assert_eq!(root_page.node.node_type_byte(), LEAF_NODE_TYPE, "Root node should be a LeafNode");

        // Downcast the node to a LeafNode
        let leaf_node = root_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode has one key
        assert_eq!(leaf_node.keys.len(), 1, "LeafNode should have one key");
        assert_eq!(leaf_node.values.len(), 1, "LeafNode should have one value");

        // Hash the tag to get the expected key
        let tag_hash = hash_tag(tag);
        let mut expected_key = [0u8; TAG_HASH_LEN];
        expected_key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

        // Verify that the key matches the expected key
        assert_eq!(leaf_node.keys[0], expected_key, "Key should match the hashed tag");

        // Verify that the value has 100 positions
        assert_eq!(leaf_node.values[0].len(), 100, "Value should have 100 elements");

        // Now insert 11 more positions to trigger the creation of a TagLeafNode
        for i in 100..111 {
            let position = i as i64;
            tag_index.insert(tag, position).unwrap();
            inserted_positions.push(position);
        }

        // Get the root page ID from the header page
        let header_node = tag_index.index_pages.header_node();
        let root_page_id = header_node.root_page_id;

        // Get the root page
        let root_page = tag_index.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page is a LeafNode
        assert_eq!(root_page.node.node_type_byte(), LEAF_NODE_TYPE, "Root node should be a LeafNode");

        // Downcast the node to a LeafNode
        let leaf_node = root_page.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode has one key
        assert_eq!(leaf_node.keys.len(), 1, "LeafNode should have one key");
        assert_eq!(leaf_node.values.len(), 1, "LeafNode should have one value");

        // Hash the tag to get the expected key
        let tag_hash = hash_tag(tag);
        let mut expected_key = [0u8; TAG_HASH_LEN];
        expected_key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);

        // Verify that the key matches the expected key
        assert_eq!(leaf_node.keys[0], expected_key, "Key should match the hashed tag");

        // Verify that the value is a PageID of a TagLeafNode (a single negative value)
        assert_eq!(leaf_node.values[0].len(), 1, "Value should have one element");
        assert!(leaf_node.values[0][0] < 0, "Value should be a negative number (PageID)");

        // Get the TagLeafNode page
        let tag_leaf_page_id = PageID((-leaf_node.values[0][0]) as u32);
        let tag_leaf_page = tag_index.index_pages.get_page(tag_leaf_page_id).unwrap();
        let tag_leaf_node = tag_leaf_page.node.as_any().downcast_ref::<TagLeafNode>().unwrap();

        // Verify that the TagLeafNode has 111 positions
        assert_eq!(tag_leaf_node.positions.len(), 111, "TagLeafNode should have 111 positions");

        // Verify that the positions are correct
        for i in 0..111 {
            assert_eq!(tag_leaf_node.positions[i], i as i64, "Position at index {} should be {}", i, i);
        }

        // Use lookup() to verify all 111 positions are returned correctly
        let positions = tag_index.lookup(tag).unwrap();
        assert_eq!(positions.len(), 111, "lookup() should return 111 positions");
        for i in 0..111 {
            assert!(positions.contains(&(i as i64)), "lookup() should return position {}", i);
        }

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create a new TagIndex instance
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Use lookup() to verify all 111 positions are still returned correctly after reopening
        let positions2 = tag_index2.lookup(tag).unwrap();
        assert_eq!(positions2.len(), 111, "lookup() should return 111 positions after reopening");
        for i in 0..111 {
            assert!(positions2.contains(&(i as i64)), "lookup() should return position {} after reopening", i);
        }

        // Get the root page again
        let root_page2 = tag_index2.index_pages.get_page(root_page_id).unwrap();

        // Verify that the root page is still a LeafNode
        assert_eq!(root_page2.node.node_type_byte(), LEAF_NODE_TYPE, "Root node should still be a LeafNode after reopening");

        // Downcast the node to a LeafNode
        let leaf_node2 = root_page2.node.as_any().downcast_ref::<LeafNode>().unwrap();

        // Verify that the LeafNode still has one key
        assert_eq!(leaf_node2.keys.len(), 1, "LeafNode should still have one key after reopening");
        assert_eq!(leaf_node2.values.len(), 1, "LeafNode should still have one value after reopening");

        // Verify that the key still matches the expected key
        assert_eq!(leaf_node2.keys[0], expected_key, "Key should still match the hashed tag after reopening");

        // Verify that the value is still a PageID of a TagLeafNode
        assert_eq!(leaf_node2.values[0].len(), 1, "Value should still have one element after reopening");
        assert!(leaf_node2.values[0][0] < 0, "Value should still be a negative number (PageID) after reopening");

        // Get the TagLeafNode page again
        let tag_leaf_page_id2 = PageID((-leaf_node2.values[0][0]) as u32);
        assert_eq!(tag_leaf_page_id, tag_leaf_page_id2, "TagLeafNode PageID should be the same after reopening");
        let tag_leaf_page2 = tag_index2.index_pages.get_page(tag_leaf_page_id2).unwrap();
        let tag_leaf_node2 = tag_leaf_page2.node.as_any().downcast_ref::<TagLeafNode>().unwrap();

        // Verify that the TagLeafNode still has 111 positions
        assert_eq!(tag_leaf_node2.positions.len(), 111, "TagLeafNode should still have 111 positions after reopening");

        // Verify that the positions are still correct
        for i in 0..111 {
            assert_eq!(tag_leaf_node2.positions[i], i as i64, "Position at index {} should still be {} after reopening", i, i);
        }
    }

    #[test]
    fn test_insert_internal_key_and_value_without_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new TagIndex instance with a large page size to avoid splitting
        let mut tag_index = TagIndex::new(&test_path, 4096).unwrap();

        // Construct a PageID
        let page_id = PageID(40);

        // Create an InternalNode with 4 keys and 5 child PageID values
        let mut keys = Vec::new();
        let mut child_ids = Vec::new();

        // Create 4 tag hashes for keys
        for i in 0..4 {
            // Create a tag hash for each key
            let tag_hash = hash_tag(&format!("Tag{}", i));
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);
            keys.push(key);
        }

        // Create 5 PageIDs for child_ids (one more than keys)
        for i in 0..5 {
            child_ids.push(PageID(100 + i as u32));
        }

        // Sort the keys to ensure they are in order
        keys.sort();

        let internal_node = InternalNode {
            keys,
            child_ids,
        };

        // Create an IndexPage with the InternalNode
        let internal_page = IndexPage {
            page_id,
            node: Box::new(internal_node),
        };

        // Add the page to the index
        tag_index.index_pages.add_page(internal_page);

        // Add a new key and child_id that will not cause the internal node to split
        let new_tag_hash = hash_tag("NewTag");
        let mut new_key = [0u8; TAG_HASH_LEN];
        new_key.copy_from_slice(&new_tag_hash[..TAG_HASH_LEN]);
        let new_child_id = PageID(200);

        // Call insert_internal_key_and_value with the page_id, key, and child_id
        let split_result = tag_index.insert_internal_key_and_value(page_id, new_key, new_child_id);

        // Verify that the internal node was not split
        assert!(split_result.is_none());

        // Get the page and verify it has 5 keys and 6 child_ids
        let page = tag_index.index_pages.get_page(page_id).unwrap();
        let internal = page.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(internal.keys.len(), 5);
        assert_eq!(internal.child_ids.len(), 6);

        // Find the index of the new key
        let mut new_key_index = 0;
        for (i, k) in internal.keys.iter().enumerate() {
            if *k == new_key {
                new_key_index = i;
                break;
            }
        }

        // Verify the new key and child_id were inserted correctly
        assert_eq!(internal.keys[new_key_index], new_key);
        assert_eq!(internal.child_ids[new_key_index + 1], new_child_id);

        // Flush changes to disk
        tag_index.index_pages.flush().unwrap();

        // Create another instance of TagIndex
        let mut tag_index2 = TagIndex::new(&test_path, 4096).unwrap();

        // Get the page and verify it still has 5 keys and 6 child_ids
        let page2 = tag_index2.index_pages.get_page(page_id).unwrap();
        let internal2 = page2.node.as_any().downcast_ref::<InternalNode>().unwrap();
        assert_eq!(internal2.keys.len(), 5);
        assert_eq!(internal2.child_ids.len(), 6);

        // Find the index of the new key
        let mut new_key_index2 = 0;
        for (i, k) in internal2.keys.iter().enumerate() {
            if *k == new_key {
                new_key_index2 = i;
                break;
            }
        }

        // Verify the new key and child_id were inserted correctly
        assert_eq!(internal2.keys[new_key_index2], new_key);
        assert_eq!(internal2.child_ids[new_key_index2 + 1], new_child_id);

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }

    #[test]
    fn test_insert_internal_key_and_value_with_split() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Construct a PageID
        let page_id = PageID(40);

        // Create an InternalNode with 5 keys and 6 child PageID values
        let mut keys = Vec::new();
        let mut child_ids = Vec::new();

        // Create 5 tag hashes for keys
        for i in 0..5 {
            // Create a tag hash for each key
            let tag_hash = hash_tag(&format!("Tag{}", i));
            let mut key = [0u8; TAG_HASH_LEN];
            key.copy_from_slice(&tag_hash[..TAG_HASH_LEN]);
            keys.push(key);
        }

        // Create 6 PageIDs for child_ids (one more than keys)
        for i in 0..6 {
            child_ids.push(PageID(100 + i as u32));
        }

        // Sort the keys to ensure they are in order
        keys.sort();

        let internal_node = InternalNode {
            keys,
            child_ids,
        };

        // Calculate the serialized size of the InternalNode
        let serialized_size = internal_node.calc_serialized_page_size();

        // Create an IndexPage with the InternalNode
        let internal_page = IndexPage {
            page_id,
            node: Box::new(internal_node),
        };

        // Create a new TagIndex instance with a page size that is the serialized length
        // This will ensure that the page needs splitting when we add another key
        let mut tag_index = TagIndex::new(&test_path, serialized_size).unwrap();

        // Add the page to the index
        tag_index.index_pages.add_page(internal_page);

        // Add a new key and child_id that will cause the internal node to split
        let new_tag_hash = hash_tag("NewTag");
        let mut new_key = [0u8; TAG_HASH_LEN];
        new_key.copy_from_slice(&new_tag_hash[..TAG_HASH_LEN]);
        let new_child_id = PageID(200);

        // Call insert_internal_key_and_value with the page_id, key, and child_id
        let split_result = tag_index.insert_internal_key_and_value(page_id, new_key, new_child_id);

        // Verify that the internal node was split
        assert!(split_result.is_some());
        let (promoted_key, new_page_id) = split_result.unwrap();

        // Check the original page
        {
            let original_page = tag_index.index_pages.get_page(page_id).unwrap();
            let original_internal = original_page.node.as_any().downcast_ref::<InternalNode>().unwrap();

            // Verify it has the correct number of keys and child_ids
            // For internal nodes, we should have one more child_id than keys
            assert_eq!(original_internal.child_ids.len(), original_internal.keys.len() + 1);

            // Check if the new key is in the original node
            let new_key_in_original = original_internal.keys.contains(&new_key);
            if new_key_in_original {
                let index = original_internal.keys.iter().position(|k| *k == new_key).unwrap();
                assert_eq!(original_internal.child_ids[index + 1], new_child_id);
            }

            // Verify that the promoted key is not in the original node
            assert!(!original_internal.keys.contains(&promoted_key));
        }

        // Check the new page
        {
            let new_page = tag_index.index_pages.get_page(new_page_id).unwrap();
            let new_internal = new_page.node.as_any().downcast_ref::<InternalNode>().unwrap();

            // Verify it has the correct number of keys and child_ids
            // For internal nodes, we should have one more child_id than keys
            assert_eq!(new_internal.child_ids.len(), new_internal.keys.len() + 1);

            // Check if the new key is in the new node
            let new_key_in_new = new_internal.keys.contains(&new_key);
            if new_key_in_new {
                let index = new_internal.keys.iter().position(|k| *k == new_key).unwrap();
                assert_eq!(new_internal.child_ids[index + 1], new_child_id);
            }

            // Verify that the promoted key is not in the new node
            assert!(!new_internal.keys.contains(&promoted_key));
        }

        // Verify that either the new key is in one of the nodes, or the new key is the same as the promoted key
        let new_key_in_original = {
            let original_page = tag_index.index_pages.get_page(page_id).unwrap();
            let original_internal = original_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
            original_internal.keys.contains(&new_key)
        };

        let new_key_in_new = {
            let new_page = tag_index.index_pages.get_page(new_page_id).unwrap();
            let new_internal = new_page.node.as_any().downcast_ref::<InternalNode>().unwrap();
            new_internal.keys.contains(&new_key)
        };

        let new_key_is_promoted = new_key == promoted_key;

        assert!(new_key_in_original || new_key_in_new || new_key_is_promoted, 
                "New key not found in either node and is not the promoted key");

        // No need to clean up the test file, it will be removed when temp_dir goes out of scope
    }
}
