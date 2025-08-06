use std::{fmt, io};

// Constants for serialization
const PAGE_TYPE_HEADER: u8 = b'1';
const PAGE_TYPE_FREELIST_LEAF: u8 = b'2';
const PAGE_TYPE_FREELIST_INTERNAL: u8 = b'3';
const PAGE_TYPE_POSITION_LEAF: u8 = b'4';
const PAGE_TYPE_POSITION_INTERNAL: u8 = b'5';

// Error types
#[derive(Debug)]
pub enum LmdbError {
    Io(io::Error),
    PageNotFound(PageID),
    DirtyPageNotFound(PageID),
    RootIDMismatchError(PageID, PageID),
    DatabaseCorrupted(String),
    SerializationError(String),
    DeserializationError(String),
    PageAlreadyFreedError(PageID),

}

impl From<io::Error> for LmdbError {
    fn from(err: io::Error) -> Self {
        LmdbError::Io(err)
    }
}

impl fmt::Display for LmdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LmdbError::Io(err) => write!(f, "IO error: {}", err),
            LmdbError::PageNotFound(page_id) => write!(f, "Page not found: {:?}", page_id),
            LmdbError::DirtyPageNotFound(page_id) => write!(f, "Dirty page not found: {:?}", page_id),
            LmdbError::RootIDMismatchError(old_id, new_id ) => write!(f, "Root ID mismatched: old {:?} new {:?}", old_id, new_id),
            LmdbError::DatabaseCorrupted(msg) => write!(f, "Database corrupted: {}", msg),
            LmdbError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            LmdbError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            LmdbError::PageAlreadyFreedError(page_id) => write!(f, "Page already freed: {:?}", page_id),
        }
    }
}

impl std::error::Error for LmdbError {}

// Result type alias
pub type Result<T> = std::result::Result<T, LmdbError>;

// NewType definitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageID(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TSN(pub u32);

// Node type definitions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderNode {
    pub tsn: TSN,
    pub next_page_id: PageID,
    pub freetree_root_id: PageID,
    pub position_root_id: PageID,
}

impl HeaderNode {
    /// Serializes the HeaderNode to a byte array with 16 bytes
    /// 4 bytes for tsn, 4 bytes for next_page_id, 4 bytes for freetree_root_id, and 4 bytes for position_root_id
    ///
    /// # Returns
    /// * `Vec<u8>` - The serialized data
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(16);
        result.extend_from_slice(&self.tsn.0.to_le_bytes());
        result.extend_from_slice(&self.next_page_id.0.to_le_bytes());
        result.extend_from_slice(&self.freetree_root_id.0.to_le_bytes());
        result.extend_from_slice(&self.position_root_id.0.to_le_bytes());
        result
    }

    /// Creates a HeaderNode from a byte slice
    /// Expects a slice with 16 bytes:
    /// - 4 bytes for tsn
    /// - 4 bytes for next_page_id
    /// - 4 bytes for freetree_root_id
    /// - 4 bytes for position_root_id
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized HeaderNode or an error
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() != 16 {
            return Err(LmdbError::DeserializationError(
                format!("Expected 16 bytes, got {}", slice.len())
            ));
        }

        let tsn = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]);
        let next_page_id = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]);
        let freetree_root_id = u32::from_le_bytes([slice[8], slice[9], slice[10], slice[11]]);
        let position_root_id = u32::from_le_bytes([slice[12], slice[13], slice[14], slice[15]]);

        Ok(HeaderNode {
            tsn: TSN(tsn),
            next_page_id: PageID(next_page_id),
            freetree_root_id: PageID(freetree_root_id),
            position_root_id: PageID(position_root_id),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListLeafValue {
    pub page_ids: Vec<PageID>,
    pub root_id: Option<PageID>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListLeafNode {
    pub keys: Vec<TSN>,
    pub values: Vec<FreeListLeafValue>,
}

impl FreeListLeafNode {
    /// Calculates the size needed to serialize the FreeListLeafNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    fn calc_serialized_node_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 4 bytes for each TSN in keys
        total_size += self.keys.len() * 4;

        // For each value:
        for value in &self.values {
            // 2 bytes for page_ids length
            total_size += 2;

            // 4 bytes for each PageID in page_ids
            total_size += value.page_ids.len() * 4;

            // 1 byte for root_id presence flag + 4 bytes if present
            total_size += 1;
            if value.root_id.is_some() {
                total_size += 4;
            }
        }

        total_size
    }

    /// Serializes the FreeListLeafNode to a byte array by manually converting its fields to bytes
    ///
    /// # Returns
    /// * `Result<Vec<u8>, LmdbError>` - The serialized data or an error
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let total_size = self.calc_serialized_node_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (4 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.0.to_le_bytes());
        }

        // Serialize each value
        for value in &self.values {
            // Serialize the length of page_ids (2 bytes)
            result.extend_from_slice(&(value.page_ids.len() as u16).to_le_bytes());

            // Serialize each PageID (4 bytes each)
            for page_id in &value.page_ids {
                result.extend_from_slice(&page_id.0.to_le_bytes());
            }

            // Serialize the root_id (1 byte flag + 4 bytes if present)
            if let Some(root_id) = value.root_id {
                result.push(1); // Flag indicating root_id is present
                result.extend_from_slice(&root_id.0.to_le_bytes());
            } else {
                result.push(0); // Flag indicating root_id is not present
            }
        }

        Ok(result)
    }

    /// Creates a FreeListLeafNode from a byte slice
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized FreeListLeafNode or an error
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least 2 bytes, got {}", slice.len())
            ));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 4);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least {} bytes for keys, got {}", min_expected_size, slice.len())
            ));
        }

        // Extract the keys (4 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 4);
            let tsn = u32::from_le_bytes([slice[start], slice[start+1], slice[start+2], slice[start+3]]);
            keys.push(TSN(tsn));
        }

        // Extract the values
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * 4);

        for _ in 0..keys_len {
            if offset + 2 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading page_ids length".to_string()
                ));
            }

            // Extract the length of page_ids (2 bytes)
            let page_ids_len = u16::from_le_bytes([slice[offset], slice[offset+1]]) as usize;
            offset += 2;

            if offset + (page_ids_len * 4) > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading page_ids".to_string()
                ));
            }

            // Extract the page_ids (4 bytes each)
            let mut page_ids = Vec::with_capacity(page_ids_len);
            for j in 0..page_ids_len {
                let start = offset + (j * 4);
                let page_id = u32::from_le_bytes([slice[start], slice[start+1], slice[start+2], slice[start+3]]);
                page_ids.push(PageID(page_id));
            }
            offset += page_ids_len * 4;

            if offset >= slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading root_id flag".to_string()
                ));
            }

            // Extract the root_id flag (1 byte)
            let has_root_id = slice[offset] != 0;
            offset += 1;

            // Extract the root_id if present (4 bytes)
            let root_id = if has_root_id {
                if offset + 4 > slice.len() {
                    return Err(LmdbError::DeserializationError(
                        "Unexpected end of data while reading root_id".to_string()
                    ));
                }

                let page_id = u32::from_le_bytes([slice[offset], slice[offset+1], slice[offset+2], slice[offset+3]]);
                offset += 4;
                Some(PageID(page_id))
            } else {
                None
            };

            values.push(FreeListLeafValue {
                page_ids,
                root_id,
            });
        }

        Ok(FreeListLeafNode {
            keys,
            values,
        })
    }

    pub fn insert_or_append(&mut self, tsn: TSN, page_id: PageID) -> Result<()> {
        // Find the place to insert the value
        let leaf_idx = self.keys.iter().position(|&k| k == tsn);

        // Insert the value
        if let Some(idx) = leaf_idx {
            // TSN already exists, append to its page_ids
            if self.values[idx].root_id.is_none() {
                self.values[idx].page_ids.push(page_id);
            } else {
                return Err(LmdbError::DatabaseCorrupted("Free list subtree not implemented".to_string()));
            }
            // println!(
            //     "Appended page ID {:?} to TSN {:?} in page {:?}: {:?}",
            //     freed_page_id, tsn, dirty_page_id, dirty_leaf_node
            // );

        } else {
            // New TSN, add a new entry
            self.keys.push(tsn);
            self.values.push(FreeListLeafValue {
                page_ids: vec![page_id],
                root_id: None,
            });
            // println!(
            //     "Inserted {:?} and appended {:?} in {:?}: {:?}",
            //     tsn, freed_page_id, dirty_page_id, dirty_leaf_node
            // );
        }
        Ok(())
    }

    pub fn pop_last_key_and_value(&mut self) -> Result<(TSN, FreeListLeafValue)> {
        let last_key = self.keys.pop().unwrap();
        let last_value = self.values.pop().unwrap();
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListInternalNode {
    pub keys: Vec<TSN>,
    pub child_ids: Vec<PageID>,
}

impl FreeListInternalNode {
    /// Calculates the size needed to serialize the FreeListInternalNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    fn calc_serialized_node_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 4 bytes for each TSN in keys
        total_size += self.keys.len() * 4;

        // 2 bytes for child_ids length
        total_size += 2;

        // 4 bytes for each PageID in child_ids
        total_size += self.child_ids.len() * 4;

        total_size
    }

    /// Serializes the FreeListInternalNode to a byte array by manually converting its fields to bytes
    ///
    /// # Returns
    /// * `Result<Vec<u8>, LmdbError>` - The serialized data or an error
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let total_size = self.calc_serialized_node_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (4 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.0.to_le_bytes());
        }

        // Serialize the length of child_ids (2 bytes)
        result.extend_from_slice(&(self.child_ids.len() as u16).to_le_bytes());

        // Serialize each child_id (4 bytes each)
        for child_id in &self.child_ids {
            result.extend_from_slice(&child_id.0.to_le_bytes());
        }

        Ok(result)
    }

    /// Creates a FreeListInternalNode from a byte slice
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized FreeListInternalNode or an error
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least 2 bytes, got {}", slice.len())
            ));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 4);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least {} bytes for keys, got {}", min_expected_size, slice.len())
            ));
        }

        // Extract the keys (4 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 4);
            let tsn = u32::from_le_bytes([slice[start], slice[start+1], slice[start+2], slice[start+3]]);
            keys.push(TSN(tsn));
        }

        // Extract the length of child_ids (2 bytes)
        let offset = 2 + (keys_len * 4);
        if offset + 2 > slice.len() {
            return Err(LmdbError::DeserializationError(
                "Unexpected end of data while reading child_ids length".to_string()
            ));
        }

        let child_ids_len = u16::from_le_bytes([slice[offset], slice[offset+1]]) as usize;

        // Calculate the minimum expected size for the child_ids
        let min_expected_size = offset + 2 + (child_ids_len * 4);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least {} bytes for child_ids, got {}", min_expected_size, slice.len())
            ));
        }

        // Extract the child_ids (4 bytes each)
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let start = offset + 2 + (i * 4);
            let page_id = u32::from_le_bytes([slice[start], slice[start+1], slice[start+2], slice[start+3]]);
            child_ids.push(PageID(page_id));
        }

        Ok(FreeListInternalNode {
            keys,
            child_ids,
        })
    }

    pub fn replace_last_child_id(&mut self, old_id: PageID, new_id: PageID) -> Result<()> {
        // Replace the last child ID.
        let last_idx = self.child_ids.len() - 1;
        if self.child_ids[last_idx] == old_id {
            self.child_ids[last_idx] = new_id;
        } else {
            return Err(LmdbError::DatabaseCorrupted("Child ID mismatch".to_string()));
        }
        Ok(())
    }

    pub fn append_promoted_key_and_page_id(&mut self, promoted_key: TSN, promoted_page_id: PageID) -> Result<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }

    pub(crate) fn split_off(&mut self) -> Result<(TSN, Vec<TSN>, Vec<PageID>)> {
        let middle_idx = self.keys.len() - 2;
        let promoted_key = self.keys.remove(middle_idx);
        let new_keys = self.keys.split_off(middle_idx);
        let new_child_ids = self.child_ids.split_off(middle_idx + 1);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionIndexRecord {
    pub segment: u32,
    pub offset: u32,
    pub type_hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionLeafNode {
    pub keys: Vec<Position>,
    pub values: Vec<PositionIndexRecord>,
    pub next_leaf_id: Option<PageID>,
}

impl PositionLeafNode {
    /// Calculates the size needed to serialize the PositionLeafNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    fn calc_serialized_node_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each Position in keys
        total_size += self.keys.len() * 8;

        // For each value (PositionIndexRecord):
        for value in &self.values {
            // 4 bytes for segment
            total_size += 4;

            // 4 bytes for offset
            total_size += 4;

            // 2 bytes for type_hash length + bytes for type_hash
            total_size += 2 + value.type_hash.len();
        }

        // 1 byte for next_leaf_id presence flag + 4 bytes if present
        total_size += 1;
        if self.next_leaf_id.is_some() {
            total_size += 4;
        }

        total_size
    }

    /// Serializes the PositionLeafNode to a byte array by manually converting its fields to bytes
    ///
    /// # Returns
    /// * `Result<Vec<u8>, LmdbError>` - The serialized data or an error
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let total_size = self.calc_serialized_node_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.0.to_le_bytes());
        }

        // Serialize each value (PositionIndexRecord)
        for value in &self.values {
            // Serialize segment (4 bytes)
            result.extend_from_slice(&value.segment.to_le_bytes());

            // Serialize offset (4 bytes)
            result.extend_from_slice(&value.offset.to_le_bytes());

            // Serialize type_hash length (2 bytes) and type_hash bytes
            result.extend_from_slice(&(value.type_hash.len() as u16).to_le_bytes());
            result.extend_from_slice(&value.type_hash);
        }

        // Serialize the next_leaf_id (1 byte flag + 4 bytes if present)
        if let Some(next_leaf_id) = self.next_leaf_id {
            result.push(1); // Flag indicating next_leaf_id is present
            result.extend_from_slice(&next_leaf_id.0.to_le_bytes());
        } else {
            result.push(0); // Flag indicating next_leaf_id is not present
        }

        Ok(result)
    }

    /// Creates a PositionLeafNode from a byte slice
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized PositionLeafNode or an error
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least 2 bytes, got {}", slice.len())
            ));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least {} bytes for keys, got {}", min_expected_size, slice.len())
            ));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 8);
            let position = u64::from_le_bytes([
                slice[start], slice[start+1], slice[start+2], slice[start+3],
                slice[start+4], slice[start+5], slice[start+6], slice[start+7]
            ]);
            keys.push(Position(position));
        }

        // Extract the values
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * 8);

        for _ in 0..keys_len {
            if offset + 8 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading PositionIndexRecord".to_string()
                ));
            }

            // Extract segment (4 bytes)
            let segment = u32::from_le_bytes([slice[offset], slice[offset+1], slice[offset+2], slice[offset+3]]);
            offset += 4;

            // Extract offset (4 bytes)
            let record_offset = u32::from_le_bytes([slice[offset], slice[offset+1], slice[offset+2], slice[offset+3]]);
            offset += 4;

            if offset + 2 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading type_hash length".to_string()
                ));
            }

            // Extract type_hash length (2 bytes)
            let type_hash_len = u16::from_le_bytes([slice[offset], slice[offset+1]]) as usize;
            offset += 2;

            if offset + type_hash_len > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading type_hash".to_string()
                ));
            }

            // Extract type_hash
            let mut type_hash = Vec::with_capacity(type_hash_len);
            type_hash.extend_from_slice(&slice[offset..offset+type_hash_len]);
            offset += type_hash_len;

            values.push(PositionIndexRecord {
                segment,
                offset: record_offset,
                type_hash,
            });
        }

        if offset >= slice.len() {
            return Err(LmdbError::DeserializationError(
                "Unexpected end of data while reading next_leaf_id flag".to_string()
            ));
        }

        // Extract the next_leaf_id flag (1 byte)
        let has_next_leaf_id = slice[offset] != 0;
        offset += 1;

        // Extract the next_leaf_id if present (4 bytes)
        let next_leaf_id = if has_next_leaf_id {
            if offset + 4 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading next_leaf_id".to_string()
                ));
            }

            let page_id = u32::from_le_bytes([slice[offset], slice[offset+1], slice[offset+2], slice[offset+3]]);
            Some(PageID(page_id))
        } else {
            None
        };

        Ok(PositionLeafNode {
            keys,
            values,
            next_leaf_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl PositionInternalNode {
    /// Calculates the size needed to serialize the PositionInternalNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    fn calc_serialized_node_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each Position in keys
        total_size += self.keys.len() * 8;

        // 2 bytes for child_ids length
        total_size += 2;

        // 4 bytes for each PageID in child_ids
        total_size += self.child_ids.len() * 4;

        total_size
    }

    /// Serializes the PositionInternalNode to a byte array by manually converting its fields to bytes
    ///
    /// # Returns
    /// * `Result<Vec<u8>, LmdbError>` - The serialized data or an error
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let total_size = self.calc_serialized_node_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.0.to_le_bytes());
        }

        // Serialize the length of child_ids (2 bytes)
        result.extend_from_slice(&(self.child_ids.len() as u16).to_le_bytes());

        // Serialize each child_id (4 bytes each)
        for child_id in &self.child_ids {
            result.extend_from_slice(&child_id.0.to_le_bytes());
        }

        Ok(result)
    }

    /// Creates a PositionInternalNode from a byte slice
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized PositionInternalNode or an error
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least 2 bytes, got {}", slice.len())
            ));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least {} bytes for keys, got {}", min_expected_size, slice.len())
            ));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 8);
            let position = u64::from_le_bytes([
                slice[start], slice[start+1], slice[start+2], slice[start+3],
                slice[start+4], slice[start+5], slice[start+6], slice[start+7]
            ]);
            keys.push(Position(position));
        }

        // Extract the length of child_ids (2 bytes)
        let offset = 2 + (keys_len * 8);
        if offset + 2 > slice.len() {
            return Err(LmdbError::DeserializationError(
                "Unexpected end of data while reading child_ids length".to_string()
            ));
        }

        let child_ids_len = u16::from_le_bytes([slice[offset], slice[offset+1]]) as usize;

        // Calculate the minimum expected size for the child_ids
        let min_expected_size = offset + 2 + (child_ids_len * 4);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(
                format!("Expected at least {} bytes for child_ids, got {}", min_expected_size, slice.len())
            ));
        }

        // Extract the child_ids (4 bytes each)
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let start = offset + 2 + (i * 4);
            let page_id = u32::from_le_bytes([slice[start], slice[start+1], slice[start+2], slice[start+3]]);
            child_ids.push(PageID(page_id));
        }

        Ok(PositionInternalNode {
            keys,
            child_ids,
        })
    }
}

// Enum to represent different node types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Header(HeaderNode),
    FreeListLeaf(FreeListLeafNode),
    FreeListInternal(FreeListInternalNode),
    PositionLeaf(PositionLeafNode),
    PositionInternal(PositionInternalNode),
}

impl Node {
    pub fn get_type_byte(&self) -> u8 {
        match self {
            Node::Header(_) => PAGE_TYPE_HEADER,
            Node::FreeListLeaf(_) => PAGE_TYPE_FREELIST_LEAF,
            Node::FreeListInternal(_) => PAGE_TYPE_FREELIST_INTERNAL,
            Node::PositionLeaf(_) => PAGE_TYPE_POSITION_LEAF,
            Node::PositionInternal(_) => PAGE_TYPE_POSITION_INTERNAL,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        match self {
            Node::Header(node) => {
                Ok(node.serialize())
            }
            Node::FreeListLeaf(node) => {
                node.serialize()
            }
            Node::FreeListInternal(node) => {
                node.serialize()
            }
            Node::PositionLeaf(node) => {
                node.serialize()
            }
            Node::PositionInternal(node) => {
                node.serialize()
            }
        }
    }

    pub fn deserialize(node_type: u8, data: &[u8]) -> Result<Self> {
        match node_type {
            PAGE_TYPE_HEADER => {
                let node = HeaderNode::from_slice(data)?;
                Ok(Node::Header(node))
            }
            PAGE_TYPE_FREELIST_LEAF => {
                let node = FreeListLeafNode::from_slice(data)?;
                Ok(Node::FreeListLeaf(node))
            }
            PAGE_TYPE_FREELIST_INTERNAL => {
                let node = FreeListInternalNode::from_slice(data)?;
                Ok(Node::FreeListInternal(node))
            }
            PAGE_TYPE_POSITION_LEAF => {
                let node = PositionLeafNode::from_slice(data)?;
                Ok(Node::PositionLeaf(node))
            }
            PAGE_TYPE_POSITION_INTERNAL => {
                let node = PositionInternalNode::from_slice(data)?;
                Ok(Node::PositionInternal(node))
            }
            _ => Err(LmdbError::DatabaseCorrupted(format!("Invalid node type: {}", node_type)))
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_header_serialize() {
        // Create a HeaderNode with known values
        let header_node = HeaderNode {
            tsn: TSN(42),
            next_page_id: PageID(123),
            freetree_root_id: PageID(456),
            position_root_id: PageID(789),
        };

        // Serialize the HeaderNode
        let serialized = header_node.serialize();

        // Verify the serialized output has the correct length
        assert_eq!(16, serialized.len());

        // Verify the serialized output has the correct byte values
        // TSN(42) = 42u32 = [42, 0, 0, 0] in little-endian
        assert_eq!(&[42, 0, 0, 0], &serialized[0..4]);

        // PageID(123) = 123u32 = [123, 0, 0, 0] in little-endian
        assert_eq!(&[123, 0, 0, 0], &serialized[4..8]);

        // PageID(456) = 456u32 = [200, 1, 0, 0] in little-endian (456 = 200 + 1*256)
        assert_eq!(&[200, 1, 0, 0], &serialized[8..12]);

        // PageID(789) = 789u32 = [21, 3, 0, 0] in little-endian (789 = 21 + 3*256)
        assert_eq!(&[21, 3, 0, 0], &serialized[12..16]);

        // Deserialize back to a HeaderNode
        let deserialized = HeaderNode::from_slice(&serialized)
            .expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized node matches the original
        assert_eq!(header_node.tsn, deserialized.tsn);
        assert_eq!(header_node.next_page_id, deserialized.next_page_id);
        assert_eq!(header_node.freetree_root_id, deserialized.freetree_root_id);
        assert_eq!(header_node.position_root_id, deserialized.position_root_id);
    }

    #[test]
    fn test_freelist_leaf_serialize() {
        // Create a FreeListLeafNode with known values
        let leaf_node = FreeListLeafNode {
            keys: vec![TSN(10), TSN(20), TSN(30)],
            values: vec![
                FreeListLeafValue {
                    page_ids: vec![PageID(100), PageID(101)],
                    root_id: Some(PageID(200)),
                },
                FreeListLeafValue {
                    page_ids: vec![PageID(102), PageID(103), PageID(104)],
                    root_id: None,
                },
                FreeListLeafValue {
                    page_ids: vec![PageID(105)],
                    root_id: Some(PageID(300)),
                },
            ],
        };

        // Serialize the FreeListLeafNode
        let serialized = leaf_node.serialize().unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Deserialize back to a FreeListLeafNode using from_slice
        let deserialized = FreeListLeafNode::from_slice(&serialized)
            .expect("Failed to deserialize FreeListLeafNode");

        // Verify that the deserialized node matches the original
        assert_eq!(leaf_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(3, deserialized.values.len());

        // Check keys
        assert_eq!(TSN(10), deserialized.keys[0]);
        assert_eq!(TSN(20), deserialized.keys[1]);
        assert_eq!(TSN(30), deserialized.keys[2]);

        // Check first value
        assert_eq!(2, deserialized.values[0].page_ids.len());
        assert_eq!(PageID(100), deserialized.values[0].page_ids[0]);
        assert_eq!(PageID(101), deserialized.values[0].page_ids[1]);
        assert_eq!(Some(PageID(200)), deserialized.values[0].root_id);

        // Check second value
        assert_eq!(3, deserialized.values[1].page_ids.len());
        assert_eq!(PageID(102), deserialized.values[1].page_ids[0]);
        assert_eq!(PageID(103), deserialized.values[1].page_ids[1]);
        assert_eq!(PageID(104), deserialized.values[1].page_ids[2]);
        assert_eq!(None, deserialized.values[1].root_id);

        // Check third value
        assert_eq!(1, deserialized.values[2].page_ids.len());
        assert_eq!(PageID(105), deserialized.values[2].page_ids[0]);
        assert_eq!(Some(PageID(300)), deserialized.values[2].root_id);
    }

    #[test]
    fn test_freelist_internal_serialize() {
        // Create a FreeListInternalNode with known values
        let internal_node = FreeListInternalNode {
            keys: vec![TSN(10), TSN(20), TSN(30)],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };

        // Serialize the FreeListInternalNode using its serialize method
        let serialized = internal_node.serialize().unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Verify the serialized output has the correct structure
        // First 2 bytes: keys_len (3) = [3, 0] in little-endian
        assert_eq!(&[3, 0], &serialized[0..2]);

        // Next 12 bytes: 3 TSNs (4 bytes each)
        // TSN(10) = [10, 0, 0, 0] in little-endian
        assert_eq!(&[10, 0, 0, 0], &serialized[2..6]);
        // TSN(20) = [20, 0, 0, 0] in little-endian
        assert_eq!(&[20, 0, 0, 0], &serialized[6..10]);
        // TSN(30) = [30, 0, 0, 0] in little-endian
        assert_eq!(&[30, 0, 0, 0], &serialized[10..14]);

        // Next 2 bytes: child_ids_len (4) = [4, 0] in little-endian
        assert_eq!(&[4, 0], &serialized[14..16]);

        // Next 16 bytes: 4 PageIDs (4 bytes each)
        // PageID(100) = [100, 0, 0, 0] in little-endian
        assert_eq!(&[100, 0, 0, 0], &serialized[16..20]);
        // PageID(200) = [200, 0, 0, 0] in little-endian
        assert_eq!(&[200, 0, 0, 0], &serialized[20..24]);
        // PageID(300) = [44, 1, 0, 0] in little-endian (300 = 44 + 1*256)
        assert_eq!(&[44, 1, 0, 0], &serialized[24..28]);
        // PageID(400) = [144, 1, 0, 0] in little-endian (400 = 144 + 1*256)
        assert_eq!(&[144, 1, 0, 0], &serialized[28..32]);

        // Deserialize back to a FreeListInternalNode using from_slice
        let deserialized = FreeListInternalNode::from_slice(&serialized)
            .expect("Failed to deserialize FreeListInternalNode");

        // Verify that the deserialized node matches the original
        assert_eq!(internal_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(4, deserialized.child_ids.len());

        // Check keys
        assert_eq!(TSN(10), deserialized.keys[0]);
        assert_eq!(TSN(20), deserialized.keys[1]);
        assert_eq!(TSN(30), deserialized.keys[2]);

        // Check child_ids
        assert_eq!(PageID(100), deserialized.child_ids[0]);
        assert_eq!(PageID(200), deserialized.child_ids[1]);
        assert_eq!(PageID(300), deserialized.child_ids[2]);
        assert_eq!(PageID(400), deserialized.child_ids[3]);

        // Also test serialization through Node enum
        let node = Node::FreeListInternal(internal_node.clone());
        let node_serialized = node.serialize().unwrap();

        // Verify that serialization through Node produces the same result
        assert_eq!(serialized, node_serialized);

        // Test deserialization through Node enum
        let node_deserialized = Node::deserialize(PAGE_TYPE_FREELIST_INTERNAL, &node_serialized).unwrap();

        // Verify that deserialization through Node produces the correct result
        if let Node::FreeListInternal(deserialized_internal) = node_deserialized {
            assert_eq!(internal_node, deserialized_internal);
        } else {
            panic!("Expected FreeListInternal node");
        }
    }

    #[test]
    fn test_position_leaf_serialize() {
        // Create a PositionLeafNode with known values
        let leaf_node = PositionLeafNode {
            keys: vec![Position(100), Position(200), Position(300)],
            values: vec![
                PositionIndexRecord {
                    segment: 1,
                    offset: 10,
                    type_hash: vec![1, 2, 3],
                },
                PositionIndexRecord {
                    segment: 2,
                    offset: 20,
                    type_hash: vec![4, 5, 6, 7],
                },
                PositionIndexRecord {
                    segment: 3,
                    offset: 30,
                    type_hash: vec![8, 9],
                },
            ],
            next_leaf_id: Some(PageID(500)),
        };

        // Serialize the PositionLeafNode using its serialize method
        let serialized = leaf_node.serialize().unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Verify the serialized output has the correct structure
        // First 2 bytes: keys_len (3) = [3, 0] in little-endian
        assert_eq!(&[3, 0], &serialized[0..2]);

        // Next 24 bytes: 3 Positions (8 bytes each)
        // Position(100) = [100, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&[100, 0, 0, 0, 0, 0, 0, 0], &serialized[2..10]);
        // Position(200) = [200, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&[200, 0, 0, 0, 0, 0, 0, 0], &serialized[10..18]);
        // Position(300) = [44, 1, 0, 0, 0, 0, 0, 0] in little-endian (300 = 44 + 1*256)
        assert_eq!(&[44, 1, 0, 0, 0, 0, 0, 0], &serialized[18..26]);

        // Deserialize back to a PositionLeafNode using from_slice
        let deserialized = PositionLeafNode::from_slice(&serialized)
            .expect("Failed to deserialize PositionLeafNode");

        // Verify that the deserialized node matches the original
        assert_eq!(leaf_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(3, deserialized.values.len());
        assert_eq!(Some(PageID(500)), deserialized.next_leaf_id);

        // Check keys
        assert_eq!(Position(100), deserialized.keys[0]);
        assert_eq!(Position(200), deserialized.keys[1]);
        assert_eq!(Position(300), deserialized.keys[2]);

        // Check first value
        assert_eq!(1, deserialized.values[0].segment);
        assert_eq!(10, deserialized.values[0].offset);
        assert_eq!(vec![1, 2, 3], deserialized.values[0].type_hash);

        // Check second value
        assert_eq!(2, deserialized.values[1].segment);
        assert_eq!(20, deserialized.values[1].offset);
        assert_eq!(vec![4, 5, 6, 7], deserialized.values[1].type_hash);

        // Check third value
        assert_eq!(3, deserialized.values[2].segment);
        assert_eq!(30, deserialized.values[2].offset);
        assert_eq!(vec![8, 9], deserialized.values[2].type_hash);

        // Also test serialization through Node enum
        let node = Node::PositionLeaf(leaf_node.clone());
        let node_serialized = node.serialize().unwrap();

        // Verify that serialization through Node produces the same result
        assert_eq!(serialized, node_serialized);

        // Test deserialization through Node enum
        let node_deserialized = Node::deserialize(PAGE_TYPE_POSITION_LEAF, &node_serialized).unwrap();

        // Verify that deserialization through Node produces the correct result
        if let Node::PositionLeaf(deserialized_leaf) = node_deserialized {
            assert_eq!(leaf_node, deserialized_leaf);
        } else {
            panic!("Expected PositionLeaf node");
        }
    }

    #[test]
    fn test_position_internal_serialize() {
        // Create a PositionInternalNode with known values
        let internal_node = PositionInternalNode {
            keys: vec![Position(100), Position(200), Position(300)],
            child_ids: vec![PageID(10), PageID(20), PageID(30), PageID(40)],
        };

        // Serialize the PositionInternalNode using its serialize method
        let serialized = internal_node.serialize().unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Verify the serialized output has the correct structure
        // First 2 bytes: keys_len (3) = [3, 0] in little-endian
        assert_eq!(&[3, 0], &serialized[0..2]);

        // Next 24 bytes: 3 Positions (8 bytes each)
        // Position(100) = [100, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&[100, 0, 0, 0, 0, 0, 0, 0], &serialized[2..10]);
        // Position(200) = [200, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&[200, 0, 0, 0, 0, 0, 0, 0], &serialized[10..18]);
        // Position(300) = [44, 1, 0, 0, 0, 0, 0, 0] in little-endian (300 = 44 + 1*256)
        assert_eq!(&[44, 1, 0, 0, 0, 0, 0, 0], &serialized[18..26]);

        // Next 2 bytes: child_ids_len (4) = [4, 0] in little-endian
        assert_eq!(&[4, 0], &serialized[26..28]);

        // Next 16 bytes: 4 PageIDs (4 bytes each)
        // PageID(10) = [10, 0, 0, 0] in little-endian
        assert_eq!(&[10, 0, 0, 0], &serialized[28..32]);
        // PageID(20) = [20, 0, 0, 0] in little-endian
        assert_eq!(&[20, 0, 0, 0], &serialized[32..36]);
        // PageID(30) = [30, 0, 0, 0] in little-endian
        assert_eq!(&[30, 0, 0, 0], &serialized[36..40]);
        // PageID(40) = [40, 0, 0, 0] in little-endian
        assert_eq!(&[40, 0, 0, 0], &serialized[40..44]);

        // Deserialize back to a PositionInternalNode using from_slice
        let deserialized = PositionInternalNode::from_slice(&serialized)
            .expect("Failed to deserialize PositionInternalNode");

        // Verify that the deserialized node matches the original
        assert_eq!(internal_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(4, deserialized.child_ids.len());

        // Check keys
        assert_eq!(Position(100), deserialized.keys[0]);
        assert_eq!(Position(200), deserialized.keys[1]);
        assert_eq!(Position(300), deserialized.keys[2]);

        // Check child_ids
        assert_eq!(PageID(10), deserialized.child_ids[0]);
        assert_eq!(PageID(20), deserialized.child_ids[1]);
        assert_eq!(PageID(30), deserialized.child_ids[2]);
        assert_eq!(PageID(40), deserialized.child_ids[3]);

        // Also test serialization through Node enum
        let node = Node::PositionInternal(internal_node.clone());
        let node_serialized = node.serialize().unwrap();

        // Verify that serialization through Node produces the same result
        assert_eq!(serialized, node_serialized);

        // Test deserialization through Node enum
        let node_deserialized = Node::deserialize(PAGE_TYPE_POSITION_INTERNAL, &node_serialized).unwrap();

        // Verify that deserialization through Node produces the correct result
        if let Node::PositionInternal(deserialized_internal) = node_deserialized {
            assert_eq!(internal_node, deserialized_internal);
        } else {
            panic!("Expected PositionInternal node");
        }
    }
}
