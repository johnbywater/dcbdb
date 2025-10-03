use crate::common::{LmdbError, LmdbResult, PageID, Tsn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListLeafValue {
    pub page_ids: Vec<PageID>,
    pub root_id: PageID,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListLeafNode {
    pub keys: Vec<Tsn>,
    pub values: Vec<FreeListLeafValue>,
}

impl FreeListLeafNode {
    /// Calculates the size needed to serialize the FreeListLeafNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each TSN in keys
        total_size += self.keys.len() * 8;

        // For each value:
        for value in &self.values {
            // 2 bytes for page_ids length
            total_size += 2;

            // 8 bytes for each PageID in page_ids
            total_size += value.page_ids.len() * 8;

            // 8 bytes for root_id (PageID), PageID(0) indicates no subtree
            total_size += 8;
        }

        total_size
    }

    /// Serializes the FreeListLeafNode to a byte array by manually converting its fields to bytes
    ///
    /// # Returns
    /// * `Result<Vec<u8>, LmdbError>` - The serialized data or an error
    pub fn serialize(&self) -> LmdbResult<Vec<u8>> {
        let total_size = self.calc_serialized_size();
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

            // Serialize the root_id (always 8 bytes); PageID(0) indicates no subtree
            result.extend_from_slice(&value.root_id.0.to_le_bytes());
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
    pub fn from_slice(slice: &[u8]) -> LmdbResult<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                min_expected_size,
                slice.len()
            )));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 8);
            let tsn = u64::from_le_bytes([
                slice[start],
                slice[start + 1],
                slice[start + 2],
                slice[start + 3],
                slice[start + 4],
                slice[start + 5],
                slice[start + 6],
                slice[start + 7],
            ]);
            keys.push(Tsn(tsn));
        }

        // Extract the values
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * 8);

        for _ in 0..keys_len {
            if offset + 2 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading page_ids length".to_string(),
                ));
            }

            // Extract the length of page_ids (2 bytes)
            let page_ids_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
            offset += 2;

            if offset + (page_ids_len * 8) > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading page_ids".to_string(),
                ));
            }

            // Extract the page_ids (8 bytes each)
            let mut page_ids = Vec::with_capacity(page_ids_len);
            for j in 0..page_ids_len {
                let start = offset + (j * 8);
                let page_id = u64::from_le_bytes([
                    slice[start],
                    slice[start + 1],
                    slice[start + 2],
                    slice[start + 3],
                    slice[start + 4],
                    slice[start + 5],
                    slice[start + 6],
                    slice[start + 7],
                ]);
                page_ids.push(PageID(page_id));
            }
            offset += page_ids_len * 8;

            if offset + 8 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading root_id".to_string(),
                ));
            }

            // Extract the root_id (always 8 bytes); PageID(0) indicates no subtree
            let page_id = u64::from_le_bytes([
                slice[offset],
                slice[offset + 1],
                slice[offset + 2],
                slice[offset + 3],
                slice[offset + 4],
                slice[offset + 5],
                slice[offset + 6],
                slice[offset + 7],
            ]);
            offset += 8;
            let root_id = PageID(page_id);

            values.push(FreeListLeafValue { page_ids, root_id });
        }

        Ok(FreeListLeafNode { keys, values })
    }

    pub fn insert_or_append(&mut self, tsn: Tsn, page_id: PageID) -> LmdbResult<()> {
        // Find the place to insert the value
        let leaf_idx = self.keys.iter().position(|&k| k == tsn);

        // Insert the value
        if let Some(idx) = leaf_idx {
            // TSN already exists, append to its page_ids
            if self.values[idx].root_id == PageID(0) {
                self.values[idx].page_ids.push(page_id);
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Free list subtree not implemented".to_string(),
                ));
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
                root_id: PageID(0),
            });
            // println!(
            //     "Inserted {:?} and appended {:?} in {:?}: {:?}",
            //     tsn, freed_page_id, dirty_page_id, dirty_leaf_node
            // );
        }
        Ok(())
    }

    pub fn pop_last_key_and_value(&mut self) -> LmdbResult<(Tsn, FreeListLeafValue)> {
        let last_key = self.keys.pop().unwrap();
        let last_value = self.values.pop().unwrap();
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeListInternalNode {
    pub keys: Vec<Tsn>,
    pub child_ids: Vec<PageID>,
}

impl FreeListInternalNode {
    /// Calculates the size needed to serialize the FreeListInternalNode
    ///
    /// # Returns
    /// * `usize` - The size in bytes
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each TSN in keys
        total_size += self.keys.len() * 8;

        // 2 bytes for child_ids length
        total_size += 2;

        // 8 bytes for each PageID in child_ids
        total_size += self.child_ids.len() * 8;

        total_size
    }

    /// Serializes the FreeListInternalNode to a byte array by manually converting its fields to bytes
    ///
    /// # Returns
    /// * `Result<Vec<u8>, LmdbError>` - The serialized data or an error
    pub fn serialize(&self) -> LmdbResult<Vec<u8>> {
        let total_size = self.calc_serialized_size();
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
    pub fn from_slice(slice: &[u8]) -> LmdbResult<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for keys, got {}",
                min_expected_size,
                slice.len()
            )));
        }

        // Extract the keys (8 bytes each)
        let mut keys = Vec::with_capacity(keys_len);
        for i in 0..keys_len {
            let start = 2 + (i * 8);
            let tsn = u64::from_le_bytes([
                slice[start],
                slice[start + 1],
                slice[start + 2],
                slice[start + 3],
                slice[start + 4],
                slice[start + 5],
                slice[start + 6],
                slice[start + 7],
            ]);
            keys.push(Tsn(tsn));
        }

        // Extract the length of child_ids (2 bytes)
        let offset = 2 + (keys_len * 8);
        if offset + 2 > slice.len() {
            return Err(LmdbError::DeserializationError(
                "Unexpected end of data while reading child_ids length".to_string(),
            ));
        }

        let child_ids_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;

        // Calculate the minimum expected size for the child_ids
        let min_expected_size = offset + 2 + (child_ids_len * 8);
        if slice.len() < min_expected_size {
            return Err(LmdbError::DeserializationError(format!(
                "Expected at least {} bytes for child_ids, got {}",
                min_expected_size,
                slice.len()
            )));
        }

        // Extract the child_ids (8 bytes each)
        let mut child_ids = Vec::with_capacity(child_ids_len);
        for i in 0..child_ids_len {
            let start = offset + 2 + (i * 8);
            let page_id = u64::from_le_bytes([
                slice[start],
                slice[start + 1],
                slice[start + 2],
                slice[start + 3],
                slice[start + 4],
                slice[start + 5],
                slice[start + 6],
                slice[start + 7],
            ]);
            child_ids.push(PageID(page_id));
        }

        Ok(FreeListInternalNode { keys, child_ids })
    }

    pub fn replace_last_child_id(
        &mut self,
        old_id: PageID,
        new_id: PageID,
    ) -> LmdbResult<()> {
        // Replace the last child ID.
        let last_idx = self.child_ids.len() - 1;
        if self.child_ids[last_idx] == old_id {
            self.child_ids[last_idx] = new_id;
        } else {
            return Err(LmdbError::DatabaseCorrupted(
                "Child ID mismatch".to_string(),
            ));
        }
        Ok(())
    }

    pub fn append_promoted_key_and_page_id(
        &mut self,
        promoted_key: Tsn,
        promoted_page_id: PageID,
    ) -> LmdbResult<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }

    pub(crate) fn split_off(&mut self) -> LmdbResult<(Tsn, Vec<Tsn>, Vec<PageID>)> {
        let middle_idx = self.keys.len() - 2;
        let promoted_key = self.keys.remove(middle_idx);
        let new_keys = self.keys.split_off(middle_idx);
        let new_child_ids = self.child_ids.split_off(middle_idx + 1);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{PageID, Tsn};
    use crate::free_lists_tree_nodes::{FreeListInternalNode, FreeListLeafNode, FreeListLeafValue};

    #[test]
    fn test_freelist_leaf_serialize() {
        // Create a FreeListLeafNode with known values
        let leaf_node = FreeListLeafNode {
            keys: vec![Tsn(10), Tsn(20), Tsn(30)],
            values: vec![
                FreeListLeafValue {
                    page_ids: vec![PageID(100), PageID(101)],
                    root_id: PageID(200),
                },
                FreeListLeafValue {
                    page_ids: vec![PageID(102), PageID(103), PageID(104)],
                    root_id: PageID(0),
                },
                FreeListLeafValue {
                    page_ids: vec![PageID(105)],
                    root_id: PageID(300),
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
        assert_eq!(Tsn(10), deserialized.keys[0]);
        assert_eq!(Tsn(20), deserialized.keys[1]);
        assert_eq!(Tsn(30), deserialized.keys[2]);

        // Check first value
        assert_eq!(2, deserialized.values[0].page_ids.len());
        assert_eq!(PageID(100), deserialized.values[0].page_ids[0]);
        assert_eq!(PageID(101), deserialized.values[0].page_ids[1]);
        assert_eq!(PageID(200), deserialized.values[0].root_id);

        // Check second value
        assert_eq!(3, deserialized.values[1].page_ids.len());
        assert_eq!(PageID(102), deserialized.values[1].page_ids[0]);
        assert_eq!(PageID(103), deserialized.values[1].page_ids[1]);
        assert_eq!(PageID(104), deserialized.values[1].page_ids[2]);
        assert_eq!(PageID(0), deserialized.values[1].root_id);

        // Check third value
        assert_eq!(1, deserialized.values[2].page_ids.len());
        assert_eq!(PageID(105), deserialized.values[2].page_ids[0]);
        assert_eq!(PageID(300), deserialized.values[2].root_id);
    }

    #[test]
    fn test_freelist_internal_serialize() {
        // Create a FreeListInternalNode with known values
        let internal_node = FreeListInternalNode {
            keys: vec![Tsn(10), Tsn(20), Tsn(30)],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };

        // Serialize the FreeListInternalNode using its serialize method
        let serialized = internal_node.serialize().unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Verify the serialized output has the correct structure
        // First 2 bytes: keys_len (3) = [3, 0] in little-endian
        assert_eq!(&[3, 0], &serialized[0..2]);

        // Next 24 bytes: 3 TSNs (8 bytes each)
        assert_eq!(&10u64.to_le_bytes(), &serialized[2..10]);
        assert_eq!(&20u64.to_le_bytes(), &serialized[10..18]);
        assert_eq!(&30u64.to_le_bytes(), &serialized[18..26]);

        // Next 2 bytes: child_ids_len (4) = [4, 0] in little-endian
        assert_eq!(&[4, 0], &serialized[26..28]);

        // Next 32 bytes: 4 PageIDs (8 bytes each)
        assert_eq!(&100u64.to_le_bytes(), &serialized[28..36]);
        assert_eq!(&200u64.to_le_bytes(), &serialized[36..44]);
        assert_eq!(&300u64.to_le_bytes(), &serialized[44..52]);
        assert_eq!(&400u64.to_le_bytes(), &serialized[52..60]);

        // Deserialize back to a FreeListInternalNode using from_slice
        let deserialized = FreeListInternalNode::from_slice(&serialized)
            .expect("Failed to deserialize FreeListInternalNode");

        // Verify that the deserialized node matches the original
        assert_eq!(internal_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(4, deserialized.child_ids.len());

        // Check keys
        assert_eq!(Tsn(10), deserialized.keys[0]);
        assert_eq!(Tsn(20), deserialized.keys[1]);
        assert_eq!(Tsn(30), deserialized.keys[2]);

        // Check child_ids
        assert_eq!(PageID(100), deserialized.child_ids[0]);
        assert_eq!(PageID(200), deserialized.child_ids[1]);
        assert_eq!(PageID(300), deserialized.child_ids[2]);
        assert_eq!(PageID(400), deserialized.child_ids[3]);
    }
}
