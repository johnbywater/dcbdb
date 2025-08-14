use crate::mvcc_common::LmdbError;
use crate::mvcc_common::Result;
use crate::mvcc_common::PageID;
use crate::mvcc_common::Position;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub event_type: String,
    pub data: Vec<u8>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLeafNode {
    pub keys: Vec<Position>,
    pub values: Vec<EventRecord>,
}

impl EventLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each Position in keys
        total_size += self.keys.len() * 8;

        // For each EventRecord in values:
        for value in &self.values {
            // 2 bytes for event_type length + bytes for the string
            total_size += 2 + value.event_type.len();

            // 2 bytes for data length + bytes for the data
            total_size += 2 + value.data.len();

            // 2 bytes for number of tags
            total_size += 2;

            // For each tag: 2 bytes for length + bytes for the string
            for tag in &value.tags {
                total_size += 2 + tag.len();
            }
        }

        total_size
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let total_size = self.calc_serialized_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.0.to_le_bytes());
        }

        // Serialize each value (EventRecord)
        for value in &self.values {
            // Serialize event_type length (2 bytes)
            result.extend_from_slice(&(value.event_type.len() as u16).to_le_bytes());

            // Serialize event_type bytes
            result.extend_from_slice(value.event_type.as_bytes());

            // Serialize data length (2 bytes)
            result.extend_from_slice(&(value.data.len() as u16).to_le_bytes());

            // Serialize data bytes
            result.extend_from_slice(&value.data);

            // Serialize number of tags (2 bytes)
            result.extend_from_slice(&(value.tags.len() as u16).to_le_bytes());

            // Serialize each tag (2 bytes for length + string bytes)
            for tag in &value.tags {
                // Serialize tag length (2 bytes)
                result.extend_from_slice(&(tag.len() as u16).to_le_bytes());

                // Serialize tag bytes
                result.extend_from_slice(tag.as_bytes());
            }
        }

        Ok(result)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
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
            let position = u64::from_le_bytes([
                slice[start],
                slice[start + 1],
                slice[start + 2],
                slice[start + 3],
                slice[start + 4],
                slice[start + 5],
                slice[start + 6],
                slice[start + 7],
            ]);
            keys.push(Position(position));
        }

        // Extract the values (EventRecord)
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * 8);

        for _ in 0..keys_len {
            // Check if there's enough data for event_type length (2 bytes)
            if offset + 2 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading event_type length".to_string(),
                ));
            }

            // Extract event_type length (2 bytes)
            let event_type_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
            offset += 2;

            // Check if there's enough data for event_type
            if offset + event_type_len > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading event_type".to_string(),
                ));
            }

            // Extract event_type as a string
            let event_type = match std::str::from_utf8(&slice[offset..offset + event_type_len]) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    return Err(LmdbError::DeserializationError(
                        "Invalid UTF-8 sequence in event_type".to_string(),
                    ));
                }
            };
            offset += event_type_len;

            // Check if there's enough data for data length (2 bytes)
            if offset + 2 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading data length".to_string(),
                ));
            }

            // Extract data length (2 bytes)
            let data_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
            offset += 2;

            // Check if there's enough data for data
            if offset + data_len > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading data".to_string(),
                ));
            }

            // Extract data
            let data = slice[offset..offset + data_len].to_vec();
            offset += data_len;

            // Check if there's enough data for number of tags (2 bytes)
            if offset + 2 > slice.len() {
                return Err(LmdbError::DeserializationError(
                    "Unexpected end of data while reading number of tags".to_string(),
                ));
            }

            // Extract number of tags (2 bytes)
            let num_tags = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
            offset += 2;

            // Extract each tag
            let mut tags = Vec::with_capacity(num_tags);
            for _ in 0..num_tags {
                // Check if there's enough data for tag length (2 bytes)
                if offset + 2 > slice.len() {
                    return Err(LmdbError::DeserializationError(
                        "Unexpected end of data while reading tag length".to_string(),
                    ));
                }

                // Extract tag length (2 bytes)
                let tag_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
                offset += 2;

                // Check if there's enough data for tag
                if offset + tag_len > slice.len() {
                    return Err(LmdbError::DeserializationError(
                        "Unexpected end of data while reading tag".to_string(),
                    ));
                }

                // Extract tag as a string
                let tag = match std::str::from_utf8(&slice[offset..offset + tag_len]) {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        return Err(LmdbError::DeserializationError(
                            "Invalid UTF-8 sequence in tag".to_string(),
                        ));
                    }
                };
                offset += tag_len;

                tags.push(tag);
            }

            values.push(EventRecord {
                event_type,
                data,
                tags,
            });
        }

        Ok(EventLeafNode { keys, values })
    }

    pub fn pop_last_key_and_value(&mut self) -> Result<(Position, EventRecord)> {
        let last_key = self.keys.pop().unwrap();
        let last_value = self.values.pop().unwrap();
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

impl EventInternalNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each Position in keys
        total_size += self.keys.len() * 8;

        // 8 bytes for each PageID in child_ids
        total_size += self.child_ids.len() * 8;

        total_size
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let total_size = self.calc_serialized_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.0.to_le_bytes());
        }

        // Serialize each child_id (4 bytes each)
        // Note: We don't need to serialize child_ids_len as it's always keys_len + 1
        for child_id in &self.child_ids {
            result.extend_from_slice(&child_id.0.to_le_bytes());
        }

        Ok(result)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
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
            let position = u64::from_le_bytes([
                slice[start],
                slice[start + 1],
                slice[start + 2],
                slice[start + 3],
                slice[start + 4],
                slice[start + 5],
                slice[start + 6],
                slice[start + 7],
            ]);
            keys.push(Position(position));
        }

        // Calculate the offset after reading keys
        let offset = 2 + (keys_len * 8);

        // Derive child_ids_len from keys_len (always keys_len + 1)
        let child_ids_len = keys_len + 1;

        // Calculate the minimum expected size for the child_ids
        let min_expected_size = offset + (child_ids_len * 8);
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
            let start = offset + (i * 8);
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

        Ok(EventInternalNode { keys, child_ids })
    }
    pub fn replace_last_child_id(
        &mut self,
        old_id: PageID,
        new_id: PageID,
    ) -> Result<()> {
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
        promoted_key: Position,
        promoted_page_id: PageID,
    ) -> Result<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }
    pub fn split_off(&mut self) -> Result<(Position, Vec<Position>, Vec<PageID>)> {
        let middle_idx = self.keys.len() - 2;
        let promoted_key = self.keys.remove(middle_idx);
        let new_keys = self.keys.split_off(middle_idx);
        let new_child_ids = self.child_ids.split_off(middle_idx + 1);
        Ok((promoted_key, new_keys, new_child_ids))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_internal_serialize() {
        // Create an EventInternalNode with known values
        let internal_node = EventInternalNode {
            keys: vec![Position(1000), Position(2000), Position(3000)],
            child_ids: vec![PageID(100), PageID(200), PageID(300), PageID(400)],
        };

        // Serialize the EventInternalNode
        let serialized = internal_node.serialize().unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Deserialize back to an EventInternalNode
        let deserialized = EventInternalNode::from_slice(&serialized)
            .expect("Failed to deserialize EventInternalNode");

        // Verify that the deserialized node matches the original
        assert_eq!(internal_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(4, deserialized.child_ids.len());

        // Check keys
        assert_eq!(Position(1000), deserialized.keys[0]);
        assert_eq!(Position(2000), deserialized.keys[1]);
        assert_eq!(Position(3000), deserialized.keys[2]);

        // Check child_ids
        assert_eq!(PageID(100), deserialized.child_ids[0]);
        assert_eq!(PageID(200), deserialized.child_ids[1]);
        assert_eq!(PageID(300), deserialized.child_ids[2]);
        assert_eq!(PageID(400), deserialized.child_ids[3]);
    }

    #[test]
    fn test_event_leaf_serialize() {
        // Create an EventLeafNode with known values
        let leaf_node = EventLeafNode {
            keys: vec![Position(1000), Position(2000), Position(3000)],
            values: vec![
                EventRecord {
                    event_type: "event_type_1".to_string(),
                    data: vec![1, 0, 0, 0], // 100 as little-endian bytes
                    tags: vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
                },
                EventRecord {
                    event_type: "event_type_2".to_string(),
                    data: vec![2, 0, 0, 0], // 200 as little-endian bytes
                    tags: vec![
                        "tag4".to_string(),
                        "tag5".to_string(),
                        "tag6".to_string(),
                        "tag7".to_string(),
                    ],
                },
                EventRecord {
                    event_type: "event_type_3".to_string(),
                    data: vec![3, 0, 0, 0], // 300 as little-endian bytes
                    tags: vec!["tag8".to_string(), "tag9".to_string()],
                },
            ],
        };

        // Serialize the EventLeafNode
        let serialized = leaf_node.serialize().unwrap();

        // Verify the serialized output is not empty
        assert!(!serialized.is_empty());

        // Deserialize back to an EventLeafNode
        let deserialized =
            EventLeafNode::from_slice(&serialized).expect("Failed to deserialize EventLeafNode");

        // Verify that the deserialized node matches the original
        assert_eq!(leaf_node, deserialized);

        // Verify specific properties
        assert_eq!(3, deserialized.keys.len());
        assert_eq!(3, deserialized.values.len());

        // Check keys
        assert_eq!(Position(1000), deserialized.keys[0]);
        assert_eq!(Position(2000), deserialized.keys[1]);
        assert_eq!(Position(3000), deserialized.keys[2]);

        // Check first value
        assert_eq!("event_type_1", deserialized.values[0].event_type);
        assert_eq!(vec![1, 0, 0, 0], deserialized.values[0].data);
        assert_eq!(
            vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
            deserialized.values[0].tags
        );

        // Check second value
        assert_eq!("event_type_2", deserialized.values[1].event_type);
        assert_eq!(vec![2, 0, 0, 0], deserialized.values[1].data);
        assert_eq!(
            vec![
                "tag4".to_string(),
                "tag5".to_string(),
                "tag6".to_string(),
                "tag7".to_string()
            ],
            deserialized.values[1].tags
        );

        // Check third value
        assert_eq!("event_type_3", deserialized.values[2].event_type);
        assert_eq!(vec![3, 0, 0, 0], deserialized.values[2].data);
        assert_eq!(
            vec!["tag8".to_string(), "tag9".to_string()],
            deserialized.values[2].tags
        );
    }
}
