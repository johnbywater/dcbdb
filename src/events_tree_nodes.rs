use crate::common::PageID;
use crate::common::Position;
use crate::dcbapi::DCBError;
use crate::dcbapi::DCBResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventRecord {
    pub event_type: String,
    pub data: Vec<u8>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventValue {
    Inline(EventRecord),
    // For large data stored across overflow pages
    Overflow {
        event_type: String,
        data_len: u64,
        tags: Vec<String>,
        root_id: PageID,
    },
}

impl PartialEq<EventValue> for EventRecord {
    fn eq(&self, other: &EventValue) -> bool {
        match other {
            EventValue::Inline(rec) => self == rec,
            EventValue::Overflow {
                event_type,
                data_len,
                tags,
                ..
            } => {
                &self.event_type == event_type
                    && &self.tags == tags
                    && (self.data.len() as u64) == *data_len
            }
        }
    }
}

impl PartialEq<EventRecord> for EventValue {
    fn eq(&self, other: &EventRecord) -> bool {
        other == self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLeafNode {
    pub keys: Vec<Position>,
    pub values: Vec<EventValue>,
}

impl EventLeafNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 2 bytes for keys_len
        let mut total_size = 2;

        // 8 bytes for each Position in keys
        total_size += self.keys.len() * 8;

        // For each value
        for value in &self.values {
            // 1 byte for discriminator
            total_size += 1;
            match value {
                EventValue::Inline(rec) => {
                    // 2 bytes for event_type length + bytes for the string
                    total_size += 2 + rec.event_type.len();
                    // 2 bytes for data length + bytes for the data
                    total_size += 2 + rec.data.len();
                    // 2 bytes for number of tags
                    total_size += 2;
                    // For each tag: 2 bytes for length + bytes for the string
                    for tag in &rec.tags {
                        total_size += 2 + tag.len();
                    }
                }
                EventValue::Overflow {
                    event_type,
                    data_len: _,
                    tags,
                    root_id: _,
                } => {
                    // 2 bytes for event_type length + bytes for the string
                    total_size += 2 + event_type.len();
                    // 8 bytes for data_len (u64)
                    total_size += 8;
                    // 2 bytes for number of tags
                    total_size += 2;
                    // For each tag: 2 bytes for length + bytes for the string
                    for tag in tags {
                        total_size += 2 + tag.len();
                    }
                    // 8 bytes for root_id
                    total_size += 8;
                }
            }
        }

        total_size
    }

    pub fn serialize(&self) -> DCBResult<Vec<u8>> {
        let total_size = self.calc_serialized_size();
        let mut result = Vec::with_capacity(total_size);

        // Serialize the length of the keys (2 bytes)
        result.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        // Serialize each key (8 bytes each)
        for key in &self.keys {
            result.extend_from_slice(&key.0.to_le_bytes());
        }

        // Serialize each value
        for value in &self.values {
            match value {
                EventValue::Inline(rec) => {
                    // kind = 0
                    result.push(0u8);
                    // Serialize event_type length (2 bytes)
                    result.extend_from_slice(&(rec.event_type.len() as u16).to_le_bytes());
                    // Serialize event_type bytes
                    result.extend_from_slice(rec.event_type.as_bytes());
                    // Serialize data length (2 bytes)
                    result.extend_from_slice(&(rec.data.len() as u16).to_le_bytes());
                    // Serialize data bytes
                    result.extend_from_slice(&rec.data);
                    // Serialize number of tags (2 bytes)
                    result.extend_from_slice(&(rec.tags.len() as u16).to_le_bytes());
                    // Serialize each tag (2 bytes for length + string bytes)
                    for tag in &rec.tags {
                        // Serialize tag length (2 bytes)
                        result.extend_from_slice(&(tag.len() as u16).to_le_bytes());
                        // Serialize tag bytes
                        result.extend_from_slice(tag.as_bytes());
                    }
                }
                EventValue::Overflow {
                    event_type,
                    data_len,
                    tags,
                    root_id,
                } => {
                    // kind = 1
                    result.push(1u8);
                    // Serialize event_type length (2 bytes)
                    result.extend_from_slice(&(event_type.len() as u16).to_le_bytes());
                    // Serialize event_type bytes
                    result.extend_from_slice(event_type.as_bytes());
                    // Serialize data_len (8 bytes)
                    result.extend_from_slice(&(*data_len).to_le_bytes());
                    // Serialize number of tags (2 bytes)
                    result.extend_from_slice(&(tags.len() as u16).to_le_bytes());
                    // Serialize each tag (2 bytes for length + string bytes)
                    for tag in tags {
                        result.extend_from_slice(&(tag.len() as u16).to_le_bytes());
                        result.extend_from_slice(tag.as_bytes());
                    }
                    // Serialize root_id (8 bytes)
                    result.extend_from_slice(&root_id.0.to_le_bytes());
                }
            }
        }

        Ok(result)
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(DCBError::DeserializationError(format!(
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

        // Extract the values (EventValue)
        let mut values = Vec::with_capacity(keys_len);
        let mut offset = 2 + (keys_len * 8);

        for _ in 0..keys_len {
            // Read discriminator (1 byte)
            if offset + 1 > slice.len() {
                return Err(DCBError::DeserializationError(
                    "Unexpected end of data while reading value kind".to_string(),
                ));
            }
            let kind = slice[offset];
            offset += 1;

            // Extract event_type length (2 bytes)
            if offset + 2 > slice.len() {
                return Err(DCBError::DeserializationError(
                    "Unexpected end of data while reading event_type length".to_string(),
                ));
            }
            let event_type_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
            offset += 2;
            if offset + event_type_len > slice.len() {
                return Err(DCBError::DeserializationError(
                    "Unexpected end of data while reading event_type".to_string(),
                ));
            }
            let event_type = match std::str::from_utf8(&slice[offset..offset + event_type_len]) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    return Err(DCBError::DeserializationError(
                        "Invalid UTF-8 sequence in event_type".to_string(),
                    ));
                }
            };
            offset += event_type_len;

            match kind {
                0 => {
                    // Inline: data_len u16 + data bytes
                    if offset + 2 > slice.len() {
                        return Err(DCBError::DeserializationError(
                            "Unexpected end of data while reading data length".to_string(),
                        ));
                    }
                    let data_len = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
                    offset += 2;
                    if offset + data_len > slice.len() {
                        return Err(DCBError::DeserializationError(
                            "Unexpected end of data while reading data".to_string(),
                        ));
                    }
                    let data = slice[offset..offset + data_len].to_vec();
                    offset += data_len;

                    // num tags
                    if offset + 2 > slice.len() {
                        return Err(DCBError::DeserializationError(
                            "Unexpected end of data while reading number of tags".to_string(),
                        ));
                    }
                    let num_tags = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
                    offset += 2;
                    let mut tags = Vec::with_capacity(num_tags);
                    for _ in 0..num_tags {
                        if offset + 2 > slice.len() {
                            return Err(DCBError::DeserializationError(
                                "Unexpected end of data while reading tag length".to_string(),
                            ));
                        }
                        let tag_len =
                            u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
                        offset += 2;
                        if offset + tag_len > slice.len() {
                            return Err(DCBError::DeserializationError(
                                "Unexpected end of data while reading tag".to_string(),
                            ));
                        }
                        let tag = match std::str::from_utf8(&slice[offset..offset + tag_len]) {
                            Ok(s) => s.to_string(),
                            Err(_) => {
                                return Err(DCBError::DeserializationError(
                                    "Invalid UTF-8 sequence in tag".to_string(),
                                ));
                            }
                        };
                        offset += tag_len;
                        tags.push(tag);
                    }

                    values.push(EventValue::Inline(EventRecord {
                        event_type,
                        data,
                        tags,
                    }));
                }
                1 => {
                    // Overflow: data_len u64 + tags + root_id
                    if offset + 8 > slice.len() {
                        return Err(DCBError::DeserializationError(
                            "Unexpected end of data while reading overflow data_len".to_string(),
                        ));
                    }
                    let data_len = u64::from_le_bytes([
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

                    if offset + 2 > slice.len() {
                        return Err(DCBError::DeserializationError(
                            "Unexpected end of data while reading number of tags".to_string(),
                        ));
                    }
                    let num_tags = u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
                    offset += 2;
                    let mut tags = Vec::with_capacity(num_tags);
                    for _ in 0..num_tags {
                        if offset + 2 > slice.len() {
                            return Err(DCBError::DeserializationError(
                                "Unexpected end of data while reading tag length".to_string(),
                            ));
                        }
                        let tag_len =
                            u16::from_le_bytes([slice[offset], slice[offset + 1]]) as usize;
                        offset += 2;
                        if offset + tag_len > slice.len() {
                            return Err(DCBError::DeserializationError(
                                "Unexpected end of data while reading tag".to_string(),
                            ));
                        }
                        let tag = match std::str::from_utf8(&slice[offset..offset + tag_len]) {
                            Ok(s) => s.to_string(),
                            Err(_) => {
                                return Err(DCBError::DeserializationError(
                                    "Invalid UTF-8 sequence in tag".to_string(),
                                ));
                            }
                        };
                        offset += tag_len;
                        tags.push(tag);
                    }
                    if offset + 8 > slice.len() {
                        return Err(DCBError::DeserializationError(
                            "Unexpected end of data while reading overflow root_id".to_string(),
                        ));
                    }
                    let root_id = PageID(u64::from_le_bytes([
                        slice[offset],
                        slice[offset + 1],
                        slice[offset + 2],
                        slice[offset + 3],
                        slice[offset + 4],
                        slice[offset + 5],
                        slice[offset + 6],
                        slice[offset + 7],
                    ]));
                    offset += 8;

                    values.push(EventValue::Overflow {
                        event_type,
                        data_len,
                        tags,
                        root_id,
                    });
                }
                _ => {
                    return Err(DCBError::DeserializationError(
                        "Invalid event value kind".to_string(),
                    ));
                }
            }
        }

        Ok(EventLeafNode { keys, values })
    }

    pub fn pop_last_key_and_value(&mut self) -> DCBResult<(Position, EventValue)> {
        let last_key = self.keys.pop().expect("EventLeafNode should have some keys");
        let last_value = self.values.pop().expect("EventLeafNode should have some values");
        Ok((last_key, last_value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOverflowNode {
    pub next: PageID, // PageID(0) indicates end of chain
    pub data: Vec<u8>,
}

impl EventOverflowNode {
    pub fn calc_serialized_size(&self) -> usize {
        // 8 bytes for next + data bytes
        8 + self.data.len()
    }

    pub fn serialize(&self) -> DCBResult<Vec<u8>> {
        let mut result = Vec::with_capacity(self.calc_serialized_size());
        result.extend_from_slice(&self.next.0.to_le_bytes());
        result.extend_from_slice(&self.data);
        Ok(result)
    }

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() < 8 {
            return Err(DCBError::DeserializationError(
                "Overflow node too small".to_string(),
            ));
        }
        let next = PageID(u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ]));
        let data = slice[8..].to_vec();
        Ok(Self { next, data })
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

    pub fn serialize(&self) -> DCBResult<Vec<u8>> {
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

    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        // Check if the slice has at least 2 bytes for keys_len
        if slice.len() < 2 {
            return Err(DCBError::DeserializationError(format!(
                "Expected at least 2 bytes, got {}",
                slice.len()
            )));
        }

        // Extract the length of the keys (first 2 bytes)
        let keys_len = u16::from_le_bytes([slice[0], slice[1]]) as usize;

        // Calculate the minimum expected size for the keys
        let min_expected_size = 2 + (keys_len * 8);
        if slice.len() < min_expected_size {
            return Err(DCBError::DeserializationError(format!(
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
            return Err(DCBError::DeserializationError(format!(
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
    pub fn replace_last_child_id(&mut self, old_id: PageID, new_id: PageID) -> DCBResult<()> {
        // Replace the last child ID.
        let last_idx = self.child_ids.len() - 1;
        if self.child_ids[last_idx] == old_id {
            self.child_ids[last_idx] = new_id;
        } else {
            return Err(DCBError::DatabaseCorrupted("Child ID mismatch".to_string()));
        }
        Ok(())
    }
    pub fn append_promoted_key_and_page_id(
        &mut self,
        promoted_key: Position,
        promoted_page_id: PageID,
    ) -> DCBResult<()> {
        self.keys.push(promoted_key);
        self.child_ids.push(promoted_page_id);
        Ok(())
    }
    pub fn split_off(&mut self) -> DCBResult<(Position, Vec<Position>, Vec<PageID>)> {
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
                EventValue::Inline(EventRecord {
                    event_type: "event_type_1".to_string(),
                    data: vec![1, 0, 0, 0], // 100 as little-endian bytes
                    tags: vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
                }),
                EventValue::Inline(EventRecord {
                    event_type: "event_type_2".to_string(),
                    data: vec![2, 0, 0, 0], // 200 as little-endian bytes
                    tags: vec![
                        "tag4".to_string(),
                        "tag5".to_string(),
                        "tag6".to_string(),
                        "tag7".to_string(),
                    ],
                }),
                EventValue::Inline(EventRecord {
                    event_type: "event_type_3".to_string(),
                    data: vec![3, 0, 0, 0], // 300 as little-endian bytes
                    tags: vec!["tag8".to_string(), "tag9".to_string()],
                }),
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
        match &deserialized.values[0] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_1", v.event_type);
                assert_eq!(vec![1, 0, 0, 0], v.data);
                assert_eq!(
                    vec!["tag1".to_string(), "tag2".to_string(), "tag3".to_string()],
                    v.tags
                );
            }
            _ => panic!("Expected Inline for first value"),
        }

        // Check second value
        match &deserialized.values[1] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_2", v.event_type);
                assert_eq!(vec![2, 0, 0, 0], v.data);
                assert_eq!(
                    vec![
                        "tag4".to_string(),
                        "tag5".to_string(),
                        "tag6".to_string(),
                        "tag7".to_string()
                    ],
                    v.tags
                );
            }
            _ => panic!("Expected Inline for second value"),
        }

        // Check third value
        match &deserialized.values[2] {
            EventValue::Inline(v) => {
                assert_eq!("event_type_3", v.event_type);
                assert_eq!(vec![3, 0, 0, 0], v.data);
                assert_eq!(vec!["tag8".to_string(), "tag9".to_string()], v.tags);
            }
            _ => panic!("Expected Inline for third value"),
        }
    }

    #[test]
    fn test_event_leaf_serialize_with_overflow_single() {
        let leaf_node = EventLeafNode {
            keys: vec![Position(111)],
            values: vec![EventValue::Overflow {
                event_type: "over_evt".to_string(),
                data_len: 1234567,
                tags: vec!["a".to_string(), "b".to_string()],
                root_id: PageID(123),
            }],
        };
        // Serialize
        let serialized = leaf_node.serialize().unwrap();
        assert!(!serialized.is_empty());
        // Deserialize
        let deserialized = EventLeafNode::from_slice(&serialized)
            .expect("Failed to deserialize EventLeafNode with overflow");
        assert_eq!(leaf_node, deserialized);

        // Check specific fields
        assert_eq!(1, deserialized.keys.len());
        assert_eq!(Position(111), deserialized.keys[0]);
        assert_eq!(1, deserialized.values.len());
        match &deserialized.values[0] {
            EventValue::Overflow {
                event_type,
                data_len,
                tags,
                root_id,
            } => {
                assert_eq!("over_evt", event_type);
                assert_eq!(1234567, *data_len);
                assert_eq!(vec!["a".to_string(), "b".to_string()], *tags);
                assert_eq!(PageID(123), *root_id);
            }
            _ => panic!("Expected Overflow variant"),
        }
    }

    #[test]
    fn test_event_leaf_serialize_mixed_inline_and_overflow() {
        let inline = EventValue::Inline(EventRecord {
            event_type: "inline_evt".to_string(),
            data: vec![1, 2, 3],
            tags: vec!["x".to_string()],
        });
        let overflow = EventValue::Overflow {
            event_type: "overflow_evt".to_string(),
            data_len: 9999,
            tags: vec!["y".to_string(), "z".to_string()],
            root_id: PageID(999),
        };
        let leaf_node = EventLeafNode {
            keys: vec![Position(10), Position(20)],
            values: vec![inline.clone(), overflow.clone()],
        };
        let serialized = leaf_node.serialize().unwrap();
        let deserialized = EventLeafNode::from_slice(&serialized).unwrap();
        assert_eq!(leaf_node, deserialized);

        // Validate order and variants
        match &deserialized.values[0] {
            EventValue::Inline(rec) => {
                assert_eq!("inline_evt", rec.event_type);
                assert_eq!(vec![1, 2, 3], rec.data);
                assert_eq!(vec!["x".to_string()], rec.tags);
            }
            _ => panic!("Expected Inline at index 0"),
        }
        match &deserialized.values[1] {
            EventValue::Overflow {
                event_type,
                data_len,
                tags,
                root_id,
            } => {
                assert_eq!("overflow_evt", event_type);
                assert_eq!(9999, *data_len);
                assert_eq!(vec!["y".to_string(), "z".to_string()], *tags);
                assert_eq!(PageID(999), *root_id);
            }
            _ => panic!("Expected Overflow at index 1"),
        }
    }

    #[test]
    fn test_event_overflow_node_serialize_roundtrip() {
        let node = EventOverflowNode {
            next: PageID(77),
            data: vec![7, 8, 9, 10],
        };
        let ser = node.serialize().unwrap();
        assert_eq!(8 + 4, ser.len()); // 8 bytes next + 4 bytes data
        let de = EventOverflowNode::from_slice(&ser).unwrap();
        assert_eq!(node, de);
        assert_eq!(PageID(77), de.next);
        assert_eq!(vec![7, 8, 9, 10], de.data);
    }
}
