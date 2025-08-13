use crate::mvcc_common;
use crate::mvcc_common::{LmdbError, PageID, Tsn};
use crate::mvcc_node_event::Position;

// Node type definitions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderNode {
    pub tsn: Tsn,
    pub next_page_id: PageID,
    pub free_page_tree_root_id: PageID,
    pub event_tree_root_id: PageID,
    pub next_position: Position,
}

impl HeaderNode {
    /// Serializes the HeaderNode to a byte array with 28 bytes
    /// 8 bytes for tsn, 4 bytes for next_page_id, 4 bytes for freetree_root_id, 4 bytes for position_root_id, and 8 bytes for next_position
    ///
    /// # Returns
    /// * `Vec<u8>` - The serialized data
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(28);
        result.extend_from_slice(&self.tsn.0.to_le_bytes());
        result.extend_from_slice(&self.next_page_id.0.to_le_bytes());
        result.extend_from_slice(&self.free_page_tree_root_id.0.to_le_bytes());
        result.extend_from_slice(&self.event_tree_root_id.0.to_le_bytes());
        result.extend_from_slice(&self.next_position.0.to_le_bytes());
        result
    }

    /// Creates a HeaderNode from a byte slice
    /// Expects a slice with 28 bytes:
    /// - 8 bytes for tsn
    /// - 4 bytes for next_page_id
    /// - 4 bytes for freetree_root_id
    /// - 4 bytes for position_root_id
    /// - 8 bytes for next_position
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized HeaderNode or an error
    pub fn from_slice(slice: &[u8]) -> mvcc_common::Result<Self> {
        if slice.len() != 28 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected 28 bytes, got {}",
                slice.len()
            )));
        }

        let tsn = u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ]);
        let next_page_id = u32::from_le_bytes([slice[8], slice[9], slice[10], slice[11]]);
        let freetree_root_id = u32::from_le_bytes([slice[12], slice[13], slice[14], slice[15]]);
        let position_root_id = u32::from_le_bytes([slice[16], slice[17], slice[18], slice[19]]);
        let next_position = u64::from_le_bytes([
            slice[20], slice[21], slice[22], slice[23], slice[24], slice[25], slice[26], slice[27],
        ]);

        Ok(HeaderNode {
            tsn: Tsn(tsn),
            next_page_id: PageID(next_page_id),
            free_page_tree_root_id: PageID(freetree_root_id),
            event_tree_root_id: PageID(position_root_id),
            next_position: Position(next_position),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_header_serialize() {
        // Create a HeaderNode with known values
        let header_node = HeaderNode {
            tsn: Tsn(42),
            next_page_id: PageID(123),
            free_page_tree_root_id: PageID(456),
            event_tree_root_id: PageID(789),
            next_position: Position(9876543210),
        };

        // Serialize the HeaderNode
        let serialized = header_node.serialize();

        // Verify the serialized output has the correct length
        assert_eq!(28, serialized.len());

        // Verify the serialized output has the correct byte values
        // TSN(42) = 42u64 = [42, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&42u64.to_le_bytes(), &serialized[0..8]);

        // PageID(123) = 123u32 = [123, 0, 0, 0] in little-endian
        assert_eq!(&[123, 0, 0, 0], &serialized[8..12]);

        // PageID(456) = 456u32 = [200, 1, 0, 0] in little-endian (456 = 200 + 1*256)
        assert_eq!(&[200, 1, 0, 0], &serialized[12..16]);

        // PageID(789) = 789u32 = [21, 3, 0, 0] in little-endian (789 = 21 + 3*256)
        assert_eq!(&[21, 3, 0, 0], &serialized[16..20]);

        // next_position 9876543210u64 = 0x000000024CB016EA => little-endian bytes
        assert_eq!(&9876543210u64.to_le_bytes(), &serialized[20..28]);

        // Deserialize back to a HeaderNode
        let deserialized =
            HeaderNode::from_slice(&serialized).expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized node matches the original
        assert_eq!(header_node.tsn, deserialized.tsn);
        assert_eq!(header_node.next_page_id, deserialized.next_page_id);
        assert_eq!(
            header_node.free_page_tree_root_id,
            deserialized.free_page_tree_root_id
        );
        assert_eq!(
            header_node.event_tree_root_id,
            deserialized.event_tree_root_id
        );
        assert_eq!(header_node.next_position, deserialized.next_position);
    }
}
