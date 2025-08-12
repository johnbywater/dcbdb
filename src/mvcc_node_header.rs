use crate::mvcc_common;
use crate::mvcc_common::{LmdbError, PageID, TSN};

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
    pub fn from_slice(slice: &[u8]) -> mvcc_common::Result<Self> {
        if slice.len() != 16 {
            return Err(LmdbError::DeserializationError(format!(
                "Expected 16 bytes, got {}",
                slice.len()
            )));
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
        let deserialized =
            HeaderNode::from_slice(&serialized).expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized node matches the original
        assert_eq!(header_node.tsn, deserialized.tsn);
        assert_eq!(header_node.next_page_id, deserialized.next_page_id);
        assert_eq!(header_node.freetree_root_id, deserialized.freetree_root_id);
        assert_eq!(header_node.position_root_id, deserialized.position_root_id);
    }
}