use crate::common::Position;
use crate::common::{PageID, Tsn};
use crate::dcb::{DCBError, DCBResult};

// Node type definitions
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderNode {
    pub tsn: Tsn,
    pub next_page_id: PageID,
    pub free_lists_tree_root_id: PageID,
    pub events_tree_root_id: PageID,
    pub tags_tree_root_id: PageID,
    pub next_position: Position,
}

impl HeaderNode {
    /// Writes the serialized HeaderNode into the provided buffer and returns the number of bytes written (48).
    /// The buffer must be at least 48 bytes long.
    pub fn serialize_into(&self, buf: &mut [u8]) -> usize {
        assert!(buf.len() >= 48, "HeaderNode::serialize_into dst must be at least 48 bytes");
        // Write fields in little-endian order
        buf[0..8].copy_from_slice(&self.tsn.0.to_le_bytes());
        buf[8..16].copy_from_slice(&self.next_page_id.0.to_le_bytes());
        buf[16..24].copy_from_slice(&self.free_lists_tree_root_id.0.to_le_bytes());
        buf[24..32].copy_from_slice(&self.events_tree_root_id.0.to_le_bytes());
        buf[32..40].copy_from_slice(&self.tags_tree_root_id.0.to_le_bytes());
        buf[40..48].copy_from_slice(&self.next_position.0.to_le_bytes());
        48
    }

    /// Serializes the HeaderNode to a newly allocated Vec<u8> of 48 bytes.
    /// This includes allocation cost; use `serialize_into` to measure encode-only cost.
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(48);
        // Safety: we will immediately fill all 48 bytes below.
        // Using extend_from_slice repeatedly will also grow length accordingly.
        result.extend_from_slice(&self.tsn.0.to_le_bytes());
        result.extend_from_slice(&self.next_page_id.0.to_le_bytes());
        result.extend_from_slice(&self.free_lists_tree_root_id.0.to_le_bytes());
        result.extend_from_slice(&self.events_tree_root_id.0.to_le_bytes());
        result.extend_from_slice(&self.tags_tree_root_id.0.to_le_bytes());
        result.extend_from_slice(&self.next_position.0.to_le_bytes());
        result
    }

    /// Creates a HeaderNode from a byte slice
    /// Expects a slice with 48 bytes:
    /// - 8 bytes for tsn
    /// - 8 bytes for next_page_id
    /// - 8 bytes for free_lists_tree_root_id
    /// - 8 bytes for events_tree_root_id
    /// - 8 bytes for tags_tree_root_id
    /// - 8 bytes for next_position
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<Self>` - The deserialized HeaderNode or an error
    pub fn from_slice(slice: &[u8]) -> DCBResult<Self> {
        if slice.len() != 48 {
            return Err(DCBError::DeserializationError(format!(
                "Expected 48 bytes, got {}",
                slice.len()
            )));
        }

        let tsn = u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
        ]);
        let next_page_id = u64::from_le_bytes([
            slice[8], slice[9], slice[10], slice[11], slice[12], slice[13], slice[14], slice[15],
        ]);
        let freetree_root_id = u64::from_le_bytes([
            slice[16], slice[17], slice[18], slice[19], slice[20], slice[21], slice[22], slice[23],
        ]);
        let position_root_id = u64::from_le_bytes([
            slice[24], slice[25], slice[26], slice[27], slice[28], slice[29], slice[30], slice[31],
        ]);
        let tags_root_id = u64::from_le_bytes([
            slice[32], slice[33], slice[34], slice[35], slice[36], slice[37], slice[38], slice[39],
        ]);
        let next_position = u64::from_le_bytes([
            slice[40], slice[41], slice[42], slice[43], slice[44], slice[45], slice[46], slice[47],
        ]);

        Ok(HeaderNode {
            tsn: Tsn(tsn),
            next_page_id: PageID(next_page_id),
            free_lists_tree_root_id: PageID(freetree_root_id),
            events_tree_root_id: PageID(position_root_id),
            tags_tree_root_id: PageID(tags_root_id),
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
            free_lists_tree_root_id: PageID(456),
            events_tree_root_id: PageID(789),
            tags_tree_root_id: PageID(321),
            next_position: Position(9876543210),
        };

        // Serialize the HeaderNode
        let serialized = header_node.serialize();

        // Verify the serialized output has the correct length
        assert_eq!(48, serialized.len());

        // Verify the serialized output has the correct byte values
        // TSN(42) = 42u64 = [42, 0, 0, 0, 0, 0, 0, 0] in little-endian
        assert_eq!(&42u64.to_le_bytes(), &serialized[0..8]);

        // PageID(123) as u64
        assert_eq!(&123u64.to_le_bytes(), &serialized[8..16]);

        // PageID(456) as u64
        assert_eq!(&456u64.to_le_bytes(), &serialized[16..24]);

        // PageID(789) as u64
        assert_eq!(&789u64.to_le_bytes(), &serialized[24..32]);

        // root_tags_tree_id PageID(321) as u64
        assert_eq!(&321u64.to_le_bytes(), &serialized[32..40]);

        // next_position 9876543210u64 => little-endian bytes
        assert_eq!(&9876543210u64.to_le_bytes(), &serialized[40..48]);

        // Deserialize back to a HeaderNode
        let deserialized =
            HeaderNode::from_slice(&serialized).expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized node matches the original
        assert_eq!(header_node.tsn, deserialized.tsn);
        assert_eq!(header_node.next_page_id, deserialized.next_page_id);
        assert_eq!(
            header_node.free_lists_tree_root_id,
            deserialized.free_lists_tree_root_id
        );
        assert_eq!(
            header_node.events_tree_root_id,
            deserialized.events_tree_root_id
        );
        assert_eq!(header_node.next_position, deserialized.next_position);
    }
}
