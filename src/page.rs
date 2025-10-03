use crate::common::PageID;
use crate::node::Node;
use crc32fast::Hasher;
use crate::dcbapi::{DCBError, DCBResult};

// Page structure
#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: PageID,
    pub node: Node,
}

// Page header format: type(1) + crc(4) + len(4)
pub const PAGE_HEADER_SIZE: usize = 9;

// Implementation for Page
impl Page {
    pub fn new(page_id: PageID, node: Node) -> Self {
        Self { page_id, node }
    }

    pub fn calc_serialized_size(&self) -> usize {
        // The total serialized size is the size of the page header plus the size of the serialized node
        PAGE_HEADER_SIZE + self.node.calc_serialized_size()
    }

    pub fn serialize(&self) -> DCBResult<Vec<u8>> {
        // Serialize the node
        let data = self.node.serialize()?;

        // Calculate CRC
        let crc = calc_crc(&data);

        // Create the serialized data with header
        let mut serialized = Vec::with_capacity(PAGE_HEADER_SIZE + data.len());
        serialized.push(self.node.get_type_byte());
        serialized.extend_from_slice(&crc.to_le_bytes());
        serialized.extend_from_slice(&(data.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&data);

        Ok(serialized)
    }

    pub fn deserialize(page_id: PageID, page_data: &[u8]) -> DCBResult<Self> {
        if page_data.len() < PAGE_HEADER_SIZE {
            return Err(DCBError::DatabaseCorrupted(
                "Page data too short".to_string(),
            ));
        }

        // Extract header information
        let node_type = page_data[0];
        let crc = u32::from_le_bytes([page_data[1], page_data[2], page_data[3], page_data[4]]);
        let data_len =
            u32::from_le_bytes([page_data[5], page_data[6], page_data[7], page_data[8]]) as usize;

        if PAGE_HEADER_SIZE + data_len > page_data.len() {
            return Err(DCBError::DatabaseCorrupted(
                "Page data length mismatch".to_string(),
            ));
        }

        // Extract the data
        let data = &page_data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len];

        // Verify CRC
        let calculated_crc = calc_crc(data);

        if calculated_crc != crc {
            return Err(DCBError::DatabaseCorrupted("CRC mismatch".to_string()));
        }

        // Deserialize the node
        let node = Node::deserialize(node_type, data)?;

        Ok(Self { page_id, node })
    }
}


/// Calculate CRC32 checksum for data
pub fn calc_crc(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Position;
    use crate::common::{PageID, Tsn};
    use crate::header_node::HeaderNode;

    #[test]
    fn test_page_serialization_and_size() {
        // Create a HeaderNode (simplest node type)
        let node = Node::Header(HeaderNode {
            tsn: Tsn(42),
            next_page_id: PageID(123),
            free_lists_tree_root_id: PageID(456),
            event_tree_root_id: PageID(789),
            tags_tree_root_id: PageID(1011),
            next_position: Position(1234),
        });

        // Create a Page with the node
        let page_id = PageID(1);
        let page = Page::new(page_id, node);

        // Calculate the serialized size
        let calculated_size = page.calc_serialized_size();

        // Serialize the page
        let serialized = page.serialize().expect("Failed to serialize page");

        // Check that the serialized data size matches the calculated size
        assert_eq!(
            calculated_size,
            serialized.len(),
            "Calculated size {} should match actual serialized size {}",
            calculated_size,
            serialized.len()
        );

        // Deserialize the serialized data
        let deserialized =
            Page::deserialize(page_id, &serialized).expect("Failed to deserialize page");

        // Check that the deserialized page is the same as the original
        assert_eq!(
            page.page_id, deserialized.page_id,
            "Original page_id {:?} should match deserialized page_id {:?}",
            page.page_id, deserialized.page_id
        );
        assert_eq!(
            page.node, deserialized.node,
            "Original node {:?} should match deserialized node {:?}",
            page.node, deserialized.node
        );
    }
}
