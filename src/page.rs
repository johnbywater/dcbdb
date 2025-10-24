use crate::common::PageID;
use crate::dcb::{DCBError, DCBResult};
use crate::node::Node;

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

    #[inline]
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

    /// No-allocation (reusable Vec) serializer. Writes a full page (header + body + zero padding)
    /// into `out`. The vector is cleared and zero-filled to its capacity (expected to be DB page size)
    /// before serialization, avoiding per-call resizing based on body length.
    pub fn serialize_into_vec(&self, out: &mut Vec<u8>) -> DCBResult<()> {
        // We assume `out` was created with capacity equal to the DB page size and reused across calls.

        // Serialize body into the front of the body region using the space after header
        let body_len = {
            let body_slice = &mut out[PAGE_HEADER_SIZE..];
            self.node.serialize_into(body_slice)?
        };

        // Zero-fill the remainder of the page after the serialized body
        let tail_start = PAGE_HEADER_SIZE + body_len;
        if tail_start < out.len() {
            out[tail_start..].fill(0);
        }

        // Compute CRC over the actual body bytes
        let crc = calc_crc(&out[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + body_len]);

        // Fill page header
        out[0] = self.node.get_type_byte();
        out[1..5].copy_from_slice(&crc.to_le_bytes());
        out[5..9].copy_from_slice(&(body_len as u32).to_le_bytes());

        // Keep `out.len()` at page size; the pager will write the full page without extra padding.
        Ok(())
    }

    #[inline]
    pub fn deserialize(page_id: PageID, page_data: &[u8]) -> DCBResult<Self> {
        if page_data.len() < PAGE_HEADER_SIZE {
            return Err(DCBError::DatabaseCorrupted(
                "Page data too short".to_string(),
            ));
        }

        // Extract header information with minimal bounds checks
        let header = &page_data[..PAGE_HEADER_SIZE];
        let node_type = header[0];
        let crc = u32::from_le_bytes(header[1..5].try_into().unwrap());
        let data_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;

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
            return Err(DCBError::DatabaseCorrupted(format!("CRC mismatch (page ID: {page_id:?})")));
        }

        // Deserialize the node
        let node = Node::deserialize(node_type, data)?;

        Ok(Self { page_id, node })
    }
}

/// Calculate CRC32 checksum for data
#[inline(always)]
pub fn calc_crc(data: &[u8]) -> u32 {
    crc32fast::hash(data)
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
            events_tree_root_id: PageID(789),
            tags_tree_root_id: PageID(1011),
            next_position: Position(1234),
        });

        // Create a Page with the node
        let page_id = PageID(1);
        let page = Page::new(page_id, node);

        // Calculate the serialized size
        let calculated_size = page.calc_serialized_size();

        // Serialize the page into a fixed-size buffer using serialize_into_vec
        let mut page_buf = vec![0u8; crate::db::DEFAULT_PAGE_SIZE];
        page
            .serialize_into_vec(&mut page_buf)
            .expect("Failed to serialize page into buffer");

        // Check that the effective serialized data size (header + body_len from header) matches the calculated size
        let body_len = u32::from_le_bytes(page_buf[5..9].try_into().unwrap()) as usize;
        let effective_len = PAGE_HEADER_SIZE + body_len;
        assert_eq!(
            calculated_size,
            effective_len,
            "Calculated size {} should match effective serialized size {}",
            calculated_size,
            effective_len
        );

        // Deserialize the serialized data from the full page buffer
        let deserialized =
            Page::deserialize(page_id, &page_buf).expect("Failed to deserialize page");

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
