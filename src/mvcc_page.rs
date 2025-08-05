use crate::crc::calc_crc;
use crate::mvcc;
use crate::mvcc_nodes::{LmdbError, Node, PageID};

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
        Self {
            page_id,
            node,
        }
    }

    pub fn serialize(&self) -> mvcc::Result<Vec<u8>> {
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

    pub fn deserialize(page_id: PageID, page_data: &[u8]) -> mvcc::Result<Self> {
        if page_data.len() < PAGE_HEADER_SIZE {
            return Err(LmdbError::DatabaseCorrupted("Page data too short".to_string()));
        }

        // Extract header information
        let node_type = page_data[0];
        let crc = u32::from_le_bytes([
            page_data[1], page_data[2], page_data[3], page_data[4]
        ]);
        let data_len = u32::from_le_bytes([
            page_data[5], page_data[6], page_data[7], page_data[8]
        ]) as usize;

        if PAGE_HEADER_SIZE + data_len > page_data.len() {
            return Err(LmdbError::DatabaseCorrupted("Page data length mismatch".to_string()));
        }

        // Extract the data
        let data = &page_data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len];

        // Verify CRC
        let calculated_crc = calc_crc(&data);

        if calculated_crc != crc {
            return Err(LmdbError::DatabaseCorrupted("CRC mismatch".to_string()));
        }

        // Deserialize the node
        let node = Node::deserialize(node_type, data)?;

        Ok(Self {
            page_id,
            node,
        })
    }
}