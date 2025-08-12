use crate::mvcc_common;
use crate::mvcc_common::LmdbError;
use crate::mvcc_node_event::{EventInternalNode, EventLeafNode};
use crate::mvcc_node_free_list::{FreeListInternalNode, FreeListLeafNode};
use crate::mvcc_node_header::HeaderNode;

// Constants for serialization
const PAGE_TYPE_HEADER: u8 = b'1';
const PAGE_TYPE_FREELIST_LEAF: u8 = b'2';
const PAGE_TYPE_FREELIST_INTERNAL: u8 = b'3';
const PAGE_TYPE_EVENT_LEAF: u8 = b'4';
const PAGE_TYPE_EVENT_INTERNAL: u8 = b'5';

// Enum to represent different node types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Header(HeaderNode),
    FreeListLeaf(FreeListLeafNode),
    FreeListInternal(FreeListInternalNode),
    EventLeaf(EventLeafNode),
    EventInternal(EventInternalNode),
}

impl Node {
    pub fn get_type_byte(&self) -> u8 {
        match self {
            Node::Header(_) => PAGE_TYPE_HEADER,
            Node::FreeListLeaf(_) => PAGE_TYPE_FREELIST_LEAF,
            Node::FreeListInternal(_) => PAGE_TYPE_FREELIST_INTERNAL,
            Node::EventLeaf(_) => PAGE_TYPE_EVENT_LEAF,
            Node::EventInternal(_) => PAGE_TYPE_EVENT_INTERNAL,
        }
    }

    pub fn calc_serialized_size(&self) -> usize {
        match self {
            Node::Header(_) => 16, // HeaderNode has a fixed size of 16 bytes
            Node::FreeListLeaf(node) => node.calc_serialized_size(),
            Node::FreeListInternal(node) => node.calc_serialized_size(),
            Node::EventLeaf(node) => node.calc_serialized_size(),
            Node::EventInternal(node) => node.calc_serialized_size(),
        }
    }

    pub fn serialize(&self) -> mvcc_common::Result<Vec<u8>> {
        match self {
            Node::Header(node) => Ok(node.serialize()),
            Node::FreeListLeaf(node) => node.serialize(),
            Node::FreeListInternal(node) => node.serialize(),
            Node::EventLeaf(node) => node.serialize(),
            Node::EventInternal(node) => node.serialize(),
        }
    }

    pub fn deserialize(node_type: u8, data: &[u8]) -> mvcc_common::Result<Self> {
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
            PAGE_TYPE_EVENT_LEAF => {
                let node = EventLeafNode::from_slice(data)?;
                Ok(Node::EventLeaf(node))
            }
            PAGE_TYPE_EVENT_INTERNAL => {
                let node = EventInternalNode::from_slice(data)?;
                Ok(Node::EventInternal(node))
            }
            _ => Err(LmdbError::DatabaseCorrupted(format!(
                "Invalid node type: {node_type}"
            ))),
        }
    }
}
