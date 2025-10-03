use crate::dcbapi::{DCBError, DCBResult};
use crate::events_tree_nodes::{EventInternalNode, EventLeafNode, EventOverflowNode};
use crate::free_lists_tree_nodes::{FreeListInternalNode, FreeListLeafNode};
use crate::header_node::HeaderNode;
use crate::tags_tree_nodes::{TagInternalNode, TagLeafNode, TagsInternalNode, TagsLeafNode};

// Constants for serialization
const PAGE_TYPE_HEADER: u8 = b'1';
const PAGE_TYPE_FREELIST_LEAF: u8 = b'2';
const PAGE_TYPE_FREELIST_INTERNAL: u8 = b'3';
const PAGE_TYPE_EVENT_LEAF: u8 = b'4';
const PAGE_TYPE_EVENT_INTERNAL: u8 = b'5';
const PAGE_TYPE_TAGS_LEAF: u8 = b'6';
const PAGE_TYPE_TAGS_INTERNAL: u8 = b'7';
const PAGE_TYPE_TAG_LEAF: u8 = b'8';
const PAGE_TYPE_TAG_INTERNAL: u8 = b'9';
const PAGE_TYPE_EVENT_OVERFLOW: u8 = b'a';

// Enum to represent different node types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Header(HeaderNode),
    FreeListLeaf(FreeListLeafNode),
    FreeListInternal(FreeListInternalNode),
    EventLeaf(EventLeafNode),
    EventInternal(EventInternalNode),
    EventOverflow(EventOverflowNode),
    TagsLeaf(TagsLeafNode),
    TagsInternal(TagsInternalNode),
    TagLeaf(TagLeafNode),
    TagInternal(TagInternalNode),
}

impl Node {
    pub fn get_type_byte(&self) -> u8 {
        match self {
            Node::Header(_) => PAGE_TYPE_HEADER,
            Node::FreeListLeaf(_) => PAGE_TYPE_FREELIST_LEAF,
            Node::FreeListInternal(_) => PAGE_TYPE_FREELIST_INTERNAL,
            Node::EventLeaf(_) => PAGE_TYPE_EVENT_LEAF,
            Node::EventInternal(_) => PAGE_TYPE_EVENT_INTERNAL,
            Node::EventOverflow(_) => PAGE_TYPE_EVENT_OVERFLOW,
            Node::TagsLeaf(_) => PAGE_TYPE_TAGS_LEAF,
            Node::TagsInternal(_) => PAGE_TYPE_TAGS_INTERNAL,
            Node::TagLeaf(_) => PAGE_TYPE_TAG_LEAF,
            Node::TagInternal(_) => PAGE_TYPE_TAG_INTERNAL,
        }
    }

    pub fn calc_serialized_size(&self) -> usize {
        match self {
            Node::Header(_) => 48, // HeaderNode has a fixed size of 48 bytes (includes root_tags_tree_id and next_position)
            Node::FreeListLeaf(node) => node.calc_serialized_size(),
            Node::FreeListInternal(node) => node.calc_serialized_size(),
            Node::EventLeaf(node) => node.calc_serialized_size(),
            Node::EventInternal(node) => node.calc_serialized_size(),
            Node::EventOverflow(node) => node.calc_serialized_size(),
            Node::TagsLeaf(node) => node.calc_serialized_size(),
            Node::TagsInternal(node) => node.calc_serialized_size(),
            Node::TagLeaf(node) => node.calc_serialized_size(),
            Node::TagInternal(node) => node.calc_serialized_size(),
        }
    }

    pub fn serialize(&self) -> DCBResult<Vec<u8>> {
        match self {
            Node::Header(node) => Ok(node.serialize()),
            Node::FreeListLeaf(node) => node.serialize(),
            Node::FreeListInternal(node) => node.serialize(),
            Node::EventLeaf(node) => node.serialize(),
            Node::EventInternal(node) => node.serialize(),
            Node::EventOverflow(node) => node.serialize(),
            Node::TagsLeaf(node) => node.serialize(),
            Node::TagsInternal(node) => node.serialize(),
            Node::TagLeaf(node) => node.serialize(),
            Node::TagInternal(node) => node.serialize(),
        }
    }

    pub fn deserialize(node_type: u8, data: &[u8]) -> DCBResult<Self> {
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
            PAGE_TYPE_EVENT_OVERFLOW => {
                let node = EventOverflowNode::from_slice(data)?;
                Ok(Node::EventOverflow(node))
            }
            PAGE_TYPE_TAGS_LEAF => {
                let node = TagsLeafNode::from_slice(data)?;
                Ok(Node::TagsLeaf(node))
            }
            PAGE_TYPE_TAGS_INTERNAL => {
                let node = TagsInternalNode::from_slice(data)?;
                Ok(Node::TagsInternal(node))
            }
            PAGE_TYPE_TAG_LEAF => {
                let node = TagLeafNode::from_slice(data)?;
                Ok(Node::TagLeaf(node))
            }
            PAGE_TYPE_TAG_INTERNAL => {
                let node = TagInternalNode::from_slice(data)?;
                Ok(Node::TagInternal(node))
            }
            _ => Err(DCBError::DatabaseCorrupted(format!(
                "Invalid node type: {node_type}"
            ))),
        }
    }
}
