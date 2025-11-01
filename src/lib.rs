//! UmaDB - Dynamic Consistency Boundaries Database
//!
//! This library provides an event store implementation with support for
//! dynamic consistency boundaries.

pub mod common;
pub mod db;
pub mod dcb;
mod events_tree;
mod events_tree_nodes;
mod free_lists_tree_nodes;
pub mod grpc;
pub mod header_node;
mod mvcc;
mod node;
mod page;
mod pager;
mod tags_tree;
mod tags_tree_nodes;

// Public bench helpers to exercise internal APIs without exposing them in the public surface
pub mod bench_api {
    use crate::common::{PageID, Position};
    use crate::dcb::DCBResult;
    use crate::events_tree_nodes::{EventLeafNode, EventRecord, EventValue};
    use crate::mvcc::Mvcc;
    use crate::node::Node;
    use crate::page::Page;
    use std::path::Path;

    /// Minimal public wrapper to allow Criterion benches to measure commit paths
    pub struct BenchDb {
        mvcc: Mvcc,
    }

    impl BenchDb {
        pub fn new(path: &Path, page_size: usize) -> DCBResult<Self> {
            let mvcc = Mvcc::new(path, page_size, false)?;
            Ok(BenchDb { mvcc })
        }

        /// Commit with no dirty pages: exercises header write + flush.
        pub fn commit_empty(&self) -> DCBResult<()> {
            let mut w = self.mvcc.writer()?;
            self.mvcc.commit(&mut w)
        }

        /// Commit with n small dirty pages (empty EventLeaf pages) to exercise write_pages + header.
        pub fn commit_with_dirty(&self, n: usize) -> DCBResult<()> {
            let mut w = self.mvcc.writer()?;
            for _ in 0..n {
                let id = w.alloc_page_id();
                let node = Node::EventLeaf(EventLeafNode {
                    keys: Vec::new(),
                    values: Vec::new(),
                });
                let page = Page::new(id, node);
                w.insert_dirty(page)?;
            }
            self.mvcc.commit(&mut w)
        }
    }

    /// Helper for Criterion to benchmark EventLeafNode inline (in-page) serialization/deserialization.
    pub struct BenchEventLeafInline {
        node: EventLeafNode,
        buf: Vec<u8>,
        last_size: usize,
    }

    impl BenchEventLeafInline {
        pub fn new(keys: usize, payload_size: usize, tags_per: usize) -> Self {
            let mut values = Vec::with_capacity(keys);
            let data = vec![0xAB; payload_size];
            let tags: Vec<String> = (0..tags_per).map(|t| format!("tag-{t}")).collect();
            for _ in 0..keys {
                values.push(EventValue::Inline(EventRecord {
                    event_type: "ev".to_string(),
                    data: data.clone(),
                    tags: tags.clone(),
                }));
            }
            let keys_vec: Vec<Position> = (0..keys).map(|i| Position(i as u64)).collect();
            let node = EventLeafNode { keys: keys_vec, values };
            let cap = node.calc_serialized_size();
            BenchEventLeafInline { node, buf: vec![0u8; cap], last_size: 0 }
        }

        pub fn serialize(&mut self) -> usize {
            let need = self.node.calc_serialized_size();
            if self.buf.len() < need {
                self.buf.resize(need, 0);
            }
            self.last_size = self.node.serialize_into(&mut self.buf);
            self.last_size
        }

        pub fn deserialize_check(&self) -> DCBResult<EventLeafNode> {
            let size = self.last_size.min(self.buf.len());
            let out = EventLeafNode::from_slice(&self.buf[..size])?;
            Ok(out)
        }
    }

    /// Helper for Criterion to benchmark EventLeafNode overflow (out-of-page) metadata serde.
    pub struct BenchEventLeafOverflow {
        node: EventLeafNode,
        buf: Vec<u8>,
        last_size: usize,
    }

    impl BenchEventLeafOverflow {
        pub fn new(keys: usize, data_len: usize, tags_per: usize) -> Self {
            let mut values = Vec::with_capacity(keys);
            let tags: Vec<String> = (0..tags_per).map(|t| format!("tag-{t}")).collect();
            for i in 0..keys {
                values.push(EventValue::Overflow {
                    event_type: "ev".to_string(),
                    data_len: data_len as u64,
                    tags: tags.clone(),
                    root_id: PageID(1 + i as u64),
                });
            }
            let keys_vec: Vec<Position> = (0..keys).map(|i| Position(i as u64)).collect();
            let node = EventLeafNode { keys: keys_vec, values };
            let cap = node.calc_serialized_size();
            BenchEventLeafOverflow { node, buf: vec![0u8; cap], last_size: 0 }
        }

        pub fn serialize(&mut self) -> usize {
            let need = self.node.calc_serialized_size();
            if self.buf.len() < need {
                self.buf.resize(need, 0);
            }
            self.last_size = self.node.serialize_into(&mut self.buf);
            self.last_size
        }

        pub fn deserialize_check(&self) -> DCBResult<EventLeafNode> {
            let size = self.last_size.min(self.buf.len());
            let out = EventLeafNode::from_slice(&self.buf[..size])?;
            Ok(out)
        }
    }
}
