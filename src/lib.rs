//! UmaDB - Dynamic Consistency Boundaries Database
//!
//! This library provides an event store implementation with support for
//! dynamic consistency boundaries.

pub mod common;
pub mod dcbapi;
pub mod event_store;
mod events_tree;
mod events_tree_nodes;
mod free_lists_tree_nodes;
pub mod grpc;
pub mod header_node;
mod lmdb;
mod node;
mod page;
mod pager;
mod tags_tree;
mod tags_tree_nodes;

// Public bench helpers to exercise internal APIs without exposing them in the public surface
pub mod bench_api {
    use crate::dcbapi::DCBResult;
    use crate::events_tree_nodes::EventLeafNode;
    use crate::lmdb::Lmdb;
    use crate::node::Node;
    use crate::page::Page;
    use std::path::Path;

    /// Minimal public wrapper to allow Criterion benches to measure commit paths
    pub struct BenchDb {
        lmdb: Lmdb,
    }

    impl BenchDb {
        pub fn new(path: &Path, page_size: usize) -> DCBResult<Self> {
            let lmdb = Lmdb::new(path, page_size, false)?;
            Ok(BenchDb { lmdb })
        }

        /// Commit with no dirty pages: exercises header write + flush.
        pub fn commit_empty(&self) -> DCBResult<()> {
            let mut w = self.lmdb.writer()?;
            self.lmdb.commit(&mut w)
        }

        /// Commit with n small dirty pages (empty EventLeaf pages) to exercise write_pages + header.
        pub fn commit_with_dirty(&self, n: usize) -> DCBResult<()> {
            let mut w = self.lmdb.writer()?;
            for _ in 0..n {
                let id = w.alloc_page_id();
                let node = Node::EventLeaf(EventLeafNode { keys: Vec::new(), values: Vec::new() });
                let page = Page::new(id, node);
                w.insert_dirty(page)?;
            }
            self.lmdb.commit(&mut w)
        }
    }
}
