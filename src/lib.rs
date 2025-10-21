//! UmaDB - Dynamic Consistency Boundaries Database
//!
//! This library provides an event store implementation with support for
//! dynamic consistency boundaries.

mod common;
pub mod dcbapi;
pub mod event_store;
mod events_tree;
mod events_tree_nodes;
mod free_lists_tree_nodes;
pub mod grpc;
mod header_node;
mod lmdb;
mod node;
mod page;
mod pager;
mod tags_tree;
mod tags_tree_nodes;
