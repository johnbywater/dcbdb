//! DCBDB - Dynamic Consistency Boundaries Database
//!
//! This library provides an event store implementation with support for
//! dynamic consistency boundaries.

pub mod dcbapi;
pub mod grpc;
pub mod dcbdb;
mod lmdb;
mod common;
mod events_tree;
mod events_tree_nodes;
mod free_lists_tree_nodes;
mod header_node;
mod node;
mod page;
mod pager;
mod tags_tree_nodes;
mod tags_tree;
