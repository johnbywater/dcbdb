//! DCBDB - Dynamic Consistency Boundaries Database
//!
//! This library provides an event store implementation with support for
//! dynamic consistency boundaries.

pub mod api;
pub mod grpc;
pub mod event_store;
mod db;
mod common;
mod mvcc_event_tree;
mod mvcc_node_event;
mod mvcc_node_free_list;
mod mvcc_node_header;
mod mvcc_nodes;
mod mvcc_page;
mod mvcc_pager;
mod mvcc_node_tags;
mod mvcc_tags_tree;
