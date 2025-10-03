//! DCBDB - Dynamic Consistency Boundaries Database
//!
//! This library provides an event store implementation with support for
//! dynamic consistency boundaries.

pub mod api;
pub mod grpc;
pub mod event_store;
mod db;
mod common;
mod events_btree;
mod events_btree_nodes;
mod free_list_nodes;
mod header_node;
mod node;
mod page;
mod pager;
mod tags_btree_nodes;
mod tags_btree;
