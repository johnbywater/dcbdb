# Event Store with Write-Ahead Log (WAL) - Design Document

## Overview

This document outlines the design of a lightweight, durable, and fast event store implemented in Rust using a write-ahead log (WAL) for persistence and crash resilience. The store supports basic event sourcing operations such as appending events and replaying them in order, with potential for extension into a full CQRS/event-driven system.

---

## Goals

* **Durability**: Events must not be lost during crashes or power failures.
* **Performance**: Fast write path using sequential disk writes.
* **Atomicity**: Batches of events are written atomically.
* **Queryability**: Events can be queried using types and tags.
* **Simplicity**: Toy-level complexity with realistic patterns.
* **Extensibility**: Modular design for snapshotting, indexing, and streaming.
* **Concurrency**: Safe concurrent reads and writes with locking or coordination.
* **Consistency**: Support for the Dynamic Consistency Boundary (DCB) specification.

---

## Core Concepts

### Event

An `Event` is an immutable record with the following fields:

```rust
struct Event {
    event_type: String,
    tags: Vec<String>,
    data: Vec<u8>,
}

struct SequencedEvent {
    position: u64,
    event: Event,
}
```

Each event is serialized using `bincode` and protected with a CRC32 checksum. Events are recorded with a monotonically increasing position and do not require an aggregate ID or version.

### Write-Ahead Log (WAL)

* Events are written to a log file sequentially.
* The format for each event record:

    * `[4 bytes]` Length of encoded event.
    * `[N bytes]` Serialized event payload.
    * `[4 bytes]` CRC32 checksum.

All events in a batch are written in one contiguous block and flushed with `sync_all()` to ensure atomicity and durability.

---

## File Structure

* `eventstore.wal`: Main WAL file where all events are appended.
* `eventstore.idx`: Index file mapping tags and types to event positions.
* `snapshots/`: Directory for event projections or snapshots.

---

## Components

### EventStore

Encapsulates all logic for file IO, locking, and event handling.

#### Key Methods

* `open(path: &str) -> Self`
* `append_all(events: Vec<Event>) -> Result<()>`: Atomically appends a batch of events.
* `append_if_unchanged(query_items: Vec<QueryItem>, after: Option<u64>, new_events: Vec<Event>) -> Result<u64>`: Appends events if no conflicting events exist since `after`. Returns the last position written.
* `read(query_items: Vec<QueryItem>, after: Option<u64>, limit: Option<usize>) -> (Vec<SequencedEvent>, Option<u64>)`: Returns all events matching query items after a given position, up to an optional limit. Returns the highest position scanned.

Uses `Mutex<File>` to ensure safe concurrent access initially.

---

## Query Model

### QueryItem

Each query item specifies matching criteria:

```rust
struct QueryItem {
    types: Vec<String>,
    tags: Vec<String>,
}
```

### Matching Semantics

* An event matches a query item if:

    * Its type is in the query item’s `types`, **or** the query item has no types and the event’s tags are a **superset** of the query item’s tags.
* A query with zero items and no `after` will select all events.
* A query with multiple items accumulates matching events across all query items.

### Append Condition

* Represented by a pair `(query: Vec<QueryItem>, after: Option<u64>)`
* Before appending, the store re-runs the query using the same `query` and `after`.
* If any new events are found, the append fails with an integrity error.
* Otherwise, the new events are written atomically.

---

## Durability and Power Failure Resilience

* All writes are flushed using `file.sync_all()` after appending.
* CRC32 is used to detect corruption due to incomplete writes.
* On startup, invalid or corrupted events at the tail of the WAL are ignored.

---

## Performance Characteristics

### Append

* Uses `OpenOptions` with `append` and `fsync()` for fast, reliable writes.
* Events are encoded in binary for minimal disk usage.
* Batches are appended atomically to avoid partial update states.

### Read

* Replays events sequentially from disk.
* Validates CRC32 at read time.
* Querying is done via filtering in memory or indexed lookup.

---

## Extensions and Future Work

### Indexing

* **Structure**: Index maps `tag/type -> Vec<position>` for fast filtering.
* **Purpose**: Efficient read queries for dynamic query items.
* **Format**: Binary format mapping tags/types to event offsets.
* **Maintenance**: Index is updated atomically after each append.

### Concurrency Control

* **Goal**: Safe concurrent readers and writers.
* **Design**:

    * Use `RwLock` or crossbeam channels for fine-grained locking.
    * For each append, acquire exclusive write lock.
    * Readers use shared lock to read and filter events.
    * Index updates are guarded by their own mutex.

### Dynamic Consistency Boundary (DCB) Support

* **Query**: A command method selects events using query items and a known position.
* **Decision**: A decision model is constructed from the selected events.
* **Append Condition**: The command attempts to append new events **if and only if** no events have been recorded since the known position, under the same query items.
* **Atomicity**: If the condition passes, all new events are written atomically with assigned positions.
* **Consistency**: If the condition fails, an integrity error is raised.
* **Dynamic Boundary**: Each command dynamically defines the scope of consistency it requires.
* **API**: The system implements a recorder interface with the following methods:

  ```rust
  fn read(query: Option<Vec<QueryItem>>, after: Option<u64>, limit: Option<usize>) -> (Vec<SequencedEvent>, Option<u64>);
  fn append(events: Vec<Event>, condition: Option<(Vec<QueryItem>, Option<u64>)>) -> Result<u64, IntegrityError>;
  ```

### Snapshotting

* Write state of projection models to snapshot files.
* Accelerates recovery from WAL.

### Segmentation

* Rotate WAL into segments: `wal-00001`, `wal-00002`, etc.
* Enables log pruning and archiving.

### Streaming Reads

* Implement tailing reads using file watchers or an event queue.

### API Layer

* Expose via gRPC or HTTP for integration with microservices.
* Implement projections and subscriptions.

---

## Technology Stack

* **Language**: Rust
* **IO Libraries**: std::fs, bincode, crc
* **Serialization**: bincode (can switch to protobuf or msgpack)
* **Concurrency Tools**: std::sync::{Mutex, RwLock}, crossbeam (future)

---

## Limitations

* Basic concurrency model with coarse locking.
* No log compaction or garbage collection.
* No in-memory caching.
* Indexing requires full WAL scan if out-of-sync.

---

## Summary

This design provides a minimal but extensible starting point for building an event sourcing engine backed by a WAL. It is resilient to power loss, supports atomic multi-event appends, and allows filtered reads by tag/type-based queries. With indexing, concurrency control, and full support for the Dynamic Consistency Boundary, it enables scalable command processing with strong consistency semantics.
