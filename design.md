# Event Store with Write-Ahead Log (WAL) and Persistent Index - Design Document

## Overview

This document outlines the design of a lightweight, durable, and fast event store implemented in Rust using a write-ahead log (WAL) for persistence and crash resilience, extended with a persistent index for queryability. The store supports event sourcing operations such as appending and querying events, with consistency guarantees using the Dynamic Consistency Boundary (DCB) pattern.

---

## Goals

* **Durability**: WAL ensures that no events are lost during crashes.
* **Atomicity**: Events are written in batches and flushed atomically.
* **Queryability**: Persistent index allows fast querying by tags and types.
* **Performance**: Fast writes, fast filtered reads.
* **Extensibility**: Snapshotting, streaming, compaction are modular add-ons.
* **Concurrency**: Safe concurrent access using synchronization primitives.
* **Consistency**: Supports DCB semantics for conditional appends.

---

## Event Representation

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

struct EventMetadata {
    offset: u64,
    event_type: String,
    tags: Vec<String>,
}
```

---

## WAL Format

Each event is encoded as:

```
[4 bytes] length
[N bytes] serialized Event
[4 bytes] CRC32
```

Events are appended in order and flushed using `sync_all()`.

---

## Persistent Index File (`eventstore.idx`)

### Format

Serialized structure containing:

```rust
struct IndexEntry {
    position: u64,
    offset: u64,
    event_type: String,
    tags: Vec<String>,
}

struct IndexData {
    entries: Vec<IndexEntry>,
    tag_index: HashMap<String, Vec<u64>>,
    type_index: HashMap<String, Vec<u64>>,
}
```

This structure is serialized using `bincode` and flushed after each successful append.

---

## EventStore Struct

```rust
struct EventStore {
    wal_file: Mutex<File>,
    metadata: Mutex<Vec<(u64, EventMetadata)>>,
    tag_index: Mutex<HashMap<String, Vec<u64>>>,
    type_index: Mutex<HashMap<String, Vec<u64>>>,
    event_count: AtomicU64,
}
```

---

## Methods

### `open(path: &str) -> Self`

* Load WAL
* Attempt to load `eventstore.idx`
* If index is missing or invalid, scan WAL to build index

### `append_all(events: Vec<Event>) -> Result<()>`

* Encode and write batch to WAL
* Capture offset and metadata for each event
* Update `metadata`, `tag_index`, `type_index`
* Serialize and flush index to `eventstore.idx`

### `append_if_unchanged(query_items: Vec<QueryItem>, after: Option<u64>, new_events: Vec<Event>) -> Result<u64>`

* Run read with `query_items` and `after`
* If any matches, return integrity error
* Else, call `append_all` and return last position

### `read(query_items: Vec<QueryItem>, after: Option<u64>, limit: Option<usize>) -> (Vec<SequencedEvent>, Option<u64>)`

* Resolve matching positions from `tag_index` and `type_index`
* Filter by `after` and limit
* Read event payloads from WAL using `offset`

---

## Query Model

```rust
struct QueryItem {
    types: Vec<String>,
    tags: Vec<String>,
}
```

**Matching Rules**:

* Match if event type is in `types`
* Or, if `types` is empty and event tags are a **superset** of query tags

**Empty query** = all events

---

## Durability

* WAL uses `sync_all()` after each batch
* CRC32 guards against corruption
* Index is written to temp file and renamed to prevent partial writes

---

## Performance

* Append: fast, sequential disk writes
* Read: in-memory index filtering, random access to WAL

---

## Extensions

### Snapshotting

* Persist projections for faster recovery

### Segmentation

* `wal-00001`, `wal-00002`, etc., plus corresponding index segments

### Streaming

* Tail file or implement subscription hooks

### API Layer

* gRPC or HTTP with JSON/Protobuf

---

## Concurrency

* Use `Mutex` and `AtomicU64`
* Future: upgrade to `RwLock` or async channels

---

## Consistency: Dynamic Consistency Boundary (DCB)

```rust
fn read(query: Option<Vec<QueryItem>>, after: Option<u64>, limit: Option<usize>) -> (Vec<SequencedEvent>, Option<u64>);
fn append(events: Vec<Event>, condition: Option<(Vec<QueryItem>, Option<u64>)>) -> Result<u64, IntegrityError>;
```

* Guarantees atomic conditional append
* Command defines consistency boundary with query + `after`

---

## Technology Stack

* **Language**: Rust
* **IO**: `std::fs`, `bincode`, `crc32fast`
* **Memory Map**: `memmap2`
* **Concurrency**: `Mutex`, `AtomicU64`, `RwLock`

---

## Limitations

* Index must be flushed periodically to persist queryability
* WAL compaction not implemented
* Index may grow large without tag/type pruning

---

## Summary

This design integrates a persistent index with a WAL-backed event store to support durable, queryable, and consistent event processing. It enables fast reads via an in-memory index that is backed by a flushed metadata file, and it ensures DCB safety for complex multi-event transactions. The modular structure supports extension into a fully-fledged event sourcing platform.
