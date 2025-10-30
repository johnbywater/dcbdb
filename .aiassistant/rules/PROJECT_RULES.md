---
apply: always
---

# Project Overview
This project implements an append-only event store for dynamic consistency boundaries in Rust, inspired by LMDB-style Copy-on-Write pages. It provides a crash-safe, high-performance event store suitable for event-sourced systems.

Key features:
- CoW pages with **two headers** per page to manage atomic updates
- B+Tree indexes for:
    - `position -> events`
    - `tags -> positions`
    - `TSN (transaction sequence number) -> PageIDs`
- Concurrency-safe commits
- Async-friendly Rust code using `tokio` and modern async/await idioms
- Rust 2024 idioms and project-specific style

---

# Coding Style & Conventions
- Rust 2024 edition
- `snake_case` for functions and variables
- `CamelCase` for structs, enums, and traits
- Explicit lifetimes where needed
- Error handling:
    - Prefer `Result<T, E>` or `anyhow::Result`
    - Avoid panics except in test code
    - Use `?` for error propagation
- Concurrency:
    - Use `Arc`, `RwLock`, `Mutex` sparingly
    - Prefer async primitives or channels
- Documentation:
    - `///` for public items
    - Inline comments sparingly
- Formatting:
    - 4-space indentation
    - 100-character line limit
    - Prefer concise, readable code

---

# Patterns & Idioms
- **CoW Pages**:
    - Each page has two headers storing page IDs
    - Atomic swap of headers allows crash-safe updates
- **B+Trees**:
    - Immutable nodes after disk write
    - `position -> events` B+Tree stores monotonic positions
    - `tags -> positions` B+Tree indexes multiple positions per tag
    - `TSN -> PageIDs` B+Tree tracks transaction sequence numbers to pages
- **WAL**:
    - Flushes are batched at checkpoint
    - Supports partial recovery from crashes
- **Query Engine**:
    - Check committed B+Tree index first
    - Fallback to segment scan / WAL tail for unindexed events
- **Memory & Performance**:
    - Avoid unnecessary cloning
    - Keep hot paths lock-free if possible
    - Minimize allocations in critical loops
