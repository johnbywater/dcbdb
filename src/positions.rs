//! Position index for event records in segment files.
//!
//! This module provides a B+tree index for positions in the event sequence.
//! The keys in this index are positions in the sequence. The values are tuples
//! with the segment file number, the offset in the segment file, and the type
//! of the event record.

use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use lru::LruCache;

use rmp_serde::{decode, encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use uuid::uuid;

use crate::segments::check_crc;
use crate::segments::SegmentError;
use crate::wal::calc_crc;
use crate::wal::Position;

// Constants
const PAGE_SIZE: usize = 4096;
const TYPE_HASH_LEN: usize = 8;
const HEADER_SIZE: usize = 7; // 1 (node_type) + 4 (crc) + 2 (data_len)

// Page ID type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct PageID(pub u32);

impl fmt::Display for PageID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Error types for index operations
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Page not found: {0}")]
    PageNotFound(PageID),

    #[error("Database corrupted: {0}")]
    DatabaseCorrupted(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid node type: {0}")]
    InvalidNodeType(String),

    #[error("Key not found: {0}")]
    KeyNotFound(Position),
}

// Result type for index operations
pub type IndexResult<T> = Result<T, IndexError>;

// Position index record
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionIndexRecord {
    pub segment: i32,
    pub offset: i32,
    pub type_hash: Vec<u8>,
}

// Leaf node for the B+tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafNode {
    pub keys: Vec<Position>,
    pub values: Vec<PositionIndexRecord>,
    pub next_leaf_id: Option<PageID>,
}

// Internal node for the B+tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

// Header node for the B+tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderNode {
    pub root_page_id: PageID,
    pub next_page_id: PageID,
}

// Node type enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    Header(HeaderNode),
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl Node {
    pub fn node_type_byte(&self) -> u8 {
        match self {
            Node::Header(_) => 1,
            Node::Leaf(_) => 2,
            Node::Internal(_) => 3,
        }
    }

    pub fn from_byte_and_data(byte: u8, data: &[u8]) -> IndexResult<Self> {
        match byte {
            1 => {
                let header: HeaderNode = decode::from_slice(data)
                    .map_err(|e| IndexError::Serialization(e.to_string()))?;
                Ok(Node::Header(header))
            }
            2 => {
                let leaf: LeafNode = decode::from_slice(data)
                    .map_err(|e| IndexError::Serialization(e.to_string()))?;
                Ok(Node::Leaf(leaf))
            }
            3 => {
                let internal: InternalNode = decode::from_slice(data)
                    .map_err(|e| IndexError::Serialization(e.to_string()))?;
                Ok(Node::Internal(internal))
            }
            _ => Err(IndexError::InvalidNodeType(format!("Invalid node type byte: {}", byte))),
        }
    }
}

// Index page
#[derive(Debug, Clone)]
pub struct IndexPage {
    pub page_id: PageID,
    pub node: Node,
}

// Cache for position index records
pub struct PositionCache {
    cache: LruCache<Position, PositionIndexRecord>,
    capacity: usize,
}

impl PositionCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::unbounded(),
            capacity,
        }
    }

    pub fn get(&mut self, key: Position) -> Option<PositionIndexRecord> {
        self.cache.get(&key).cloned()
    }

    pub fn get_mut(&mut self, key: &Position) -> Option<&mut PositionIndexRecord> {
        self.cache.get_mut(key)
    }

    pub fn insert(&mut self, key: Position, value: PositionIndexRecord) {
        self.cache.put(key, value);
    }

    pub fn reduce_to_capacity(&mut self) {
        // Evict entries until we're at capacity
        while self.cache.len() > self.capacity {
            self.cache.pop_lru();
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

// Page cache
pub struct PageCache {
    cache: LruCache<PageID, IndexPage>,
    capacity: usize,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::unbounded(),
            capacity,
        }
    }

    pub fn get(&mut self, page_id: PageID) -> Option<IndexPage> {
        self.cache.get(&page_id).cloned()
    }

    pub fn get_mut(&mut self, page_id: &PageID) -> Option<&mut IndexPage> {
        self.cache.get_mut(page_id)
    }

    pub fn insert(&mut self, page_id: PageID, page: IndexPage) {
        self.cache.put(page_id, page);
    }

    pub fn reduce_to_capacity(&mut self) {
        // Evict entries until we're at capacity
        while self.cache.len() > self.capacity {
            self.cache.pop_lru();
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

// Hash a type string to a fixed-length byte array
pub fn hash_type(type_str: &str) -> Vec<u8> {
    let namespace = uuid!("6ba7b810-9dad-11d1-80b4-00c04fd430c8"); // NAMESPACE_URL
    let uuid = Uuid::new_v5(&namespace, format!("/type/{}", type_str).as_bytes());
    uuid.as_bytes()[..TYPE_HASH_LEN].to_vec()
}

// PageGuard struct to hold both the MutexGuard and provide access to the mutable page
// pub struct PageGuard<'a> {
//     _guard: std::sync::MutexGuard<'a, PageCache>,
//     page: &'a mut IndexPage,
// }

// impl<'a> PageGuard<'a> {
//     // Create a new PageGuard
//     fn new(guard: std::sync::MutexGuard<'a, PageCache>, page: &'a mut IndexPage) -> Self {
//         Self { _guard: guard, page }
//     }
// 
//     // Get a mutable reference to the page
//     pub fn page_mut(&mut self) -> &mut IndexPage {
//         self.page
//     }
// 
//     // Get an immutable reference to the page
//     pub fn page(&self) -> &IndexPage {
//         self.page
//     }
// }
// 
// impl<'a> std::ops::Deref for PageGuard<'a> {
//     type Target = IndexPage;
// 
//     fn deref(&self) -> &Self::Target {
//         self.page
//     }
// }
// 
// impl<'a> std::ops::DerefMut for PageGuard<'a> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         self.page
//     }
// }

// Position index
pub struct PositionIndex {
    file: Mutex<File>,
    header_page_id: PageID,
    root_page_id: PageID,
    next_page_id: PageID,
    page_cache: Mutex<PageCache>,
    position_cache: Mutex<PositionCache>,
    dirty_pages: Mutex<HashMap<PageID, bool>>,
    page_size: usize,
}

impl PositionIndex {
    // Create a new position index
    pub fn new<P: AsRef<Path>>(path: P, cache_size: usize) -> IndexResult<Self> {
        Self::new_with_page_size(path, cache_size, PAGE_SIZE)
    }

    // Create a new position index with a custom page size
    pub fn new_with_page_size<P: AsRef<Path>>(path: P, cache_size: usize, page_size: usize) -> IndexResult<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file_exists = path_buf.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path_buf)?;

        let mut index = Self {
            file: Mutex::new(file),
            header_page_id: PageID(0),
            root_page_id: PageID(1),
            next_page_id: PageID(2),
            page_cache: Mutex::new(PageCache::new(cache_size)),
            position_cache: Mutex::new(PositionCache::new(cache_size)),
            dirty_pages: Mutex::new(HashMap::new()),
            page_size,
        };

        if !file_exists {
            // Create a new B+tree root page
            let root_node = Node::Leaf(LeafNode {
                keys: Vec::new(),
                values: Vec::new(),
                next_leaf_id: None,
            });
            let root_page = IndexPage {
                page_id: index.root_page_id,
                node: root_node,
            };
            index.add_page(&root_page)?;
            index.write_header()?;
            index.flush()?;
        } else {
            index.read_header()?;
        }

        Ok(index)
    }

    // Write the header to the file
    fn write_header(&self) -> IndexResult<()> {
        let header_node = Node::Header(HeaderNode {
            root_page_id: self.root_page_id,
            next_page_id: self.next_page_id,
        });
        let header_page = IndexPage {
            page_id: self.header_page_id,
            node: header_node,
        };
        self.add_page(&header_page)
    }

    // Read the header from the file
    fn read_header(&mut self) -> IndexResult<()> {
        let header_page = self.get_page(self.header_page_id)?;
        match &header_page.node {
            Node::Header(header) => {
                self.root_page_id = header.root_page_id;
                self.next_page_id = header.next_page_id;
                Ok(())
            }
            _ => Err(IndexError::DatabaseCorrupted("Invalid header node".to_string())),
        }
    }

    // Allocate a new page ID
    fn alloc_page_id(&mut self) -> PageID {
        let page_id = self.next_page_id;
        self.next_page_id = PageID(self.next_page_id.0 + 1);
        page_id
    }

    // Add a page to the index
    fn add_page(&self, page: &IndexPage) -> IndexResult<()> {
        let mut page_cache = self.page_cache.lock().unwrap();
        let page_id = page.page_id;
        page_cache.insert(page_id, page.clone());
        self.mark_dirty(page_id);
        Ok(())
    }

    fn mark_dirty(&self, page_id: PageID) {
        let mut dirty_pages = self.dirty_pages.lock().unwrap();
        dirty_pages.insert(page_id, true);
    }

    // Get a page from the index
    fn get_page(&self, page_id: PageID) -> IndexResult<IndexPage> {
        let mut page_cache = self.page_cache.lock().unwrap();
        if let Some(page) = page_cache.get_mut(&page_id) {
            return Ok(page.clone());
        }

        // We'll try to read the page from disk first
        // If that fails, and it's the root page, we'll create a new empty leaf node
        let mut file = self.file.lock().unwrap();
        let offset = page_id.0 as u64 * self.page_size as u64;

        // Check if the file is large enough to contain this page
        let file_size = file.seek(SeekFrom::End(0))?;
        if offset + self.page_size as u64 <= file_size {
            // File is large enough, seek back to the page offset
            file.seek(SeekFrom::Start(offset))?;

            let mut buffer = vec![0u8; self.page_size];
            match file.read_exact(&mut buffer) {
                Ok(_) => {
                    // Parse the page
                    let node_type_byte = buffer[0];
                    let crc = u32::from_le_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
                    let data_len = u16::from_le_bytes([buffer[5], buffer[6]]);

                    let data = &buffer[HEADER_SIZE..HEADER_SIZE + data_len as usize];
                    check_crc(data, crc).map_err(|e| {
                        if let SegmentError::DatabaseCorrupted(msg) = e {
                            IndexError::DatabaseCorrupted(msg)
                        } else {
                            IndexError::DatabaseCorrupted("CRC check failed".to_string())
                        }
                    })?;

                    // Deserialize the node based on its type
                    let node = Node::from_byte_and_data(node_type_byte, data)?;

                    let page = IndexPage {
                        page_id,
                        node,
                    };

                    page_cache.insert(page_id, page.clone());
                    return Ok(page);
                }
                Err(e) => {
                    // Fall through to the special case below
                    if e.kind() != io::ErrorKind::UnexpectedEof {
                        return Err(IndexError::Io(e));
                    }
                }
            }
        }


        // If we get here, the page doesn't exist
        return Err(IndexError::PageNotFound(page_id));
    }

    // Get a page from the index and apply a function to it with a mutable reference
    // This uses a callback-based approach to avoid lifetime issues
    fn with_page_mut<F, R>(&self, page_id: PageID, f: F) -> IndexResult<R>
    where
        F: FnOnce(&mut IndexPage) -> R,
    {
        // First try to get the page from the cache
        let mut page_cache = self.page_cache.lock().unwrap();
        if let Some(page) = page_cache.get_mut(&page_id) {
            let result = f(page);
            // Mark the page as dirty since it was modified
            self.mark_dirty(page_id);
            return Ok(result);
        }

        // If not in cache, try to load it from disk
        drop(page_cache); // Release the lock before calling get_page

        // Get the page (this will load it from disk if necessary)
        let page = self.get_page(page_id)?;

        // Now that we have the page, put it back in the cache and get a mutable reference
        let mut page_cache = self.page_cache.lock().unwrap();
        page_cache.insert(page_id, page);

        // Get a mutable reference to the page and apply the function
        if let Some(page) = page_cache.get_mut(&page_id) {
            let result = f(page);
            // Mark the page as dirty since it was modified
            self.mark_dirty(page_id);
            return Ok(result);
        }

        // This should never happen since we just inserted the page
        Err(IndexError::DatabaseCorrupted("Failed to get page after inserting it into cache".to_string()))
    }

    // // Get a page from the index with a mutable reference
    // // This returns a borrowed mutable reference instead of cloning the page
    // fn get_page_mut(&self, page_id: PageID) -> IndexResult<&mut IndexPage> {
    //     // We need to modify the implementation to return a borrowed mutable reference
    //     // However, this is challenging because:
    //     // 1. The page_cache is wrapped in a Mutex, which means we need to keep the lock
    //     // 2. The method has multiple return points, including cases where it reads from disk
    //     // 3. The method signature has &self, which means it can't return a mutable reference
    //     //    to something owned by self without using interior mutability
    // 
    //     // Given these constraints, it's not straightforward to modify get_page() to return
    //     // a mutable reference. We would need to redesign the API to support this use case.
    //     // Some possible approaches:
    //     // 1. Create a custom PageGuard struct that holds both the MutexGuard and the page reference
    //     // 2. Use a callback-based API where the caller provides a function to operate on the page
    //     //    (see the with_page_mut method above for an implementation of this approach)
    //     // 3. Use interior mutability with RefCell or Mutex for the pages themselves
    // 
    //     // For now, we'll return an error to indicate that this functionality is not implemented
    //     Err(IndexError::DatabaseCorrupted("get_page_mut() is not implemented".to_string()))
    // }

    // Serialize a page
    fn serialize_page(&self, page: &IndexPage) -> IndexResult<Vec<u8>> {
        let node_type_byte = page.node.node_type_byte();

        let data = match &page.node {
            Node::Header(header) => encode::to_vec(header),
            Node::Leaf(leaf) => encode::to_vec(leaf),
            Node::Internal(internal) => encode::to_vec(internal),
        }.map_err(|e| IndexError::Serialization(e.to_string()))?;

        let crc = calc_crc(&data);
        let data_len = data.len() as u16;

        // Create a buffer just large enough for the header and data
        let mut result = Vec::with_capacity(HEADER_SIZE + data.len());
        result.push(node_type_byte);
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&data_len.to_le_bytes());
        result.extend_from_slice(&data);

        Ok(result)
    }

    // Flush dirty pages to disk
    pub fn flush(&self) -> IndexResult<()> {
        let mut file = self.file.lock().unwrap();
        let mut dirty_pages = self.dirty_pages.lock().unwrap();
        let mut page_cache = self.page_cache.lock().unwrap();

        for (page_id, _) in dirty_pages.iter() {
            if let Some(page) = page_cache.get(*page_id) {
                let serialized = self.serialize_page(&page)?;
                let offset = page_id.0 as u64 * self.page_size as u64;
                file.seek(SeekFrom::Start(offset))?;

                // Create a buffer of page_size filled with zeros
                let mut padded_data = vec![0u8; self.page_size];

                // Copy the serialized data into the buffer
                padded_data[..serialized.len()].copy_from_slice(&serialized);

                // Write the padded data to the file
                file.write_all(&padded_data)?;
            }
        }

        file.flush()?;
        file.sync_all()?;
        dirty_pages.clear();

        // Reduce caches to capacity after flush
        page_cache.reduce_to_capacity();
        drop(page_cache); // Release the lock before acquiring another one

        let mut position_cache = self.position_cache.lock().unwrap();
        position_cache.reduce_to_capacity();

        Ok(())
    }

    // Insert a key-value pair into the index
    pub fn insert(&mut self, key: Position, value: PositionIndexRecord) -> IndexResult<()> {
        // Stack to keep track of the path from root to leaf
        // Each entry is a page ID and the index of the child to follow
        let mut stack: Vec<(PageID, usize)> = Vec::new();
        let mut current_page_id = self.root_page_id;

        // First phase: traverse down to the leaf node
        loop {
            let page = self.get_page(current_page_id)?;

            match &page.node {
                Node::Leaf(_) => {
                    // Found the leaf node, break out of the loop
                    break;
                },
                Node::Internal(internal) => {
                    // Find the child to follow
                    let mut child_idx = 0;
                    while child_idx < internal.keys.len() && internal.keys[child_idx] <= key {
                        child_idx += 1;
                    }

                    // Push the current page and child index onto the stack
                    stack.push((current_page_id, child_idx));

                    // Move to the child
                    current_page_id = internal.child_ids[child_idx];
                },
                Node::Header(_) => {
                    return Err(IndexError::InvalidNodeType("Expected leaf or internal node".to_string()));
                }
            }
        }

        // Second phase: insert into the leaf node
        let page = self.get_page(current_page_id)?;
        if let Node::Leaf(mut leaf) = page.node {
            // Insert the key-value pair into the leaf
            if leaf.keys.is_empty() || key > *leaf.keys.last().unwrap() {
                leaf.keys.push(key);
                leaf.values.push(value.clone());
            } else {
                // Find the insertion point
                let mut i = 0;
                while i < leaf.keys.len() && leaf.keys[i] < key {
                    i += 1;
                }

                if i < leaf.keys.len() && leaf.keys[i] == key {
                    // Key already exists, update the value
                    leaf.values[i] = value.clone();
                } else {
                    // Insert the key-value pair
                    leaf.keys.insert(i, key);
                    leaf.values.insert(i, value.clone());
                }
            }

            // Create a new page with the modified leaf node
            let new_page = IndexPage {
                page_id: current_page_id,
                node: Node::Leaf(leaf),
            };

            // Check if this page needs splitting
            let mut split_info = None;
            if self.needs_splitting(&new_page)? {
                split_info = Some(self.split_leaf(&new_page)?);
            } else {
                // Just mark the page as dirty if it doesn't need splitting
                let mut page_cache = self.page_cache.lock().unwrap();
                page_cache.insert(current_page_id, new_page);
                self.mark_dirty(current_page_id);
            }

            // Third phase: propagate splits up the tree
            while let Some(split_child) = split_info {
                if stack.is_empty() {
                    // We've reached the root, create a new root
                    let (promoted_position, new_page_id) = split_child;
                    let new_root = IndexPage {
                        page_id: self.alloc_page_id(),
                        node: Node::Internal(InternalNode {
                            keys: vec![promoted_position],
                            child_ids: vec![self.root_page_id, new_page_id],
                        }),
                    };
                    self.add_page(&new_root)?;
                    self.root_page_id = new_root.page_id;
                    self.write_header()?;
                    break;
                }

                // Pop the parent from the stack
                let (parent_id, _) = stack.pop().unwrap();
                let parent_page = self.get_page(parent_id)?;

                if let Node::Internal(mut internal) = parent_page.node {
                    // Insert the promoted key and new page ID
                    let (promoted_key, new_page_id) = split_child;

                    let mut i = 0;
                    while i < internal.keys.len() && internal.keys[i] < promoted_key {
                        i += 1;
                    }

                    internal.keys.insert(i, promoted_key);
                    internal.child_ids.insert(i + 1, new_page_id);

                    // Create a new page with the modified internal node
                    let new_page = IndexPage {
                        page_id: parent_id,
                        node: Node::Internal(internal),
                    };

                    // Check if this page needs splitting
                    if self.needs_splitting(&new_page)? {
                        split_info = Some(self.split_internal(&new_page)?);
                    } else {
                        // Just mark the page as dirty if it doesn't need splitting
                        let mut page_cache = self.page_cache.lock().unwrap();
                        page_cache.insert(parent_id, new_page);
                        self.mark_dirty(parent_id);
                        split_info = None;
                    }
                } else {
                    return Err(IndexError::InvalidNodeType("Expected internal node".to_string()));
                }
            }
        } else {
            return Err(IndexError::InvalidNodeType("Expected leaf node".to_string()));
        }

        // Update the position cache
        let mut position_cache = self.position_cache.lock().unwrap();
        position_cache.insert(key, value);

        Ok(())
    }


    // Check if a page needs splitting
    fn needs_splitting(&self, page: &IndexPage) -> IndexResult<bool> {
        let serialized = self.serialize_page(page)?;
        Ok(serialized.len() > self.page_size - 100) // Keep some space free
    }

    // Split a leaf node
    fn split_leaf(&mut self, page: &IndexPage) -> IndexResult<(Position, PageID)> {
        if let Node::Leaf(leaf_node) = &page.node {
            let mut leaf = leaf_node.clone();

            // Create a new leaf node with the last half of the keys and values
            let split_point = leaf.keys.len() / 2;
            let new_keys = leaf.keys.split_off(split_point);
            let new_values = leaf.values.split_off(split_point);

            let new_page_id = self.alloc_page_id();
            // Extract the first key from new_keys before moving it
            let first_key = new_keys[0];

            let new_leaf = LeafNode {
                keys: new_keys,
                values: new_values,
                next_leaf_id: leaf.next_leaf_id,
            };

            // Update the original leaf node
            leaf.next_leaf_id = Some(new_page_id);

            // Add both pages
            let leaf_page = IndexPage {
                page_id: page.page_id,
                node: Node::Leaf(leaf),
            };
            self.add_page(&leaf_page)?;

            let new_leaf_page = IndexPage {
                page_id: new_page_id,
                node: Node::Leaf(new_leaf),
            };
            self.add_page(&new_leaf_page)?;

            // Return the first key in the new leaf and the new page ID
            Ok((first_key, new_page_id))
        } else {
            Err(IndexError::InvalidNodeType("Expected leaf node".to_string()))
        }
    }

    // Split an internal node
    fn split_internal(&mut self, page: &IndexPage) -> IndexResult<(Position, PageID)> {
        if let Node::Internal(internal_node) = &page.node {
            let mut internal = internal_node.clone();

            // Create a new internal node with the last half of the keys and child IDs
            let split_point = internal.keys.len() / 2;
            let promoted_key = internal.keys[split_point];

            let new_keys = internal.keys.split_off(split_point + 1);
            let new_child_ids = internal.child_ids.split_off(split_point + 1);

            // Remove the promoted key from the original node
            internal.keys.pop();

            let new_page_id = self.alloc_page_id();
            let new_internal = InternalNode {
                keys: new_keys,
                child_ids: new_child_ids,
            };

            // Add both pages
            let internal_page = IndexPage {
                page_id: page.page_id,
                node: Node::Internal(internal),
            };
            self.add_page(&internal_page)?;

            let new_internal_page = IndexPage {
                page_id: new_page_id,
                node: Node::Internal(new_internal),
            };
            self.add_page(&new_internal_page)?;

            // Return the promoted key and the new page ID
            Ok((promoted_key, new_page_id))
        } else {
            Err(IndexError::InvalidNodeType("Expected internal node".to_string()))
        }
    }

    // Look up a key in the index
    pub fn lookup(&self, key: Position) -> IndexResult<Option<PositionIndexRecord>> {
        // Check the position cache first
        let mut position_cache = self.position_cache.lock().unwrap();
        if let Some(record) = position_cache.get(key) {
            return Ok(Some(record));
        }

        // Not in cache, search the B+tree
        let mut page_id = self.root_page_id;

        loop {
            let page = self.get_page(page_id)?;

            match &page.node {
                Node::Leaf(leaf_node) => {
                    // Found a leaf node, search for the key
                    for i in 0..leaf_node.keys.len() {
                        if leaf_node.keys[i] == key {
                            let record = leaf_node.values[i].clone();
                            let record_for_cache = record.clone();
                            position_cache.insert(key, record_for_cache);
                            return Ok(Some(record));
                        }
                    }
                    return Ok(None);
                }
                Node::Internal(internal_node) => {
                    // Find the child to recurse into
                    let mut child_idx = 0;
                    while child_idx < internal_node.keys.len() && internal_node.keys[child_idx] <= key {
                        child_idx += 1;
                    }
                    page_id = internal_node.child_ids[child_idx];
                }
                _ => {
                    return Err(IndexError::InvalidNodeType("Expected leaf or internal node".to_string()));
                }
            }
        }
    }

    // Get a key from the index, raising KeyError if not found
    pub fn get(&self, key: Position) -> IndexResult<PositionIndexRecord> {
        match self.lookup(key)? {
            Some(record) => Ok(record),
            None => Err(IndexError::KeyNotFound(key)),
        }
    }

    // Get the last record in the index
    pub fn last_record(&self) -> IndexResult<Option<(Position, PositionIndexRecord)>> {
        let mut page_id = self.root_page_id;

        loop {
            let page = self.get_page(page_id)?;

            match &page.node {
                Node::Leaf(leaf_node) => {
                    if leaf_node.keys.is_empty() {
                        return Ok(None);
                    }

                    let last_idx = leaf_node.keys.len() - 1;
                    return Ok(Some((leaf_node.keys[last_idx], leaf_node.values[last_idx].clone())));
                }
                Node::Internal(internal_node) => {
                    // Go to the rightmost child
                    page_id = *internal_node.child_ids.last().unwrap();
                }
                _ => {
                    return Err(IndexError::InvalidNodeType("Expected leaf or internal node".to_string()));
                }
            }
        }
    }

    // Scan the index for records after a given position
    pub fn scan(&self, after: Position) -> IndexResult<Vec<(Position, PositionIndexRecord)>> {
        let mut result = Vec::new();

        // Find the leaf that contains or would contain the key
        let mut page_id = self.root_page_id;

        // Step 1: Find the leaf that contains the start key
        loop {
            let page = self.get_page(page_id)?;

            match &page.node {
                Node::Leaf(leaf_node) => {
                    // Found a leaf node, start scanning
                    let mut current_leaf = leaf_node.clone();

                    loop {
                        // Add all keys greater than 'after'
                        for i in 0..current_leaf.keys.len() {
                            if current_leaf.keys[i] > after {
                                result.push((current_leaf.keys[i], current_leaf.values[i].clone()));
                            }
                        }

                        // Move to the next leaf if there is one
                        if let Some(next_id) = current_leaf.next_leaf_id {
                            let next_page = self.get_page(next_id)?;
                            if let Node::Leaf(next_leaf) = &next_page.node {
                                current_leaf = next_leaf.clone();
                            } else {
                                return Err(IndexError::DatabaseCorrupted("Expected leaf node in leaf chain".to_string()));
                            }
                        } else {
                            break;
                        }
                    }

                    break;
                }
                Node::Internal(internal_node) => {
                    // Find the child to recurse into
                    let mut child_idx = 0;
                    while child_idx < internal_node.keys.len() && internal_node.keys[child_idx] <= after {
                        child_idx += 1;
                    }
                    page_id = internal_node.child_ids[child_idx];
                }
                _ => {
                    return Err(IndexError::InvalidNodeType("Expected leaf or internal node".to_string()));
                }
            }
        }

        Ok(result)
    }

    // Close the index
    pub fn close(&self) -> IndexResult<()> {
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_page_cache_get_mut() {
        // Test initialization with capacity
        let mut cache = PageCache::new(3);

        // Create a test page
        let page = IndexPage {
            page_id: PageID(1),
            node: Node::Leaf(LeafNode {
                keys: vec![1, 2],
                values: vec![
                    PositionIndexRecord {
                        segment: 1,
                        offset: 10,
                        type_hash: vec![1, 2, 3],
                    },
                    PositionIndexRecord {
                        segment: 2,
                        offset: 20,
                        type_hash: vec![4, 5, 6],
                    },
                ],
                next_leaf_id: None,
            }),
        };

        // Insert the page into the cache
        cache.insert(page.page_id, page.clone());

        // Get a mutable reference to the page
        let page_mut = cache.get_mut(&PageID(1)).unwrap();

        // Modify the page through the mutable reference
        if let Node::Leaf(leaf) = &mut page_mut.node {
            // Add a new key-value pair
            leaf.keys.push(3);
            leaf.values.push(PositionIndexRecord {
                segment: 3,
                offset: 30,
                type_hash: vec![7, 8, 9],
            });
        } else {
            panic!("Expected leaf node");
        }

        // Get the page again to verify the modification
        let modified_page = cache.get(PageID(1)).unwrap();
        if let Node::Leaf(leaf) = &modified_page.node {
            // Verify that the new key-value pair was added
            assert_eq!(leaf.keys.len(), 3);
            assert_eq!(leaf.keys[2], 3);
            assert_eq!(leaf.values[2].segment, 3);
            assert_eq!(leaf.values[2].offset, 30);
            assert_eq!(leaf.values[2].type_hash, vec![7, 8, 9]);
        } else {
            panic!("Expected leaf node");
        }
    }

    #[test]
    fn test_with_page_mut() {
        // Create a temporary directory for the test
        let temp_dir = TempDir::new().unwrap();
        let index_path = temp_dir.path().join("test_with_page_mut.idx");

        // Create a new index
        let index = PositionIndex::new(&index_path, 10).unwrap();

        // Get the root page ID
        let root_page_id = index.root_page_id;

        // Use with_page_mut to modify the root page
        index.with_page_mut(root_page_id, |page| {
            if let Node::Leaf(leaf) = &mut page.node {
                // Add a new key-value pair
                leaf.keys.push(1);
                leaf.values.push(PositionIndexRecord {
                    segment: 1,
                    offset: 10,
                    type_hash: vec![1, 2, 3],
                });
            } else {
                panic!("Expected leaf node");
            }
        }).unwrap();

        // Get the page again to verify the modification
        let modified_page = index.get_page(root_page_id).unwrap();
        if let Node::Leaf(leaf) = &modified_page.node {
            // Verify that the new key-value pair was added
            assert_eq!(leaf.keys.len(), 1);
            assert_eq!(leaf.keys[0], 1);
            assert_eq!(leaf.values[0].segment, 1);
            assert_eq!(leaf.values[0].offset, 10);
            assert_eq!(leaf.values[0].type_hash, vec![1, 2, 3]);
        } else {
            panic!("Expected leaf node");
        }
    }

    #[test]
    fn test_position_cache_get_mut() {
        // Test initialization with capacity
        let mut cache = PositionCache::new(3);

        // Create a test record
        let record = PositionIndexRecord {
            segment: 1,
            offset: 10,
            type_hash: vec![1, 2, 3],
        };

        // Insert the record into the cache
        let position = 1;
        cache.insert(position, record.clone());

        // Get a mutable reference to the record
        let record_mut = cache.get_mut(&position).unwrap();

        // Modify the record through the mutable reference
        record_mut.segment = 2;
        record_mut.offset = 20;
        record_mut.type_hash = vec![4, 5, 6];

        // Get the record again to verify the modification
        let modified_record = cache.get(position).unwrap();
        assert_eq!(modified_record.segment, 2);
        assert_eq!(modified_record.offset, 20);
        assert_eq!(modified_record.type_hash, vec![4, 5, 6]);
    }

    #[test]
    fn test_page_cache() {
        // Test initialization with capacity
        let mut cache = PageCache::new(3);

        // Create some test pages
        let page1 = IndexPage {
            page_id: PageID(1),
            node: Node::Leaf(LeafNode {
                keys: vec![1, 2],
                values: vec![
                    PositionIndexRecord {
                        segment: 1,
                        offset: 10,
                        type_hash: vec![1, 2, 3],
                    },
                    PositionIndexRecord {
                        segment: 2,
                        offset: 20,
                        type_hash: vec![4, 5, 6],
                    },
                ],
                next_leaf_id: None,
            }),
        };

        let page2 = IndexPage {
            page_id: PageID(2),
            node: Node::Leaf(LeafNode {
                keys: vec![3, 4],
                values: vec![
                    PositionIndexRecord {
                        segment: 3,
                        offset: 30,
                        type_hash: vec![7, 8, 9],
                    },
                    PositionIndexRecord {
                        segment: 4,
                        offset: 40,
                        type_hash: vec![10, 11, 12],
                    },
                ],
                next_leaf_id: None,
            }),
        };

        let page3 = IndexPage {
            page_id: PageID(3),
            node: Node::Leaf(LeafNode {
                keys: vec![5, 6],
                values: vec![
                    PositionIndexRecord {
                        segment: 5,
                        offset: 50,
                        type_hash: vec![13, 14, 15],
                    },
                    PositionIndexRecord {
                        segment: 6,
                        offset: 60,
                        type_hash: vec![16, 17, 18],
                    },
                ],
                next_leaf_id: None,
            }),
        };

        let page4 = IndexPage {
            page_id: PageID(4),
            node: Node::Leaf(LeafNode {
                keys: vec![7, 8],
                values: vec![
                    PositionIndexRecord {
                        segment: 7,
                        offset: 70,
                        type_hash: vec![19, 20, 21],
                    },
                    PositionIndexRecord {
                        segment: 8,
                        offset: 80,
                        type_hash: vec![22, 23, 24],
                    },
                ],
                next_leaf_id: None,
            }),
        };

        // Test inserting and retrieving pages
        cache.insert(page1.page_id, page1.clone());
        let retrieved = cache.get(PageID(1)).unwrap();
        assert_eq!(retrieved.page_id, page1.page_id);
        if let Node::Leaf(retrieved_leaf) = &retrieved.node {
            if let Node::Leaf(original_leaf) = &page1.node {
                assert_eq!(retrieved_leaf.keys, original_leaf.keys);
                assert_eq!(retrieved_leaf.values, original_leaf.values);
                assert_eq!(retrieved_leaf.next_leaf_id, original_leaf.next_leaf_id);
            } else {
                panic!("Expected leaf node");
            }
        } else {
            panic!("Expected leaf node");
        }

        // Test cache eviction when capacity is exceeded
        cache.insert(page2.page_id, page2.clone());
        cache.insert(page3.page_id, page3.clone());

        // All three pages should be in the cache
        assert!(cache.get(PageID(1)).is_some());
        assert!(cache.get(PageID(2)).is_some());
        assert!(cache.get(PageID(3)).is_some());

        // Insert a fourth page, which should cause the least recently used page to be evicted
        // With unbounded LruCache, we need to call reduce_to_capacity to enforce the capacity
        cache.insert(page4.page_id, page4.clone());
        cache.reduce_to_capacity();

        // One of the pages should have been evicted, but we don't know which one
        // since the LRU behavior depends on the order of access
        let pages_in_cache = [
            cache.get(PageID(1)).is_some(),
            cache.get(PageID(2)).is_some(),
            cache.get(PageID(3)).is_some(),
            cache.get(PageID(4)).is_some(),
        ];

        // Count how many pages are still in the cache
        let count = pages_in_cache.iter().filter(|&&x| x).count();

        // We should have exactly 3 pages in the cache (capacity is 3)
        assert_eq!(count, 3);

        // The most recently inserted page (page4) should definitely be in the cache
        assert!(cache.get(PageID(4)).is_some());

        // Test clearing the cache
        cache.clear();
        assert!(cache.get(PageID(1)).is_none());
        assert!(cache.get(PageID(2)).is_none());
        assert!(cache.get(PageID(3)).is_none());
        assert!(cache.get(PageID(4)).is_none());
    }

    #[test]
    fn test_position_cache() {
        // Test initialization with capacity
        let mut cache = PositionCache::new(3);
        assert_eq!(cache.capacity, 3);

        // Create some test records
        let record1 = PositionIndexRecord {
            segment: 1,
            offset: 10,
            type_hash: vec![1, 2, 3],
        };

        let record2 = PositionIndexRecord {
            segment: 2,
            offset: 20,
            type_hash: vec![4, 5, 6],
        };

        let record3 = PositionIndexRecord {
            segment: 3,
            offset: 30,
            type_hash: vec![7, 8, 9],
        };

        let record4 = PositionIndexRecord {
            segment: 4,
            offset: 40,
            type_hash: vec![10, 11, 12],
        };

        // Test inserting and retrieving records
        cache.insert(1, record1.clone());
        let retrieved = cache.get(1).unwrap();
        assert_eq!(retrieved.segment, record1.segment);
        assert_eq!(retrieved.offset, record1.offset);
        assert_eq!(retrieved.type_hash, record1.type_hash);

        // Test cache eviction when capacity is exceeded
        cache.insert(2, record2.clone());
        cache.insert(3, record3.clone());

        // All three records should be in the cache
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());

        // Insert a fourth record, which should cause the first record to be evicted
        // when reduce_to_capacity is called
        cache.insert(4, record4.clone());

        // Reduce to capacity
        cache.reduce_to_capacity();

        // One of the records should have been evicted, but we don't know which one
        // since the LRU behavior depends on the order of access
        let records_in_cache = [
            cache.get(1).is_some(),
            cache.get(2).is_some(),
            cache.get(3).is_some(),
            cache.get(4).is_some(),
        ];

        // Count how many records are still in the cache
        let count = records_in_cache.iter().filter(|&&x| x).count();

        // We should have exactly 3 records in the cache (capacity is 3)
        assert_eq!(count, 3);

        // The most recently inserted record (record4) should definitely be in the cache
        assert!(cache.get(4).is_some());

        // Test clearing the cache
        cache.clear();
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_none());
        assert!(cache.get(3).is_none());
        assert!(cache.get(4).is_none());
    }

    #[test]
    fn test_cache_lru_behavior() {
        // Test LRU behavior of PageCache
        let mut page_cache = PageCache::new(3);

        // Create some test pages
        let page1 = IndexPage {
            page_id: PageID(1),
            node: Node::Leaf(LeafNode {
                keys: vec![1],
                values: vec![PositionIndexRecord {
                    segment: 1,
                    offset: 10,
                    type_hash: vec![1, 2, 3],
                }],
                next_leaf_id: None,
            }),
        };

        let page2 = IndexPage {
            page_id: PageID(2),
            node: Node::Leaf(LeafNode {
                keys: vec![2],
                values: vec![PositionIndexRecord {
                    segment: 2,
                    offset: 20,
                    type_hash: vec![4, 5, 6],
                }],
                next_leaf_id: None,
            }),
        };

        let page3 = IndexPage {
            page_id: PageID(3),
            node: Node::Leaf(LeafNode {
                keys: vec![3],
                values: vec![PositionIndexRecord {
                    segment: 3,
                    offset: 30,
                    type_hash: vec![7, 8, 9],
                }],
                next_leaf_id: None,
            }),
        };

        let page4 = IndexPage {
            page_id: PageID(4),
            node: Node::Leaf(LeafNode {
                keys: vec![4],
                values: vec![PositionIndexRecord {
                    segment: 4,
                    offset: 40,
                    type_hash: vec![10, 11, 12],
                }],
                next_leaf_id: None,
            }),
        };

        // Insert three pages
        page_cache.insert(page1.page_id, page1.clone());
        page_cache.insert(page2.page_id, page2.clone());
        page_cache.insert(page3.page_id, page3.clone());

        // Access page1, which should move it to the end of the queue
        page_cache.get(PageID(1));

        // Insert page4, which should cause page2 to be evicted (since page1 was recently accessed)
        page_cache.insert(page4.page_id, page4.clone());
        page_cache.reduce_to_capacity();

        // page2 should be evicted, but page1, page3, and page4 should still be in the cache
        assert!(page_cache.get(PageID(2)).is_none());
        assert!(page_cache.get(PageID(1)).is_some());
        assert!(page_cache.get(PageID(3)).is_some());
        assert!(page_cache.get(PageID(4)).is_some());

        // Test LRU behavior of PositionCache
        let mut position_cache = PositionCache::new(3);

        // Create some test records
        let record1 = PositionIndexRecord {
            segment: 1,
            offset: 10,
            type_hash: vec![1, 2, 3],
        };

        let record2 = PositionIndexRecord {
            segment: 2,
            offset: 20,
            type_hash: vec![4, 5, 6],
        };

        let record3 = PositionIndexRecord {
            segment: 3,
            offset: 30,
            type_hash: vec![7, 8, 9],
        };

        let record4 = PositionIndexRecord {
            segment: 4,
            offset: 40,
            type_hash: vec![10, 11, 12],
        };

        // Insert three records
        position_cache.insert(1, record1.clone());
        position_cache.insert(2, record2.clone());
        position_cache.insert(3, record3.clone());

        // Access record1, which should move it to the end of the queue
        position_cache.get(1);

        // Insert record4, which should cause record2 to be evicted (since record1 was recently accessed)
        position_cache.insert(4, record4.clone());
        position_cache.reduce_to_capacity();

        // record2 should be evicted, but record1, record3, and record4 should still be in the cache
        assert!(position_cache.get(2).is_none());
        assert!(position_cache.get(1).is_some());
        assert!(position_cache.get(3).is_some());
        assert!(position_cache.get(4).is_some());
    }

    #[test]
    fn test_write_read_page() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let index = PositionIndex::new(&path, 1).unwrap();

        // Create a leaf node with some records
        let leaf_node = LeafNode {
            keys: vec![1, 2, 3],
            values: vec![
                PositionIndexRecord {
                    segment: 1,
                    offset: 10,
                    type_hash: hash_type("test-type-1"),
                },
                PositionIndexRecord {
                    segment: 2,
                    offset: 20,
                    type_hash: hash_type("test-type-2"),
                },
                PositionIndexRecord {
                    segment: 3,
                    offset: 30,
                    type_hash: hash_type("test-type-3"),
                },
            ],
            next_leaf_id: None,
        };

        // Create a page with the leaf node
        let page_id = PageID(10);
        let page = IndexPage {
            page_id,
            node: Node::Leaf(leaf_node.clone()),
        };

        // Add the page to the index and flush it to disk
        index.add_page(&page).unwrap();
        index.flush().unwrap();

        // Clear the cache
        index.page_cache.lock().unwrap().clear();

        // Read the page back from disk
        let retrieved_page = index.get_page(page_id).unwrap();

        // Check that the retrieved page has the same data as the original page
        if let Node::Leaf(retrieved_node) = &retrieved_page.node {
            assert_eq!(retrieved_node.keys, leaf_node.keys);
            assert_eq!(retrieved_node.values, leaf_node.values);
            assert_eq!(retrieved_node.next_leaf_id, leaf_node.next_leaf_id);
        } else {
            panic!("Expected leaf node");
        }
    }

    #[test]
    fn test_simple_insert_lookup() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let mut index = PositionIndex::new(&path, 10).unwrap();

        // Insert a few records
        let mut inserted: Vec<(Position, PositionIndexRecord)> = Vec::new();
        for i in 0..5 {
            let position = (i + 1).into();
            let record = PositionIndexRecord {
                segment: i,
                offset: i * 10,
                type_hash: hash_type(&format!("test-{}", i)),
            };

            index.insert(position, record.clone()).unwrap();
            inserted.push((position, record));
        }

        // Check lookup before flush
        for (position, record) in &inserted {
            let result = index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }

        // Flush the index
        index.flush().unwrap();

        // Clear the caches
        index.page_cache.lock().unwrap().clear();
        index.position_cache.lock().unwrap().clear();

        // Check lookup after flush
        for (position, record) in &inserted {
            let result = index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }
    }

    #[test]
    fn test_header() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let index = PositionIndex::new(&path, 1).unwrap();

        assert_eq!(index.root_page_id, PageID(1));
        assert_eq!(index.next_page_id, PageID(2));

        index.flush().unwrap();

        // Reopen the index
        let index = PositionIndex::new(&path, 1).unwrap();
        assert_eq!(index.root_page_id, PageID(1));
        assert_eq!(index.next_page_id, PageID(2));
    }

    #[test]
    fn test_index() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        // Use a smaller page size to trigger node splits with fewer inserts
        let mut index = PositionIndex::new_with_page_size(&path, 100, 256).unwrap(); // Use a larger cache size

        // Insert some positions
        let mut inserted: Vec<(Position, PositionIndexRecord)> = Vec::new();
        let num_inserts = 500; // Using 1500 instead of 150,000 for performance in tests

        for i in 0..num_inserts {
            let position = (i + 1).into();
            let seg = i / 100;
            let offset = (i % 100) * 32;
            let type_hash = hash_type(&format!("test-{}", i));

            let record = PositionIndexRecord {
                segment: seg,
                offset,
                type_hash,
            };

            index.insert(position, record.clone()).unwrap();
            inserted.push((position, record));
        }
        
        // Check lookup
        for (position, record) in &inserted {
            let result = index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }
        
        // Check last record
        let last_record = index.last_record().unwrap();
        assert!(last_record.is_some());
        let (position, record) = last_record.unwrap();
        assert_eq!(position, inserted.last().unwrap().0);
        assert_eq!(record, inserted.last().unwrap().1);
        
        // Check scan
        for i in (0..num_inserts).step_by(1) { // Using step of 67 instead of 678 due to smaller dataset
            let after = i as i64;
            let results = index.scan(after).unwrap();
        
            let expected: Vec<(Position, PositionIndexRecord)> = inserted
                .iter()
                .filter(|(pos, _)| *pos > after)
                .cloned()
                .collect();
        
            assert_eq!(results, expected);
        }
        
        // Check lookup after flush
        index.flush().unwrap();
        
        // Check only the first few keys
        for i in (0..num_inserts).step_by(1) {
            let (position, record) = &inserted[i as usize];
            index.page_cache.lock().unwrap().clear();
            index.position_cache.lock().unwrap().clear();
            let result = index.lookup(*position).unwrap();
            assert!(result.is_some());
            assert_eq!(&result.unwrap(), record);
        }
        
        // Check last record after flush and cache clear
        index.page_cache.lock().unwrap().clear();
        index.position_cache.lock().unwrap().clear();
        let last_record = index.last_record().unwrap();
        assert!(last_record.is_some());
        let (position, record) = last_record.unwrap();
        assert_eq!(position, inserted.last().unwrap().0);
        assert_eq!(record, inserted.last().unwrap().1);
        
        // Check scan after flush and cache clear
        for i in (0..num_inserts).step_by(1) {
            index.page_cache.lock().unwrap().clear();
            index.position_cache.lock().unwrap().clear();
            let after = i as i64;
            let results = index.scan(after).unwrap();
        
            let expected: Vec<(Position, PositionIndexRecord)> = inserted
                .iter()
                .filter(|(pos, _)| *pos > after)
                .cloned()
                .collect();
        
            assert_eq!(results, expected);
        }
    }



    #[test]
    fn test_get() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let mut index = PositionIndex::new(&path, 1).unwrap();

        let position = 1;

        // Key should not exist yet
        let result = index.get(position);
        assert!(matches!(result, Err(IndexError::KeyNotFound(_))));

        // Insert a record
        let record = PositionIndexRecord {
            segment: 1,
            offset: 10,
            type_hash: b"type_hash".to_vec(),
        };

        index.insert(position, record.clone()).unwrap();

        // Get should return the record
        let copy = index.get(position).unwrap();
        assert_eq!(copy, record);
    }

    #[test]
    fn test_leaf_node() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let index = PositionIndex::new(&path, 1).unwrap();

        // Use a different page ID, not the root page ID
        let page_id = PageID(10);
        let leaf_node = LeafNode {
            keys: vec![2, 3],
            values: vec![
                PositionIndexRecord {
                    segment: 12,
                    offset: 34,
                    type_hash: hash_type("test-type"),
                },
                PositionIndexRecord {
                    segment: 56,
                    offset: 79,
                    type_hash: hash_type("test-type"),
                },
            ],
            next_leaf_id: Some(PageID(9)),
        };

        let page = IndexPage {
            page_id,
            node: Node::Leaf(leaf_node.clone()),
        };

        index.add_page(&page).unwrap();
        index.flush().unwrap();

        let retrieved_page = index.get_page(page_id).unwrap();
        if let Node::Leaf(retrieved_node) = &retrieved_page.node {
            assert_eq!(retrieved_node.keys, leaf_node.keys);
            assert_eq!(retrieved_node.values, leaf_node.values);
            assert_eq!(retrieved_node.next_leaf_id, leaf_node.next_leaf_id);
        } else {
            panic!("Expected leaf node");
        }

        index.flush().unwrap();
        index.page_cache.lock().unwrap().clear();

        let retrieved_page = index.get_page(page_id).unwrap();
        if let Node::Leaf(retrieved_node) = &retrieved_page.node {
            assert_eq!(retrieved_node.keys, leaf_node.keys);
            assert_eq!(retrieved_node.values, leaf_node.values);
            assert_eq!(retrieved_node.next_leaf_id, leaf_node.next_leaf_id);
        } else {
            panic!("Expected leaf node");
        }
    }

    #[test]
    fn test_internal_node() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let index = PositionIndex::new(&path, 1).unwrap();

        // Use a different page ID, not the root page ID
        let page_id = PageID(11);
        let internal_node = InternalNode {
            keys: vec![1, 2],
            child_ids: vec![PageID(3), PageID(4)],
        };

        let page = IndexPage {
            page_id,
            node: Node::Internal(internal_node.clone()),
        };

        index.add_page(&page).unwrap();
        index.flush().unwrap();

        let retrieved_page = index.get_page(page_id).unwrap();
        if let Node::Internal(retrieved_node) = &retrieved_page.node {
            assert_eq!(retrieved_node.keys, internal_node.keys);
            assert_eq!(retrieved_node.child_ids, internal_node.child_ids);
        } else {
            panic!("Expected internal node");
        }

        index.flush().unwrap();
        index.page_cache.lock().unwrap().clear();

        let retrieved_page = index.get_page(page_id).unwrap();
        if let Node::Internal(retrieved_node) = &retrieved_page.node {
            assert_eq!(retrieved_node.keys, internal_node.keys);
            assert_eq!(retrieved_node.child_ids, internal_node.child_ids);
        } else {
            panic!("Expected internal node");
        }
    }

    #[test]
    fn test_serialize_page_leaf_node() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let page_size = 4096;
        let index = PositionIndex::new_with_page_size(&path, 1, page_size).unwrap();

        // Create a simple leaf node
        let leaf_node = LeafNode {
            keys: vec![1, 2, 3],
            values: vec![
                PositionIndexRecord {
                    segment: 1,
                    offset: 10,
                    type_hash: hash_type("test-type-1"),
                },
                PositionIndexRecord {
                    segment: 2,
                    offset: 20,
                    type_hash: hash_type("test-type-2"),
                },
                PositionIndexRecord {
                    segment: 3,
                    offset: 30,
                    type_hash: hash_type("test-type-3"),
                },
            ],
            next_leaf_id: None,
        };

        let page = IndexPage {
            page_id: PageID(1),
            node: Node::Leaf(leaf_node),
        };

        // Serialize the page
        let serialized = index.serialize_page(&page).unwrap();

        // Print the actual size for debugging
        // println!("Leaf node serialized size: {}", serialized.len());

        // Check that the serialized data is compact (msgpack is more compact than bincode)
        // assert!(serialized.len() < 200); // Allow some flexibility in the exact size
        // assert!(serialized.len() > 50); // Reduced threshold for msgpack's more compact format
        assert!(serialized.len() == 59);

        // Check that the node type byte is correct
        assert_eq!(serialized[0], 2); // 2 is the node type byte for leaf nodes

        // Extract the data length from the serialized data
        let data_len = u16::from_le_bytes([serialized[5], serialized[6]]);

        // Check that the data length is reasonable (greater than 0 and less than page_size - HEADER_SIZE)
        assert!(data_len > 0);
        assert!(data_len < (page_size - HEADER_SIZE) as u16);

        // Check that the actual data is present in the serialized buffer
        let data = &serialized[HEADER_SIZE..HEADER_SIZE + data_len as usize];
        assert!(!data.is_empty());

        // The serialized data should be exactly HEADER_SIZE + data_len bytes
        assert_eq!(serialized.len(), HEADER_SIZE + data_len as usize);
    }

    #[test]
    fn test_serialize_page_internal_node() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("position_index.db");
        let page_size = 4096;
        let index = PositionIndex::new_with_page_size(&path, 1, page_size).unwrap();

        // Create a simple internal node
        let internal_node = InternalNode {
            keys: vec![1, 2, 3],
            child_ids: vec![PageID(5), PageID(6), PageID(7), PageID(8)],
        };

        let page = IndexPage {
            page_id: PageID(1),
            node: Node::Internal(internal_node),
        };

        // Serialize the page
        let serialized = index.serialize_page(&page).unwrap();

        // Print the actual size for debugging
        // println!("Internal node serialized size: {}", serialized.len());

        // Check that the serialized data is compact (msgpack is more compact than bincode)
        // assert!(serialized.len() < 200); // Allow some flexibility in the exact size
        // assert!(serialized.len() > 10); // Reduced threshold for msgpack's more compact format
        assert!(serialized.len() == 17);

        // Check that the node type byte is correct
        assert_eq!(serialized[0], 3); // 3 is the node type byte for internal nodes

        // Extract the data length from the serialized data
        let data_len = u16::from_le_bytes([serialized[5], serialized[6]]);

        // Check that the data length is reasonable (greater than 0 and less than page_size - HEADER_SIZE)
        assert!(data_len > 0);
        assert!(data_len < (page_size - HEADER_SIZE) as u16);

        // Check that the actual data is present in the serialized buffer
        let data = &serialized[HEADER_SIZE..HEADER_SIZE + data_len as usize];
        assert!(!data.is_empty());

        // The serialized data should be exactly HEADER_SIZE + data_len bytes
        assert_eq!(serialized.len(), HEADER_SIZE + data_len as usize);
    }
}
