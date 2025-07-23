use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::marker::PhantomData;
use std::collections::HashMap;
use std::sync::Arc;
use std::fmt;
use std::os::unix::io::AsRawFd;
use libc;
use std::mem;

use serde::{Serialize, Deserialize};
use rmp_serde::{Serializer, Deserializer, decode, encode};
use crc32fast::Hasher;

// NewType definitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PageID(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Position(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TSN(pub u32);

// Node type definitions
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderNode {
    pub tsn: TSN,
    pub next_page_id: PageID,
    pub free_list_root_id: PageID,
    pub position_root_id: PageID,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreeListLeafValue {
    pub page_ids: Vec<PageID>,
    pub root_id: Option<PageID>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreeListLeafNode {
    pub keys: Vec<TSN>,
    pub values: Vec<FreeListLeafValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreeListInternalNode {
    pub keys: Vec<TSN>,
    pub child_ids: Vec<PageID>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionIndexRecord {
    pub segment: u32,
    pub offset: u32,
    pub type_hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionLeafNode {
    pub keys: Vec<Position>,
    pub values: Vec<PositionIndexRecord>,
    pub next_leaf_id: Option<PageID>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionInternalNode {
    pub keys: Vec<Position>,
    pub child_ids: Vec<PageID>,
}

// Enum to represent different node types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    Header(HeaderNode),
    FreeListLeaf(FreeListLeafNode),
    FreeListInternal(FreeListInternalNode),
    PositionLeaf(PositionLeafNode),
    PositionInternal(PositionInternalNode),
}

impl Node {
    pub fn get_type_byte(&self) -> u8 {
        match self {
            Node::Header(_) => PAGE_TYPE_HEADER,
            Node::FreeListLeaf(_) => PAGE_TYPE_FREELIST_LEAF,
            Node::FreeListInternal(_) => PAGE_TYPE_FREELIST_INTERNAL,
            Node::PositionLeaf(_) => PAGE_TYPE_POSITION_LEAF,
            Node::PositionInternal(_) => PAGE_TYPE_POSITION_INTERNAL,
        }
    }
    
    pub fn serialize(&self) -> Result<Vec<u8>> {
        match self {
            Node::Header(node) => {
                encode::to_vec(node).map_err(|e| LmdbError::SerializationError(e.to_string()))
            }
            Node::FreeListLeaf(node) => {
                encode::to_vec(node).map_err(|e| LmdbError::SerializationError(e.to_string()))
            }
            Node::FreeListInternal(node) => {
                encode::to_vec(node).map_err(|e| LmdbError::SerializationError(e.to_string()))
            }
            Node::PositionLeaf(node) => {
                encode::to_vec(node).map_err(|e| LmdbError::SerializationError(e.to_string()))
            }
            Node::PositionInternal(node) => {
                encode::to_vec(node).map_err(|e| LmdbError::SerializationError(e.to_string()))
            }
        }
    }
    
    pub fn deserialize(node_type: u8, data: &[u8]) -> Result<Self> {
        match node_type {
            PAGE_TYPE_HEADER => {
                let node: HeaderNode = decode::from_slice(data)
                    .map_err(|e| LmdbError::DeserializationError(e.to_string()))?;
                Ok(Node::Header(node))
            }
            PAGE_TYPE_FREELIST_LEAF => {
                let node: FreeListLeafNode = decode::from_slice(data)
                    .map_err(|e| LmdbError::DeserializationError(e.to_string()))?;
                Ok(Node::FreeListLeaf(node))
            }
            PAGE_TYPE_FREELIST_INTERNAL => {
                let node: FreeListInternalNode = decode::from_slice(data)
                    .map_err(|e| LmdbError::DeserializationError(e.to_string()))?;
                Ok(Node::FreeListInternal(node))
            }
            PAGE_TYPE_POSITION_LEAF => {
                let node: PositionLeafNode = decode::from_slice(data)
                    .map_err(|e| LmdbError::DeserializationError(e.to_string()))?;
                Ok(Node::PositionLeaf(node))
            }
            PAGE_TYPE_POSITION_INTERNAL => {
                let node: PositionInternalNode = decode::from_slice(data)
                    .map_err(|e| LmdbError::DeserializationError(e.to_string()))?;
                Ok(Node::PositionInternal(node))
            }
            _ => Err(LmdbError::DatabaseCorrupted(format!("Invalid node type: {}", node_type)))
        }
    }
}

// Page structure
#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: PageID,
    pub node: Node,
    pub serialized: Vec<u8>,
}

// Reader transaction
pub struct LmdbReader {
    pub header_page_id: PageID,
    pub tsn: TSN,
    pub position_root_id: PageID,
}

// Writer transaction
pub struct LmdbWriter {
    pub header_page_id: PageID,
    pub tsn: TSN,
    pub next_page_id: PageID,
    pub free_list_root_id: PageID,
    pub position_root_id: PageID,
    pub free_page_ids: VecDeque<(PageID, TSN)>,
    pub freed_page_ids: VecDeque<PageID>,
    pub dirty: HashMap<PageID, Page>,
    pub reused_page_ids: VecDeque<(PageID, TSN)>,
}

// Pager for file I/O
pub struct Pager {
    pub file: File,
    pub page_size: usize,
    pub is_file_new: bool,
}

// Main LMDB structure
pub struct Lmdb {
    pub pager: Pager,
    pub reader_tsns: HashMap<usize, TSN>,
    pub reader_tsns_lock: Mutex<()>,
    pub writer_lock: Mutex<()>,
    pub page_size: usize,
    pub header_page_id0: PageID,
    pub header_page_id1: PageID,
}

impl Lmdb {
    pub fn new(path: &Path, page_size: usize) -> Result<Self> {
        let pager = Pager::new(path, page_size)?;
        let header_page_id0 = PageID(0);
        let header_page_id1 = PageID(1);
        
        let mut lmdb = Self {
            pager,
            reader_tsns: HashMap::new(),
            reader_tsns_lock: Mutex::new(()),
            writer_lock: Mutex::new(()),
            page_size,
            header_page_id0,
            header_page_id1,
        };
        
        if lmdb.pager.is_file_new {
            // Initialize new database
            let free_list_root_id = PageID(2);
            let position_root_id = PageID(3);
            let next_page_id = PageID(4);
            
            // Create and write header pages
            let header_node0 = HeaderNode {
                tsn: TSN(0),
                next_page_id,
                free_list_root_id,
                position_root_id,
            };
            
            let header_page0 = Page::new(header_page_id0, Node::Header(header_node0.clone()));
            lmdb.write_page(header_page0)?;
            
            let header_page1 = Page::new(header_page_id1, Node::Header(header_node0));
            lmdb.write_page(header_page1)?;
            
            // Create and write empty free list root page
            let free_list_leaf = FreeListLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let free_list_page = Page::new(free_list_root_id, Node::FreeListLeaf(free_list_leaf));
            lmdb.write_page(free_list_page)?;
            
            // Create and write empty position index root page
            let position_leaf = PositionLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
                next_leaf_id: None,
            };
            let position_page = Page::new(position_root_id, Node::PositionLeaf(position_leaf));
            lmdb.write_page(position_page)?;
            
            lmdb.flush()?;
        }
        
        Ok(lmdb)
    }
    
    pub fn get_latest_header(&mut self) -> Result<Page> {
        let header0 = self.read_page(self.header_page_id0)?;
        let header1 = self.read_page(self.header_page_id1)?;
        
        let header0_tsn = match &header0.node {
            Node::Header(node) => node.tsn,
            _ => return Err(LmdbError::DatabaseCorrupted("Invalid header node type".to_string())),
        };
        
        let header1_tsn = match &header1.node {
            Node::Header(node) => node.tsn,
            _ => return Err(LmdbError::DatabaseCorrupted("Invalid header node type".to_string())),
        };
        
        if header1_tsn > header0_tsn {
            Ok(header1)
        } else {
            Ok(header0)
        }
    }
    
    pub fn read_page(&mut self, page_id: PageID) -> Result<Page> {
        let page_data = self.pager.read_page(page_id)?;
        Page::deserialize(page_id, &page_data)
    }
    
    pub fn write_page(&mut self, mut page: Page) -> Result<()> {
        page.serialize()?;
        self.pager.write_page(page.page_id, &page.serialized)?;
        Ok(())
    }
    
    pub fn flush(&mut self) -> Result<()> {
        self.pager.flush()?;
        Ok(())
    }
    
    pub fn reader<'a>(&'a mut self) -> Result<LmdbReader> {
        let header = self.get_latest_header()?;
        
        let header_node = match &header.node {
            Node::Header(node) => node,
            _ => return Err(LmdbError::DatabaseCorrupted("Invalid header node type".to_string())),
        };
        
        let reader = LmdbReader {
            header_page_id: header.page_id,
            tsn: header_node.tsn,
            position_root_id: header_node.position_root_id,
        };
        
        // Register the reader TSN
        let reader_id = &reader as *const _ as usize;
        let _lock = self.reader_tsns_lock.lock().unwrap();
        self.reader_tsns.insert(reader_id, reader.tsn);
        
        Ok(reader)
    }
    
    // Helper method to get writer information without holding the lock
    fn get_writer_info(&mut self) -> Result<(PageID, TSN, PageID, PageID, PageID, VecDeque<(PageID, TSN)>)> {
        // Get the latest header
        let header = self.get_latest_header()?;
        let header_page_id = header.page_id;
        
        // Extract information from the header
        let (tsn, next_page_id, free_list_root_id, position_root_id) = match &header.node {
            Node::Header(node) => (
                TSN(node.tsn.0 + 1),
                node.next_page_id,
                node.free_list_root_id,
                node.position_root_id
            ),
            _ => return Err(LmdbError::DatabaseCorrupted("Invalid header node type".to_string())),
        };
        
        // Get free page IDs
        let free_page_ids = self.get_free_page_ids(&header)?;
        
        Ok((header_page_id, tsn, next_page_id, free_list_root_id, position_root_id, free_page_ids))
    }
    
    pub fn writer<'a>(&'a mut self) -> Result<LmdbWriter> {
        // Get all the information we need before acquiring the lock
        let (header_page_id, tsn, next_page_id, free_list_root_id, position_root_id, free_page_ids) = 
            self.get_writer_info()?;
        
        // Acquire the writer lock
        let _lock = self.writer_lock.lock().unwrap();
        
        // Create the writer
        let writer = LmdbWriter::new(
            header_page_id,
            tsn,
            next_page_id,
            free_list_root_id,
            position_root_id,
            free_page_ids,
        );
        
        Ok(writer)
    }
    
    pub fn get_free_page_ids(&mut self, header: &Page) -> Result<VecDeque<(PageID, TSN)>> {
        let header_node = match &header.node {
            Node::Header(node) => node,
            _ => return Err(LmdbError::DatabaseCorrupted("Invalid header node type".to_string())),
        };
        
        let root_page = self.read_page(header_node.free_list_root_id)?;
        let mut free_page_ids = VecDeque::new();
        
        // Find the smallest reader TSN
        let smallest_reader_tsn = {
            let _lock = self.reader_tsns_lock.lock().unwrap();
            self.reader_tsns.values().min().cloned()
        };
        
        // Walk the tree to find leaf nodes
        let mut stack = vec![(root_page, 0)];
        
        while let Some((page, idx)) = stack.pop() {
            match &page.node {
                Node::FreeListInternal(node) => {
                    if idx < node.child_ids.len() {
                        stack.push((page.clone(), idx + 1));
                        let child_id = node.child_ids[idx];
                        let child = self.read_page(child_id)?;
                        stack.push((child, 0));
                    }
                }
                Node::FreeListLeaf(node) => {
                    for i in 0..node.keys.len() {
                        let tsn = node.keys[i];
                        if let Some(smallest) = smallest_reader_tsn {
                            if tsn > smallest {
                                return Ok(free_page_ids);
                            }
                        }
                        
                        let leaf_value = &node.values[i];
                        if leaf_value.root_id.is_none() {
                            for &page_id in &leaf_value.page_ids {
                                free_page_ids.push_back((page_id, tsn));
                            }
                        } else {
                            // TODO: Traverse into free list subtree
                            return Err(LmdbError::DatabaseCorrupted("Free list subtree not implemented".to_string()));
                        }
                    }
                }
                _ => return Err(LmdbError::DatabaseCorrupted("Invalid node type in free list tree".to_string())),
            }
        }
        
        Ok(free_page_ids)
    }
    
    pub fn commit(&mut self, writer: &mut LmdbWriter) -> Result<()> {
        // Process reused and freed page IDs
        while !writer.reused_page_ids.is_empty() || !writer.freed_page_ids.is_empty() {
            // Process reused page IDs
            while let Some((reused_page_id, tsn)) = writer.reused_page_ids.pop_front() {
                // Remove the reused page ID from the freed list tree
                remove_freed_page_id(self, writer, tsn, reused_page_id)?;
            }
            
            // Process freed page IDs
            while let Some(freed_page_id) = writer.freed_page_ids.pop_front() {
                // Remove dirty pages that were also freed
                writer.dirty.remove(&freed_page_id);
                
                // Insert the page ID in the freed list tree
                insert_freed_page_id(self, writer, writer.tsn, freed_page_id)?;
            }
        }
        
        // Write all dirty pages
        for page in writer.dirty.values() {
            self.write_page(page.clone())?;
        }
        
        // Write the new header page
        let header_page_id = if writer.header_page_id == self.header_page_id0 {
            self.header_page_id1
        } else {
            self.header_page_id0
        };
        
        let header_node = HeaderNode {
            tsn: writer.tsn,
            next_page_id: writer.next_page_id,
            free_list_root_id: writer.free_list_root_id,
            position_root_id: writer.position_root_id,
        };
        
        let header_page = Page::new(header_page_id, Node::Header(header_node));
        self.write_page(header_page)?;
        
        // Flush changes to disk
        self.flush()?;
        
        Ok(())
    }
    
    pub fn get_page(&mut self, writer: &LmdbWriter, page_id: PageID) -> Result<Page> {
        // Check if the page is in the dirty pages
        if let Some(page) = writer.dirty.get(&page_id) {
            return Ok(page.clone());
        }
        
        // Otherwise read it from disk
        self.read_page(page_id)
    }
}

// Implementation for Page
impl Page {
    pub fn new(page_id: PageID, node: Node) -> Self {
        Self {
            page_id,
            node,
            serialized: Vec::new(),
        }
    }
    
    pub fn serialize(&mut self) -> Result<()> {
        // Serialize the node
        let data = self.node.serialize()?;
        
        // Calculate CRC
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let crc = hasher.finalize();
        
        // Create the serialized data with header
        let mut serialized = Vec::with_capacity(PAGE_HEADER_SIZE + data.len());
        serialized.push(self.node.get_type_byte());
        serialized.extend_from_slice(&crc.to_le_bytes());
        serialized.extend_from_slice(&(data.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&data);
        
        self.serialized = serialized;
        Ok(())
    }
    
    pub fn deserialize(page_id: PageID, page_data: &[u8]) -> Result<Self> {
        if page_data.len() < PAGE_HEADER_SIZE {
            return Err(LmdbError::DatabaseCorrupted("Page data too short".to_string()));
        }
        
        // Extract header information
        let node_type = page_data[0];
        let crc = u32::from_le_bytes([
            page_data[1], page_data[2], page_data[3], page_data[4]
        ]);
        let data_len = u32::from_le_bytes([
            page_data[5], page_data[6], page_data[7], page_data[8]
        ]) as usize;
        
        if PAGE_HEADER_SIZE + data_len > page_data.len() {
            return Err(LmdbError::DatabaseCorrupted("Page data length mismatch".to_string()));
        }
        
        // Extract the data
        let data = &page_data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len];
        
        // Verify CRC
        let mut hasher = Hasher::new();
        hasher.update(data);
        let calculated_crc = hasher.finalize();
        
        if calculated_crc != crc {
            return Err(LmdbError::DatabaseCorrupted("CRC mismatch".to_string()));
        }
        
        // Deserialize the node
        let node = Node::deserialize(node_type, data)?;
        
        Ok(Self {
            page_id,
            node,
            serialized: page_data.to_vec(),
        })
    }
}

// Implementation for LmdbWriter
impl LmdbWriter {
    pub fn new(
        header_page_id: PageID,
        tsn: TSN,
        next_page_id: PageID,
        free_list_root_id: PageID,
        position_root_id: PageID,
        free_page_ids: VecDeque<(PageID, TSN)>,
    ) -> Self {
        Self {
            header_page_id,
            tsn,
            next_page_id,
            free_list_root_id,
            position_root_id,
            free_page_ids,
            freed_page_ids: VecDeque::new(),
            dirty: HashMap::new(),
            reused_page_ids: VecDeque::new(),
        }
    }

    pub fn mark_dirty(&mut self, page: Page) {
        self.dirty.insert(page.page_id, page);
    }

    pub fn alloc_page_id(&mut self) -> PageID {
        if let Some((free_page_id, tsn)) = self.free_page_ids.pop_front() {
            self.reused_page_ids.push_back((free_page_id, tsn));
            return free_page_id;
        }
        
        let next_page_id = self.next_page_id;
        self.next_page_id = PageID(next_page_id.0 + 1);
        next_page_id
    }

    pub fn make_dirty(&mut self, page: Page) -> (Page, Option<(PageID, PageID)>) {
        let mut replacement_info = None;
        
        if !self.freed_page_ids.iter().any(|&id| id == page.page_id) {
            if !self.dirty.contains_key(&page.page_id) {
                let old_page_id = page.page_id;
                self.freed_page_ids.push_back(old_page_id);
                
                let new_page_id = self.alloc_page_id();
                let new_page = Page {
                    page_id: new_page_id,
                    node: page.node.clone(),
                    serialized: Vec::new(),
                };
                
                self.dirty.insert(new_page_id, new_page.clone());
                replacement_info = Some((old_page_id, new_page_id));
                
                return (new_page, replacement_info);
            }
        }
        
        (page, replacement_info)
    }

    pub fn append_freed_page_id(&mut self, page_id: PageID) {
        if !self.freed_page_ids.iter().any(|&id| id == page_id) {
            self.freed_page_ids.push_back(page_id);
            self.dirty.remove(&page_id);
        }
    }
}

// Implementation for Pager
impl Pager {
    pub fn new(path: &Path, page_size: usize) -> io::Result<Self> {
        let is_file_new = !path.exists();
        
        let file = if is_file_new {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?
        } else {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?
        };
        
        Ok(Self {
            file,
            page_size,
            is_file_new,
        })
    }

    pub fn write_page(&mut self, page_id: PageID, page: &[u8]) -> io::Result<()> {
        if page.len() > self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Page overflow: page_id={:?} size={} > PAGE_SIZE={}", 
                        page_id, page.len(), self.page_size),
            ));
        }
        
        // Seek to the correct position
        self.file.seek(SeekFrom::Start((page_id.0 as u64) * (self.page_size as u64)))?;
        
        // Write the page data
        self.file.write_all(page)?;
        
        // Pad with zeros if needed
        let padding_size = self.page_size - page.len();
        if padding_size > 0 {
            let padding = vec![0u8; padding_size];
            self.file.write_all(&padding)?;
        }
        
        Ok(())
    }

    pub fn read_page(&mut self, page_id: PageID) -> io::Result<Vec<u8>> {
        let offset = (page_id.0 as u64) * (self.page_size as u64);
        self.file.seek(SeekFrom::Start(offset))?;
        
        let mut page = vec![0u8; self.page_size];
        let bytes_read = self.file.read(&mut page)?;
        
        if bytes_read < self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {:?} not found", page_id),
            ));
        }
        
        Ok(page)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        // fsync equivalent in Rust
        #[cfg(unix)]
        unsafe {
            let result = libc::fsync(self.file.as_raw_fd());
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

// Constants for serialization
const PAGE_TYPE_HEADER: u8 = b'1';
const PAGE_TYPE_FREELIST_LEAF: u8 = b'2';
const PAGE_TYPE_FREELIST_INTERNAL: u8 = b'3';
const PAGE_TYPE_POSITION_LEAF: u8 = b'4';
const PAGE_TYPE_POSITION_INTERNAL: u8 = b'5';

// Page header format: type(1) + crc(4) + len(4)
const PAGE_HEADER_SIZE: usize = 9;

// Error types
#[derive(Debug)]
pub enum LmdbError {
    Io(io::Error),
    PageNotFound(PageID),
    DatabaseCorrupted(String),
    SerializationError(String),
    DeserializationError(String),
}

impl From<io::Error> for LmdbError {
    fn from(err: io::Error) -> Self {
        LmdbError::Io(err)
    }
}

impl fmt::Display for LmdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LmdbError::Io(err) => write!(f, "IO error: {}", err),
            LmdbError::PageNotFound(page_id) => write!(f, "Page not found: {:?}", page_id),
            LmdbError::DatabaseCorrupted(msg) => write!(f, "Database corrupted: {}", msg),
            LmdbError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            LmdbError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
        }
    }
}

impl std::error::Error for LmdbError {}

// Result type alias
pub type Result<T> = std::result::Result<T, LmdbError>;

// Free list management functions
pub fn insert_freed_page_id(db: &mut Lmdb, writer: &mut LmdbWriter, tsn: TSN, freed_page_id: PageID) -> Result<()> {
    // Get the root page
    let current_page = db.get_page(writer, writer.free_list_root_id)?;
    
    // Traverse the tree to find a leaf node
    let mut stack: Vec<PageID> = Vec::new();
    let mut current_node = current_page.node.clone();
    
    while !matches!(current_node, Node::FreeListLeaf(_)) {
        if let Node::FreeListInternal(internal_node) = &current_node {
            stack.push(current_page.page_id);
            let child_page_id = internal_node.child_ids.last().unwrap();
            let child_page = db.get_page(writer, *child_page_id)?;
            current_node = child_page.node.clone();
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
        }
    }
    
    // Make the leaf page dirty
    let (mut leaf_page, replacement_info) = writer.make_dirty(current_page);
    
    // Find the place to insert the value
    if let Node::FreeListLeaf(ref mut leaf_node) = leaf_page.node {
        let leaf_idx = leaf_node.keys.iter().position(|&k| k == tsn);
        
        // Insert the value
        if let Some(idx) = leaf_idx {
            // TSN already exists, append to its page_ids
            if leaf_node.values[idx].root_id.is_none() {
                leaf_node.values[idx].page_ids.push(freed_page_id);
            } else {
                return Err(LmdbError::DatabaseCorrupted("Free list subtree not implemented".to_string()));
            }
        } else {
            // New TSN, add a new entry
            leaf_node.keys.push(tsn);
            leaf_node.values.push(FreeListLeafValue {
                page_ids: vec![freed_page_id],
                root_id: None,
            });
        }
        
        // Extract the node for serialization check
        let node_clone = leaf_page.node.clone();
        
        // Check if the page needs splitting by estimating the serialized size
        let data = node_clone.serialize()?;
        let estimated_size = PAGE_HEADER_SIZE + data.len();
        let needs_splitting = estimated_size > db.page_size;
        
        if needs_splitting {
            // Split the leaf node
            let last_key = leaf_node.keys.pop().unwrap();
            let last_value = leaf_node.values.pop().unwrap();
            
            let new_leaf_node = FreeListLeafNode {
                keys: vec![last_key],
                values: vec![last_value],
            };
            
            let new_leaf_page_id = writer.alloc_page_id();
            let new_leaf_page = Page::new(new_leaf_page_id, Node::FreeListLeaf(new_leaf_node));
            writer.mark_dirty(new_leaf_page.clone());
            
            // Check if the new leaf page needs splitting
            let new_leaf_needs_splitting = {
                let mut page_copy = new_leaf_page.clone();
                page_copy.serialize()?;
                page_copy.serialized.len() > db.page_size
            };
            
            if new_leaf_needs_splitting {
                return Err(LmdbError::DatabaseCorrupted("Overflow freed page IDs for TSN to subtree not implemented".to_string()));
            }
            
            // Propagate the split up the tree
            let mut split_info = Some((last_key, new_leaf_page_id));
            let mut current_replacement_info = replacement_info;
            
            // Propagate splits and replacements up the stack
            while let Some(parent_page_id) = stack.pop() {
                let parent_page = db.get_page(writer, parent_page_id)?;
                let (mut parent_page, parent_replacement_info) = writer.make_dirty(parent_page);
                
                if let Some((old_id, new_id)) = current_replacement_info {
                    if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                        // Replace the child ID
                        let last_idx = internal_node.child_ids.len() - 1;
                        if internal_node.child_ids[last_idx] == old_id {
                            internal_node.child_ids[last_idx] = new_id;
                        } else {
                            return Err(LmdbError::DatabaseCorrupted("Child ID mismatch".to_string()));
                        }
                    } else {
                        return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
                    }
                }
                
                if let Some((promoted_key, promoted_page_id)) = split_info {
                    if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                        // Add the promoted key and page ID
                        internal_node.keys.push(promoted_key);
                        internal_node.child_ids.push(promoted_page_id);
                    } else {
                        return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
                    }
                }
                
                // Check if the parent page needs splitting
                let parent_needs_splitting = {
                    parent_page.serialize()?;
                    parent_page.serialized.len() > db.page_size
                };
                
                if parent_needs_splitting {
                    if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                        // Split the internal node
                        let last_key = internal_node.keys.pop().unwrap();
                        let promoted_key = internal_node.keys.pop().unwrap();
                        let last_child_id = internal_node.child_ids.pop().unwrap();
                        let next_last_child_id = internal_node.child_ids.pop().unwrap();
                        
                        let new_internal_node = FreeListInternalNode {
                            keys: vec![last_key],
                            child_ids: vec![next_last_child_id, last_child_id],
                        };
                        
                        let new_internal_page_id = writer.alloc_page_id();
                        let new_internal_page = Page::new(new_internal_page_id, Node::FreeListInternal(new_internal_node));
                        writer.mark_dirty(new_internal_page);
                        
                        split_info = Some((promoted_key, new_internal_page_id));
                    } else {
                        return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
                    }
                } else {
                    split_info = None;
                }
                
                current_replacement_info = parent_replacement_info;
            }
            
            // Update the root if needed
            if let Some((old_id, new_id)) = current_replacement_info {
                if writer.free_list_root_id == old_id {
                    writer.free_list_root_id = new_id;
                } else {
                    return Err(LmdbError::DatabaseCorrupted("Root ID mismatch".to_string()));
                }
            }
            
            if let Some((promoted_key, promoted_page_id)) = split_info {
                // Create a new root
                let new_internal_node = FreeListInternalNode {
                    keys: vec![promoted_key],
                    child_ids: vec![writer.free_list_root_id, promoted_page_id],
                };
                
                let new_root_page_id = writer.alloc_page_id();
                let new_root_page = Page::new(new_root_page_id, Node::FreeListInternal(new_internal_node));
                writer.mark_dirty(new_root_page);
                
                writer.free_list_root_id = new_root_page_id;
            }
        } else {
            // No splitting needed, just update the page
            writer.mark_dirty(leaf_page);
            
            // Update the root if needed
            if let Some((old_id, new_id)) = replacement_info {
                if writer.free_list_root_id == old_id {
                    writer.free_list_root_id = new_id;
                }
            }
        }
    } else {
        return Err(LmdbError::DatabaseCorrupted("Expected FreeListLeaf node".to_string()));
    }
    
    Ok(())
}

// Position index functionality
pub fn insert_position(db: &mut Lmdb, writer: &mut LmdbWriter, key: Position, value: PositionIndexRecord) -> Result<()> {
    // Get the root page
    let current_page = db.get_page(writer, writer.position_root_id)?;
    
    // Traverse the tree to find a leaf node
    let mut stack: Vec<(PageID, usize)> = Vec::new();
    let mut current_node = current_page.node.clone();
    
    while !matches!(current_node, Node::PositionLeaf(_)) {
        if let Node::PositionInternal(internal_node) = &current_node {
            // Find the child node to traverse
            let mut child_index = 0;
            for (i, &k) in internal_node.keys.iter().enumerate() {
                if key < k {
                    break;
                }
                child_index = i + 1;
            }
            
            stack.push((current_page.page_id, child_index));
            let child_page_id = internal_node.child_ids[child_index];
            let child_page = db.get_page(writer, child_page_id)?;
            current_node = child_page.node.clone();
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected PositionInternal node".to_string()));
        }
    }
    
    // Make the leaf page dirty
    let (mut leaf_page, replacement_info) = writer.make_dirty(current_page);
    
    // Insert the key and value into the leaf node
    if let Node::PositionLeaf(ref mut leaf_node) = leaf_page.node {
        // Find the insertion point
        let mut insert_index = 0;
        for (i, &k) in leaf_node.keys.iter().enumerate() {
            if key < k {
                break;
            } else if key == k {
                // Replace existing value
                leaf_node.values[i] = value.clone();
                writer.mark_dirty(leaf_page);
                
                // Update the root if needed
                if let Some((old_id, new_id)) = replacement_info {
                    if writer.position_root_id == old_id {
                        writer.position_root_id = new_id;
                    }
                }
                
                return Ok(());
            }
            insert_index = i + 1;
        }
        
        // Insert the new key and value
        leaf_node.keys.insert(insert_index, key);
        leaf_node.values.insert(insert_index, value.clone());
        
        // Check if the page needs splitting
        let needs_splitting = {
            leaf_page.serialize()?;
            leaf_page.serialized.len() > db.page_size
        };
        
        if needs_splitting {
            // Split the leaf node
            let split_point = leaf_node.keys.len() / 2;
            let promoted_key = leaf_node.keys[split_point];
            
            // Create a new leaf node with the right half of the keys and values
            let new_leaf_node = PositionLeafNode {
                keys: leaf_node.keys.split_off(split_point),
                values: leaf_node.values.split_off(split_point),
                next_leaf_id: leaf_node.next_leaf_id,
            };
            
            // Update the next_leaf_id of the original leaf
            leaf_node.next_leaf_id = Some(writer.alloc_page_id());
            
            // Create a new page for the new leaf node
            let new_leaf_page = Page::new(leaf_node.next_leaf_id.unwrap(), Node::PositionLeaf(new_leaf_node));
            writer.mark_dirty(new_leaf_page.clone());
            
            // Propagate the split up the tree
            let mut split_info = Some((promoted_key, new_leaf_page.page_id));
            let mut current_replacement_info = replacement_info;
            
            // Propagate splits and replacements up the stack
            while let Some((parent_page_id, child_index)) = stack.pop() {
                let parent_page = db.get_page(writer, parent_page_id)?;
                let (mut parent_page, parent_replacement_info) = writer.make_dirty(parent_page);
                
                if let Some((old_id, new_id)) = current_replacement_info {
                    if let Node::PositionInternal(ref mut internal_node) = parent_page.node {
                        // Replace the child ID
                        if internal_node.child_ids[child_index] == old_id {
                            internal_node.child_ids[child_index] = new_id;
                        } else {
                            return Err(LmdbError::DatabaseCorrupted("Child ID mismatch".to_string()));
                        }
                    } else {
                        return Err(LmdbError::DatabaseCorrupted("Expected PositionInternal node".to_string()));
                    }
                }
                
                if let Some((promoted_key, promoted_page_id)) = split_info {
                    if let Node::PositionInternal(ref mut internal_node) = parent_page.node {
                        // Insert the promoted key and page ID
                        internal_node.keys.insert(child_index, promoted_key);
                        internal_node.child_ids.insert(child_index + 1, promoted_page_id);
                    } else {
                        return Err(LmdbError::DatabaseCorrupted("Expected PositionInternal node".to_string()));
                    }
                }
                
                // Check if the parent page needs splitting
                let parent_needs_splitting = {
                    parent_page.serialize()?;
                    parent_page.serialized.len() > db.page_size
                };
                
                if parent_needs_splitting {
                    if let Node::PositionInternal(ref mut internal_node) = parent_page.node {
                        // Split the internal node
                        let split_point = internal_node.keys.len() / 2;
                        let promoted_key = internal_node.keys[split_point];
                        
                        // Create a new internal node with the right half of the keys and child IDs
                        let right_keys = internal_node.keys.split_off(split_point + 1);
                        let right_child_ids = internal_node.child_ids.split_off(split_point + 1);
                        
                        let new_internal_node = PositionInternalNode {
                            keys: right_keys,
                            child_ids: right_child_ids,
                        };
                        
                        // Remove the promoted key from the left node
                        internal_node.keys.pop();
                        
                        // Create a new page for the new internal node
                        let new_internal_page_id = writer.alloc_page_id();
                        let new_internal_page = Page::new(new_internal_page_id, Node::PositionInternal(new_internal_node));
                        writer.mark_dirty(new_internal_page);
                        
                        split_info = Some((promoted_key, new_internal_page_id));
                    } else {
                        return Err(LmdbError::DatabaseCorrupted("Expected PositionInternal node".to_string()));
                    }
                } else {
                    split_info = None;
                }
                
                current_replacement_info = parent_replacement_info;
            }
            
            // Update the root if needed
            if let Some((old_id, new_id)) = current_replacement_info {
                if writer.position_root_id == old_id {
                    writer.position_root_id = new_id;
                } else {
                    return Err(LmdbError::DatabaseCorrupted("Root ID mismatch".to_string()));
                }
            }
            
            if let Some((promoted_key, promoted_page_id)) = split_info {
                // Create a new root
                let new_internal_node = PositionInternalNode {
                    keys: vec![promoted_key],
                    child_ids: vec![writer.position_root_id, promoted_page_id],
                };
                
                let new_root_page_id = writer.alloc_page_id();
                let new_root_page = Page::new(new_root_page_id, Node::PositionInternal(new_internal_node));
                writer.mark_dirty(new_root_page);
                
                writer.position_root_id = new_root_page_id;
            }
        } else {
            // No splitting needed, just update the page
            writer.mark_dirty(leaf_page);
            
            // Update the root if needed
            if let Some((old_id, new_id)) = replacement_info {
                if writer.position_root_id == old_id {
                    writer.position_root_id = new_id;
                }
            }
        }
    } else {
        return Err(LmdbError::DatabaseCorrupted("Expected PositionLeaf node".to_string()));
    }
    
    Ok(())
}

pub fn remove_freed_page_id(db: &mut Lmdb, writer: &mut LmdbWriter, tsn: TSN, used_page_id: PageID) -> Result<()> {
    // Get the root page
    let current_page = db.get_page(writer, writer.free_list_root_id)?;
    
    // Traverse the tree to find a leaf node
    let mut stack: Vec<PageID> = Vec::new();
    let mut current_node = current_page.node.clone();
    
    while !matches!(current_node, Node::FreeListLeaf(_)) {
        if let Node::FreeListInternal(internal_node) = &current_node {
            stack.push(current_page.page_id);
            let child_page_id = internal_node.child_ids[0];
            let child_page = db.get_page(writer, child_page_id)?;
            current_node = child_page.node.clone();
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
        }
    }
    
    // Make the leaf page dirty
    let (mut leaf_page, replacement_info) = writer.make_dirty(current_page);
    
    // Remove the page ID from the leaf node
    let mut removal_info = None;
    
    if let Node::FreeListLeaf(ref mut leaf_node) = leaf_page.node {
        // Assume we are exhausting page IDs from the lowest TSNs first
        if leaf_node.keys.is_empty() || leaf_node.keys[0] != tsn {
            return Err(LmdbError::DatabaseCorrupted(format!("Expected TSN {} not found", tsn.0)));
        }
        
        let leaf_value = &mut leaf_node.values[0];
        
        if leaf_value.root_id.is_some() {
            return Err(LmdbError::DatabaseCorrupted("Free list subtree not implemented".to_string()));
        } else {
            // Remove the page ID from the list
            if let Some(pos) = leaf_value.page_ids.iter().position(|&id| id == used_page_id) {
                leaf_value.page_ids.remove(pos);
            } else {
                return Err(LmdbError::DatabaseCorrupted(format!("Page ID {:?} not found in TSN {}", used_page_id, tsn.0)));
            }
            
            // If no more page IDs, remove the TSN entry
            if leaf_value.page_ids.is_empty() {
                leaf_node.keys.remove(0);
                leaf_node.values.remove(0);
                
                // If leaf is empty, mark it for removal
                if leaf_node.keys.is_empty() {
                    removal_info = Some(leaf_page.page_id);
                }
            }
        }
    } else {
        return Err(LmdbError::DatabaseCorrupted("Expected FreeListLeaf node".to_string()));
    }
    
    // Update the page
    writer.mark_dirty(leaf_page);
    
    // Propagate replacements and removals up the stack
    let mut current_replacement_info = replacement_info;
    
    while let Some(parent_page_id) = stack.pop() {
        let parent_page = db.get_page(writer, parent_page_id)?;
        let (mut parent_page, parent_replacement_info) = writer.make_dirty(parent_page);
        
        if let Some((old_id, new_id)) = current_replacement_info {
            if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                // Replace the child ID
                if internal_node.child_ids[0] == old_id {
                    internal_node.child_ids[0] = new_id;
                } else {
                    return Err(LmdbError::DatabaseCorrupted("Child ID mismatch".to_string()));
                }
            } else {
                return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
            }
        }
        
        if let Some(removed_page_id) = removal_info {
            writer.append_freed_page_id(removed_page_id);
            
            if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                // Remove the child ID and key
                if internal_node.child_ids[0] == removed_page_id {
                    internal_node.child_ids.remove(0);
                    
                    if !internal_node.keys.is_empty() {
                        internal_node.keys.remove(0);
                    }
                    
                    // If internal node is empty or has only one child, mark it for removal
                    if internal_node.keys.is_empty() {
                        assert_eq!(internal_node.child_ids.len(), 1);
                        let orphaned_child_id = internal_node.child_ids[0];
                        
                        writer.append_freed_page_id(parent_page.page_id);
                        
                        if let Some((old_id, _)) = parent_replacement_info {
                            current_replacement_info = Some((old_id, orphaned_child_id));
                        } else {
                            current_replacement_info = Some((parent_page.page_id, orphaned_child_id));
                        }
                    }
                } else {
                    return Err(LmdbError::DatabaseCorrupted("Child ID mismatch".to_string()));
                }
            } else {
                return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
            }
            
            removal_info = None;
        } else {
            current_replacement_info = parent_replacement_info;
        }
        
        // Update the page
        writer.mark_dirty(parent_page);
    }
    
    // Update the root if needed
    if let Some((old_id, new_id)) = current_replacement_info {
        if writer.free_list_root_id == old_id {
            writer.free_list_root_id = new_id;
        } else {
            return Err(LmdbError::DatabaseCorrupted("Root ID mismatch".to_string()));
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_lmdb_init() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        
        {
            let mut db = Lmdb::new(&db_path, 4096).unwrap();
            assert!(db.pager.is_file_new);
        }
        
        {
            let mut db = Lmdb::new(&db_path, 4096).unwrap();
            assert!(!db.pager.is_file_new);
        }
    }

    #[test]
    fn test_write_transaction_incrementing_tsn_and_alternating_header() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let mut db = Lmdb::new(&db_path, 4096).unwrap();
        
        {
            let writer = db.writer().unwrap();
            assert_eq!(TSN(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
        }
        
        {
            let writer = db.writer().unwrap();
            assert_eq!(TSN(2), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
        }
        
        {
            let writer = db.writer().unwrap();
            assert_eq!(TSN(3), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
        }
        
        {
            let writer = db.writer().unwrap();
            assert_eq!(TSN(4), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
        }
        
        {
            let writer = db.writer().unwrap();
            assert_eq!(TSN(5), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
        }
    }

    #[test]
    fn test_write_transaction_lock() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let db = Arc::new(Mutex::new(Lmdb::new(&db_path, 4096).unwrap()));
        let event = Arc::new(Mutex::new(false));
        
        let db_clone = Arc::clone(&db);
        let event_clone = Arc::clone(&event);
        
        let thread = thread::spawn(move || {
            let mut db = db_clone.lock().unwrap();
            let writer1 = db.writer().unwrap();
            
            // This should block because we already have a writer
            let _writer2 = db.writer().unwrap();
            
            // If we get here, set the event to true
            let mut event = event_clone.lock().unwrap();
            *event = true;
        });
        
        // Give the thread time to try to acquire the second writer
        thread::sleep(std::time::Duration::from_millis(100));
        
        // Check that the event is still false (thread is blocked)
        let event_value = *event.lock().unwrap();
        assert!(!event_value);
        
        // Let the thread complete
        drop(thread);
    }

    #[test]
    fn test_read_transaction_header_and_tsn() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let mut db = Lmdb::new(&db_path, 4096).unwrap();
        
        // Initial reader should see TSN 0
        {
            assert_eq!(0, db.reader_tsns.len());
            let reader = db.reader().unwrap();
            assert_eq!(1, db.reader_tsns.len());
            assert_eq!(vec![TSN(0)], db.reader_tsns.values().cloned().collect::<Vec<_>>());
            assert_eq!(PageID(0), reader.header_page_id);
            assert_eq!(TSN(0), reader.tsn);
        }
        assert_eq!(0, db.reader_tsns.len());
        
        // Multiple nested readers
        {
            let reader1 = db.reader().unwrap();
            assert_eq!(vec![TSN(0)], db.reader_tsns.values().cloned().collect::<Vec<_>>());
            assert_eq!(PageID(0), reader1.header_page_id);
            assert_eq!(TSN(0), reader1.tsn);
            
            {
                let reader2 = db.reader().unwrap();
                assert_eq!(vec![TSN(0), TSN(0)], db.reader_tsns.values().cloned().collect::<Vec<_>>());
                assert_eq!(PageID(0), reader2.header_page_id);
                assert_eq!(TSN(0), reader2.tsn);
                
                {
                    let reader3 = db.reader().unwrap();
                    assert_eq!(vec![TSN(0), TSN(0), TSN(0)], db.reader_tsns.values().cloned().collect::<Vec<_>>());
                    assert_eq!(PageID(0), reader3.header_page_id);
                    assert_eq!(TSN(0), reader3.tsn);
                }
            }
        }
        assert_eq!(0, db.reader_tsns.len());
        
        // Writer transaction
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(0, db.reader_tsns.len());
            assert_eq!(TSN(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
        
        // Reader after writer
        {
            let reader = db.reader().unwrap();
            assert_eq!(vec![TSN(1)], db.reader_tsns.values().cloned().collect::<Vec<_>>());
            assert_eq!(PageID(1), reader.header_page_id);
            assert_eq!(TSN(1), reader.tsn);
        }
    }

    #[test]
    fn test_write_transaction_recycles_pages() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let mut db = Lmdb::new(&db_path, 4096).unwrap();
        
        let key1 = Position(1);
        let value1 = PositionIndexRecord {
            segment: 0,
            offset: 0,
            type_hash: Vec::new(),
        };
        
        // First transaction
        {
            let mut writer = db.writer().unwrap();
            
            // Check there are no free pages
            assert_eq!(0, writer.free_page_ids.len());
            
            // Check the free list root is page 2
            assert_eq!(PageID(2), writer.free_list_root_id);
            
            // Check the position root is page 3
            assert_eq!(PageID(3), writer.position_root_id);
            
            // Insert position
            insert_position(&mut db, &mut writer, key1, value1.clone()).unwrap();
            
            // Check the dirty page IDs
            assert_eq!(1, writer.dirty.len());
            assert!(writer.dirty.contains_key(&PageID(4)));
            
            // Check the freed page IDs
            assert_eq!(1, writer.freed_page_ids.len());
            assert!(writer.freed_page_ids.contains(&PageID(3)));
            
            db.commit(&mut writer).unwrap();
        }
        
        // Read the position
        {
            let reader = db.reader().unwrap();
            
            // Read the position root
            let page = db.read_page(reader.position_root_id).unwrap();
            
            // Check it's a position leaf node
            if let Node::PositionLeaf(leaf_node) = &page.node {
                // Check the keys and values
                assert_eq!(vec![key1], leaf_node.keys);
                assert_eq!(vec![value1.clone()], leaf_node.values);
            } else {
                panic!("Expected PositionLeaf node");
            }
        }
        
        // Second transaction
        let key2 = Position(2);
        let value2 = PositionIndexRecord {
            segment: 0,
            offset: 100,
            type_hash: Vec::new(),
        };
        
        {
            let mut writer = db.writer().unwrap();
            
            // Check there are two free pages (the old position root and the old free list root)
            assert_eq!(2, writer.free_page_ids.len());
            assert!(writer.free_page_ids.iter().any(|(id, _)| *id == PageID(3)));
            assert!(writer.free_page_ids.iter().any(|(id, _)| *id == PageID(2)));
            
            // Insert position
            insert_position(&mut db, &mut writer, key2, value2.clone()).unwrap();
            
            db.commit(&mut writer).unwrap();
        }
        
        // Read the positions
        {
            let reader = db.reader().unwrap();
            
            // Read the position root
            let page = db.read_page(reader.position_root_id).unwrap();
            
            // Check it's a position leaf node
            if let Node::PositionLeaf(leaf_node) = &page.node {
                // Check the keys and values
                assert_eq!(vec![key1, key2], leaf_node.keys);
                assert_eq!(vec![value1.clone(), value2.clone()], leaf_node.values);
            } else {
                panic!("Expected PositionLeaf node");
            }
        }
    }

    // FreeListTree tests
    mod free_list_tree_tests {
        use super::*;
        use tempfile::tempdir;

        // Helper function to create a test database with a specified page size
        fn construct_db(page_size: usize) -> (tempfile::TempDir, Lmdb) {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("lmdb-test.db");
            let db = Lmdb::new(&db_path, page_size).unwrap();
            (temp_dir, db)
        }

        #[test]
        fn test_insert_freed_page_id_to_empty_leaf_root() {
            let (_temp_dir, mut db) = construct_db(32);
            
            // Get latest header
            let header = db.get_latest_header().unwrap();
            assert_eq!(PageID(0), header.page_id);
            
            // Get next page ID
            let next_page_id = match &header.node {
                Node::Header(node) => node.next_page_id,
                _ => panic!("Expected Header node"),
            };
            assert_eq!(PageID(4), next_page_id);
            
            // Construct a writer
            let tsn = TSN(1001);
            let free_list_root_id = match &header.node {
                Node::Header(node) => node.free_list_root_id,
                _ => panic!("Expected Header node"),
            };
            let position_root_id = match &header.node {
                Node::Header(node) => node.position_root_id,
                _ => panic!("Expected Header node"),
            };
            
            let mut txn = LmdbWriter::new(
                header.page_id,
                tsn,
                next_page_id,
                free_list_root_id,
                position_root_id,
                VecDeque::new(),
            );
            
            // Check the free list tree root ID
            let initial_root_id = txn.free_list_root_id;
            assert_eq!(PageID(2), initial_root_id);
            
            // Allocate a page ID (to be inserted as "freed")
            let page_id = txn.alloc_page_id();
            
            // Check the allocated page ID is the "next" page ID
            assert_eq!(next_page_id, page_id);
            
            // Insert allocated page ID in free list tree
            let current_tsn = txn.tsn;
            insert_freed_page_id(&mut db, &mut txn, current_tsn, page_id).unwrap();
            
            // Check root page has been CoW-ed
            let expected_new_root_id = PageID(next_page_id.0 + 1);
            assert_eq!(expected_new_root_id, txn.free_list_root_id);
            assert_eq!(1, txn.dirty.len());
            assert!(txn.dirty.contains_key(&expected_new_root_id));
            
            let new_root_page = txn.dirty.get(&expected_new_root_id).unwrap();
            assert_eq!(expected_new_root_id, new_root_page.page_id);
            
            let freed_page_ids: Vec<PageID> = txn.freed_page_ids.iter().cloned().collect();
            assert_eq!(vec![initial_root_id], freed_page_ids);
            
            // Check keys and values of the new root page
            match &new_root_page.node {
                Node::FreeListLeaf(node) => {
                    let expected_keys = vec![tsn];
                    assert_eq!(expected_keys, node.keys);
                    
                    let expected_values = vec![FreeListLeafValue {
                        page_ids: vec![next_page_id],
                        root_id: None,
                    }];
                    assert_eq!(expected_values, node.values);
                },
                _ => panic!("Expected FreeListLeaf node"),
            }
        }
        
        #[test]
        fn test_remove_freed_page_id_from_root_leaf_root() {
            let (_temp_dir, mut db) = construct_db(32);
            
            // First, insert a page ID
            let mut txn;
            let inserted_tsn;
            let inserted_page_id;
            let previous_root_id;
            
            {
                // Get a writer
                txn = db.writer().unwrap();
                
                // Remember the initial root ID
                previous_root_id = txn.free_list_root_id;
                
                // Allocate a page ID
                inserted_page_id = txn.alloc_page_id();
                
                // Insert the page ID
                inserted_tsn = txn.tsn;
                insert_freed_page_id(&mut db, &mut txn, inserted_tsn, inserted_page_id).unwrap();
                
                // Commit the transaction
                db.commit(&mut txn).unwrap();
            }
            
            // Block inserted page IDs from being reused
            {
                let _lock = db.reader_tsns_lock.lock().unwrap();
                db.reader_tsns.insert(0, TSN(0));
            }
            
            // Start a new writer to remove inserted freed page ID
            {
                txn = db.writer().unwrap();
                
                // Check there are no free pages (because we blocked them with a reader)
                assert_eq!(0, txn.free_page_ids.len());
                
                // Remember the initial root ID and next page ID
                let initial_root_id = txn.free_list_root_id;
                let next_page_id = txn.next_page_id;
                
                // Remove what was inserted
                remove_freed_page_id(&mut db, &mut txn, inserted_tsn, inserted_page_id).unwrap();
                
                // Check root page has been CoW-ed
                let expected_new_root_id = next_page_id;
                assert_eq!(expected_new_root_id, txn.free_list_root_id);
                assert_eq!(1, txn.dirty.len());
                assert!(txn.dirty.contains_key(&expected_new_root_id));
                
                let new_root_page = txn.dirty.get(&expected_new_root_id).unwrap();
                assert_eq!(expected_new_root_id, new_root_page.page_id);
                
                let freed_page_ids: Vec<PageID> = txn.freed_page_ids.iter().cloned().collect();
                assert_eq!(vec![initial_root_id], freed_page_ids);
                
                // Check keys and values of the new root page
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        let expected_keys = vec![inserted_tsn];
                        assert_eq!(expected_keys, node.keys);
                        
                        let expected_values = vec![FreeListLeafValue {
                            page_ids: vec![previous_root_id],
                            root_id: None,
                        }];
                        assert_eq!(expected_values, node.values);
                    },
                    _ => panic!("Expected FreeListLeaf node"),
                }
            }
        }
        
        #[test]
        fn test_insert_freed_page_ids_until_split_leaf() {
            let (_temp_dir, mut db) = construct_db(32);
            
            // Get latest header
            let header = db.get_latest_header().unwrap();
            
            // Extract header information
            let header_node = match &header.node {
                Node::Header(node) => node,
                _ => panic!("Expected Header node"),
            };
            
            // Create a writer
            let mut tsn = TSN(100);
            let mut txn = LmdbWriter::new(
                header.page_id,
                header_node.tsn,
                header_node.next_page_id,
                header_node.free_list_root_id,
                header_node.position_root_id,
                VecDeque::new(),
            );
            
            let mut has_split_leaf = false;
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            
            // Insert page IDs until we split a leaf
            while !has_split_leaf {
                // Allocate and insert first page ID
                let page_id1 = txn.alloc_page_id();
                insert_freed_page_id(&mut db, &mut txn, tsn, page_id1).unwrap();
                inserted.push((tsn, page_id1));
                
                // Allocate and insert second page ID
                let page_id2 = txn.alloc_page_id();
                insert_freed_page_id(&mut db, &mut txn, tsn, page_id2).unwrap();
                inserted.push((tsn, page_id2));
                
                // Increment TSN for next iteration
                tsn = TSN(tsn.0 + 1);
                
                // Check if we've split the leaf
                let root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(_) => {
                        has_split_leaf = true;
                    },
                    _ => {}
                }
            }
            
            // Check keys and values of all pages
            let mut copy_inserted = inserted.clone();
            
            // Get the root node
            let root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };
            
            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                txn.free_list_root_id,
                txn.position_root_id,
            ];
            
            // Collect freed page IDs
            let mut freed_page_ids: Vec<PageID> = txn.freed_page_ids.iter().cloned().collect();
            
            // Check each child of the root
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);
                
                let child_page = txn.dirty.get(&child_id).unwrap();
                assert_eq!(child_id, child_page.page_id);
                
                let child_node = match &child_page.node {
                    Node::FreeListLeaf(node) => node,
                    _ => panic!("Expected FreeListLeaf node"),
                };
                
                // Check that the keys are properly ordered
                if i > 0 {
                    assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
                }
                
                // Check each key and value in the child
                for (k, &key) in child_node.keys.iter().enumerate() {
                    for &value in &child_node.values[k].page_ids {
                        let (inserted_tsn, inserted_page_id) = copy_inserted.remove(0);
                        assert_eq!(inserted_tsn, key);
                        assert_eq!(inserted_page_id, value);
                        freed_page_ids.push(value);
                    }
                }
            }
            
            // We just split a leaf, so now we have 6 pages
            assert_eq!(6, active_page_ids.len());
            
            // Audit page IDs
            let mut all_page_ids = active_page_ids.clone();
            all_page_ids.extend(freed_page_ids.clone());
            all_page_ids.sort();
            all_page_ids.dedup();
            
            assert_eq!(
                all_page_ids.len(),
                active_page_ids.len() + freed_page_ids.len() - active_page_ids.iter().filter(|id| freed_page_ids.contains(id)).count()
            );
            
            // Check that all page IDs are accounted for
            let expected_page_ids: Vec<PageID> = (0..txn.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }
        
        #[test]
        fn test_remove_freed_page_ids_from_split_leaf() {
            let (_temp_dir, mut db) = construct_db(32);
            
            // First, insert page IDs until we split a leaf
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;
            
            {
                // Get a writer
                let mut txn = db.writer().unwrap();
                
                // Remember the initial root ID
                previous_root_id = txn.free_list_root_id;
                
                // Insert page IDs until we split a leaf
                let mut has_split_leaf = false;
                let mut tsn = txn.tsn;
                
                while !has_split_leaf {
                    // Increment TSN
                    tsn = TSN(tsn.0 + 1);
                    txn.tsn = tsn;
                    
                    // Allocate and insert first page ID
                    let page_id1 = txn.alloc_page_id();
                    insert_freed_page_id(&mut db, &mut txn, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));
                    
                    // Allocate and insert second page ID
                    let page_id2 = txn.alloc_page_id();
                    insert_freed_page_id(&mut db, &mut txn, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));
                    
                    // Check if we've split the leaf
                    let root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(_) => {
                            has_split_leaf = true;
                        },
                        _ => {}
                    }
                }
                
                // Remember the final TSN
                previous_writer_tsn = txn.tsn;
                
                // Commit the transaction
                db.commit(&mut txn).unwrap();
            }
            
            // Now remove all the inserted freed page IDs
            {
                // Get latest header
                let header = db.get_latest_header().unwrap();
                
                // Extract header information
                let (tsn, next_page_id, free_list_root_id, position_root_id) = match &header.node {
                    Node::Header(node) => (
                        TSN(node.tsn.0 + 1),
                        node.next_page_id,
                        node.free_list_root_id,
                        node.position_root_id
                    ),
                    _ => panic!("Expected Header node"),
                };
                
                // Create a new writer
                let mut txn = LmdbWriter::new(
                    header.page_id,
                    tsn,
                    next_page_id,
                    free_list_root_id,
                    position_root_id,
                    VecDeque::new(),
                );
                
                // Remember the initial root ID
                let old_root_id = txn.free_list_root_id;
                
                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    remove_freed_page_id(&mut db, &mut txn, *tsn, *page_id).unwrap();
                }
                
                // Check root page has been CoW-ed
                assert_ne!(old_root_id, txn.free_list_root_id);
                assert_eq!(1, txn.dirty.len());
                assert!(txn.dirty.contains_key(&txn.free_list_root_id));
                
                let new_root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
                assert_eq!(txn.free_list_root_id, new_root_page.page_id);
                
                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = txn.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));
                
                // There were three pages, and now we have one. We have
                // freed page IDs for three old pages and two CoW pages.
                assert_eq!(5, txn.freed_page_ids.len());
                
                // Check keys and values of the new root page
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        let expected_keys = vec![previous_writer_tsn];
                        assert_eq!(expected_keys, node.keys);
                        
                        let expected_values = vec![FreeListLeafValue {
                            page_ids: vec![previous_root_id],
                            root_id: None,
                        }];
                        assert_eq!(expected_values, node.values);
                    },
                    _ => panic!("Expected FreeListLeaf node"),
                }
                
                // Audit page IDs
                let active_page_ids = vec![
                    db.header_page_id0,
                    db.header_page_id1,
                    txn.free_list_root_id,
                    txn.position_root_id,
                ];
                
                // Collect all freed page IDs
                let mut all_freed_page_ids = freed_page_ids.clone();
                
                // Add page IDs from the leaf node
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        for (_, value) in node.keys.iter().zip(node.values.iter()) {
                            all_freed_page_ids.extend(value.page_ids.clone());
                        }
                    },
                    _ => panic!("Expected FreeListLeaf node"),
                }
                
                // Add inserted page IDs
                let inserted_page_ids: Vec<PageID> = inserted.iter().map(|(_, id)| *id).collect();
                
                // Collect all page IDs
                let mut all_page_ids = active_page_ids.clone();
                all_page_ids.extend(all_freed_page_ids.clone());
                all_page_ids.extend(inserted_page_ids.clone());
                all_page_ids.sort();
                all_page_ids.dedup();
                
                // Check that all page IDs are accounted for
                let expected_page_ids: Vec<PageID> = (0..txn.next_page_id.0).map(PageID).collect();
                assert_eq!(expected_page_ids, all_page_ids);
            }
        }
        
        #[test]
        fn test_insert_freed_page_ids_until_split_internal() {
            let (_temp_dir, mut db) = construct_db(32);
            
            // Get latest header
            let header = db.get_latest_header().unwrap();
            
            // Extract header information
            let (tsn, next_page_id, free_list_root_id, position_root_id) = match &header.node {
                Node::Header(node) => (
                    node.tsn,
                    node.next_page_id,
                    node.free_list_root_id,
                    node.position_root_id
                ),
                _ => panic!("Expected Header node"),
            };
            
            // Create a writer
            let mut txn = LmdbWriter::new(
                header.page_id,
                tsn,
                next_page_id,
                free_list_root_id,
                position_root_id,
                VecDeque::new(),
            );
            
            // Start with TSN 100
            let mut tsn = TSN(100);
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            let mut has_split_internal = false;
            
            // Insert page IDs until we split an internal node
            while !has_split_internal {
                // Allocate and insert first page ID
                let page_id1 = txn.alloc_page_id();
                insert_freed_page_id(&mut db, &mut txn, tsn, page_id1).unwrap();
                inserted.push((tsn, page_id1));
                
                // Allocate and insert second page ID
                let page_id2 = txn.alloc_page_id();
                insert_freed_page_id(&mut db, &mut txn, tsn, page_id2).unwrap();
                inserted.push((tsn, page_id2));
                
                // Increment TSN for next iteration
                tsn = TSN(tsn.0 + 1);
                
                // Check if we've split an internal node
                let root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(root_node) => {
                        // Check if the first child is an internal node
                        if !root_node.child_ids.is_empty() {
                            let child_id = root_node.child_ids[0];
                            if let Some(child_page) = txn.dirty.get(&child_id) {
                                match &child_page.node {
                                    Node::FreeListInternal(_) => {
                                        has_split_internal = true;
                                    },
                                    _ => {}
                                }
                            }
                        }
                    },
                    _ => {}
                }
            }
            
            // Check keys and values of all pages
            let root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };
            
            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                txn.free_list_root_id,
                txn.position_root_id,
            ];
            
            // Collect freed page IDs
            let mut freed_page_ids: Vec<PageID> = txn.freed_page_ids.iter().cloned().collect();
            
            // Track the previous child for key ordering checks
            let mut previous_child: Option<&FreeListInternalNode> = None;
            
            // Check each child of the root
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);
                
                let child_page = txn.dirty.get(&child_id).unwrap();
                assert_eq!(child_id, child_page.page_id);
                
                let child_node = match &child_page.node {
                    Node::FreeListInternal(node) => node,
                    _ => panic!("Expected FreeListInternal node"),
                };
                
                // Check key ordering between root and child
                if i > 0 {
                    assert!(root_node.keys[i - 1] < child_node.keys[0]);
                    
                    // Check key ordering between previous child and current child
                    if let Some(prev_child) = previous_child {
                        assert!(root_node.keys[i - 1] > *prev_child.keys.last().unwrap());
                    }
                }
                
                previous_child = Some(child_node);
                
                // Check each grandchild
                for (j, &grand_child_id) in child_node.child_ids.iter().enumerate() {
                    active_page_ids.push(grand_child_id);
                    
                    let grand_child_page = txn.dirty.get(&grand_child_id).unwrap();
                    assert_eq!(grand_child_id, grand_child_page.page_id);
                    
                    let grand_child_node = match &grand_child_page.node {
                        Node::FreeListLeaf(node) => node,
                        _ => panic!("Expected FreeListLeaf node"),
                    };
                    
                    // Check key ordering between child and grandchild
                    if j > 0 {
                        assert_eq!(child_node.keys[j - 1], grand_child_node.keys[0]);
                    }
                    
                    // Check each key and value in the grandchild
                    for (k, &key) in grand_child_node.keys.iter().enumerate() {
                        for &value in &grand_child_node.values[k].page_ids {
                            // Find the matching inserted item
                            let pos = inserted.iter().position(|&(t, p)| t == key && p == value);
                            if let Some(idx) = pos {
                                inserted.remove(idx);
                            }
                            freed_page_ids.push(value);
                        }
                    }
                }
            }
            
            // We should have processed all inserted items
            assert!(inserted.is_empty());
            
            // We should have 18 active pages
            assert_eq!(18, active_page_ids.len());
            
            // Audit page IDs
            let mut all_page_ids = active_page_ids.clone();
            all_page_ids.extend(freed_page_ids.clone());
            all_page_ids.sort();
            all_page_ids.dedup();
            
            // Check that all page IDs are accounted for
            let expected_page_ids: Vec<PageID> = (0..txn.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }
        
        #[test]
        fn test_remove_freed_page_ids_from_split_internal() {
            let (_temp_dir, mut db) = construct_db(32);
            
            // First, insert page IDs until we split an internal node
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;
            
            {
                // Get a writer
                let mut txn = db.writer().unwrap();
                
                // Remember the initial root ID
                previous_root_id = txn.free_list_root_id;
                
                // Insert page IDs until we split an internal node
                let mut has_split_internal = false;
                let mut tsn = txn.tsn;
                
                while !has_split_internal {
                    // Increment TSN
                    tsn = TSN(tsn.0 + 1);
                    txn.tsn = tsn;
                    
                    // Allocate and insert first page ID
                    let page_id1 = txn.alloc_page_id();
                    insert_freed_page_id(&mut db, &mut txn, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));
                    
                    // Allocate and insert second page ID
                    let page_id2 = txn.alloc_page_id();
                    insert_freed_page_id(&mut db, &mut txn, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));
                    
                    // Check if we've split an internal node
                    let root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(root_node) => {
                            // Check if the first child is an internal node
                            if !root_node.child_ids.is_empty() {
                                let child_id = root_node.child_ids[0];
                                if let Some(child_page) = txn.dirty.get(&child_id) {
                                    match &child_page.node {
                                        Node::FreeListInternal(_) => {
                                            has_split_internal = true;
                                        },
                                        _ => {}
                                    }
                                }
                            }
                        },
                        _ => {}
                    }
                }
                
                // Remember the final TSN
                previous_writer_tsn = txn.tsn;
                
                // Commit the transaction
                db.commit(&mut txn).unwrap();
            }
            
            // Now remove all the inserted freed page IDs
            {
                // Get latest header
                let header = db.get_latest_header().unwrap();
                
                // Extract header information
                let (tsn, next_page_id, free_list_root_id, position_root_id) = match &header.node {
                    Node::Header(node) => (
                        TSN(node.tsn.0 + 1),
                        node.next_page_id,
                        node.free_list_root_id,
                        node.position_root_id
                    ),
                    _ => panic!("Expected Header node"),
                };
                
                // Create a new writer
                let mut txn = LmdbWriter::new(
                    header.page_id,
                    tsn,
                    next_page_id,
                    free_list_root_id,
                    position_root_id,
                    VecDeque::new(),
                );
                
                // Remember the initial root ID
                let old_root_id = txn.free_list_root_id;
                
                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    remove_freed_page_id(&mut db, &mut txn, *tsn, *page_id).unwrap();
                }
                
                // Check root page has been CoW-ed
                assert_ne!(old_root_id, txn.free_list_root_id);
                assert_eq!(1, txn.dirty.len());
                assert!(txn.dirty.contains_key(&txn.free_list_root_id));
                
                let new_root_page = txn.dirty.get(&txn.free_list_root_id).unwrap();
                assert_eq!(txn.free_list_root_id, new_root_page.page_id);
                
                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = txn.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));
                
                // There were 15 pages, and now we have 1. We have
                // freed page IDs for 15 old pages and 14 CoW pages.
                assert_eq!(29, txn.freed_page_ids.len());
                
                // Check keys and values of the new root page
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        let expected_keys = vec![previous_writer_tsn];
                        assert_eq!(expected_keys, node.keys);
                        
                        let expected_values = vec![FreeListLeafValue {
                            page_ids: vec![previous_root_id],
                            root_id: None,
                        }];
                        assert_eq!(expected_values, node.values);
                    },
                    _ => panic!("Expected FreeListLeaf node"),
                }
                
                // Audit page IDs
                let active_page_ids = vec![
                    db.header_page_id0,
                    db.header_page_id1,
                    txn.free_list_root_id,
                    txn.position_root_id,
                ];
                
                // Collect all freed page IDs
                let mut all_freed_page_ids = freed_page_ids.clone();
                
                // Add page IDs from the leaf node
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        for (_, value) in node.keys.iter().zip(node.values.iter()) {
                            all_freed_page_ids.extend(value.page_ids.clone());
                        }
                    },
                    _ => panic!("Expected FreeListLeaf node"),
                }
                
                // Add inserted page IDs
                let inserted_page_ids: Vec<PageID> = inserted.iter().map(|(_, id)| *id).collect();
                
                // Collect all page IDs
                let mut all_page_ids = active_page_ids.clone();
                all_page_ids.extend(all_freed_page_ids.clone());
                all_page_ids.extend(inserted_page_ids.clone());
                all_page_ids.sort();
                all_page_ids.dedup();
                
                // Check that all page IDs are accounted for
                let expected_page_ids: Vec<PageID> = (0..txn.next_page_id.0).map(PageID).collect();
                assert_eq!(expected_page_ids, all_page_ids);
            }
        }
    }
}