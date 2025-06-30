use std::path::Path;
use std::collections::HashMap;
use crate::pagedfile::{PagedFile, PAGE_SIZE, PageID, PagedFileError};
use serde::{Serialize, Deserialize};
use rmp_serde::{encode, decode};
use lru::LruCache;
use crate::wal::calc_crc;
use std::any::Any;

/// Constant for the header node type
pub const HEADER_NODE_TYPE: u8 = 1;

/// A trait for nodes that can be serialized to msgpack format
pub trait Node: Any {
    /// Serializes the node to msgpack format
    ///
    /// # Returns
    /// * `Result<Vec<u8>, rmp_serde::encode::Error>` - The serialized data or an error
    fn to_msgpack(&self) -> Result<Vec<u8>, encode::Error>;

    /// Returns a byte that identifies the type of node
    ///
    /// # Returns
    /// * `u8` - A byte that identifies the type of node
    fn node_type_byte(&self) -> u8;

    /// Returns self as Any for downcasting
    fn as_any(&self) -> &dyn Any;

    /// Returns self as mutable Any for downcasting
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Serializes the node to a byte array
    ///
    /// # Returns
    /// * `Vec<u8>` - The serialized data
    fn serialize(&self) -> Vec<u8>;

    /// Calculates the size of the serialized node
    ///
    /// # Returns
    /// * `usize` - The size of the serialized node in bytes
    fn calc_serialized_size(&self) -> usize;

    /// Calculates the size of the serialized page including the node type byte, CRC, and data length
    ///
    /// # Returns
    /// * `usize` - The size of the serialized page in bytes
    fn calc_serialized_page_size(&self) -> usize {
        // 1 byte for node type + 4 bytes for CRC + 4 bytes for data length + serialized size
        self.calc_serialized_size() + 9
    }

    /// Serializes the node to a page format including node type byte, CRC, and data length
    ///
    /// # Returns
    /// * `Vec<u8>` - The serialized page data
    fn serialize_page(&self) -> Vec<u8> {
        // Get the node data using serialize
        let node_data = self.serialize();

        // Get the node type byte
        let node_type_byte = self.node_type_byte();

        // Calculate CRC
        let crc = calc_crc(&node_data);

        // Get the length of the serialized data
        let data_len = node_data.len() as u32;

        // Create a buffer with enough capacity for all components
        let mut result = Vec::with_capacity(1 + 4 + 4 + node_data.len());

        // Concatenate: node type byte + CRC + length + serialized data
        result.push(node_type_byte);
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&data_len.to_le_bytes());
        result.extend_from_slice(&node_data);

        result
    }
}

/// A structure that represents a header node in the index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderNode {
    pub root_page_id: PageID,
    pub next_page_id: PageID,
}

impl HeaderNode {
    /// Serializes the HeaderNode to msgpack format
    ///
    /// # Returns
    /// * `Result<Vec<u8>, rmp_serde::encode::Error>` - The serialized data or an error
    pub fn to_msgpack(&self) -> Result<Vec<u8>, encode::Error> {
        encode::to_vec(self)
    }

    /// Serializes the HeaderNode to a byte array with 8 bytes
    /// First 4 bytes for root_page_id and next 4 bytes for next_page_id
    ///
    /// # Returns
    /// * `Vec<u8>` - The serialized data
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(8);
        result.extend_from_slice(&self.root_page_id.0.to_le_bytes());
        result.extend_from_slice(&self.next_page_id.0.to_le_bytes());
        result
    }

    /// Calculates the size of the serialized HeaderNode
    ///
    /// # Returns
    /// * `usize` - The size of the serialized HeaderNode in bytes
    pub fn calc_serialized_size(&self) -> usize {
        8 // 4 bytes for root_page_id + 4 bytes for next_page_id
    }

    /// Creates a HeaderNode from a byte slice
    /// Expects a slice with 8 bytes, first 4 bytes for root_page_id and next 4 bytes for next_page_id
    ///
    /// # Arguments
    /// * `slice` - The byte slice to deserialize from
    ///
    /// # Returns
    /// * `Result<HeaderNode, decode::Error>` - The deserialized HeaderNode or an error
    pub fn from_slice(slice: &[u8]) -> Result<Self, decode::Error> {
        if slice.len() != 8 {
            return Err(decode::Error::InvalidMarkerRead(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Expected 8 bytes, got {}", slice.len()),
            )));
        }

        let root_page_id = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]);
        let next_page_id = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]);

        Ok(HeaderNode {
            root_page_id: PageID(root_page_id),
            next_page_id: PageID(next_page_id),
        })
    }
}

impl Node for HeaderNode {
    fn to_msgpack(&self) -> Result<Vec<u8>, encode::Error> {
        encode::to_vec(self)
    }

    fn node_type_byte(&self) -> u8 {
        HEADER_NODE_TYPE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        self.serialize()
    }

    fn calc_serialized_size(&self) -> usize {
        self.calc_serialized_size()
    }
}

/// A structure that represents an index page
pub struct IndexPage {
    pub page_id: PageID,
    pub node: Box<dyn Node>,
    pub serialized: Vec<u8>,
}

/// A type for node-specific decode functions
pub type DecodeFn = fn(&[u8]) -> Result<Box<dyn Node>, decode::Error>;

/// A structure that can deserialize index pages
pub struct Deserializer {
    /// Map of node type bytes to decode functions
    decoders: HashMap<u8, DecodeFn>,
}

impl Deserializer {
    /// Creates a new Deserializer
    pub fn new() -> Self {
        Deserializer {
            decoders: HashMap::new(),
        }
    }

    /// Registers a decode function for a node type byte
    ///
    /// # Arguments
    /// * `node_type_byte` - The node type byte
    /// * `decode_fn` - The decode function
    pub fn register(&mut self, node_type_byte: u8, decode_fn: DecodeFn) {
        self.decoders.insert(node_type_byte, decode_fn);
    }

    /// Deserializes a page
    ///
    /// # Arguments
    /// * `data` - The serialized page data
    /// * `page_id` - The page ID
    ///
    /// # Returns
    /// * `Result<IndexPage, decode::Error>` - The deserialized page or an error
    pub fn deserialize_page(&self, data: &[u8], page_id: PageID) -> Result<IndexPage, decode::Error> {
        // Extract the node type byte
        if data.is_empty() {
            return Err(decode::Error::InvalidMarkerRead(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Empty data",
            )));
        }
        let node_type_byte = data[0];

        // Extract the CRC and blob length
        if data.len() < 9 {
            return Err(decode::Error::InvalidMarkerRead(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Data too short for header",
            )));
        }
        let crc_bytes = &data[1..5];
        let len_bytes = &data[5..9];
        let crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        let blob_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

        // Extract the blob
        if data.len() < 9 + blob_len {
            return Err(decode::Error::InvalidMarkerRead(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Data too short for blob",
            )));
        }
        let blob = &data[9..9 + blob_len];

        // Verify the CRC
        let calculated_crc = calc_crc(blob);
        if calculated_crc != crc {
            return Err(decode::Error::InvalidMarkerRead(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "CRC mismatch",
            )));
        }

        // Get the decode function for the node type
        let decode_fn = self.decoders.get(&node_type_byte).ok_or_else(|| {
            decode::Error::InvalidMarkerRead(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("No decoder registered for node type byte: {}", node_type_byte),
            ))
        })?;

        // Deserialize the node
        let node = decode_fn(blob)?;

        // Create and return the IndexPage
        Ok(IndexPage {
            page_id,
            node,
            serialized: Vec::new(),
        })
    }
}

impl IndexPage {
    /// Serializes the page data
    ///
    /// # Returns
    /// * `Result<Vec<u8>, encode::Error>` - The serialized data or an error
    pub fn serialize_page(&self) -> Result<Vec<u8>, encode::Error> {
        // Call node.to_msgpack() only once
        let node_data = self.node.serialize_page();
        Ok(node_data)
    }
}

/// A structure that manages index pages
pub struct IndexPages {
    pub paged_file: PagedFile,
    dirty: HashMap<PageID, bool>,
    header_page_id: PageID,
    /// The header page containing the header node
    pub header_page: IndexPage,
    /// Cache of IndexPage objects keyed by PageID
    /// This cache must be unbounded and should never be changed to a bounded cache
    cache: LruCache<PageID, IndexPage>,
    /// Deserializer for index pages
    pub deserializer: Deserializer,
    /// Cache capacity
    pub cache_capacity: usize,
}

impl IndexPages {
    /// Gets a reference to the HeaderNode from the header page
    ///
    /// # Returns
    /// * `&HeaderNode` - A reference to the HeaderNode
    pub fn header_node(&self) -> &HeaderNode {
        self.header_page.node.as_any().downcast_ref::<HeaderNode>()
            .expect("Failed to downcast node to HeaderNode")
    }

    /// Gets a mutable reference to the HeaderNode from the header page
    ///
    /// # Returns
    /// * `&mut HeaderNode` - A mutable reference to the HeaderNode
    fn header_node_mut(&mut self) -> &mut HeaderNode {
        self.header_page.node.as_any_mut().downcast_mut::<HeaderNode>()
            .expect("Failed to downcast node to HeaderNode")
    }

    /// Creates a new IndexPages with the given path and page size
    pub fn new<P: AsRef<Path>>(path: P, page_size: usize) -> std::io::Result<Self> {
        Self::new_with_cache_capacity(path, page_size, None)
    }

    /// Creates a new IndexPages with the given path, page size, and cache capacity
    pub fn new_with_cache_capacity<P: AsRef<Path>>(path: P, page_size: usize, cache_capacity: Option<usize>) -> std::io::Result<Self> {
        let cache_capacity = cache_capacity.unwrap_or(1024);
        let mut paged_file = PagedFile::new(path, Some(page_size))?;

        // Create a new Deserializer
        let mut deserializer = Deserializer::new();

        // Register the HEADER_NODE_TYPE with a decode function for HeaderNode
        deserializer.register(HEADER_NODE_TYPE, |data| {
            let header_node: HeaderNode = HeaderNode::from_slice(data)?;
            Ok(Box::new(header_node) as Box<dyn Node>)
        });

        // Define the header page ID
        let header_page_id = PageID(0);

        // Check if the file exists
        let header_page = if paged_file.new {
            // File exists, read the header page from disk
            let header_page_data = paged_file.read_page(header_page_id)
                .map_err(|e| std::io::Error::new(
                    match e {
                        PagedFileError::Io(ref io_err) => io_err.kind(),
                        _ => std::io::ErrorKind::Other,
                    },
                    format!("Failed to read header page: {}", e)
                ))?;

            // Deserialize the header page
            deserializer.deserialize_page(&header_page_data, header_page_id)
                .map_err(|e| std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to deserialize header page: {}", e)
                ))?
        } else {
            // File doesn't exist, create a new header node with default values
            let header_node = HeaderNode {
                root_page_id: PageID(1),
                next_page_id: PageID(2),
            };

            // Create the header page
            IndexPage {
                page_id: header_page_id,
                node: Box::new(header_node),
                serialized: Vec::new(),
            }
        };

        // Create the IndexPages instance
        let mut index_pages = IndexPages {
            paged_file,
            dirty: HashMap::new(),
            header_page_id,
            header_page,
            // Initialize the cache as unbounded - this is a requirement and must not be changed
            cache: LruCache::unbounded(),
            deserializer,
            cache_capacity,
        };

        // If the file doesn't exist, mark the header page as dirty and flush it to disk
        if !index_pages.paged_file.new {
            index_pages.mark_dirty(header_page_id);
            index_pages.flush()?;
        }

        Ok(index_pages)
    }

    /// Marks a page as dirty
    ///
    /// # Arguments
    /// * `page_id` - The page ID to mark as dirty
    pub fn mark_dirty(&mut self, page_id: PageID) {
        self.dirty.insert(page_id, true);
    }

    /// Clears all entries from the dirty HashMap
    pub fn clear_dirty(&mut self) {
        self.dirty.clear();
    }

    /// Reduces the cache to the cache_capacity by removing the least recently used items
    pub fn reduce_cache(&mut self) {
        // Evict entries until we're at capacity
        while self.cache.len() > self.cache_capacity {
            self.cache.pop_lru();
        }
    }

    /// Adds a page to the cache and marks it as dirty
    ///
    /// # Arguments
    /// * `page` - The IndexPage to add
    pub fn add_page(&mut self, page: IndexPage) {
        let page_id = page.page_id;
        self.cache.put(page_id, page);
        self.mark_dirty(page_id);
    }

    /// Sets the root page ID in the header node and marks the header page as dirty
    ///
    /// # Arguments
    /// * `root_page_id` - The new root page ID
    pub fn set_root_page_id(&mut self, root_page_id: PageID) {
        // Get a mutable reference to the HeaderNode in the heap
        let header_node = self.header_node_mut();

        // Update the root_page_id directly in the heap
        header_node.root_page_id = root_page_id;

        // Mark the header page as dirty
        self.mark_dirty(self.header_page_id);
    }

    /// Sets the next page ID in the header node and marks the header page as dirty
    ///
    /// # Arguments
    /// * `next_page_id` - The new next page ID
    pub fn set_next_page_id(&mut self, next_page_id: PageID) {
        // Get a mutable reference to the HeaderNode in the heap
        let header_node = self.header_node_mut();

        // Update the next_page_id directly in the heap
        header_node.next_page_id = next_page_id;

        // Mark the header page as dirty
        self.mark_dirty(self.header_page_id);
    }

    /// Allocates a new page ID
    ///
    /// # Returns
    /// * `PageID` - The allocated page ID
    pub fn alloc_page_id(&mut self) -> PageID {
        // Get the current next_page_id
        let current_next_page_id = self.header_node().next_page_id;

        // Increment the next_page_id
        self.set_next_page_id(PageID(current_next_page_id.0 + 1));

        // Return the original next_page_id
        current_next_page_id
    }

    /// Gets a page from the cache or reads it from disk
    ///
    /// # Arguments
    /// * `page_id` - The page ID to get
    ///
    /// # Returns
    /// * `std::io::Result<&IndexPage>` - A reference to the page or an error
    pub fn get_page(&mut self, page_id: PageID) -> std::io::Result<&IndexPage> {
        // If the page is the header page, return a reference to the header page
        if page_id == self.header_page_id {
            return Ok(&self.header_page);
        }

        // Check if the page is in the cache
        if !self.cache.contains(&page_id) {
            // Page is not in the cache, read it from disk
            let page_data = self.paged_file.read_page(page_id)
                .map_err(|e| std::io::Error::new(
                    match e {
                        PagedFileError::Io(ref io_err) => io_err.kind(),
                        _ => std::io::ErrorKind::Other,
                    },
                    format!("Failed to read page: {}", e)
                ))?;

            // Deserialize the page
            let page = self.deserializer.deserialize_page(&page_data, page_id)
                .map_err(|e| std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to deserialize page: {}", e)
                ))?;

            // Add the page to the cache
            self.cache.put(page_id, page);
        }

        // Get the page from the cache
        Ok(self.cache.get(&page_id).unwrap())
    }

    /// Gets a mutable reference to a page from the cache
    ///
    /// # Arguments
    /// * `page_id` - The page ID to get
    ///
    /// # Returns
    /// * `std::io::Result<&mut IndexPage>` - A mutable reference to the page or an error
    pub fn get_page_mut(&mut self, page_id: PageID) -> std::io::Result<&mut IndexPage> {
        // If the page is the header page, return an error
        if page_id == self.header_page_id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot get header page from cache",
            ));
        }

        let page_ref = self.cache.get_mut(&page_id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Page not found in cache: {:?}", page_id),
            )
        })?;
        Ok(page_ref)
    }

    /// Flushes all dirty pages to disk
    ///
    /// # Returns
    /// * `std::io::Result<()>` - Success or an error
    pub fn flush(&mut self) -> std::io::Result<()> {
        // Iterate over the PageID values in the dirty HashMap
        for page_id in self.dirty.keys() {
            // Get the page
            let page = if *page_id == self.header_page_id {
                // If the page is the header page, use header_page
                &self.header_page
            } else {
                // Otherwise, get the page from the cache
                match self.cache.get(page_id) {
                    Some(page) => page,
                    None => return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Page not found in cache: {:?}", page_id)
                    )),
                }
            };

            // Serialize the page
            let serialized_data = page.serialize_page()
                .map_err(|e| std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to serialize page: {}", e)
                ))?;

            // Write the page to the paged file
            self.paged_file.write_page(*page_id, &serialized_data)
                .map_err(|e| std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to write page: {}", e)
                ))?;
        }

        // Flush and fsync the paged file
        self.paged_file.flush_and_fsync()
            .map_err(|e| std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to flush and fsync: {}", e)
            ))?;

        // Clear the dirty HashMap
        self.clear_dirty();

        // Reduce the cache to the cache_capacity
        self.reduce_cache();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::iter;

    #[test]
    fn test_index_pages_creation() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");

        // Append the filename to the directory path
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages using the constructor
        let index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Check that header_page_id equals PageID(0)
        assert_eq!(index_pages.header_page_id, PageID(0), 
                   "header_page_id should be initialized to PageID(0)");

        // Check that header_node.root_page_id equals PageID(1)
        assert_eq!(index_pages.header_node().root_page_id, PageID(1),
                   "header_node.root_page_id should be initialized to PageID(1)");

        // Check that header_node.next_page_id equals PageID(2)
        assert_eq!(index_pages.header_node().next_page_id, PageID(2),
                   "header_node.next_page_id should be initialized to PageID(2)");

        // Check that the cache is initialized and empty
        assert!(index_pages.cache.is_empty(), 
                "Cache should be initialized as empty");

        // Check that cache_capacity equals 1024
        assert_eq!(index_pages.cache_capacity, 1024,
                   "cache_capacity should be initialized to 1024");

        // Check that header_page has the header_node
        assert_eq!(index_pages.header_page.page_id, index_pages.header_page_id,
                   "header_page.page_id should match header_page_id");
        assert_eq!(index_pages.header_page.node.node_type_byte(), HEADER_NODE_TYPE,
                   "header_page.node should be a HeaderNode");

        // Check that the deserializer field exists and has HEADER_NODE_TYPE registered
        // Create an IndexPage with a HeaderNode
        let header_node = HeaderNode {
            root_page_id: PageID(3),
            next_page_id: PageID(4),
        };
        let index_page = IndexPage {
            page_id: PageID(5),
            node: Box::new(header_node),
            serialized: Vec::new(),
        };

        // Serialize the page data
        let serialized_data = index_page.serialize_page().expect("Failed to serialize page data");

        // Use the deserializer to deserialize the page data
        let deserialized_page = index_pages.deserializer.deserialize_page(&serialized_data, PageID(5))
            .expect("Failed to deserialize page data");

        // Verify that the deserialized page has the correct page_id
        assert_eq!(deserialized_page.page_id, PageID(5), 
                   "page_id should match after deserialization");

        // Verify that the deserialized page has the correct node type
        assert_eq!(deserialized_page.node.node_type_byte(), HEADER_NODE_TYPE, 
                   "node_type_byte should match after deserialization");

        // Serialize the node data from the deserialized page
        let reserialized_data = deserialized_page.node.to_msgpack()
            .expect("Failed to serialize node data from deserialized page");

        // Deserialize the original node data for comparison
        let original_node_data = &serialized_data[9..];
        assert_eq!(reserialized_data, original_node_data, 
                   "Reserialized node data should match the original node data");

        // The temporary directory will be automatically deleted when temp_dir goes out of scope
    }

    #[test]
    fn test_mark_dirty() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Create a few PageID instances
        let page_id1 = PageID(1);
        let page_id2 = PageID(2);
        let page_id3 = PageID(3);

        // Mark some pages as dirty, including duplicates
        index_pages.mark_dirty(page_id1);
        index_pages.mark_dirty(page_id2);
        index_pages.mark_dirty(page_id3);
        index_pages.mark_dirty(page_id1); // Duplicate

        // Create a set of expected PageIDs
        let mut expected_page_ids = HashMap::new();
        expected_page_ids.insert(page_id1, true);
        expected_page_ids.insert(page_id2, true);
        expected_page_ids.insert(page_id3, true);

        // Verify that each unique PageID added exists in the HashMap
        for (page_id, _) in &index_pages.dirty {
            assert!(expected_page_ids.contains_key(page_id), 
                    "Unexpected PageID in dirty HashMap: {:?}", page_id);
        }

        // Verify that all expected PageIDs exist in the HashMap
        for (page_id, _) in &expected_page_ids {
            assert!(index_pages.dirty.contains_key(page_id), 
                    "Expected PageID not found in dirty HashMap: {:?}", page_id);
        }

        // Verify that the number of entries in the dirty HashMap matches the expected count
        assert_eq!(index_pages.dirty.len(), expected_page_ids.len(), 
                   "Number of entries in dirty HashMap does not match expected count");
    }

    #[test]
    fn test_clear_dirty() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Create a few PageID instances
        let page_id1 = PageID(1);
        let page_id2 = PageID(2);
        let page_id3 = PageID(3);

        // Mark some pages as dirty
        index_pages.mark_dirty(page_id1);
        index_pages.mark_dirty(page_id2);
        index_pages.mark_dirty(page_id3);

        // Verify that the dirty HashMap is not empty
        assert!(!index_pages.dirty.is_empty(), "Dirty HashMap should not be empty before clearing");

        // Clear the dirty HashMap
        index_pages.clear_dirty();

        // Verify that the dirty HashMap is now empty
        assert!(index_pages.dirty.is_empty(), "Dirty HashMap should be empty after clearing");
    }

    #[test]
    fn test_header_node() {
        // Create initial PageID values
        let initial_root_page_id = PageID(1);
        let initial_next_page_id = PageID(2);

        // Create a HeaderNode instance
        let mut header_node = HeaderNode {
            root_page_id: initial_root_page_id,
            next_page_id: initial_next_page_id,
        };

        // Verify initial values
        assert_eq!(header_node.root_page_id, initial_root_page_id, 
                   "root_page_id should be initialized to the provided value");
        assert_eq!(header_node.next_page_id, initial_next_page_id, 
                   "next_page_id should be initialized to the provided value");

        // Change the values
        let new_root_page_id = PageID(3);
        let new_next_page_id = PageID(4);

        header_node.root_page_id = new_root_page_id;
        header_node.next_page_id = new_next_page_id;

        // Verify the new values
        assert_eq!(header_node.root_page_id, new_root_page_id, 
                   "root_page_id should be updated to the new value");
        assert_eq!(header_node.next_page_id, new_next_page_id, 
                   "next_page_id should be updated to the new value");
    }

    #[test]
    fn test_header_node_msgpack() {
        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(5),
            next_page_id: PageID(6),
        };

        // Serialize the HeaderNode to msgpack
        let serialized = header_node.to_msgpack().expect("Failed to serialize HeaderNode");

        // Deserialize the msgpack data back to a HeaderNode
        let deserialized: HeaderNode = decode::from_slice(&serialized)
            .expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized HeaderNode matches the original
        assert_eq!(deserialized, header_node, 
                   "Deserialized HeaderNode should match the original");
        assert_eq!(deserialized.root_page_id, header_node.root_page_id, 
                   "root_page_id should match after serialization/deserialization");
        assert_eq!(deserialized.next_page_id, header_node.next_page_id, 
                   "next_page_id should match after serialization/deserialization");
    }

    #[test]
    fn test_header_serialize_deserialize() {
        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(5),
            next_page_id: PageID(6),
        };

        // Serialize the HeaderNode to msgpack
        let serialized = header_node.serialize();

        // Deserialize the msgpack data back to a HeaderNode
        let deserialized: HeaderNode = HeaderNode::from_slice(&serialized)
            .expect("Failed to deserialize HeaderNode");

        // Verify that the deserialized HeaderNode matches the original
        assert_eq!(deserialized, header_node, 
                   "Deserialized HeaderNode should match the original");
        assert_eq!(deserialized.root_page_id, header_node.root_page_id, 
                   "root_page_id should match after serialization/deserialization");
        assert_eq!(deserialized.next_page_id, header_node.next_page_id, 
                   "next_page_id should match after serialization/deserialization");

        // Calculate the serialized page size
        let page_size = header_node.calc_serialized_page_size();

        // Verify that the page size is correct (8 bytes for the node + 9 bytes for the page overhead)
        assert_eq!(page_size, 17, 
                   "Page size should be 17 bytes (8 bytes for the node + 9 bytes for the page overhead)");

        // Serialize the HeaderNode to a page format
        let page_data = header_node.serialize_page();

        // Verify that the page data is not empty
        assert!(!page_data.is_empty(), "Page data should not be empty");

        // Verify that the page data has the correct length (1 byte for node type + 4 bytes for CRC + 4 bytes for data length + 8 bytes for the node)
        assert_eq!(page_data.len(), 17, "Page data should be 17 bytes");

        // Verify that the page data starts with the correct node type byte
        assert_eq!(page_data[0], HEADER_NODE_TYPE, "Page data should start with the header node type byte");
    }

    #[test]
    fn test_index_page_with_header_node() {
        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(7),
            next_page_id: PageID(8),
        };

        // Create an IndexPage with the HeaderNode and empty serialized data
        let index_page = IndexPage {
            page_id: PageID(9),
            node: Box::new(header_node),
            serialized: Vec::new(), // Empty value for serialized
        };

        // Verify that the IndexPage has the correct values
        assert_eq!(index_page.page_id, PageID(9), 
                   "page_id should be initialized to the provided value");

        // Serialize the node data using the new method
        let serialized_data = index_page.serialize_page().expect("Failed to serialize node data");

        // Get the raw node data for comparison
        let node_data = index_page.node.to_msgpack().expect("Failed to re-serialize HeaderNode");

        // Verify that the serialized data contains the node data
        assert!(serialized_data.len() > node_data.len(), 
                "Serialized data should be larger than just the node data");
        assert!(serialized_data.ends_with(&node_data), 
                "Serialized data should end with the node data");
    }

    #[test]
    fn test_add_page() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(7),
            next_page_id: PageID(8),
        };

        // Create an IndexPage with the HeaderNode and empty serialized data
        let page_id = PageID(10);
        let index_page = IndexPage {
            page_id,
            node: Box::new(header_node),
            serialized: Vec::new(), // Empty value for serialized
        };

        // Add the IndexPage to the IndexPages
        index_pages.add_page(index_page);

        // Verify that the page_id is in the dirty HashMap
        assert!(index_pages.dirty.contains_key(&page_id), 
                "page_id should be in the dirty HashMap after adding the page");

        // Verify that the page_id is in the cache
        assert!(index_pages.cache.contains(&page_id), 
                "page_id should be in the cache after adding the page");
    }

    #[test]
    fn test_serialize_page() {
        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(11),
            next_page_id: PageID(12),
        };

        // Create an IndexPage with the HeaderNode and empty serialized data
        let index_page = IndexPage {
            page_id: PageID(13),
            node: Box::new(header_node),
            serialized: Vec::new(), // Empty value for serialized
        };

        // Serialize the page data using the new method
        let serialized_data = index_page.serialize_page().expect("Failed to serialize page data");

        // Get the raw node data
        let node_data = index_page.node.to_msgpack().expect("Failed to serialize node data");

        // Verify the structure of the serialized data
        assert!(serialized_data.len() >= 1 + 4 + 4 + node_data.len(), 
                "Serialized data should include node type byte, CRC, length, and node data");

        // Extract components from the serialized data
        let node_type_byte = serialized_data[0];
        let crc_bytes = &serialized_data[1..5];
        let len_bytes = &serialized_data[5..9];
        let data = &serialized_data[9..];

        // Verify node type byte
        assert_eq!(node_type_byte, HEADER_NODE_TYPE, 
                   "Node type byte should be HEADER_NODE_TYPE");

        // Verify data length
        let data_len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
        assert_eq!(data_len as usize, node_data.len(), 
                   "Data length should match the length of the node data");

        // Verify data
        assert_eq!(data, node_data, 
                   "Data should match the node data");

        // Verify CRC
        let crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        assert_eq!(crc, calc_crc(&node_data), 
                   "CRC should match the calculated CRC of the node data");

        // Verify that the node data can be deserialized back to a HeaderNode
        let deserialized: HeaderNode = decode::from_slice(&node_data)
            .expect("Failed to deserialize node data");

        // Verify that the deserialized HeaderNode matches the original
        assert_eq!(deserialized.root_page_id, header_node.root_page_id, 
                   "root_page_id should match after serialization/deserialization");
        assert_eq!(deserialized.next_page_id, header_node.next_page_id, 
                   "next_page_id should match after serialization/deserialization");
    }

    #[test]
    fn test_node_type_byte() {
        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(14),
            next_page_id: PageID(15),
        };

        // Verify that node_type_byte returns HEADER_NODE_TYPE
        assert_eq!(header_node.node_type_byte(), HEADER_NODE_TYPE, 
                   "node_type_byte should return HEADER_NODE_TYPE");

        // Create an IndexPage with the HeaderNode
        let index_page = IndexPage {
            page_id: PageID(16),
            node: Box::new(header_node),
            serialized: Vec::new(),
        };

        // Verify that node_type_byte can be called through the trait object
        assert_eq!(index_page.node.node_type_byte(), HEADER_NODE_TYPE, 
                   "node_type_byte should return HEADER_NODE_TYPE when called through trait object");
    }

    #[test]
    fn test_deserializer() {
        // Create a HeaderNode instance
        let header_node = HeaderNode {
            root_page_id: PageID(17),
            next_page_id: PageID(18),
        };

        // Create an IndexPage with the HeaderNode
        let index_page = IndexPage {
            page_id: PageID(19),
            node: Box::new(header_node),
            serialized: Vec::new(),
        };

        // Serialize the page data
        let serialized_data = index_page.serialize_page().expect("Failed to serialize page data");

        // Create a Deserializer
        let mut deserializer = Deserializer::new();

        // Register a decode function for HeaderNode
        deserializer.register(HEADER_NODE_TYPE, |data| {
            let header_node: HeaderNode = HeaderNode::from_slice(data)?;
            Ok(Box::new(header_node) as Box<dyn Node>)
        });

        // Deserialize the page data
        let deserialized_page = deserializer.deserialize_page(&serialized_data, PageID(19))
            .expect("Failed to deserialize page data");

        // Verify that the deserialized page has the correct page_id
        assert_eq!(deserialized_page.page_id, PageID(19), 
                   "page_id should match after deserialization");

        // Verify that the deserialized page has the correct node type
        assert_eq!(deserialized_page.node.node_type_byte(), HEADER_NODE_TYPE, 
                   "node_type_byte should match after deserialization");

        // Serialize the node data from the deserialized page
        let reserialized_data = deserialized_page.node.to_msgpack()
            .expect("Failed to serialize node data from deserialized page");

        // Deserialize the original node data for comparison
        let original_node_data = &serialized_data[9..];
        assert_eq!(reserialized_data, original_node_data, 
                   "Reserialized node data should match the original node data");
    }

    #[test]
    fn test_deserializer_error_handling() {
        // Create a Deserializer
        let mut deserializer = Deserializer::new();

        // Register a decode function for HeaderNode
        deserializer.register(HEADER_NODE_TYPE, |data| {
            let header_node: HeaderNode = HeaderNode::from_slice(data)?;
            Ok(Box::new(header_node) as Box<dyn Node>)
        });

        // Test with empty data
        let result = deserializer.deserialize_page(&[], PageID(20));
        assert!(result.is_err(), "Should return an error for empty data");

        // Test with data that's too short for the header
        let result = deserializer.deserialize_page(&[HEADER_NODE_TYPE], PageID(20));
        assert!(result.is_err(), "Should return an error for data that's too short for the header");

        // Test with an unregistered node type
        let unregistered_type = 99;
        let mut invalid_data = Vec::new();
        invalid_data.push(unregistered_type);
        invalid_data.extend_from_slice(&[0, 0, 0, 0]); // CRC
        invalid_data.extend_from_slice(&[0, 0, 0, 0]); // Length
        let result = deserializer.deserialize_page(&invalid_data, PageID(20));
        assert!(result.is_err(), "Should return an error for an unregistered node type");

        // Test with invalid CRC
        let mut invalid_crc_data = Vec::new();
        invalid_crc_data.push(HEADER_NODE_TYPE);
        invalid_crc_data.extend_from_slice(&[1, 2, 3, 4]); // Invalid CRC
        invalid_crc_data.extend_from_slice(&[4, 0, 0, 0]); // Length = 4
        invalid_crc_data.extend_from_slice(&[0, 0, 0, 0]); // Some data
        let result = deserializer.deserialize_page(&invalid_crc_data, PageID(20));
        assert!(result.is_err(), "Should return an error for invalid CRC");
    }

    #[test]
    fn test_set_root_page_id() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Check the initial root_page_id
        assert_eq!(index_pages.header_node().root_page_id, PageID(1),
                   "Initial root_page_id should be PageID(1)");

        // Set a new root_page_id
        let new_root_page_id = PageID(42);
        index_pages.set_root_page_id(new_root_page_id);

        // Check that the root_page_id was updated
        assert_eq!(index_pages.header_node().root_page_id, new_root_page_id,
                   "root_page_id should be updated to the new value");

        // Check that the header_page_id was marked as dirty
        assert!(index_pages.dirty.contains_key(&index_pages.header_page_id),
                "header_page_id should be marked as dirty");

        // Serialize the header_page
        let serialized_data = index_pages.header_page.serialize_page()
            .expect("Failed to serialize header_page");

        // Deserialize the serialized data into another instance of IndexPage
        let deserialized_page = index_pages.deserializer.deserialize_page(&serialized_data, index_pages.header_page_id)
            .expect("Failed to deserialize header_page");

        // Check that the deserialized page has the correct page_id
        assert_eq!(deserialized_page.page_id, index_pages.header_page_id,
                   "Deserialized page_id should match header_page_id");

        // Check that the deserialized page has the correct node type
        assert_eq!(deserialized_page.node.node_type_byte(), HEADER_NODE_TYPE,
                   "Deserialized node_type_byte should be HEADER_NODE_TYPE");

        // Get the header_node from the deserialized page
        let node_data = deserialized_page.node.to_msgpack()
            .expect("Failed to serialize node data");
        let deserialized_header_node: HeaderNode = decode::from_slice(&node_data)
            .expect("Failed to deserialize node data");

        // Check that the root_page_id in the deserialized header_node matches the new value
        assert_eq!(deserialized_header_node.root_page_id, new_root_page_id,
                   "root_page_id in deserialized header_node should match the new value");
    }

    #[test]
    fn test_set_next_page_id() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Check the initial next_page_id
        assert_eq!(index_pages.header_node().next_page_id, PageID(2),
                   "Initial next_page_id should be PageID(2)");

        // Set a new next_page_id
        let new_next_page_id = PageID(42);
        index_pages.set_next_page_id(new_next_page_id);

        // Check that the next_page_id was updated
        assert_eq!(index_pages.header_node().next_page_id, new_next_page_id,
                   "next_page_id should be updated to the new value");

        // Check that the header_page_id was marked as dirty
        assert!(index_pages.dirty.contains_key(&index_pages.header_page_id),
                "header_page_id should be marked as dirty");

        // Serialize the header_page
        let serialized_data = index_pages.header_page.serialize_page()
            .expect("Failed to serialize header_page");

        // Deserialize the serialized data into another instance of IndexPage
        let deserialized_page = index_pages.deserializer.deserialize_page(&serialized_data, index_pages.header_page_id)
            .expect("Failed to deserialize header_page");

        // Check that the deserialized page has the correct page_id
        assert_eq!(deserialized_page.page_id, index_pages.header_page_id,
                   "Deserialized page_id should match header_page_id");

        // Check that the deserialized page has the correct node type
        assert_eq!(deserialized_page.node.node_type_byte(), HEADER_NODE_TYPE,
                   "Deserialized node_type_byte should be HEADER_NODE_TYPE");

        // Get the header_node from the deserialized page
        let node_data = deserialized_page.node.to_msgpack()
            .expect("Failed to serialize node data");
        let deserialized_header_node: HeaderNode = decode::from_slice(&node_data)
            .expect("Failed to deserialize node data");

        // Check that the next_page_id in the deserialized header_node matches the new value
        assert_eq!(deserialized_header_node.next_page_id, new_next_page_id,
                   "next_page_id in deserialized header_node should match the new value");
    }

    #[test]
    fn test_flush() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path.clone(), PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Mark the header page as dirty
        index_pages.mark_dirty(index_pages.header_page_id);

        // Create and add a few more pages to the cache
        let page_id1 = PageID(1);
        let page_id2 = PageID(2);

        // Create a HeaderNode instance for page 1
        let header_node1 = HeaderNode {
            root_page_id: PageID(3),
            next_page_id: PageID(4),
        };

        // Create an IndexPage with the HeaderNode
        let index_page1 = IndexPage {
            page_id: page_id1,
            node: Box::new(header_node1),
            serialized: Vec::new(),
        };

        // Create a HeaderNode instance for page 2
        let header_node2 = HeaderNode {
            root_page_id: PageID(5),
            next_page_id: PageID(6),
        };

        // Create an IndexPage with the HeaderNode
        let index_page2 = IndexPage {
            page_id: page_id2,
            node: Box::new(header_node2),
            serialized: Vec::new(),
        };

        // Add the pages to the cache and mark them as dirty
        index_pages.add_page(index_page1);
        index_pages.add_page(index_page2);

        // Verify that the pages are marked as dirty
        assert!(index_pages.dirty.contains_key(&index_pages.header_page_id),
                "header_page_id should be marked as dirty");
        assert!(index_pages.dirty.contains_key(&page_id1),
                "page_id1 should be marked as dirty");
        assert!(index_pages.dirty.contains_key(&page_id2),
                "page_id2 should be marked as dirty");

        // Flush the pages
        index_pages.flush().expect("Failed to flush pages");

        // Verify that the dirty HashMap is cleared
        assert!(index_pages.dirty.is_empty(),
                "dirty HashMap should be empty after flushing");

        // Create a new IndexPages instance to read the pages from disk
        let mut new_index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create new IndexPages");

        // Read the header page from disk
        let header_page_data = new_index_pages.paged_file.read_page(index_pages.header_page_id)
            .expect("Failed to read header page from disk");

        // Deserialize the header page
        let deserialized_header_page = new_index_pages.deserializer.deserialize_page(&header_page_data, index_pages.header_page_id)
            .expect("Failed to deserialize header page");

        // Verify that the deserialized header page has the correct page_id
        assert_eq!(deserialized_header_page.page_id, index_pages.header_page_id,
                   "Deserialized header page_id should match header_page_id");

        // Read page 1 from disk
        let page1_data = new_index_pages.paged_file.read_page(page_id1)
            .expect("Failed to read page 1 from disk");

        // Deserialize page 1
        let deserialized_page1 = new_index_pages.deserializer.deserialize_page(&page1_data, page_id1)
            .expect("Failed to deserialize page 1");

        // Verify that the deserialized page 1 has the correct page_id
        assert_eq!(deserialized_page1.page_id, page_id1,
                   "Deserialized page 1 page_id should match page_id1");

        // Read page 2 from disk
        let page2_data = new_index_pages.paged_file.read_page(page_id2)
            .expect("Failed to read page 2 from disk");

        // Deserialize page 2
        let deserialized_page2 = new_index_pages.deserializer.deserialize_page(&page2_data, page_id2)
            .expect("Failed to deserialize page 2");

        // Verify that the deserialized page 2 has the correct page_id
        assert_eq!(deserialized_page2.page_id, page_id2,
                   "Deserialized page 2 page_id should match page_id2");
    }

    #[test]
    fn test_alloc_page_id() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages
        let mut index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Check the initial next_page_id
        assert_eq!(index_pages.header_node().next_page_id, PageID(2),
                   "Initial next_page_id should be PageID(2)");

        // Call alloc_page_id multiple times and verify the returned values
        let page_id1 = index_pages.alloc_page_id();
        assert_eq!(page_id1, PageID(2), "First allocated page_id should be PageID(2)");
        assert_eq!(index_pages.header_node().next_page_id, PageID(3),
                   "next_page_id should be incremented to PageID(3)");

        let page_id2 = index_pages.alloc_page_id();
        assert_eq!(page_id2, PageID(3), "Second allocated page_id should be PageID(3)");
        assert_eq!(index_pages.header_node().next_page_id, PageID(4),
                   "next_page_id should be incremented to PageID(4)");

        let page_id3 = index_pages.alloc_page_id();
        assert_eq!(page_id3, PageID(4), "Third allocated page_id should be PageID(4)");
        assert_eq!(index_pages.header_node().next_page_id, PageID(5),
                   "next_page_id should be incremented to PageID(5)");

        // Verify that the allocated page IDs are monotonically increasing
        assert!(page_id1.0 < page_id2.0, "page_id1 should be less than page_id2");
        assert!(page_id2.0 < page_id3.0, "page_id2 should be less than page_id3");
    }

    #[test]
    fn test_persistence() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Step 1: Create an initial IndexPages instance
        let index_pages1 = IndexPages::new(test_path.clone(), PAGE_SIZE)
            .expect("Failed to create first IndexPages instance");

        // Check the initial values
        assert_eq!(index_pages1.header_node().root_page_id, PageID(1),
                   "Initial root_page_id should be PageID(1)");
        assert_eq!(index_pages1.header_node().next_page_id, PageID(2),
                   "Initial next_page_id should be PageID(2)");

        // Step 2: Create a second IndexPages instance
        let mut index_pages2 = IndexPages::new(test_path.clone(), PAGE_SIZE)
            .expect("Failed to create second IndexPages instance");

        // Check that the values are the same as in the first instance
        assert_eq!(index_pages2.header_node().root_page_id, index_pages1.header_node().root_page_id,
                   "root_page_id should be the same in the second instance");
        assert_eq!(index_pages2.header_node().next_page_id, index_pages1.header_node().next_page_id,
                   "next_page_id should be the same in the second instance");

        // Set new values for root_page_id and next_page_id
        let new_root_page_id = PageID(42);
        let new_next_page_id = PageID(43);

        index_pages2.set_root_page_id(new_root_page_id);
        index_pages2.set_next_page_id(new_next_page_id);

        // Flush the changes to disk
        index_pages2.flush().expect("Failed to flush changes");

        // Step 3: Create a third IndexPages instance
        let index_pages3 = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create third IndexPages instance");

        // Check that the values are the same as the modified values in the second instance
        assert_eq!(index_pages3.header_node().root_page_id, new_root_page_id,
                   "root_page_id should be the modified value in the third instance");
        assert_eq!(index_pages3.header_node().next_page_id, new_next_page_id,
                   "next_page_id should be the modified value in the third instance");
    }

    #[test]
    fn test_get_page() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages instance
        let mut index_pages = IndexPages::new(test_path.clone(), PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Create and add a page to the cache
        let page_id = PageID(1);

        // Create a HeaderNode instance for the page
        let header_node = HeaderNode {
            root_page_id: PageID(3),
            next_page_id: PageID(4),
        };

        // Create an IndexPage with the HeaderNode
        let index_page = IndexPage {
            page_id,
            node: Box::new(header_node),
            serialized: Vec::new(),
        };

        // Add the page to the cache and mark it as dirty
        index_pages.add_page(index_page);

        // Flush the page to disk
        index_pages.flush().expect("Failed to flush page to disk");

        // Create a new IndexPages instance to read the page from disk
        let mut new_index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create new IndexPages");

        // Get the page using get_page()
        let page = new_index_pages.get_page(page_id)
            .expect("Failed to get page");

        // Verify that the page has the correct page_id
        assert_eq!(page.page_id, page_id,
                   "Page should have the correct page_id");

        // Verify that the page has the correct node type
        assert_eq!(page.node.node_type_byte(), HEADER_NODE_TYPE,
                   "Page should have the correct node type");

        // Get the HeaderNode from the page
        let node = page.node.as_any().downcast_ref::<HeaderNode>()
            .expect("Failed to downcast node to HeaderNode");

        // Verify that the HeaderNode has the correct values
        assert_eq!(node.root_page_id, PageID(3),
                   "HeaderNode should have the correct root_page_id");
        assert_eq!(node.next_page_id, PageID(4),
                   "HeaderNode should have the correct next_page_id");
    }

    #[test]
    fn test_get_page_mut() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages instance
        let mut index_pages = IndexPages::new(test_path.clone(), PAGE_SIZE)
            .expect("Failed to create IndexPages");

        // Create and add a page to the cache
        let page_id = PageID(1);

        // Create a HeaderNode instance for the page
        let header_node = HeaderNode {
            root_page_id: PageID(3),
            next_page_id: PageID(4),
        };

        // Create an IndexPage with the HeaderNode
        let index_page = IndexPage {
            page_id,
            node: Box::new(header_node),
            serialized: Vec::new(),
        };

        // Add the page to the cache and mark it as dirty
        index_pages.add_page(index_page);

        // Get a mutable reference to the page using get_page_mut()
        let page_mut = index_pages.get_page_mut(page_id)
            .expect("Failed to get mutable page");

        // Modify the node in the page
        let node_mut = page_mut.node.as_any_mut().downcast_mut::<HeaderNode>()
            .expect("Failed to downcast node to HeaderNode");
        node_mut.next_page_id = PageID(5);

        // Flush the changes to disk
        index_pages.flush().expect("Failed to flush changes");

        // Create a new IndexPages instance to read the page from disk
        let mut new_index_pages = IndexPages::new(test_path.clone(), PAGE_SIZE)
            .expect("Failed to create new IndexPages");

        // Get the page using get_page()
        let page = new_index_pages.get_page(page_id)
            .expect("Failed to get page");

        // Verify that the page has the correct page_id
        assert_eq!(page.page_id, page_id,
                   "Page should have the correct page_id");

        // Verify that the page has the correct node type
        assert_eq!(page.node.node_type_byte(), HEADER_NODE_TYPE,
                   "Page should have the correct node type");

        // Get the HeaderNode from the page
        let node = page.node.as_any().downcast_ref::<HeaderNode>()
            .expect("Failed to downcast node to HeaderNode");

        // Verify that the HeaderNode has the modified next_page_id
        assert_eq!(node.root_page_id, PageID(3),
                   "HeaderNode should have the correct root_page_id");
        assert_eq!(node.next_page_id, PageID(5),
                   "HeaderNode should have the modified next_page_id");

        // Get a mutable reference to the page using get_page_mut()
        let page_mut = new_index_pages.get_page_mut(page_id)
            .expect("Failed to get mutable page");

        // Modify the node in the page again
        let node_mut = page_mut.node.as_any_mut().downcast_mut::<HeaderNode>()
            .expect("Failed to downcast node to HeaderNode");
        node_mut.next_page_id = PageID(6);

        // Mark the page as dirty and flush the changes to disk
        new_index_pages.mark_dirty(page_id);
        new_index_pages.flush().expect("Failed to flush changes");

        // Create another IndexPages instance to read the page from disk
        let mut third_index_pages = IndexPages::new(test_path, PAGE_SIZE)
            .expect("Failed to create third IndexPages");

        // Get the page using get_page()
        let page = third_index_pages.get_page(page_id)
            .expect("Failed to get page");

        // Verify that the page has the correct page_id
        assert_eq!(page.page_id, page_id,
                   "Page should have the correct page_id");

        // Verify that the page has the correct node type
        assert_eq!(page.node.node_type_byte(), HEADER_NODE_TYPE,
                   "Page should have the correct node type");

        // Get the HeaderNode from the page
        let node = page.node.as_any().downcast_ref::<HeaderNode>()
            .expect("Failed to downcast node to HeaderNode");

        // Verify that the HeaderNode has the second modified next_page_id
        assert_eq!(node.root_page_id, PageID(3),
                   "HeaderNode should have the correct root_page_id");
        assert_eq!(node.next_page_id, PageID(6),
                   "HeaderNode should have the second modified next_page_id");
    }

    #[test]
    fn test_reduce_cache() {
        // Create a temporary directory
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        let test_path = temp_dir.path().join("index.dat");

        // Create a new IndexPages with cache_capacity of 5
        let mut index_pages = IndexPages::new_with_cache_capacity(test_path, PAGE_SIZE, Some(5))
            .expect("Failed to create IndexPages");

        // Create and add seven pages to the cache
        for i in 1..=7 {
            let page_id = PageID(i);

            // Create a HeaderNode instance for the page
            let header_node = HeaderNode {
                root_page_id: PageID(i * 10),
                next_page_id: PageID(i * 10 + 1),
            };

            // Create an IndexPage with the HeaderNode
            let index_page = IndexPage {
                page_id,
                node: Box::new(header_node),
                serialized: Vec::new(),
            };

            // Add the page to the cache
            index_pages.add_page(index_page);
        }

        // Get the first and third pages to make them recently used
        let _ = index_pages.get_page(PageID(1)).expect("Failed to get page 1");
        let _ = index_pages.get_page(PageID(3)).expect("Failed to get page 3");

        // Call reduce_cache to remove the least recently used items
        index_pages.reduce_cache();

        // Check that the second and fourth pages are not in the cache
        assert!(!index_pages.cache.contains(&PageID(2)), 
                "Page 2 should not be in the cache after reduce_cache");
        assert!(!index_pages.cache.contains(&PageID(4)), 
                "Page 4 should not be in the cache after reduce_cache");

        // Check that the other five pages are in the cache
        assert!(index_pages.cache.contains(&PageID(1)), 
                "Page 1 should be in the cache after reduce_cache");
        assert!(index_pages.cache.contains(&PageID(3)), 
                "Page 3 should be in the cache after reduce_cache");
        assert!(index_pages.cache.contains(&PageID(5)), 
                "Page 5 should be in the cache after reduce_cache");
        assert!(index_pages.cache.contains(&PageID(6)), 
                "Page 6 should be in the cache after reduce_cache");
        assert!(index_pages.cache.contains(&PageID(7)), 
                "Page 7 should be in the cache after reduce_cache");

        // Verify that the cache size is equal to the cache_capacity
        assert_eq!(index_pages.cache.len(), index_pages.cache_capacity,
                   "Cache size should be equal to cache_capacity after reduce_cache");
    }
}
