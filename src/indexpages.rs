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
    pub fn deserialise_page(&self, data: &[u8], page_id: PageID) -> Result<IndexPage, decode::Error> {
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
        let node_data = self.node.to_msgpack()?;

        // Get the node type byte
        let node_type_byte = self.node.node_type_byte();

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

        Ok(result)
    }
}

/// A structure that manages index pages
pub struct IndexPages {
    paged_file: PagedFile,
    dirty: HashMap<PageID, bool>,
    header_page_id: PageID,
    /// The header page containing the header node
    pub header_page: IndexPage,
    /// Cache of IndexPage objects keyed by PageID
    /// This cache must be unbounded and should never be changed to a bounded cache
    cache: LruCache<PageID, IndexPage>,
    /// Deserializer for index pages
    pub deserializer: Deserializer,
}

impl IndexPages {
    /// Gets a reference to the HeaderNode from the header page
    ///
    /// # Returns
    /// * `&HeaderNode` - A reference to the HeaderNode
    fn header_node(&self) -> &HeaderNode {
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
        let mut paged_file = PagedFile::new(path, Some(page_size))?;

        // Create a new Deserializer
        let mut deserializer = Deserializer::new();

        // Register the HEADER_NODE_TYPE with a decode function for HeaderNode
        deserializer.register(HEADER_NODE_TYPE, |data| {
            let header_node: HeaderNode = decode::from_slice(data)?;
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
            deserializer.deserialise_page(&header_page_data, header_page_id)
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
        let deserialized_page = index_pages.deserializer.deserialise_page(&serialized_data, PageID(5))
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
            let header_node: HeaderNode = decode::from_slice(data)?;
            Ok(Box::new(header_node) as Box<dyn Node>)
        });

        // Deserialize the page data
        let deserialized_page = deserializer.deserialise_page(&serialized_data, PageID(19))
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
            let header_node: HeaderNode = decode::from_slice(data)?;
            Ok(Box::new(header_node) as Box<dyn Node>)
        });

        // Test with empty data
        let result = deserializer.deserialise_page(&[], PageID(20));
        assert!(result.is_err(), "Should return an error for empty data");

        // Test with data that's too short for the header
        let result = deserializer.deserialise_page(&[HEADER_NODE_TYPE], PageID(20));
        assert!(result.is_err(), "Should return an error for data that's too short for the header");

        // Test with an unregistered node type
        let unregistered_type = 99;
        let mut invalid_data = Vec::new();
        invalid_data.push(unregistered_type);
        invalid_data.extend_from_slice(&[0, 0, 0, 0]); // CRC
        invalid_data.extend_from_slice(&[0, 0, 0, 0]); // Length
        let result = deserializer.deserialise_page(&invalid_data, PageID(20));
        assert!(result.is_err(), "Should return an error for an unregistered node type");

        // Test with invalid CRC
        let mut invalid_crc_data = Vec::new();
        invalid_crc_data.push(HEADER_NODE_TYPE);
        invalid_crc_data.extend_from_slice(&[1, 2, 3, 4]); // Invalid CRC
        invalid_crc_data.extend_from_slice(&[4, 0, 0, 0]); // Length = 4
        invalid_crc_data.extend_from_slice(&[0, 0, 0, 0]); // Some data
        let result = deserializer.deserialise_page(&invalid_crc_data, PageID(20));
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
        let deserialized_page = index_pages.deserializer.deserialise_page(&serialized_data, index_pages.header_page_id)
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
        let deserialized_page = index_pages.deserializer.deserialise_page(&serialized_data, index_pages.header_page_id)
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
        let deserialized_header_page = new_index_pages.deserializer.deserialise_page(&header_page_data, index_pages.header_page_id)
            .expect("Failed to deserialize header page");

        // Verify that the deserialized header page has the correct page_id
        assert_eq!(deserialized_header_page.page_id, index_pages.header_page_id,
                   "Deserialized header page_id should match header_page_id");

        // Read page 1 from disk
        let page1_data = new_index_pages.paged_file.read_page(page_id1)
            .expect("Failed to read page 1 from disk");

        // Deserialize page 1
        let deserialized_page1 = new_index_pages.deserializer.deserialise_page(&page1_data, page_id1)
            .expect("Failed to deserialize page 1");

        // Verify that the deserialized page 1 has the correct page_id
        assert_eq!(deserialized_page1.page_id, page_id1,
                   "Deserialized page 1 page_id should match page_id1");

        // Read page 2 from disk
        let page2_data = new_index_pages.paged_file.read_page(page_id2)
            .expect("Failed to read page 2 from disk");

        // Deserialize page 2
        let deserialized_page2 = new_index_pages.deserializer.deserialise_page(&page2_data, page_id2)
            .expect("Failed to deserialize page 2");

        // Verify that the deserialized page 2 has the correct page_id
        assert_eq!(deserialized_page2.page_id, page_id2,
                   "Deserialized page 2 page_id should match page_id2");
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
}
