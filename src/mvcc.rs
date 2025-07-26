use std::collections::VecDeque;
use std::path::Path;
use std::sync::Mutex;
use std::collections::HashMap;

use crate::crc::calc_crc;
use crate::mvcc_nodes::{FreeListInternalNode, FreeListLeafNode, FreeListLeafValue, HeaderNode, LmdbError, Node, PageID, Position, PositionIndexRecord, PositionInternalNode, PositionLeafNode, TSN};
use crate::mvcc_pager::Pager;

// Result type alias
pub type Result<T> = std::result::Result<T, LmdbError>;


// Page structure
#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: PageID,
    pub node: Node,
}

// Reader transaction
pub struct LmdbReader {
    pub header_page_id: PageID,
    pub tsn: TSN,
    pub position_root_id: PageID,
    reader_id: usize,
    reader_tsns: *const Mutex<HashMap<usize, TSN>>,
}

impl Drop for LmdbReader {
    fn drop(&mut self) {
        // Safety: The Mutex is valid as long as the Lmdb instance is valid,
        // and the LmdbReader doesn't outlive the Lmdb instance.
        unsafe {
            if !self.reader_tsns.is_null() {
                if let Ok(mut map) = (*self.reader_tsns).lock() {
                    map.remove(&self.reader_id);
                }
            }
        }
    }
}

// Writer transaction
pub struct LmdbWriter {
    pub header_page_id: PageID,
    pub tsn: TSN,
    pub next_page_id: PageID,
    pub freetree_root_id: PageID,
    pub position_root_id: PageID,
    pub reusable_page_ids: VecDeque<(PageID, TSN)>,
    pub freed_page_ids: VecDeque<PageID>,
    pub deserialized: HashMap<PageID, Page>,
    pub dirty: HashMap<PageID, Page>,
    pub reused_page_ids: VecDeque<(PageID, TSN)>,
}

// Main LMDB structure
pub struct Lmdb {
    pub pager: Pager,
    pub reader_tsns: Mutex<HashMap<usize, TSN>>,
    pub writer_lock: Mutex<()>,
    pub page_size: usize,
    pub header_page_id0: PageID,
    pub header_page_id1: PageID,
    reader_id_counter: Mutex<usize>,
}

impl Lmdb {
    pub fn new(path: &Path, page_size: usize) -> Result<Self> {
        let pager = Pager::new(path, page_size)?;
        let header_page_id0 = PageID(0);
        let header_page_id1 = PageID(1);
        
        let mut lmdb = Self {
            pager,
            reader_tsns: Mutex::new(HashMap::new()),
            writer_lock: Mutex::new(()),
            page_size,
            header_page_id0,
            header_page_id1,
            reader_id_counter: Mutex::new(0),
        };
        
        if lmdb.pager.is_file_new {
            // Initialize new database
            let freetree_root_id = PageID(2);
            let position_root_id = PageID(3);
            let next_page_id = PageID(4);
            
            // Create and write header pages
            let header_node0 = HeaderNode {
                tsn: TSN(0),
                next_page_id,
                freetree_root_id,
                position_root_id,
            };
            
            let header_page0 = Page::new(header_page_id0, Node::Header(header_node0.clone()));
            lmdb.write_page(&header_page0)?;
            
            let header_page1 = Page::new(header_page_id1, Node::Header(header_node0));
            lmdb.write_page(&header_page1)?;
            
            // Create and write empty free list root page
            let free_list_leaf = FreeListLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let free_list_page = Page::new(freetree_root_id, Node::FreeListLeaf(free_list_leaf));
            lmdb.write_page(&free_list_page)?;
            
            // Create and write empty position index root page
            let position_leaf = PositionLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
                next_leaf_id: None,
            };
            let position_page = Page::new(position_root_id, Node::PositionLeaf(position_leaf));
            lmdb.write_page(&position_page)?;
            
            lmdb.flush()?;
        }
        
        Ok(lmdb)
    }
    
    pub fn get_latest_header(&mut self) -> Result<(PageID, HeaderNode)> {
        let header0 = self.read_header(self.header_page_id0)?;
        let header1 = self.read_header(self.header_page_id1)?;

        if header1.tsn > header0.tsn {
            Ok((self.header_page_id1, header1))
        } else {
            Ok((self.header_page_id0, header0))
        }
    }

    pub fn read_header(&mut self, page_id: PageID) -> Result<HeaderNode> {
        let header = self.read_page(page_id)?;
        let header_node = match header.node {
            Node::Header(node) => node,
            _ => return Err(LmdbError::DatabaseCorrupted("Invalid header node type".to_string())),
        };
        Ok(header_node)
    }

    pub fn read_page(&self, page_id: PageID) -> Result<Page> {
        let page_data = self.pager.read_page(page_id)?;
        Page::deserialize(page_id, &page_data)
    }
    
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let serialized = &page.serialize()?;
        self.pager.write_page(page.page_id, serialized)?;
        Ok(())
    }
    
    pub fn flush(&mut self) -> Result<()> {
        self.pager.flush()?;
        Ok(())
    }
    
    pub fn reader<'a>(&'a mut self) -> Result<LmdbReader> {
        let (header_page_id, header_node) = self.get_latest_header()?;
        
        // Generate a unique ID for this reader using the counter
        let reader_id = {
            let mut counter = self.reader_id_counter.lock().unwrap();
            *counter += 1;
            *counter
        };
        
        // Create the reader with the unique ID
        let reader = LmdbReader {
            header_page_id,
            tsn: header_node.tsn,
            position_root_id: header_node.position_root_id,
            reader_id,
            reader_tsns: &self.reader_tsns,
        };
        
        // Register the reader TSN
        self.reader_tsns.lock().unwrap().insert(reader_id, reader.tsn);
        
        Ok(reader)
    }
    
    pub fn writer<'a>(&'a mut self) -> Result<LmdbWriter> {
        // Get the latest header
        let (header_page_id, header_node) = self.get_latest_header()?;

        // Create the writer
        let mut writer = LmdbWriter::new(
            header_page_id,
            TSN(header_node.tsn.0 + 1),
            header_node.next_page_id,
            header_node.freetree_root_id,
            header_node.position_root_id,
        );

        // Find the reusable page IDs.
        get_reusable_page_ids(self, &mut writer)?;

        Ok(writer)
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
            self.write_page(&page)?;
        }

        // Flush changes to disk
        self.flush()?;

        // Write the new header page
        let header_page_id = if writer.header_page_id == self.header_page_id0 {
            self.header_page_id1
        } else {
            self.header_page_id0
        };
        
        let header_node = HeaderNode {
            tsn: writer.tsn,
            next_page_id: writer.next_page_id,
            freetree_root_id: writer.freetree_root_id,
            position_root_id: writer.position_root_id,
        };
        
        let header_page = Page::new(header_page_id, Node::Header(header_node));
        self.write_page(&header_page)?;
        
        // Flush changes to disk
        self.flush()?;
        
        Ok(())
    }
    
    pub fn get_page(&self, writer: &mut LmdbWriter, page_id: PageID) -> Result<Page> {
        // Check if the page is in the dirty pages
        if let Some(page) = writer.dirty.get(&page_id) {
            return Ok(page.clone());
        }

        // Check if the page is already deserialized
        if let Some(page) = writer.deserialized.get(&page_id) {
            return Ok(page.clone());
        }

        // Deserialize page
        let deserialized_page = self.read_page(page_id).unwrap();
        writer.insert_deserialized(deserialized_page);

        // Return page
        Ok(writer.deserialized.get(&page_id).unwrap().clone())
    }
    
    pub fn get_page_mut<'a>(&self, writer: &'a mut LmdbWriter, page_id: PageID) -> Result<&'a mut Page> {
        // First check if we need to deserialize the page
        let needs_deserialization = !writer.dirty.contains_key(&page_id) && !writer.deserialized.contains_key(&page_id);
        
        if needs_deserialization {
            // Deserialize page and insert it into deserialized collection
            let deserialized_page = self.read_page(page_id).unwrap();
            writer.insert_deserialized(deserialized_page);
        }
        
        // Now get a mutable reference to the page
        if let Some(page) = writer.dirty.get_mut(&page_id) {
            return Ok(page);
        }
        
        if let Some(page) = writer.deserialized.get_mut(&page_id) {
            return Ok(page);
        }
        
        Err(LmdbError::PageNotFound(page_id))
    }
}

// Page header format: type(1) + crc(4) + len(4)
const PAGE_HEADER_SIZE: usize = 9;


// Implementation for Page
impl Page {
    pub fn new(page_id: PageID, node: Node) -> Self {
        Self {
            page_id,
            node,
        }
    }
    
    pub fn serialize(&self) -> Result<Vec<u8>> {
        // Serialize the node
        let data = self.node.serialize()?;
        
        // Calculate CRC
        let crc = calc_crc(&data);
        
        // Create the serialized data with header
        let mut serialized = Vec::with_capacity(PAGE_HEADER_SIZE + data.len());
        serialized.push(self.node.get_type_byte());
        serialized.extend_from_slice(&crc.to_le_bytes());
        serialized.extend_from_slice(&(data.len() as u32).to_le_bytes());
        serialized.extend_from_slice(&data);
        
        Ok(serialized)
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
        let calculated_crc = calc_crc(&data);
        
        if calculated_crc != crc {
            return Err(LmdbError::DatabaseCorrupted("CRC mismatch".to_string()));
        }
        
        // Deserialize the node
        let node = Node::deserialize(node_type, data)?;
        
        Ok(Self {
            page_id,
            node,
        })
    }
}

// Implementation for LmdbWriter
impl LmdbWriter {
    pub fn new(
        header_page_id: PageID,
        tsn: TSN,
        next_page_id: PageID,
        freetree_root_id: PageID,
        position_root_id: PageID,
    ) -> Self {
        Self {
            header_page_id,
            tsn,
            next_page_id,
            freetree_root_id,
            position_root_id,
            reusable_page_ids: VecDeque::new(),
            freed_page_ids: VecDeque::new(),
            deserialized: HashMap::new(),
            dirty: HashMap::new(),
            reused_page_ids: VecDeque::new(),
        }
    }

    pub fn insert_deserialized(&mut self, page: Page) {
        self.deserialized.insert(page.page_id, page);
    }

    pub fn mark_dirty(&mut self, page: Page) {
        if !self.freed_page_ids.contains(&page.page_id) {
            println!("Marking dirty {:?}: {:?}", page.page_id, page.node);
            self.dirty.insert(page.page_id, page);
        }
    }

    pub fn alloc_page_id(&mut self) -> PageID {
        if let Some((free_page_id, tsn)) = self.reusable_page_ids.pop_front() {
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
                };
                
                self.dirty.insert(new_page_id, new_page.clone());
                replacement_info = Some((old_page_id, new_page_id));
                println!("Copied {:?} to {:?}: {:?}", old_page_id.0, new_page_id.0, page);
                println!("Freed page IDs: {:?}", self.freed_page_ids);
                return (new_page, replacement_info);
            }
        }
        
        (page, replacement_info)
    }

    pub fn append_freed_page_id(&mut self, page_id: PageID) {
        if !self.freed_page_ids.iter().any(|&id| id == page_id) {
            self.freed_page_ids.push_back(page_id);
            println!("Appended {:?} to freed_page_ids", page_id);
            if self.dirty.contains_key(&page_id) {
                println!("Page ID {:?} was in dirty and was removed", page_id);
                self.dirty.remove(&page_id);
            }
            if self.dirty.contains_key(&page_id) {
                println!("Page ID {:?} is still in dirty!!!!!", page_id);
            }
        }
    }
}

pub fn get_reusable_page_ids(db: &mut Lmdb, writer: &mut LmdbWriter) -> Result<()> {
    // Get free page IDs
    println!("Getting free page IDs for TSN {:?}...", writer.tsn);

    // Find the smallest reader TSN
    let smallest_reader_tsn = {
        db.reader_tsns.lock().unwrap().values().min().cloned()
    };
    println!("Smallest reader TSN: {:?}", smallest_reader_tsn);

    let root_page = db.get_page(writer, writer.freetree_root_id)?;
    println!("Root page is {:?}", writer.freetree_root_id);

    // Walk the tree to find leaf nodes
    let mut stack = vec![(root_page, 0)];

    while let Some((page, idx)) = stack.pop() {
        match &page.node {
            Node::FreeListInternal(node) => {
                println!("Page {:?} is internal node", page.page_id);

                if idx < node.child_ids.len() {
                    stack.push((page.clone(), idx + 1));
                    let child_id = node.child_ids[idx];
                    let child = db.get_page(writer, child_id)?;
                    stack.push((child, 0));
                }
            }
            Node::FreeListLeaf(node) => {
                println!("Page {:?} is leaf node", page.page_id);
                for i in 0..node.keys.len() {
                    let tsn = node.keys[i];
                    if let Some(smallest) = smallest_reader_tsn {
                        if tsn > smallest {
                            return Ok(());
                        }
                    }

                    let leaf_value = &node.values[i];
                    if leaf_value.root_id.is_none() {
                        for &page_id in &leaf_value.page_ids {
                            writer.reusable_page_ids.push_back((page_id, tsn));
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

    Ok(())
}

// Free list management functions
pub fn insert_freed_page_id(db: &mut Lmdb, writer: &mut LmdbWriter, tsn: TSN, freed_page_id: PageID) -> Result<()> {
    println!("");
    println!("Inserting freed page ID {:?} for TSN {:?}...", freed_page_id, tsn);
    println!("Root page is {:?}", writer.freetree_root_id);
    // Get the root page
    let mut current_page = db.get_page(writer, writer.freetree_root_id)?;
    
    // Traverse the tree to find a leaf node
    let mut stack: Vec<PageID> = Vec::new();
    let mut current_node = current_page.node.clone();

    while !matches!(current_node, Node::FreeListLeaf(_)) {
        if let Node::FreeListInternal(internal_node) = &current_node {
            println!("Page {:?} is internal node", current_page.page_id);
            stack.push(current_page.page_id);
            let child_page_id = internal_node.child_ids.last().unwrap();
            current_page = db.get_page(writer, *child_page_id)?;
            current_node = current_page.node.clone();
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
        }
    }
    println!("Page {:?} is leaf node", current_page.page_id);

    // Make the leaf page dirty
    let (mut leaf_page, replacement_info) = writer.make_dirty(current_page);
    
    // Extract the leaf node
    let mut modified_leaf_node = if let Node::FreeListLeaf(ref node) = leaf_page.node {
        node.clone()
    } else {
        return Err(LmdbError::DatabaseCorrupted("Expected FreeListLeaf node".to_string()));
    };
    
    // Find the place to insert the value
    let leaf_idx = modified_leaf_node.keys.iter().position(|&k| k == tsn);
    
    // Insert the value
    if let Some(idx) = leaf_idx {
        // TSN already exists, append to its page_ids
        if modified_leaf_node.values[idx].root_id.is_none() {
            modified_leaf_node.values[idx].page_ids.push(freed_page_id);
        } else {
            return Err(LmdbError::DatabaseCorrupted("Free list subtree not implemented".to_string()));
        }
        println!(
            "Appended page ID {:?} to TSN {:?} in page {:?}: {:?}",
            freed_page_id, tsn, leaf_page.page_id, modified_leaf_node
        );

    } else {
        // New TSN, add a new entry
        modified_leaf_node.keys.push(tsn);
        modified_leaf_node.values.push(FreeListLeafValue {
            page_ids: vec![freed_page_id],
            root_id: None,
        });
        println!(
            "Inserted {:?} and appended {:?} in {:?}: {:?}",
            tsn, freed_page_id, leaf_page.page_id, modified_leaf_node
        );
    }
    
    // Update the page with the modified node
    leaf_page.node = Node::FreeListLeaf(modified_leaf_node.clone());
    
    // Check if the page needs splitting by estimating the serialized size
    let needs_splitting = leaf_page.serialize()?.len() > db.page_size;
    
    if needs_splitting {
        println!("Splitting leaf page {:?}...", leaf_page.page_id);

        // Split the leaf node - we need to extract the node again since we updated it
        let mut modified_leaf_node = if let Node::FreeListLeaf(ref node) = leaf_page.node {
            node.clone()
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected FreeListLeaf node".to_string()));
        };
        
        // Split the leaf node
        let last_key = modified_leaf_node.keys.pop().unwrap();
        let last_value = modified_leaf_node.values.pop().unwrap();
        
        // Update the leaf page with the modified node (after popping)
        leaf_page.node = Node::FreeListLeaf(modified_leaf_node);
        
        let new_leaf_node = FreeListLeafNode {
            keys: vec![last_key],
            values: vec![last_value],
        };
        
        let new_leaf_page_id = writer.alloc_page_id();
        let new_leaf_page = Page::new(new_leaf_page_id, Node::FreeListLeaf(new_leaf_node));
        writer.mark_dirty(new_leaf_page.clone());
        println!("Created page {:?}: {:?}", new_leaf_page_id, new_leaf_page.node);

        // Check if the new leaf page needs splitting
        let new_leaf_needs_splitting = {
            new_leaf_page.serialize()?.len() > db.page_size
        };
        
        if new_leaf_needs_splitting {
            return Err(LmdbError::DatabaseCorrupted("Overflow freed page IDs for TSN to subtree not implemented".to_string()));
        }
        
        // Propagate the split up the tree
        println!("Promoting TSN {:?} and page {:?}", last_key, new_leaf_page_id);

        let mut split_info = Some((last_key, new_leaf_page_id));
        let mut current_replacement_info = replacement_info;
        
        // Propagate splits and replacements up the stack
        while let Some(parent_page_id) = stack.pop() {
            let parent_page = db.get_page(writer, parent_page_id)?;
            let (mut parent_page, parent_replacement_info) = writer.make_dirty(parent_page);
            
            if let Some((old_id, new_id)) = current_replacement_info {
                println!(
                    "Replacing page {:?} with {:?} in {:?}: {:?}",
                    old_id, new_id, parent_page.page_id, parent_page.node
                );

                if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                    // Replace the child ID
                    let last_idx = internal_node.child_ids.len() - 1;
                    if internal_node.child_ids[last_idx] == old_id {
                        internal_node.child_ids[last_idx] = new_id;
                    } else {
                        return Err(LmdbError::DatabaseCorrupted("Child ID mismatch".to_string()));
                    }
                    println!(
                        "Replaced page {:?} with {:?} in {:?}: {:?}",
                        old_id, new_id, parent_page.page_id, internal_node
                    );

                } else {
                    return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
                }
            }
            
            if let Some((promoted_key, promoted_page_id)) = split_info {
                if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                    // Add the promoted key and page ID
                    internal_node.keys.push(promoted_key);
                    internal_node.child_ids.push(promoted_page_id);
                    println!(
                        "Promoted ({:?}, {:?}) to {:?}: {:?}",
                        promoted_key, promoted_page_id, parent_page.page_id, internal_node
                    )

                } else {
                    return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
                }
            }
            
            // Check if the parent page needs splitting
            let serialized_len = parent_page.serialize()?.len();
            let parent_needs_splitting = {
                serialized_len > db.page_size
            };
            
            if parent_needs_splitting {
                if let Node::FreeListInternal(ref mut internal_node) = parent_page.node {
                    println!("Splitting internal page {:?}...", parent_page.page_id);
                    // Split the internal node
                    // Ensure we have at least 3 keys and 4 child IDs before splitting
                    if internal_node.keys.len() < 3 || internal_node.child_ids.len() < 4 {
                        return Err(LmdbError::DatabaseCorrupted("Cannot split internal node with too few keys/children".to_string()));
                    }
                    
                    // Move the right-most key to new node. Promote the next right-most key.
                    let middle_idx = internal_node.keys.len() - 2;
                    let promoted_key = internal_node.keys.remove(middle_idx);
                    
                    // Create a new internal node with the right half of keys and children
                    let new_keys = internal_node.keys.split_off(middle_idx);
                    let new_child_ids = internal_node.child_ids.split_off(middle_idx + 1);
                    
                    // Ensure both nodes maintain the B-tree invariant: n keys should have n+1 child pointers
                    assert_eq!(internal_node.keys.len() + 1, internal_node.child_ids.len());
                    
                    let new_internal_node = FreeListInternalNode {
                        keys: new_keys,
                        child_ids: new_child_ids,
                    };
                    
                    // Ensure the new node also maintains the invariant
                    assert_eq!(new_internal_node.keys.len() + 1, new_internal_node.child_ids.len());
                    
                    let new_internal_page_id = writer.alloc_page_id();
                    let new_internal_page = Page::new(new_internal_page_id, Node::FreeListInternal(new_internal_node));
                    println!(
                        "Created page {:?}: {:?}", new_internal_page_id, new_internal_page.node
                    );
                    writer.mark_dirty(new_internal_page);

                    split_info = Some((promoted_key, new_internal_page_id));
                } else {
                    return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
                }
            } else {
                split_info = None;
            }

            writer.mark_dirty(parent_page);

            
            current_replacement_info = parent_replacement_info;
        }
        
        // Update the root if needed
        if let Some((old_id, new_id)) = current_replacement_info {
            println!(
                "Replacing root page {:?} with {:?} in header", old_id, new_id
            );

            if writer.freetree_root_id == old_id {
                writer.freetree_root_id = new_id;
            } else {
                return Err(LmdbError::DatabaseCorrupted("Root ID mismatch".to_string()));
            }
        }
        
        if let Some((promoted_key, promoted_page_id)) = split_info {
            // Create a new root
            let new_internal_node = FreeListInternalNode {
                keys: vec![promoted_key],
                child_ids: vec![writer.freetree_root_id, promoted_page_id],
            };
            
            let new_root_page_id = writer.alloc_page_id();
            let new_root_page = Page::new(new_root_page_id, Node::FreeListInternal(new_internal_node));
            println!(
                "Created new internal root node {:?}: {:?}",
                new_root_page_id, new_root_page.node
            );
            writer.mark_dirty(new_root_page);

            writer.freetree_root_id = new_root_page_id;
        }
    } else {
        // No splitting needed, just update the page
        writer.mark_dirty(leaf_page);
        
        // Update the root if needed
        if let Some((old_id, new_id)) = replacement_info {
            if writer.freetree_root_id == old_id {
                writer.freetree_root_id = new_id;
                println!(
                    "Replacing root page {:?} with {:?} in header", old_id, new_id
                );
            }
        }
    }
    
    Ok(())
}

pub fn remove_freed_page_id(db: &mut Lmdb, writer: &mut LmdbWriter, tsn: TSN, used_page_id: PageID) -> Result<()> {
    println!("");
    println!("Removing {:?} from {:?}...", used_page_id, tsn);
    println!("Root is {:?}", writer.freetree_root_id);
    // Get the root page
    let mut current_page = db.get_page(writer, writer.freetree_root_id)?;

    // Traverse the tree to find a leaf node
    let mut stack: Vec<PageID> = Vec::new();
    let mut current_node = current_page.node.clone();

    while !matches!(current_node, Node::FreeListLeaf(_)) {
        if let Node::FreeListInternal(internal_node) = &current_node {
            println!("{:?} is internal node: {:?}", current_page.page_id, internal_node);
            stack.push(current_page.page_id);
            let child_page_id = internal_node.child_ids[0];
            current_page = db.get_page(writer, child_page_id)?;
            current_node = current_page.node.clone();
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
        }
    }
    println!("{:?} is leaf node: {:?}", current_page.page_id, current_node);


    // Make the leaf page dirty
    let (mut leaf_page, replacement_info) = writer.make_dirty(current_page);

    // Remove the page ID from the leaf node
    let mut removal_info = None;

    if let Node::FreeListLeaf(ref mut leaf_node) = leaf_page.node {
        // Assume we are exhausting page IDs from the lowest TSNs first
        if leaf_node.keys.is_empty() || leaf_node.keys[0] != tsn {
            return Err(LmdbError::DatabaseCorrupted(format!("Expected TSN {} not found: {:?}", tsn.0, leaf_node)));
        }

        let leaf_value = &mut leaf_node.values[0];

        if leaf_value.root_id.is_some() {
            return Err(LmdbError::DatabaseCorrupted("Free list subtree not implemented".to_string()));
        } else {
            // Remove the page ID from the list
            if let Some(pos) = leaf_value.page_ids.iter().position(|&id| id == used_page_id) {
                leaf_value.page_ids.remove(pos);
            } else {
                return Err(LmdbError::DatabaseCorrupted(format!("{:?} not found in {:?}", used_page_id, tsn)));
            }
            println!("Removed {:?} from {:?} in {:?}", used_page_id, tsn, leaf_page.page_id);

            // If no more page IDs, remove the TSN entry
            if leaf_value.page_ids.is_empty() {
                leaf_node.keys.remove(0);
                leaf_node.values.remove(0);
                println!("Removed {:?} from {:?}", tsn, leaf_page.page_id);
                // If leaf is empty, mark it for removal
                if leaf_node.keys.is_empty() {
                    println!("Empty leaf page {:?}: {:?}", leaf_page.page_id, leaf_page.node);
                    removal_info = Some(leaf_page.page_id);
                }
            } else {
                println!("Leaf page not empty {:?}: {:?}", leaf_page.page_id, leaf_page.node);
            }
        }
    } else {
        return Err(LmdbError::DatabaseCorrupted("Expected FreeListLeaf node".to_string()));
    }

    writer.mark_dirty(leaf_page);

    // Propagate replacements and removals up the stack
    let mut current_replacement_info = replacement_info;

    while let Some(internal_page_id) = stack.pop() {
        let internal_page = db.get_page(writer, internal_page_id)?;
        let (mut internal_page, parent_replacement_info) = writer.make_dirty(internal_page);

        if let Some((old_id, new_id)) = current_replacement_info {
            println!(
                "Replacing page {:?} with {:?} in {:?}: {:?}",
                old_id, new_id, internal_page.page_id, internal_page.node
            );
            if let Node::FreeListInternal(ref mut internal_node) = internal_page.node {
                // Replace the child ID
                if internal_node.child_ids[0] == old_id {
                    internal_node.child_ids[0] = new_id;
                } else {
                    return Err(LmdbError::DatabaseCorrupted("Child ID mismatch".to_string()));
                }
            } else {
                return Err(LmdbError::DatabaseCorrupted("Expected FreeListInternal node".to_string()));
            }
            println!(
                "Replaced page {:?} with {:?} in {:?}: {:?}",
                old_id, new_id, internal_page.page_id, internal_page.node
            );
        }

        if let Some(removed_page_id) = removal_info {
            writer.append_freed_page_id(removed_page_id);

            if let Node::FreeListInternal(ref mut internal_node) = internal_page.node {
                println!(
                    "Removing child ID {:?} from {:?}: {:?}",
                    removed_page_id, internal_page.page_id, internal_node
                );
                // Remove the child ID and key
                if internal_node.child_ids[0] == removed_page_id {
                    internal_node.child_ids.remove(0);

                    if !internal_node.keys.is_empty() {
                        internal_node.keys.remove(0);
                    }

                    // If internal node is empty or has only one child, mark it for removal
                    if internal_node.keys.is_empty() {
                        println!(
                            "Empty internal page {:?}: {:?}",
                            internal_page.page_id, internal_node
                        );
                        assert_eq!(internal_node.child_ids.len(), 1);
                        let orphaned_child_id = internal_node.child_ids[0];

                        writer.append_freed_page_id(internal_page.page_id);

                        if let Some((old_id, _)) = parent_replacement_info {
                            current_replacement_info = Some((old_id, orphaned_child_id));
                        } else {
                            current_replacement_info = Some((internal_page.page_id, orphaned_child_id));
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
        writer.mark_dirty(internal_page);

    }

    // Update the root if needed
    if let Some((old_id, new_id)) = current_replacement_info {
        if writer.freetree_root_id == old_id {
            writer.freetree_root_id = new_id;
        } else {
            return Err(LmdbError::DatabaseCorrupted("Root ID mismatch".to_string()));
        }
        println!(
            "Updated header with replacement root page {:?}",
            writer.freetree_root_id
        );
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

    // Extract the leaf node
    let mut modified_leaf_node = if let Node::PositionLeaf(ref node) = leaf_page.node {
        node.clone()
    } else {
        return Err(LmdbError::DatabaseCorrupted("Expected PositionLeaf node".to_string()));
    };

    // Find the insertion point
    let mut insert_index = 0;
    for (i, &k) in modified_leaf_node.keys.iter().enumerate() {
        if key < k {
            break;
        } else if key == k {
            // Replace existing value
            modified_leaf_node.values[i] = value.clone();

            // Update the page with the modified node
            leaf_page.node = Node::PositionLeaf(modified_leaf_node);
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
    modified_leaf_node.keys.insert(insert_index, key);
    modified_leaf_node.values.insert(insert_index, value.clone());

    // Update the page with the modified node
    leaf_page.node = Node::PositionLeaf(modified_leaf_node.clone());

    // Check if the page needs splitting
    let data = leaf_page.node.serialize()?;
    let estimated_size = PAGE_HEADER_SIZE + data.len();
    let needs_splitting = estimated_size > db.page_size;

    if needs_splitting {
        // Split the leaf node - we need to extract the node again since we updated it
        let mut modified_leaf_node = if let Node::PositionLeaf(ref node) = leaf_page.node {
            node.clone()
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected PositionLeaf node".to_string()));
        };

        // Split the leaf node
        let split_point = modified_leaf_node.keys.len() / 2;
        let promoted_key = modified_leaf_node.keys[split_point];

        // Create a new leaf node with the right half of the keys and values
        let new_leaf_node = PositionLeafNode {
            keys: modified_leaf_node.keys.split_off(split_point),
            values: modified_leaf_node.values.split_off(split_point),
            next_leaf_id: modified_leaf_node.next_leaf_id,
        };

        // Update the next_leaf_id of the original leaf
        modified_leaf_node.next_leaf_id = Some(writer.alloc_page_id());

        // Update the leaf page with the modified node
        leaf_page.node = Node::PositionLeaf(modified_leaf_node);

        // Create a new page for the new leaf node
        let next_leaf_id = if let Node::PositionLeaf(ref node) = leaf_page.node {
            node.next_leaf_id.unwrap()
        } else {
            return Err(LmdbError::DatabaseCorrupted("Expected PositionLeaf node".to_string()));
        };
        let new_leaf_page = Page::new(next_leaf_id, Node::PositionLeaf(new_leaf_node));
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
            let parent_data = parent_page.node.serialize()?;
            let parent_estimated_size = PAGE_HEADER_SIZE + parent_data.len();
            let parent_needs_splitting = parent_estimated_size > db.page_size;

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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_lmdb_init() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        
        {
            let db = Lmdb::new(&db_path, 4096).unwrap();
            assert!(db.pager.is_file_new);
        }
        
        {
            let db = Lmdb::new(&db_path, 4096).unwrap();
            assert!(!db.pager.is_file_new);
        }
    }

    #[test]
    fn test_write_transaction_incrementing_tsn_and_alternating_header() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let mut db = Lmdb::new(&db_path, 4096).unwrap();
        
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(TSN(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
        
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(TSN(2), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
        
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(TSN(3), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
        
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(TSN(4), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
        
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(TSN(5), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
    }

    #[test]
    fn test_read_transaction_header_and_tsn() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let mut db = Lmdb::new(&db_path, 4096).unwrap();
        
        // Initial reader should see TSN 0
        {
            assert_eq!(0, db.reader_tsns.lock().unwrap().len());
            let reader = db.reader().unwrap();
            assert_eq!(1, db.reader_tsns.lock().unwrap().len());
            assert_eq!(vec![TSN(0)], db.reader_tsns.lock().unwrap().values().cloned().collect::<Vec<_>>());
            assert_eq!(PageID(0), reader.header_page_id);
            assert_eq!(TSN(0), reader.tsn);
        }
        assert_eq!(0, db.reader_tsns.lock().unwrap().len());
        
        // Multiple nested readers
        {
            let reader1 = db.reader().unwrap();
            assert_eq!(vec![TSN(0)], db.reader_tsns.lock().unwrap().values().cloned().collect::<Vec<_>>());
            assert_eq!(PageID(0), reader1.header_page_id);
            assert_eq!(TSN(0), reader1.tsn);
            
            {
                let reader2 = db.reader().unwrap();
                assert_eq!(vec![TSN(0), TSN(0)], db.reader_tsns.lock().unwrap().values().cloned().collect::<Vec<_>>());
                assert_eq!(PageID(0), reader2.header_page_id);
                assert_eq!(TSN(0), reader2.tsn);
                
                {
                    let reader3 = db.reader().unwrap();
                    assert_eq!(vec![TSN(0), TSN(0), TSN(0)], db.reader_tsns.lock().unwrap().values().cloned().collect::<Vec<_>>());
                    assert_eq!(PageID(0), reader3.header_page_id);
                    assert_eq!(TSN(0), reader3.tsn);
                }
            }
        }
        assert_eq!(0, db.reader_tsns.lock().unwrap().len());
        
        // Writer transaction
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(0, db.reader_tsns.lock().unwrap().len());
            assert_eq!(TSN(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
        
        // Reader after writer
        {
            let reader = db.reader().unwrap();
            assert_eq!(vec![TSN(1)], db.reader_tsns.lock().unwrap().values().cloned().collect::<Vec<_>>());
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
            assert_eq!(0, writer.reusable_page_ids.len());
            
            // Check the free list root is page 2
            assert_eq!(PageID(2), writer.freetree_root_id);
            
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
            assert_eq!(2, writer.reusable_page_ids.len());
            assert!(writer.reusable_page_ids.iter().any(|(id, _)| *id == PageID(3)));
            assert!(writer.reusable_page_ids.iter().any(|(id, _)| *id == PageID(2)));
            
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
            let (_temp_dir, mut db) = construct_db(64);
            
            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();
            assert_eq!(PageID(0), header_page_id);
            
            // Get next page ID
            assert_eq!(PageID(4), header_node.next_page_id);
            
            // Construct a writer
            let tsn = TSN(1001);

            let mut txn = LmdbWriter::new(
                header_page_id,
                tsn,
                header_node.next_page_id,
                header_node.freetree_root_id,
                header_node.position_root_id,
            );
            
            // Check the free list tree root ID
            let initial_root_id = txn.freetree_root_id;
            assert_eq!(PageID(2), initial_root_id);
            
            // Allocate a page ID (to be inserted as "freed")
            let page_id = txn.alloc_page_id();
            
            // Check the allocated page ID is the "next" page ID
            assert_eq!(header_node.next_page_id, page_id);
            
            // Insert allocated page ID in free list tree
            let current_tsn = txn.tsn;
            insert_freed_page_id(&mut db, &mut txn, current_tsn, page_id).unwrap();
            
            // Check root page has been CoW-ed
            let expected_new_root_id = PageID(header_node.next_page_id.0 + 1);
            assert_eq!(expected_new_root_id, txn.freetree_root_id);
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
                        page_ids: vec![header_node.next_page_id],
                        root_id: None,
                    }];
                    assert_eq!(expected_values, node.values);
                },
                _ => panic!("Expected FreeListLeaf node"),
            }
        }
        
        #[test]
        fn test_remove_freed_page_id_from_root_leaf_root() {
            let (_temp_dir, mut db) = construct_db(64);
            
            // First, insert a page ID
            let mut txn;
            let inserted_tsn;
            let inserted_page_id;
            let previous_root_id;
            
            {
                // Get a writer
                txn = db.writer().unwrap();
                
                // Remember the initial root ID
                previous_root_id = txn.freetree_root_id;
                
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
                db.reader_tsns.lock().unwrap().insert(0, TSN(0));
            }
            
            // Start a new writer to remove inserted freed page ID
            {
                txn = db.writer().unwrap();
                
                // Check there are no free pages (because we blocked them with a reader)
                assert_eq!(0, txn.reusable_page_ids.len());
                
                // Remember the initial root ID and next page ID
                let initial_root_id = txn.freetree_root_id;
                let next_page_id = txn.next_page_id;
                
                // Remove what was inserted
                remove_freed_page_id(&mut db, &mut txn, inserted_tsn, inserted_page_id).unwrap();
                
                // Check root page has been CoW-ed
                let expected_new_root_id = next_page_id;
                assert_eq!(expected_new_root_id, txn.freetree_root_id);
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
            let (_temp_dir, mut db) = construct_db(64);
            
            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();

            // Create a writer
            let mut tsn = TSN(100);
            let mut txn = LmdbWriter::new(
                header_page_id,
                TSN(header_node.tsn.0 + 1),
                header_node.next_page_id,
                header_node.freetree_root_id,
                header_node.position_root_id,
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
                let root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
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
            let root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };
            
            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                txn.freetree_root_id,
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
            let (_temp_dir, mut db) = construct_db(64);
            
            // First, insert page IDs until we split a leaf
            println!("Inserting page IDs......");
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;
            
            {
                // Get a writer
                let mut txn = db.writer().unwrap();
                
                // Remember the initial root ID
                previous_root_id = txn.freetree_root_id;
                
                // Insert page IDs until we split a leaf
                let mut has_split_leaf = false;
                let mut tsn = txn.tsn;
                
                while !has_split_leaf {
                    // Increment TSN
                    tsn = TSN(tsn.0 + 1);

                    // Allocate and insert first page ID
                    let page_id1 = txn.alloc_page_id();
                    insert_freed_page_id(&mut db, &mut txn, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));
                    
                    // Allocate and insert second page ID
                    let page_id2 = txn.alloc_page_id();
                    insert_freed_page_id(&mut db, &mut txn, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));
                    
                    // Check if we've split the leaf
                    let root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
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
            println!("");
            println!("Removing all inserted page IDs......");

            {
                // Get latest header
                let (header_page_id, header_node) = db.get_latest_header().unwrap();

                // Create a new writer
                let mut txn = LmdbWriter::new(
                    header_page_id,
                    TSN(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.freetree_root_id,
                    header_node.position_root_id,
                );
                
                // Remember the initial root ID
                let old_root_id = txn.freetree_root_id;
                
                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    remove_freed_page_id(&mut db, &mut txn, *tsn, *page_id).unwrap();
                    println!("Dirty pages: {:?}", txn.dirty.keys());

                }
                
                // Check root page has been CoW-ed
                assert_ne!(old_root_id, txn.freetree_root_id);
                println!("Dirty pages: {:?}", txn.dirty.keys());

                assert_eq!(1, txn.dirty.len());
                assert!(txn.dirty.contains_key(&txn.freetree_root_id));
                
                let new_root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
                assert_eq!(txn.freetree_root_id, new_root_page.page_id);
                
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
                    txn.freetree_root_id,
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
            let (_temp_dir, mut db) = construct_db(64);

            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();

            // Create a writer
            let mut txn = LmdbWriter::new(
                header_page_id,
                TSN(header_node.tsn.0 + 1),
                header_node.next_page_id,
                header_node.freetree_root_id,
                header_node.position_root_id,
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
                let root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
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
                if inserted.len() > 100 {
                    panic!("Too many inserted page IDs");
                }
            }

            // Check keys and values of all pages
            let root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                txn.freetree_root_id,
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

            // We should have 13 active pages
            assert_eq!(13, active_page_ids.len());

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
            let (_temp_dir, mut db) = construct_db(64);

            // First, insert page IDs until we split an internal node
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;

            {
                // Get a writer
                let mut txn = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = txn.freetree_root_id;

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
                    let root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
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
                let (header_page_id, header_node) = db.get_latest_header().unwrap();

                // Create a new writer
                let mut txn = LmdbWriter::new(
                    header_page_id,
                    TSN(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.freetree_root_id,
                    header_node.position_root_id,
                );

                // Remember the initial root ID
                let old_root_id = txn.freetree_root_id;

                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    remove_freed_page_id(&mut db, &mut txn, *tsn, *page_id).unwrap();
                }

                // Check root page has been CoW-ed
                assert_ne!(old_root_id, txn.freetree_root_id);
                assert_eq!(1, txn.dirty.len());
                assert!(txn.dirty.contains_key(&txn.freetree_root_id));

                let new_root_page = txn.dirty.get(&txn.freetree_root_id).unwrap();
                assert_eq!(txn.freetree_root_id, new_root_page.page_id);

                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = txn.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));

                // There were 20 pages, and now we have 1. We have
                // freed page IDs for 10 old pages and 9 CoW pages.
                assert_eq!(19, txn.freed_page_ids.len());

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
                    txn.freetree_root_id,
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