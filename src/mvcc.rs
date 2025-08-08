use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Mutex;

use crate::mvcc_nodes::{
    FreeListInternalNode, FreeListLeafNode, FreeListLeafValue, HeaderNode, LmdbError, Node, PageID,
    Position, PositionIndexRecord, PositionInternalNode, PositionLeafNode, TSN,
};
use crate::mvcc_page::{PAGE_HEADER_SIZE, Page};
use crate::mvcc_pager::Pager;

// Result type alias
pub type Result<T> = std::result::Result<T, LmdbError>;

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

            // Create and write an empty free list root page
            let free_list_leaf = FreeListLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let free_list_page = Page::new(freetree_root_id, Node::FreeListLeaf(free_list_leaf));
            lmdb.write_page(&free_list_page)?;

            // Create and write an empty position index root page
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
            _ => {
                return Err(LmdbError::DatabaseCorrupted(
                    "Invalid header node type".to_string(),
                ));
            }
        };
        Ok(header_node)
    }

    pub fn read_page(&self, page_id: PageID) -> Result<Page> {
        let page_data = self.pager.read_page(page_id)?;
        println!("Read {:?} from file, deserializing...", page_id);
        Page::deserialize(page_id, &page_data)
    }

    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let serialized = &page.serialize()?;
        self.pager.write_page(page.page_id, serialized)?;
        println!("Wrote {:?} to file", page.page_id);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.pager.flush()?;
        Ok(())
    }

    pub fn reader(&mut self) -> Result<LmdbReader> {
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
        self.reader_tsns
            .lock()
            .unwrap()
            .insert(reader_id, reader.tsn);

        Ok(reader)
    }

    pub fn writer(&mut self) -> Result<LmdbWriter> {
        println!();
        println!("Constructing writer...");

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

        println!("Constructed writer with {:?}", writer.tsn);

        // Find the reusable page IDs.
        writer.find_reusable_page_ids(self)?;

        Ok(writer)
    }

    pub fn commit(&mut self, writer: &mut LmdbWriter) -> Result<()> {
        // Process reused and freed page IDs
        println!();
        println!("Commiting writer with {:?}", writer.tsn);

        while !writer.reused_page_ids.is_empty() || !writer.freed_page_ids.is_empty() {
            // Process reused page IDs
            while let Some((reused_page_id, tsn)) = writer.reused_page_ids.pop_front() {
                // Remove the reused page ID from the freed list tree
                writer.remove_freed_page_id(self, tsn, reused_page_id)?;
            }

            // Process freed page IDs
            while let Some(freed_page_id) = writer.freed_page_ids.pop_front() {
                // Remove dirty pages that were also freed
                writer.dirty.remove(&freed_page_id);

                // Insert the page ID in the freed list tree
                writer.insert_freed_page_id(self, writer.tsn, freed_page_id)?;
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

        println!("Committed writer with {:?}", writer.tsn);

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
        let deserialized_page = self.read_page(page_id)?;
        writer.insert_deserialized(deserialized_page);

        // Return page
        Ok(writer.deserialized.get(&page_id).unwrap().clone())
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

    pub fn get_page_ref(&mut self, db: &Lmdb, page_id: PageID) -> Result<&Page> {
        // Check the dirty pages first
        if self.dirty.contains_key(&page_id) {
            return Ok(self.dirty.get(&page_id).unwrap());
        }

        // Then check deserialized pages
        if self.deserialized.contains_key(&page_id) {
            return Ok(self.deserialized.get(&page_id).unwrap());
        }

        // Need to deserialize the page
        let deserialized_page = db.read_page(page_id)?;
        self.insert_deserialized(deserialized_page);

        // Return the deserialized page
        Ok(self.deserialized.get(&page_id).unwrap())
    }

    pub fn get_mut_dirty(&mut self, page_id: PageID) -> Result<&mut Page> {
        if let Some(page) = self.dirty.get_mut(&page_id) {
            Ok(page)
        } else {
            Err(LmdbError::DirtyPageNotFound(page_id))
        }
    }

    pub fn insert_deserialized(&mut self, page: Page) {
        self.deserialized.insert(page.page_id, page);
    }

    pub fn insert_dirty(&mut self, page: Page) -> Result<()> {
        if self.freed_page_ids.contains(&page.page_id) {
            return Err(LmdbError::PageAlreadyFreedError(page.page_id));
        }
        if self.dirty.contains_key(&page.page_id) {
            return Err(LmdbError::PageAlreadyDirtyError(page.page_id));
        }
        self.dirty.insert(page.page_id, page);
        Ok(())
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
                println!("Copied {:?} to {:?}: {:?}", old_page_id, new_page_id, page);
                return (new_page, replacement_info);
            }
        }

        (page, replacement_info)
    }

    pub fn get_dirty_page_id(&mut self, page_id: PageID) -> Result<PageID> {
        let mut dirty_page_id = page_id;
        if !self.freed_page_ids.iter().any(|&id| id == page_id) {
            if !self.dirty.contains_key(&page_id) {
                let old_page_id = page_id;
                self.freed_page_ids.push_back(old_page_id);

                let new_page_id = self.alloc_page_id();
                let old_page = self.deserialized.get(&old_page_id).unwrap();
                let new_page = Page {
                    page_id: new_page_id,
                    node: old_page.node.clone(),
                };

                self.dirty.insert(new_page_id, new_page);
                println!(
                    "Copied {:?} to {:?}: {:?}",
                    old_page_id, new_page_id, old_page.node
                );
                dirty_page_id = new_page_id;
            } else {
                println!("{:?} is already dirty", page_id);
            }
        } else {
            return Err(LmdbError::PageAlreadyFreedError(page_id));
        }
        Ok(dirty_page_id)
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

    pub fn find_reusable_page_ids(&mut self, db: &Lmdb) -> Result<()> {
        let mut reusable_page_ids: VecDeque<(PageID, TSN)> = VecDeque::new();
        // Get free page IDs
        println!("Finding reusable page IDs for TSN {:?}...", self.tsn);

        // Find the smallest reader TSN
        let smallest_reader_tsn = { db.reader_tsns.lock().unwrap().values().min().cloned() };
        println!("Smallest reader TSN: {:?}", smallest_reader_tsn);

        println!("Root is {:?}", self.freetree_root_id);

        // Walk the tree to find leaf nodes
        let mut stack = vec![(self.freetree_root_id, 0)];
        let mut is_finished = false;

        while let Some((page_id, idx)) = stack.pop() {
            if is_finished {
                break;
            }
            let page = { self.get_page_ref(db, page_id)? };
            match &page.node {
                Node::FreeListInternal(node) => {
                    println!("{:?} is internal node", page.page_id);

                    if idx < node.child_ids.len() {
                        let child_page_id = node.child_ids[idx];
                        stack.push((page.page_id, idx + 1));
                        stack.push((child_page_id, 0));
                    }
                }
                Node::FreeListLeaf(node) => {
                    println!("{:?} is leaf node", page.page_id);
                    for i in 0..node.keys.len() {
                        let tsn = node.keys[i];
                        if let Some(smallest) = smallest_reader_tsn {
                            if tsn > smallest {
                                is_finished = true;
                                break;
                            }
                        }

                        let leaf_value = &node.values[i];
                        if leaf_value.root_id.is_none() {
                            for &page_id in &leaf_value.page_ids {
                                reusable_page_ids.push_back((page_id, tsn));
                            }
                        } else {
                            // TODO: Traverse into free list subtree
                            return Err(LmdbError::DatabaseCorrupted(
                                "Free list subtree not implemented".to_string(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Invalid node type in free list tree".to_string(),
                    ));
                }
            }
        }

        self.reusable_page_ids = reusable_page_ids;
        println!("Found reusable page IDs: {:?}", self.reusable_page_ids);
        Ok(())
    }

    // Free list tree methods
    pub fn insert_freed_page_id(
        &mut self,
        db: &Lmdb,
        tsn: TSN,
        freed_page_id: PageID,
    ) -> Result<()> {
        println!("Inserting {:?} for {:?}", freed_page_id, tsn);
        println!("Root is {:?}", self.freetree_root_id);
        // Get the root page ID.
        let mut current_page_id = self.freetree_root_id;

        // Traverse the tree to find a leaf node
        let mut stack: Vec<PageID> = Vec::new();
        loop {
            let current_page_ref = self.get_page_ref(db, current_page_id)?;
            if matches!(current_page_ref.node, Node::FreeListLeaf(_)) {
                break;
            }
            if let Node::FreeListInternal(internal_node) = &current_page_ref.node {
                println!("{:?} is internal node", current_page_ref.page_id);
                stack.push(current_page_id);
                current_page_id = *internal_node.child_ids.last().unwrap();
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }
        }
        println!("{:?} is leaf node", current_page_id);

        // Make the leaf page dirty
        let dirty_page_id = { self.get_dirty_page_id(current_page_id)? };
        let replacement_info: Option<(PageID, PageID)> = {
            if dirty_page_id != current_page_id {
                Some((current_page_id, dirty_page_id))
            } else {
                None
            }
        };
        // Get a mutable leaf node....
        let dirty_leaf_page = self.get_mut_dirty(dirty_page_id)?;

        // Insert or append freed page ID to the list of page IDs for the TSN.
        if let Node::FreeListLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
            dirty_leaf_node.insert_or_append(tsn, freed_page_id)?;
            println!(
                "Inserted {:?} for {:?} in {:?}: {:?}",
                freed_page_id, tsn, dirty_page_id, dirty_leaf_node
            );
        } else {
            return Err(LmdbError::DatabaseCorrupted(
                "Expected FreeListLeaf node".to_string(),
            ));
        }

        // Check if the leaf needs splitting by estimating the serialized size
        let mut split_info: Option<(TSN, PageID)> = None;

        if dirty_leaf_page.calc_serialized_size() > db.page_size {
            // Split the leaf node
            if let Node::FreeListLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
                let (last_key, last_value) = dirty_leaf_node.pop_last_key_and_value()?;

                println!(
                    "Split leaf {:?}: {:?}",
                    dirty_page_id,
                    dirty_leaf_node.clone()
                );

                let new_leaf_node = FreeListLeafNode {
                    keys: vec![last_key],
                    values: vec![last_value],
                };

                let new_leaf_page_id = self.alloc_page_id();
                let new_leaf_page = Page::new(new_leaf_page_id, Node::FreeListLeaf(new_leaf_node));

                // Check if the new leaf page needs splitting

                if new_leaf_page.calc_serialized_size() > db.page_size {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Overflow freed page IDs for TSN to subtree not implemented".to_string(),
                    ));
                }

                println!(
                    "Created new leaf {:?}: {:?}",
                    new_leaf_page_id, new_leaf_page.node
                );
                self.insert_dirty(new_leaf_page)?;

                // Propagate the split up the tree
                println!("Promoting {:?} and {:?}", last_key, new_leaf_page_id);

                split_info = Some((last_key, new_leaf_page_id));
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected FreeListLeaf node".to_string(),
                ));
            }
        }
        // Propagate splits and replacements up the stack
        let mut current_replacement_info = replacement_info;
        while let Some(parent_page_id) = stack.pop() {
            // Make the internal page dirty
            let dirty_page_id = { self.get_dirty_page_id(parent_page_id)? };
            let parent_replacement_info: Option<(PageID, PageID)> = {
                if dirty_page_id != parent_page_id {
                    Some((parent_page_id, dirty_page_id))
                } else {
                    None
                }
            };
            // Get a mutable internal node....
            let dirty_internal_page = self.get_mut_dirty(dirty_page_id)?;

            if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                if let Some((old_id, new_id)) = current_replacement_info {
                    dirty_internal_node.replace_last_child_id(old_id, new_id)?;
                    println!(
                        "Replaced {:?} with {:?} in {:?}: {:?}",
                        old_id, new_id, dirty_page_id, dirty_internal_node
                    );
                } else {
                    println!("Nothing to replace in {:?}", dirty_page_id)
                }
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }

            if let Some((promoted_key, promoted_page_id)) = split_info {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    // Add the promoted key and page ID
                    dirty_internal_node
                        .append_promoted_key_and_page_id(promoted_key, promoted_page_id)?;

                    println!(
                        "Appended promoted key {:?} and child {:?} in {:?}: {:?}",
                        promoted_key, promoted_page_id, dirty_page_id, dirty_internal_node
                    )
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            }

            // Check if the internal page needs splitting

            if dirty_internal_page.calc_serialized_size() > db.page_size {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    println!("Splitting internal {:?}...", dirty_page_id);
                    // Split the internal node
                    // Ensure we have at least 3 keys and 4 child IDs before splitting
                    if dirty_internal_node.keys.len() < 3 || dirty_internal_node.child_ids.len() < 4
                    {
                        return Err(LmdbError::DatabaseCorrupted(
                            "Cannot split internal node with too few keys/children".to_string(),
                        ));
                    }

                    // Move the right-most key to a new node. Promote the next right-most key.
                    let (promoted_key, new_keys, new_child_ids) =
                        dirty_internal_node.split_off().unwrap();

                    // Ensure old node maintain the B-tree invariant: n keys should have n+1 child pointers
                    assert_eq!(
                        dirty_internal_node.keys.len() + 1,
                        dirty_internal_node.child_ids.len()
                    );

                    let new_internal_node = FreeListInternalNode {
                        keys: new_keys,
                        child_ids: new_child_ids,
                    };

                    // Ensure the new node also maintains the invariant
                    assert_eq!(
                        new_internal_node.keys.len() + 1,
                        new_internal_node.child_ids.len()
                    );

                    // Create a new internal page.
                    let new_internal_page_id = self.alloc_page_id();
                    let new_internal_page = Page::new(
                        new_internal_page_id,
                        Node::FreeListInternal(new_internal_node),
                    );
                    println!(
                        "Created internal {:?}: {:?}",
                        new_internal_page_id, new_internal_page.node
                    );
                    self.insert_dirty(new_internal_page)?;

                    split_info = Some((promoted_key, new_internal_page_id));
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            } else {
                split_info = None;
            }
            current_replacement_info = parent_replacement_info;
        }

        if let Some((promoted_key, promoted_page_id)) = split_info {
            // Create a new root
            let new_internal_node = FreeListInternalNode {
                keys: vec![promoted_key],
                child_ids: vec![self.freetree_root_id, promoted_page_id],
            };

            let new_root_page_id = self.alloc_page_id();
            let new_root_page =
                Page::new(new_root_page_id, Node::FreeListInternal(new_internal_node));
            println!(
                "Created new internal root {:?}: {:?}",
                new_root_page_id, new_root_page.node
            );
            self.insert_dirty(new_root_page)?;

            self.freetree_root_id = new_root_page_id;
        } else if let Some((old_id, new_id)) = current_replacement_info {
            if self.freetree_root_id == old_id {
                self.freetree_root_id = new_id;
                println!("Replaced root {:?} with {:?}", old_id, new_id);
            } else {
                return Err(LmdbError::RootIDMismatchError(old_id, new_id));
            }
        }

        Ok(())
    }

    pub fn remove_freed_page_id(
        &mut self,
        db: &Lmdb,
        tsn: TSN,
        used_page_id: PageID,
    ) -> Result<()> {
        println!();
        println!("Removing {:?} from {:?}...", used_page_id, tsn);
        println!("Root is {:?}", self.freetree_root_id);
        // Get the root page
        let mut current_page_id = self.freetree_root_id;

        // Traverse the tree to find a leaf node
        let mut stack: Vec<PageID> = Vec::new();
        let mut removed_page_ids: Vec<PageID> = Vec::new();

        loop {
            let current_page_ref = self.get_page_ref(db, current_page_id)?;
            if matches!(current_page_ref.node, Node::FreeListLeaf(_)) {
                break;
            }
            if let Node::FreeListInternal(internal_node) = &current_page_ref.node {
                println!("Page {:?} is internal node", current_page_ref.page_id);
                stack.push(current_page_id);
                current_page_id = *internal_node.child_ids.first().unwrap();
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }
        }
        println!("Page {:?} is leaf node", current_page_id);

        // Make the leaf page dirty
        let dirty_page_id = { self.get_dirty_page_id(current_page_id)? };
        let replacement_info: Option<(PageID, PageID)> = {
            if dirty_page_id != current_page_id {
                Some((current_page_id, dirty_page_id))
            } else {
                None
            }
        };
        // Get a mutable leaf node....
        let dirty_leaf_page = self.get_mut_dirty(dirty_page_id)?;

        // Remove the page ID from the leaf node
        let mut removal_info = None;

        if let Node::FreeListLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
            // Assume we are exhausting page IDs from the lowest TSNs first
            if dirty_leaf_node.keys.is_empty() || dirty_leaf_node.keys[0] != tsn {
                return Err(LmdbError::DatabaseCorrupted(format!(
                    "Expected TSN {} not found: {:?}",
                    tsn.0, dirty_leaf_node
                )));
            }

            let leaf_value = &mut dirty_leaf_node.values[0];

            if leaf_value.root_id.is_some() {
                return Err(LmdbError::DatabaseCorrupted(
                    "Free list subtree not implemented".to_string(),
                ));
            } else {
                // Remove the page ID from the list
                if let Some(pos) = leaf_value
                    .page_ids
                    .iter()
                    .position(|&id| id == used_page_id)
                {
                    leaf_value.page_ids.remove(pos);
                } else {
                    return Err(LmdbError::DatabaseCorrupted(format!(
                        "{:?} not found in {:?}",
                        used_page_id, tsn
                    )));
                }
                println!(
                    "Removed {:?} from {:?} in {:?}",
                    used_page_id, tsn, dirty_page_id
                );

                // If no more page IDs, remove the TSN entry
                if leaf_value.page_ids.is_empty() {
                    dirty_leaf_node.keys.remove(0);
                    dirty_leaf_node.values.remove(0);
                    println!("Removed {:?} from {:?}", tsn, dirty_page_id);
                    // If the leaf is empty, mark it for removal
                    if dirty_leaf_node.keys.is_empty() {
                        println!("Empty leaf page {:?}: {:?}", dirty_page_id, dirty_leaf_node);
                        removal_info = Some(dirty_page_id);
                    } else {
                        println!(
                            "Leaf page not empty {:?}: {:?}",
                            dirty_page_id, dirty_leaf_node
                        );
                    }
                } else {
                    println!("Leaf value not empty {:?}: {:?}", tsn, leaf_value);
                }
            }
        } else {
            return Err(LmdbError::DatabaseCorrupted(
                "Expected FreeListLeaf node".to_string(),
            ));
        }

        // Propagate replacements and removals up the stack
        let mut current_replacement_info = replacement_info;

        while let Some(parent_page_id) = stack.pop() {
            // Make the internal page dirty
            let dirty_page_id = { self.get_dirty_page_id(parent_page_id)? };
            let parent_replacement_info: Option<(PageID, PageID)> = {
                if dirty_page_id != parent_page_id {
                    Some((parent_page_id, dirty_page_id))
                } else {
                    None
                }
            };
            // Get a mutable internal node....
            let dirty_internal_page = self.get_mut_dirty(dirty_page_id)?;

            if let Some((old_id, new_id)) = current_replacement_info {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    // Replace the child ID
                    if dirty_internal_node.child_ids[0] == old_id {
                        dirty_internal_node.child_ids[0] = new_id;
                        println!(
                            "Replaced {:?} with {:?} in {:?}: {:?}",
                            old_id, new_id, dirty_page_id, dirty_internal_page
                        );
                    } else {
                        return Err(LmdbError::DatabaseCorrupted(
                            "Child ID mismatch".to_string(),
                        ));
                    }
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            }
            current_replacement_info = parent_replacement_info;

            if let Some(removed_page_id) = removal_info {
                removed_page_ids.push(removed_page_id);

                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    // Remove the child ID and key
                    if dirty_internal_node.child_ids[0] != removed_page_id {
                        return Err(LmdbError::DatabaseCorrupted(
                            "Child ID mismatch".to_string(),
                        ));
                    }
                    if dirty_internal_node.keys.is_empty() {
                        return Err(LmdbError::DatabaseCorrupted(
                            "Empty internal node keys".to_string(),
                        ));
                    }
                    dirty_internal_node.child_ids.remove(0);
                    dirty_internal_node.keys.remove(0);
                    println!(
                        "Removed {:?} from {:?}: {:?}",
                        removed_page_id, dirty_page_id, dirty_internal_node
                    );

                    // If the internal node is empty or has only one child, mark it for removal
                    if dirty_internal_node.keys.is_empty() {
                        println!(
                            "Empty internal page {:?}: {:?}",
                            dirty_page_id, dirty_internal_node
                        );
                        assert_eq!(dirty_internal_node.child_ids.len(), 1);
                        let orphaned_child_id = dirty_internal_node.child_ids[0];

                        removed_page_ids.push(dirty_page_id);

                        if let Some((old_id, _)) = parent_replacement_info {
                            current_replacement_info = Some((old_id, orphaned_child_id));
                        } else {
                            current_replacement_info = Some((dirty_page_id, orphaned_child_id));
                        }
                    }
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }

                removal_info = None;
            }
        }

        for &removed_page_id in &removed_page_ids {
            self.append_freed_page_id(removed_page_id);
        }

        // Update the root if needed
        if let Some((old_id, new_id)) = current_replacement_info {
            if self.freetree_root_id == old_id {
                self.freetree_root_id = new_id;
                println!("Replaced root {:?} with {:?}", old_id, new_id);
            } else {
                return Err(LmdbError::RootIDMismatchError(old_id, new_id));
            }
        }

        Ok(())
    }
}

// Position index functionality
pub fn insert_position(
    db: &mut Lmdb,
    writer: &mut LmdbWriter,
    key: Position,
    value: PositionIndexRecord,
) -> Result<()> {
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
            return Err(LmdbError::DatabaseCorrupted(
                "Expected PositionInternal node".to_string(),
            ));
        }
    }

    // Make the leaf page dirty
    let (mut leaf_page, replacement_info) = writer.make_dirty(current_page);

    // Extract the leaf node
    let mut modified_leaf_node = if let Node::PositionLeaf(ref node) = leaf_page.node {
        node.clone()
    } else {
        return Err(LmdbError::DatabaseCorrupted(
            "Expected PositionLeaf node".to_string(),
        ));
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
            writer.insert_dirty(leaf_page)?;

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
    modified_leaf_node
        .values
        .insert(insert_index, value.clone());

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
            return Err(LmdbError::DatabaseCorrupted(
                "Expected PositionLeaf node".to_string(),
            ));
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
            return Err(LmdbError::DatabaseCorrupted(
                "Expected PositionLeaf node".to_string(),
            ));
        };
        let new_leaf_page = Page::new(next_leaf_id, Node::PositionLeaf(new_leaf_node));
        writer.insert_dirty(new_leaf_page.clone())?;

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
                        return Err(LmdbError::DatabaseCorrupted(
                            "Child ID mismatch".to_string(),
                        ));
                    }
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Expected PositionInternal node".to_string(),
                    ));
                }
            }

            if let Some((promoted_key, promoted_page_id)) = split_info {
                if let Node::PositionInternal(ref mut internal_node) = parent_page.node {
                    // Insert the promoted key and page ID
                    internal_node.keys.insert(child_index, promoted_key);
                    internal_node
                        .child_ids
                        .insert(child_index + 1, promoted_page_id);
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Expected PositionInternal node".to_string(),
                    ));
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
                    let new_internal_page = Page::new(
                        new_internal_page_id,
                        Node::PositionInternal(new_internal_node),
                    );
                    writer.insert_dirty(new_internal_page)?;

                    split_info = Some((promoted_key, new_internal_page_id));
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Expected PositionInternal node".to_string(),
                    ));
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
            let new_root_page =
                Page::new(new_root_page_id, Node::PositionInternal(new_internal_node));
            writer.insert_dirty(new_root_page)?;

            writer.position_root_id = new_root_page_id;
        }
    } else {
        // No splitting needed, just update the page
        writer.insert_dirty(leaf_page)?;

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
    use serial_test::serial;
    use tempfile::tempdir;

    #[test]
    #[serial]
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
    #[serial]
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
    #[serial]
    fn test_read_transaction_header_and_tsn() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let mut db = Lmdb::new(&db_path, 4096).unwrap();

        // Initial reader should see TSN 0
        {
            assert_eq!(0, db.reader_tsns.lock().unwrap().len());
            let reader = db.reader().unwrap();
            assert_eq!(1, db.reader_tsns.lock().unwrap().len());
            assert_eq!(
                vec![TSN(0)],
                db.reader_tsns
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(0), reader.header_page_id);
            assert_eq!(TSN(0), reader.tsn);
        }
        assert_eq!(0, db.reader_tsns.lock().unwrap().len());

        // Multiple nested readers
        {
            let reader1 = db.reader().unwrap();
            assert_eq!(
                vec![TSN(0)],
                db.reader_tsns
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(0), reader1.header_page_id);
            assert_eq!(TSN(0), reader1.tsn);

            {
                let reader2 = db.reader().unwrap();
                assert_eq!(
                    vec![TSN(0), TSN(0)],
                    db.reader_tsns
                        .lock()
                        .unwrap()
                        .values()
                        .cloned()
                        .collect::<Vec<_>>()
                );
                assert_eq!(PageID(0), reader2.header_page_id);
                assert_eq!(TSN(0), reader2.tsn);

                {
                    let reader3 = db.reader().unwrap();
                    assert_eq!(
                        vec![TSN(0), TSN(0), TSN(0)],
                        db.reader_tsns
                            .lock()
                            .unwrap()
                            .values()
                            .cloned()
                            .collect::<Vec<_>>()
                    );
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
            assert_eq!(
                vec![TSN(1)],
                db.reader_tsns
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(1), reader.header_page_id);
            assert_eq!(TSN(1), reader.tsn);
        }
    }

    // #[test]
    // #[serial]
    // fn test_write_transaction_recycles_pages() {
    //     let temp_dir = tempdir().unwrap();
    //     let db_path = temp_dir.path().join("lmdb-test.db");
    //     let mut db = Lmdb::new(&db_path, 4096).unwrap();
    //
    //     let key1 = Position(1);
    //     let value1 = PositionIndexRecord {
    //         segment: 0,
    //         offset: 0,
    //         type_hash: Vec::new(),
    //     };
    //
    //     // First transaction
    //     {
    //         let mut writer = db.writer().unwrap();
    //
    //         // Check there are no free pages
    //         assert_eq!(0, writer.reusable_page_ids.len());
    //
    //         // Check the free list root is page 2
    //         assert_eq!(PageID(2), writer.freetree_root_id);
    //
    //         // Check the position root is page 3
    //         assert_eq!(PageID(3), writer.position_root_id);
    //
    //         // Insert position
    //         insert_position(&mut db, &mut writer, key1, value1.clone()).unwrap();
    //
    //         // Check the dirty page IDs
    //         assert_eq!(1, writer.dirty.len());
    //         assert!(writer.dirty.contains_key(&PageID(4)));
    //
    //         // Check the freed page IDs
    //         assert_eq!(1, writer.freed_page_ids.len());
    //         assert!(writer.freed_page_ids.contains(&PageID(3)));
    //
    //         db.commit(&mut writer).unwrap();
    //     }
    //
    //     // Read the position
    //     {
    //         let reader = db.reader().unwrap();
    //
    //         // Read the position root
    //         let page = db.read_page(reader.position_root_id).unwrap();
    //
    //         // Check it's a position leaf node
    //         if let Node::PositionLeaf(leaf_node) = &page.node {
    //             // Check the keys and values
    //             assert_eq!(vec![key1], leaf_node.keys);
    //             assert_eq!(vec![value1.clone()], leaf_node.values);
    //         } else {
    //             panic!("Expected PositionLeaf node");
    //         }
    //     }
    //
    //     // Second transaction
    //     let key2 = Position(2);
    //     let value2 = PositionIndexRecord {
    //         segment: 0,
    //         offset: 100,
    //         type_hash: Vec::new(),
    //     };
    //
    //     {
    //         let mut writer = db.writer().unwrap();
    //
    //         // Check there are two free pages (the old position root and the old free list root)
    //         assert_eq!(2, writer.reusable_page_ids.len());
    //         assert!(writer.reusable_page_ids.iter().any(|(id, _)| *id == PageID(3)));
    //         assert!(writer.reusable_page_ids.iter().any(|(id, _)| *id == PageID(2)));
    //
    //         // Insert position
    //         insert_position(&mut db, &mut writer, key2, value2.clone()).unwrap();
    //
    //         db.commit(&mut writer).unwrap();
    //     }
    //
    //     // Read the positions
    //     {
    //         let reader = db.reader().unwrap();
    //
    //         // Read the position root
    //         let page = db.read_page(reader.position_root_id).unwrap();
    //
    //         // Check it's a position leaf node
    //         if let Node::PositionLeaf(leaf_node) = &page.node {
    //             // Check the keys and values
    //             assert_eq!(vec![key1, key2], leaf_node.keys);
    //             assert_eq!(vec![value1.clone(), value2.clone()], leaf_node.values);
    //         } else {
    //             panic!("Expected PositionLeaf node");
    //         }
    //     }
    // }

    // FreeListTree tests
    mod free_list_tree_tests {
        use super::*;
        use serial_test::serial;
        use tempfile::tempdir;

        // Helper function to create a test database with a specified page size
        fn construct_db(page_size: usize) -> (tempfile::TempDir, Lmdb) {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("lmdb-test.db");
            let db = Lmdb::new(&db_path, page_size).unwrap();
            (temp_dir, db)
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_id_to_empty_leaf_root() {
            let (_temp_dir, mut db) = construct_db(64);

            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();
            assert_eq!(PageID(0), header_page_id);

            // Get next page ID
            assert_eq!(PageID(4), header_node.next_page_id);

            // Construct a writer
            let tsn = TSN(1001);

            let mut writer = LmdbWriter::new(
                header_page_id,
                tsn,
                header_node.next_page_id,
                header_node.freetree_root_id,
                header_node.position_root_id,
            );

            // Check the free list tree root ID
            let initial_root_id = writer.freetree_root_id;
            assert_eq!(PageID(2), initial_root_id);

            // Allocate a page ID (to be inserted as "freed")
            let page_id = writer.alloc_page_id();

            // Check the allocated page ID is the "next" page ID
            assert_eq!(header_node.next_page_id, page_id);

            // Insert the allocated page ID in the free list tree
            let current_tsn = writer.tsn;
            writer
                .insert_freed_page_id(&mut db, current_tsn, page_id)
                .unwrap();

            // Check the root page has been CoW-ed
            let expected_new_root_id = PageID(header_node.next_page_id.0 + 1);
            assert_eq!(expected_new_root_id, writer.freetree_root_id);
            assert_eq!(1, writer.dirty.len());
            assert!(writer.dirty.contains_key(&expected_new_root_id));

            let new_root_page = writer.dirty.get(&expected_new_root_id).unwrap();
            assert_eq!(expected_new_root_id, new_root_page.page_id);

            let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
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
                }
                _ => panic!("Expected FreeListLeaf node"),
            }
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_id_from_root_leaf_root() {
            let (_temp_dir, mut db) = construct_db(64);

            // First, insert a page ID
            let mut writer;
            let inserted_tsn;
            let inserted_page_id;
            let previous_root_id;

            {
                // Get a writer
                writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.freetree_root_id;

                // Allocate a page ID
                inserted_page_id = writer.alloc_page_id();

                // Insert the page ID
                inserted_tsn = writer.tsn;
                writer
                    .insert_freed_page_id(&mut db, inserted_tsn, inserted_page_id)
                    .unwrap();

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Block inserted page IDs from being reused
            {
                db.reader_tsns.lock().unwrap().insert(0, TSN(0));
            }

            // Start a new writer to remove inserted freed page ID
            {
                writer = db.writer().unwrap();

                // Check there are no free pages (because we blocked them with a reader)
                assert_eq!(0, writer.reusable_page_ids.len());

                // Remember the initial root ID and next page ID
                let initial_root_id = writer.freetree_root_id;
                let next_page_id = writer.next_page_id;

                // Remove what was inserted
                writer
                    .remove_freed_page_id(&db, inserted_tsn, inserted_page_id)
                    .unwrap();

                // Check the root page has been CoW-ed
                let expected_new_root_id = next_page_id;
                assert_eq!(expected_new_root_id, writer.freetree_root_id);
                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&expected_new_root_id));

                let new_root_page = writer.dirty.get(&expected_new_root_id).unwrap();
                assert_eq!(expected_new_root_id, new_root_page.page_id);

                let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
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
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }
            }
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_ids_until_split_leaf() {
            let (_temp_dir, mut db) = construct_db(64);

            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();

            // Create a writer
            let mut tsn = TSN(100);
            let mut writer = LmdbWriter::new(
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
                // Allocate and insert the first page ID
                let page_id1 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                inserted.push((tsn, page_id1));

                // Allocate and insert second page ID
                let page_id2 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                inserted.push((tsn, page_id2));

                // Increment TSN for next iteration
                tsn = TSN(tsn.0 + 1);

                // Check if we've split the leaf
                let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(_) => {
                        has_split_leaf = true;
                    }
                    _ => {}
                }
            }

            // Check keys and values of all pages
            let mut copy_inserted = inserted.clone();

            // Get the root node
            let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                writer.freetree_root_id,
                writer.position_root_id,
            ];

            // Collect freed page IDs
            let mut freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();

            // Check each child of the root
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);

                let child_page = writer.dirty.get(&child_id).unwrap();
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
                active_page_ids.len() + freed_page_ids.len()
                    - active_page_ids
                        .iter()
                        .filter(|id| freed_page_ids.contains(id))
                        .count()
            );

            // Check that all page IDs are accounted for
            let expected_page_ids: Vec<PageID> = (0..writer.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_ids_until_replace_internal_node_child_id() {
            let (_temp_dir, mut db) = construct_db(64);

            // Block inserted page IDs from being reused
            {
                db.reader_tsns.lock().unwrap().insert(0, TSN(0));
            }

            let mut has_split_leaf = false;
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();

            // Insert page IDs until we split a leaf
            while !has_split_leaf {
                // Create a writer
                let mut writer = db.writer().unwrap();
                // Allocate and insert the first page ID
                let page_id1 = writer.alloc_page_id();
                writer
                    .insert_freed_page_id(&db, writer.tsn, page_id1)
                    .unwrap();
                inserted.push((writer.tsn, page_id1));

                // Allocate and insert second page ID
                let page_id2 = writer.alloc_page_id();
                writer
                    .insert_freed_page_id(&db, writer.tsn, page_id2)
                    .unwrap();
                inserted.push((writer.tsn, page_id2));

                // Check if we've split the leaf
                let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(_) => {
                        has_split_leaf = true;
                    }
                    _ => {}
                }

                db.commit(&mut writer).unwrap();
            }

            let mut writer = db.writer().unwrap();
            let page_id3 = writer.alloc_page_id();
            writer
                .insert_freed_page_id(&db, writer.tsn, page_id3)
                .unwrap();
            db.commit(&mut writer).unwrap();
            inserted.push((writer.tsn, page_id3));

            writer = db.writer().unwrap();
            let page_id4 = writer.alloc_page_id();
            writer
                .insert_freed_page_id(&db, writer.tsn, page_id4)
                .unwrap();
            db.commit(&mut writer).unwrap();
            inserted.push((writer.tsn, page_id4));

            // Get the root node
            writer = db.writer().unwrap();
            let root_page = db.read_page(writer.freetree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                writer.freetree_root_id,
                writer.position_root_id,
            ];

            // Collect freed page IDs from the writer
            let mut freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();

            // Collect child page IDs
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);

                // Check each child page
                let child_page = db.read_page(child_id).unwrap();
                assert_eq!(child_id, child_page.page_id);

                let child_node = match &child_page.node {
                    Node::FreeListLeaf(node) => node,
                    _ => panic!("Expected FreeListLeaf node"),
                };

                // Check that the keys are properly ordered
                if i > 0 {
                    assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
                }

                // Collect freed page IDs from the child page values
                for child_value in child_node.values.clone() {
                    for page_id in child_value.page_ids {
                        freed_page_ids.push(page_id);
                    }
                }
            }

            // We have split two leaf nodes, so now we have 7 active pages
            assert_eq!(7, active_page_ids.len());

            // Audit page IDs
            let mut all_page_ids = active_page_ids.clone();
            all_page_ids.extend(freed_page_ids.clone());
            all_page_ids.sort();
            all_page_ids.dedup();

            assert_eq!(
                all_page_ids.len(),
                active_page_ids.len() + freed_page_ids.len()
                    - active_page_ids
                        .iter()
                        .filter(|id| freed_page_ids.contains(id))
                        .count()
            );

            // Check that all page IDs are accounted for
            let expected_page_ids: Vec<PageID> = (0..writer.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_ids_from_split_leaf() {
            let (_temp_dir, mut db) = construct_db(64);

            // First, insert page IDs until we split a leaf
            println!("Inserting page IDs......");
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.freetree_root_id;

                // Insert page IDs until we split a leaf
                let mut has_split_leaf = false;
                let mut tsn = writer.tsn;

                while !has_split_leaf {
                    // Increment TSN
                    tsn = TSN(tsn.0 + 1);

                    // Allocate and insert the first page ID
                    let page_id1 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));

                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));

                    // Check if we've split the leaf
                    let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(_) => {
                            has_split_leaf = true;
                        }
                        _ => {}
                    }
                }

                // Remember the final TSN
                previous_writer_tsn = writer.tsn;

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Now remove all the inserted freed page IDs
            println!();
            println!("Removing all inserted page IDs......");

            {
                // Get latest header
                let (header_page_id, header_node) = db.get_latest_header().unwrap();

                // Create a new writer
                let mut writer = LmdbWriter::new(
                    header_page_id,
                    TSN(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.freetree_root_id,
                    header_node.position_root_id,
                );

                // Remember the initial root ID
                let old_root_id = writer.freetree_root_id;

                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    writer.remove_freed_page_id(&db, *tsn, *page_id).unwrap();
                    println!("Dirty pages: {:?}", writer.dirty.keys());
                }

                // Check the root page has been CoW-ed
                assert_ne!(old_root_id, writer.freetree_root_id);

                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&writer.freetree_root_id));

                let new_root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                assert_eq!(writer.freetree_root_id, new_root_page.page_id);

                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));

                // There were three pages, and now we have one. We have
                // freed page IDs for three old pages and two CoW pages.
                assert_eq!(5, writer.freed_page_ids.len());

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
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Audit page IDs
                let active_page_ids = vec![
                    db.header_page_id0,
                    db.header_page_id1,
                    writer.freetree_root_id,
                    writer.position_root_id,
                ];

                // Collect all freed page IDs
                let mut all_freed_page_ids = freed_page_ids.clone();

                // Add page IDs from the leaf node
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        for (_, value) in node.keys.iter().zip(node.values.iter()) {
                            all_freed_page_ids.extend(value.page_ids.clone());
                        }
                    }
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
                let expected_page_ids: Vec<PageID> =
                    (0..writer.next_page_id.0).map(PageID).collect();
                assert_eq!(expected_page_ids, all_page_ids);
            }
        }

        #[test]
        #[serial]
        fn test_insert_freed_page_ids_until_split_internal() {
            let (_temp_dir, mut db) = construct_db(64);

            // Get latest header
            let (header_page_id, header_node) = db.get_latest_header().unwrap();

            // Create a writer
            let mut writer = LmdbWriter::new(
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
                // Allocate and insert the first page ID
                let page_id1 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                inserted.push((tsn, page_id1));

                // Allocate and insert second page ID
                let page_id2 = writer.alloc_page_id();
                writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                inserted.push((tsn, page_id2));

                // Increment TSN for next iteration
                tsn = TSN(tsn.0 + 1);

                // Check if we've split an internal node
                let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(root_node) => {
                        // Check if the first child is an internal node
                        if !root_node.child_ids.is_empty() {
                            let child_id = root_node.child_ids[0];
                            if let Some(child_page) = writer.dirty.get(&child_id) {
                                match &child_page.node {
                                    Node::FreeListInternal(_) => {
                                        has_split_internal = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    _ => {}
                }
                if inserted.len() > 100 {
                    panic!("Too many inserted page IDs");
                }
            }

            // Check keys and values of all pages
            let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                writer.freetree_root_id,
                writer.position_root_id,
            ];

            // Collect freed page IDs
            let mut freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();

            // Track the previous child for key ordering checks
            let mut previous_child: Option<&FreeListInternalNode> = None;

            // Check each child of the root
            for (i, &child_id) in root_node.child_ids.iter().enumerate() {
                active_page_ids.push(child_id);

                let child_page = writer.dirty.get(&child_id).unwrap();
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

                    let grand_child_page = writer.dirty.get(&grand_child_id).unwrap();
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
            let expected_page_ids: Vec<PageID> = (0..writer.next_page_id.0).map(PageID).collect();
            assert_eq!(expected_page_ids, all_page_ids);
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_ids_from_split_internal() {
            let (_temp_dir, mut db) = construct_db(64);

            // First, insert page IDs until we split an internal node
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.freetree_root_id;

                // Insert page IDs until we split an internal node
                let mut has_split_internal = false;
                let mut tsn = writer.tsn;

                while !has_split_internal {
                    // Increment TSN
                    tsn = TSN(tsn.0 + 1);
                    writer.tsn = tsn;

                    // Allocate and insert the first page ID
                    let page_id1 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));

                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));

                    // Check if we've split an internal node
                    let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(root_node) => {
                            // Check if the first child is an internal node
                            if !root_node.child_ids.is_empty() {
                                let child_id = root_node.child_ids[0];
                                if let Some(child_page) = writer.dirty.get(&child_id) {
                                    match &child_page.node {
                                        Node::FreeListInternal(_) => {
                                            has_split_internal = true;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // Remember the final TSN
                previous_writer_tsn = writer.tsn;

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Now remove all the inserted freed page IDs
            {
                // Get latest header
                let (header_page_id, header_node) = db.get_latest_header().unwrap();

                // Create a new writer
                let mut writer = LmdbWriter::new(
                    header_page_id,
                    TSN(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.freetree_root_id,
                    header_node.position_root_id,
                );

                // Remember the initial root ID
                let old_root_id = writer.freetree_root_id;

                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    writer.remove_freed_page_id(&db, *tsn, *page_id).unwrap();
                }

                // Check the root page has been CoW-ed
                assert_ne!(old_root_id, writer.freetree_root_id);
                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&writer.freetree_root_id));

                let new_root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                assert_eq!(writer.freetree_root_id, new_root_page.page_id);

                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));

                // There were 20 pages, and now we have 1. We have
                // freed page IDs for 10 old pages and 9 CoW pages.
                assert_eq!(19, writer.freed_page_ids.len());

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
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Audit page IDs
                let active_page_ids = vec![
                    db.header_page_id0,
                    db.header_page_id1,
                    writer.freetree_root_id,
                    writer.position_root_id,
                ];

                // Collect all freed page IDs
                let mut all_freed_page_ids = freed_page_ids.clone();

                // Add page IDs from the leaf node
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        for (_, value) in node.keys.iter().zip(node.values.iter()) {
                            all_freed_page_ids.extend(value.page_ids.clone());
                        }
                    }
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
                let expected_page_ids: Vec<PageID> =
                    (0..writer.next_page_id.0).map(PageID).collect();
                assert_eq!(expected_page_ids, all_page_ids);
            }
        }

        #[test]
        #[serial]
        fn test_remove_freed_page_ids_until_replace_old_id_with_orphaned_child_id() {
            // This test covers the case where an internal node is removed but leaving a
            // child page ID that must be replaced in the parent page, when the parent
            // page is not yet dirty. So, here, we must contrive to replace an old ID
            // with an orphaned child ID. We can do this by inserting freed page IDs
            // until there is an internal page split, and then removing them in order
            // until the first internal node is removed, using a new writer each time
            // to avoid the internal node becoming dirty before the last key is removed,
            // so that its parent internal node (which is the root internal node here)
            // will not have already has the old child page ID replaced with a dirty
            // child page ID.
            let (_temp_dir, mut db) = construct_db(64);

            // First, insert page IDs until we split an internal node
            let mut inserted: Vec<(TSN, PageID)> = Vec::new();

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Insert page IDs until we split an internal node
                let mut has_split_internal = false;
                let mut tsn = writer.tsn;

                while !has_split_internal {
                    // Increment TSN
                    tsn = TSN(tsn.0 + 1);
                    writer.tsn = tsn;

                    // Allocate and insert the first page ID
                    let page_id1 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));

                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));

                    // Check if we've split an internal node
                    let root_page = writer.dirty.get(&writer.freetree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(root_node) => {
                            // Check if the first child is an internal node
                            if !root_node.child_ids.is_empty() {
                                let child_id = root_node.child_ids[0];
                                if let Some(child_page) = writer.dirty.get(&child_id) {
                                    match &child_page.node {
                                        Node::FreeListInternal(_) => {
                                            has_split_internal = true;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // Commit the transaction
                db.commit(&mut writer).unwrap();
            }

            // Get all the free page IDs.
            db.reader_tsns.lock().unwrap().remove(&0);
            let writer = db.writer().unwrap();
            let free_page_ids = writer.reusable_page_ids.clone();

            // Block inserted page IDs from being reused
            db.reader_tsns.lock().unwrap().insert(0, TSN(0));

            // Remove each free page ID.
            for (page_id, tsn) in free_page_ids {
                let mut writer = db.writer().unwrap();
                writer.remove_freed_page_id(&db, tsn, page_id).unwrap();
                db.commit(&mut writer).unwrap();
            }
        }
    }
}
