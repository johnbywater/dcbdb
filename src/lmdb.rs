use crate::common::Position;
use crate::common::{PageID, Tsn};
use crate::dcbapi::{DCBError, DCBResult};
use crate::events_tree_nodes::EventLeafNode;
use crate::free_lists_tree_nodes::{FreeListInternalNode, FreeListLeafNode};
use crate::header_node::HeaderNode;
use crate::node::Node;
use crate::page::Page;
use crate::pager::Pager;
use crate::tags_tree_nodes::TagsLeafNode;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Mutex;

// Reader transaction
pub struct Reader {
    pub header_page_id: PageID,
    pub tsn: Tsn,
    pub event_tree_root_id: PageID,
    pub tags_tree_root_id: PageID,
    reader_id: usize,
    reader_tsns: *const Mutex<HashMap<usize, Tsn>>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        // Safety: The Mutex is valid as long as the Db instance is valid,
        // and the Reader doesn't outlive the Db instance.
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
pub struct Writer {
    pub header_page_id: PageID,
    pub tsn: Tsn,
    pub next_page_id: PageID,
    pub free_lists_tree_root_id: PageID,
    pub events_tree_root_id: PageID,
    pub tags_tree_root_id: PageID,
    pub next_position: Position,
    pub reusable_page_ids: VecDeque<(PageID, Tsn)>,
    pub freed_page_ids: VecDeque<PageID>,
    pub deserialized: HashMap<PageID, Page>,
    pub dirty: HashMap<PageID, Page>,
    pub reused_page_ids: VecDeque<(PageID, Tsn)>,
    pub verbose: bool,
}

// Main LMDB structure
pub struct Lmdb {
    pub pager: Pager,
    pub reader_tsns: Mutex<HashMap<usize, Tsn>>,
    pub writer_lock: Mutex<()>,
    pub page_size: usize,
    pub header_page_id0: PageID,
    pub header_page_id1: PageID,
    reader_id_counter: Mutex<usize>,
    pub verbose: bool,
}

impl Lmdb {
    pub fn new(path: &Path, page_size: usize, verbose: bool) -> DCBResult<Self> {
        let pager = Pager::new(path, page_size)?;
        let header_page_id0 = PageID(0);
        let header_page_id1 = PageID(1);

        let lmdb = Self {
            pager,
            reader_tsns: Mutex::new(HashMap::new()),
            writer_lock: Mutex::new(()),
            page_size,
            header_page_id0,
            header_page_id1,
            reader_id_counter: Mutex::new(0),
            verbose,
        };

        if lmdb.pager.is_file_new {
            // Initialize new database
            let freetree_root_id = PageID(2);
            let position_root_id = PageID(3);
            let tags_root_id = PageID(4);
            let next_page_id = PageID(5);

            // Create and write header pages
            let header_node0 = HeaderNode {
                tsn: Tsn(0),
                next_page_id,
                free_lists_tree_root_id: freetree_root_id,
                event_tree_root_id: position_root_id,
                tags_tree_root_id: tags_root_id,
                next_position: Position(1),
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
            let event_leaf = EventLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let position_page = Page::new(position_root_id, Node::EventLeaf(event_leaf));
            lmdb.write_page(&position_page)?;

            // Create and write an empty tags index root page
            let tags_leaf = TagsLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
            };
            let tags_page = Page::new(tags_root_id, Node::TagsLeaf(tags_leaf));
            lmdb.write_page(&tags_page)?;

            lmdb.flush()?;
        }

        Ok(lmdb)
    }

    pub fn get_latest_header(&self) -> DCBResult<(PageID, HeaderNode)> {
        use std::thread::sleep;
        use std::time::Duration;

        let retries: usize = 5;
        let delay = Duration::from_millis(10);

        for attempt in 0..retries {
            let h0 = self.read_header(self.header_page_id0);
            let h1 = self.read_header(self.header_page_id1);

            match (h0, h1) {
                (Ok(header0), Ok(header1)) => {
                    if header1.tsn > header0.tsn {
                        return Ok((self.header_page_id1, header1));
                    } else {
                        return Ok((self.header_page_id0, header0));
                    }
                }
                (Ok(header0), Err(_)) => {
                    return Ok((self.header_page_id0, header0));
                }
                (Err(_), Ok(header1)) => {
                    return Ok((self.header_page_id1, header1));
                }
                (Err(e0), Err(e1)) => {
                    if attempt + 1 < retries {
                        if self.verbose {
                            println!(
                                "Both headers invalid on attempt {}: {:?} | {:?}. Retrying...",
                                attempt + 1,
                                e0,
                                e1
                            );
                        }
                        sleep(delay);
                        continue;
                    } else {
                        return Err(DCBError::DatabaseCorrupted(
                            format!(
                                "Both header pages appear corrupted after {} attempts: ({:?}) and ({:?})",
                                retries, e0, e1
                            ),
                        ));
                    }
                }
            }
        }
        // Unreachable due to returns in match and final error, but keep to satisfy type checker
        Err(DCBError::DatabaseCorrupted(
            "Unable to read a valid header".to_string(),
        ))
    }

    pub fn read_header(&self, page_id: PageID) -> DCBResult<HeaderNode> {
        let header = self.read_page(page_id)?;
        let header_node = match header.node {
            Node::Header(node) => node,
            _ => {
                return Err(DCBError::DatabaseCorrupted(
                    "Invalid header node type".to_string(),
                ));
            }
        };
        Ok(header_node)
    }

    pub fn read_page(&self, page_id: PageID) -> DCBResult<Page> {
        let mapped = self.pager.read_page_mmap_slice(page_id)?;
        if self.verbose {
            println!("Read {page_id:?} from file, deserializing...");
        }
        Page::deserialize(page_id, mapped.as_slice())
    }

    pub fn write_page(&self, page: &Page) -> DCBResult<()> {
        let serialized = &page.serialize()?;
        self.pager.write_page(page.page_id, serialized)?;
        if self.verbose {
            println!("Wrote {:?} to file", page.page_id);
        }
        Ok(())
    }

    pub fn flush(&self) -> DCBResult<()> {
        self.pager.flush()?;
        Ok(())
    }

    pub fn reader(&self) -> DCBResult<Reader> {
        let (header_page_id, header_node) = self.get_latest_header()?;

        // Generate a unique ID for this reader using the counter
        let reader_id = {
            let mut counter = self.reader_id_counter.lock().unwrap();
            *counter += 1;
            *counter
        };

        // Create the reader with the unique ID
        let reader = Reader {
            header_page_id,
            tsn: header_node.tsn,
            event_tree_root_id: header_node.event_tree_root_id,
            tags_tree_root_id: header_node.tags_tree_root_id,
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

    pub fn writer(&self) -> DCBResult<Writer> {
        if self.verbose {
            println!();
            println!("Constructing writer...");
        }

        // Get the latest header
        let (header_page_id, header_node) = self.get_latest_header()?;

        // Create the writer
        let mut writer = Writer::new(
            header_page_id,
            Tsn(header_node.tsn.0 + 1),
            header_node.next_page_id,
            header_node.free_lists_tree_root_id,
            header_node.event_tree_root_id,
            header_node.tags_tree_root_id,
            header_node.next_position,
            self.verbose,
        );

        if self.verbose {
            println!("Constructed writer with {:?}", writer.tsn);
        }

        // Find the reusable page IDs.
        writer.find_reusable_page_ids(self)?;

        Ok(writer)
    }

    pub fn commit(&self, writer: &mut Writer) -> DCBResult<()> {
        // Process reused and freed page IDs
        if self.verbose {
            println!();
            println!("Commiting writer with {:?}", writer.tsn);
        }

        while !writer.reused_page_ids.is_empty() || !writer.freed_page_ids.is_empty() {
            // Remove reused page IDs from free lists.
            while let Some((reused_page_id, tsn)) = writer.reused_page_ids.pop_front() {
                // Remove the reused page ID from the freed list tree
                writer.remove_free_page_id(self, tsn, reused_page_id)?;
            }

            // Process freed page IDs
            while let Some(freed_page_id) = writer.freed_page_ids.pop_front() {
                // Remove dirty pages that were also freed
                writer.dirty.remove(&freed_page_id);

                // Insert the page ID in the freed list tree
                writer.insert_freed_page_id(self, writer.tsn, freed_page_id)?;
            }
        }

        // Write all dirty pages in a single file lock
        if !writer.dirty.is_empty() {
            let mut serialized_pages: Vec<(PageID, Vec<u8>)> = Vec::with_capacity(writer.dirty.len());
            for page in writer.dirty.values() {
                let buf = page.serialize()?;
                serialized_pages.push((page.page_id, buf));
            }
            self.pager.write_pages(&serialized_pages)?;
            if self.verbose {
                println!("Wrote {} dirty page(s) to file", serialized_pages.len());
            }
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
            free_lists_tree_root_id: writer.free_lists_tree_root_id,
            event_tree_root_id: writer.events_tree_root_id,
            tags_tree_root_id: writer.tags_tree_root_id,
            next_position: writer.next_position,
        };

        let header_page = Page::new(header_page_id, Node::Header(header_node));
        self.write_page(&header_page)?;

        // Flush changes to disk
        self.flush()?;

        if self.verbose {
            println!("Committed writer with {:?}", writer.tsn);
        }

        Ok(())
    }
}

// Implementation for LmdbWriter
impl Writer {
    pub fn new(
        header_page_id: PageID,
        tsn: Tsn,
        next_page_id: PageID,
        free_lists_tree_root_id: PageID,
        events_tree_root_id: PageID,
        tags_tree_root_id: PageID,
        next_position: Position,
        verbose: bool,
    ) -> Self {
        Self {
            header_page_id,
            tsn,
            next_page_id,
            free_lists_tree_root_id,
            events_tree_root_id,
            tags_tree_root_id,
            next_position,
            reusable_page_ids: VecDeque::new(),
            freed_page_ids: VecDeque::new(),
            deserialized: HashMap::new(),
            dirty: HashMap::new(),
            reused_page_ids: VecDeque::new(),
            verbose,
        }
    }

    pub fn issue_position(&mut self) -> Position {
        let pos = self.next_position;
        self.next_position = Position(self.next_position.0 + 1);
        pos
    }

    pub fn get_page_ref(&mut self, lmdb: &Lmdb, page_id: PageID) -> DCBResult<&Page> {
        // Check the dirty pages first
        if self.dirty.contains_key(&page_id) {
            return Ok(self.dirty.get(&page_id).unwrap());
        }

        // Then check deserialized pages
        if self.deserialized.contains_key(&page_id) {
            return Ok(self.deserialized.get(&page_id).unwrap());
        }

        // Need to deserialize the page
        let deserialized_page = lmdb.read_page(page_id)?;
        self.insert_deserialized(deserialized_page);

        // Return the deserialized page
        Ok(self.deserialized.get(&page_id).unwrap())
    }

    pub fn get_mut_dirty(&mut self, page_id: PageID) -> DCBResult<&mut Page> {
        if let Some(page) = self.dirty.get_mut(&page_id) {
            Ok(page)
        } else {
            Err(DCBError::DirtyPageNotFound(page_id))
        }
    }

    pub fn insert_deserialized(&mut self, page: Page) {
        self.deserialized.insert(page.page_id, page);
    }

    pub fn insert_dirty(&mut self, page: Page) -> DCBResult<()> {
        if self.freed_page_ids.contains(&page.page_id) {
            return Err(DCBError::PageAlreadyFreed(page.page_id));
        }
        if self.dirty.contains_key(&page.page_id) {
            return Err(DCBError::PageAlreadyDirty(page.page_id));
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

    pub fn get_dirty_page_id(&mut self, page_id: PageID) -> DCBResult<PageID> {
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
                if self.verbose {
                    println!(
                        "Copied {:?} to {:?}: {:?}",
                        old_page_id, new_page_id, old_page.node
                    );
                }
                dirty_page_id = new_page_id;
            } else if self.verbose {
                println!("{page_id:?} is already dirty");
            }
        } else {
            return Err(DCBError::PageAlreadyFreed(page_id));
        }
        Ok(dirty_page_id)
    }

    pub fn append_freed_page_id(&mut self, page_id: PageID) {
        let verbose = self.verbose;
        if !self.freed_page_ids.iter().any(|&id| id == page_id) {
            self.freed_page_ids.push_back(page_id);
            if verbose {
                println!("Appended {page_id:?} to freed_page_ids");
            }
            if self.dirty.contains_key(&page_id) {
                if verbose {
                    println!("Page ID {page_id:?} was in dirty and was removed");
                }
                self.dirty.remove(&page_id);
            }
            if verbose && self.dirty.contains_key(&page_id) {
                println!("Page ID {page_id:?} is still in dirty!!!!!");
            }
        }
    }

    pub fn find_reusable_page_ids(&mut self, lmdb: &Lmdb) -> DCBResult<()> {
        let verbose = self.verbose;
        let mut reusable_page_ids: VecDeque<(PageID, Tsn)> = VecDeque::new();
        // Get free page IDs
        if verbose {
            println!("Finding reusable page IDs for TSN {:?}...", self.tsn);
        }

        // Find the smallest reader TSN
        let smallest_reader_tsn = { lmdb.reader_tsns.lock().unwrap().values().min().cloned() };
        if verbose {
            println!("Smallest reader TSN: {smallest_reader_tsn:?}");
        }

        if verbose {
            println!("Root is {:?}", self.free_lists_tree_root_id);
        }
        // Walk the tree to find leaf nodes
        let mut stack = vec![(self.free_lists_tree_root_id, 0)];
        let mut is_finished = false;

        while let Some((page_id, idx)) = stack.pop() {
            if is_finished {
                break;
            }
            let page = { self.get_page_ref(lmdb, page_id)? };
            match &page.node {
                Node::FreeListInternal(node) => {
                    if verbose {
                        println!("{:?} is internal node", page.page_id);
                    }
                    if idx < node.child_ids.len() {
                        let child_page_id = node.child_ids[idx];
                        stack.push((page.page_id, idx + 1));
                        stack.push((child_page_id, 0));
                    }
                }
                Node::FreeListLeaf(node) => {
                    if verbose {
                        println!("{:?} is leaf node", page.page_id);
                    }
                    for i in 0..node.keys.len() {
                        let tsn = node.keys[i];
                        if let Some(smallest) = smallest_reader_tsn {
                            if tsn > smallest {
                                is_finished = true;
                                break;
                            }
                        }

                        let leaf_value = &node.values[i];
                        if leaf_value.root_id == PageID(0) {
                            for &page_id in &leaf_value.page_ids {
                                reusable_page_ids.push_back((page_id, tsn));
                            }
                        } else {
                            // TODO: Traverse into free list subtree
                            return Err(DCBError::DatabaseCorrupted(
                                "Free list subtree not implemented".to_string(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(DCBError::DatabaseCorrupted(
                        "Invalid node type in free list tree".to_string(),
                    ));
                }
            }
        }

        self.reusable_page_ids = reusable_page_ids;
        if verbose {
            println!("Found reusable page IDs: {:?}", self.reusable_page_ids);
        }
        Ok(())
    }

    // Free list tree methods
    pub fn insert_freed_page_id(
        &mut self,
        lmdb: &Lmdb,
        tsn: Tsn,
        freed_page_id: PageID,
    ) -> DCBResult<()> {
        let verbose = self.verbose;
        if verbose {
            println!("Inserting {freed_page_id:?} for {tsn:?}");
            println!("Root is {:?}", self.free_lists_tree_root_id);
        }
        // Get the root page ID.
        let mut current_page_id = self.free_lists_tree_root_id;

        // Traverse the tree to find a leaf node
        let mut stack: Vec<PageID> = Vec::new();
        loop {
            let current_page_ref = self.get_page_ref(lmdb, current_page_id)?;
            if matches!(current_page_ref.node, Node::FreeListLeaf(_)) {
                break;
            }
            if let Node::FreeListInternal(internal_node) = &current_page_ref.node {
                if verbose {
                    println!("{:?} is internal node", current_page_ref.page_id);
                }
                stack.push(current_page_id);
                current_page_id = *internal_node.child_ids.last().unwrap();
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }
        }
        if verbose {
            println!("{current_page_id:?} is leaf node");
        }
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
            if verbose {
                println!(
                    "Inserted {freed_page_id:?} for {tsn:?} in {dirty_page_id:?}: {dirty_leaf_node:?}"
                );
            }
        } else {
            return Err(DCBError::DatabaseCorrupted(
                "Expected FreeListLeaf node".to_string(),
            ));
        }

        // Check if the leaf needs splitting by estimating the serialized size
        let mut split_info: Option<(Tsn, PageID)> = None;

        if dirty_leaf_page.calc_serialized_size() > lmdb.page_size {
            // Split the leaf node
            if let Node::FreeListLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
                let (last_key, last_value) = dirty_leaf_node.pop_last_key_and_value()?;

                if verbose {
                    println!(
                        "Split leaf {:?}: {:?}",
                        dirty_page_id,
                        dirty_leaf_node.clone()
                    );
                }
                let new_leaf_node = FreeListLeafNode {
                    keys: vec![last_key],
                    values: vec![last_value],
                };

                let new_leaf_page_id = self.alloc_page_id();
                let new_leaf_page = Page::new(new_leaf_page_id, Node::FreeListLeaf(new_leaf_node));

                // Check if the new leaf page needs splitting

                let serialized_size = new_leaf_page.calc_serialized_size();
                if serialized_size > lmdb.page_size {
                    return Err(DCBError::DatabaseCorrupted(
                        "Overflow freed page IDs for TSN to subtree not implemented".to_string(),
                    ));
                }

                if verbose {
                    println!(
                        "Created new leaf {:?}: {:?}",
                        new_leaf_page_id, new_leaf_page.node
                    );
                }
                self.insert_dirty(new_leaf_page)?;

                // Propagate the split up the tree
                if verbose {
                    println!("Promoting {last_key:?} and {new_leaf_page_id:?}");
                }
                split_info = Some((last_key, new_leaf_page_id));
            } else {
                return Err(DCBError::DatabaseCorrupted(
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
                    if verbose {
                        println!(
                            "Replaced {old_id:?} with {new_id:?} in {dirty_page_id:?}: {dirty_internal_node:?}"
                        );
                    }
                } else if verbose {
                    println!("Nothing to replace in {dirty_page_id:?}")
                }
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }

            if let Some((promoted_key, promoted_page_id)) = split_info {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    // Add the promoted key and page ID
                    dirty_internal_node
                        .append_promoted_key_and_page_id(promoted_key, promoted_page_id)?;

                    if verbose {
                        println!(
                            "Appended promoted key {promoted_key:?} and child {promoted_page_id:?} in {dirty_page_id:?}: {dirty_internal_node:?}"
                        );
                    }
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            }

            // Check if the internal page needs splitting

            if dirty_internal_page.calc_serialized_size() > lmdb.page_size {
                if let Node::FreeListInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                    if verbose {
                        println!("Splitting internal {dirty_page_id:?}...");
                    }
                    // Split the internal node
                    // Ensure we have at least 3 keys and 4 child IDs before splitting
                    if dirty_internal_node.keys.len() < 3 || dirty_internal_node.child_ids.len() < 4
                    {
                        return Err(DCBError::DatabaseCorrupted(
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
                    if verbose {
                        println!(
                            "Created internal {:?}: {:?}",
                            new_internal_page_id, new_internal_page.node
                        );
                    }
                    self.insert_dirty(new_internal_page)?;

                    split_info = Some((promoted_key, new_internal_page_id));
                } else {
                    return Err(DCBError::DatabaseCorrupted(
                        "Expected FreeListInternal node".to_string(),
                    ));
                }
            } else {
                split_info = None;
            }
            current_replacement_info = parent_replacement_info;
        }

        if let Some((old_id, new_id)) = current_replacement_info {
            if self.free_lists_tree_root_id == old_id {
                self.free_lists_tree_root_id = new_id;
                if verbose {
                    println!("Replaced root {old_id:?} with {new_id:?}");
                }
            } else {
                return Err(DCBError::RootIDMismatch(old_id, new_id));
            }
        }

        if let Some((promoted_key, promoted_page_id)) = split_info {
            // Create a new root
            let new_internal_node = FreeListInternalNode {
                keys: vec![promoted_key],
                child_ids: vec![self.free_lists_tree_root_id, promoted_page_id],
            };

            let new_root_page_id = self.alloc_page_id();
            let new_root_page =
                Page::new(new_root_page_id, Node::FreeListInternal(new_internal_node));
            if verbose {
                println!(
                    "Created new internal root {:?}: {:?}",
                    new_root_page_id, new_root_page.node
                );
            }
            self.insert_dirty(new_root_page)?;

            self.free_lists_tree_root_id = new_root_page_id;
        }

        Ok(())
    }

    pub fn remove_free_page_id(
        &mut self,
        lmdb: &Lmdb,
        tsn: Tsn,
        used_page_id: PageID,
    ) -> DCBResult<()> {
        let verbose = self.verbose;
        if verbose {
            println!();
            println!("Removing {used_page_id:?} from {tsn:?}...");
            println!("Root is {:?}", self.free_lists_tree_root_id);
        }
        // Get the root page
        let mut current_page_id = self.free_lists_tree_root_id;

        // Traverse the tree to find a leaf node
        let mut stack: Vec<PageID> = Vec::new();
        let mut removed_page_ids: Vec<PageID> = Vec::new();

        loop {
            let current_page_ref = self.get_page_ref(lmdb, current_page_id)?;
            if matches!(current_page_ref.node, Node::FreeListLeaf(_)) {
                break;
            }
            if let Node::FreeListInternal(internal_node) = &current_page_ref.node {
                if verbose {
                    println!("Page {:?} is internal node", current_page_ref.page_id);
                }
                stack.push(current_page_id);
                current_page_id = *internal_node.child_ids.first().unwrap();
            } else {
                return Err(DCBError::DatabaseCorrupted(
                    "Expected FreeListInternal node".to_string(),
                ));
            }
        }
        if verbose {
            println!("Page {current_page_id:?} is leaf node");
        }

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
                return Err(DCBError::DatabaseCorrupted(format!(
                    "Expected TSN {} not found: {:?}",
                    tsn.0, dirty_leaf_node
                )));
            }

            let leaf_value = &mut dirty_leaf_node.values[0];

            if leaf_value.root_id != PageID(0) {
                return Err(DCBError::DatabaseCorrupted(
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
                    return Err(DCBError::DatabaseCorrupted(format!(
                        "{used_page_id:?} not found in {tsn:?}"
                    )));
                }
                if verbose {
                    println!("Removed {used_page_id:?} from {tsn:?} in {dirty_page_id:?}");
                }

                // If no more page IDs, remove the TSN entry
                if leaf_value.page_ids.is_empty() {
                    dirty_leaf_node.keys.remove(0);
                    dirty_leaf_node.values.remove(0);
                    if verbose {
                        println!("Removed {tsn:?} from {dirty_page_id:?}");
                    }
                    // If the leaf is empty, mark it for removal
                    if dirty_leaf_node.keys.is_empty() {
                        if verbose {
                            println!("Empty leaf page {dirty_page_id:?}: {dirty_leaf_node:?}");
                        }
                        removal_info = Some(dirty_page_id);
                    } else if verbose {
                        println!("Leaf page not empty {dirty_page_id:?}: {dirty_leaf_node:?}");
                    }
                } else if verbose {
                    println!("Leaf value not empty {tsn:?}: {leaf_value:?}");
                }
            }
        } else {
            return Err(DCBError::DatabaseCorrupted(
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
                        if verbose {
                            println!(
                                "Replaced {old_id:?} with {new_id:?} in {dirty_page_id:?}: {dirty_internal_page:?}"
                            );
                        }
                    } else {
                        return Err(DCBError::DatabaseCorrupted("Child ID mismatch".to_string()));
                    }
                } else {
                    return Err(DCBError::DatabaseCorrupted(
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
                        return Err(DCBError::DatabaseCorrupted("Child ID mismatch".to_string()));
                    }
                    if dirty_internal_node.keys.is_empty() {
                        return Err(DCBError::DatabaseCorrupted(
                            "Empty internal node keys".to_string(),
                        ));
                    }
                    dirty_internal_node.child_ids.remove(0);
                    dirty_internal_node.keys.remove(0);
                    if verbose {
                        println!(
                            "Removed {removed_page_id:?} from {dirty_page_id:?}: {dirty_internal_node:?}"
                        );
                    }

                    // If the internal node is empty or has only one child, mark it for removal
                    if dirty_internal_node.keys.is_empty() {
                        if verbose {
                            println!(
                                "Empty internal page {dirty_page_id:?}: {dirty_internal_node:?}"
                            );
                        }
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
                    return Err(DCBError::DatabaseCorrupted(
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
            if self.free_lists_tree_root_id == old_id {
                self.free_lists_tree_root_id = new_id;
                if verbose {
                    println!("Replaced root {old_id:?} with {new_id:?}");
                }
            } else {
                return Err(DCBError::RootIDMismatch(old_id, new_id));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::free_lists_tree_nodes::FreeListLeafValue;
    use serial_test::serial;
    use tempfile::tempdir;

    static VERBOSE: bool = false;

    #[test]
    #[serial]
    fn test_lmdb_init() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");

        {
            let db = Lmdb::new(&db_path, 4096, VERBOSE).unwrap();
            assert!(db.pager.is_file_new);
        }

        {
            let db = Lmdb::new(&db_path, 4096, VERBOSE).unwrap();
            assert!(!db.pager.is_file_new);
        }
    }

    #[test]
    #[serial]
    fn test_write_transaction_incrementing_tsn_and_alternating_header() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let db = Lmdb::new(&db_path, 4096, VERBOSE).unwrap();

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(2), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(3), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(4), writer.tsn);
            assert_eq!(PageID(1), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        {
            let mut writer = db.writer().unwrap();
            assert_eq!(Tsn(5), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }
    }

    #[test]
    #[serial]
    fn test_read_transaction_header_and_tsn() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let db = Lmdb::new(&db_path, 4096, VERBOSE).unwrap();

        // Initial reader should see TSN 0
        {
            assert_eq!(0, db.reader_tsns.lock().unwrap().len());
            let reader = db.reader().unwrap();
            assert_eq!(1, db.reader_tsns.lock().unwrap().len());
            assert_eq!(
                vec![Tsn(0)],
                db.reader_tsns
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(0), reader.header_page_id);
            assert_eq!(Tsn(0), reader.tsn);
        }
        assert_eq!(0, db.reader_tsns.lock().unwrap().len());

        // Multiple nested readers
        {
            let reader1 = db.reader().unwrap();
            assert_eq!(
                vec![Tsn(0)],
                db.reader_tsns
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(0), reader1.header_page_id);
            assert_eq!(Tsn(0), reader1.tsn);

            {
                let reader2 = db.reader().unwrap();
                assert_eq!(
                    vec![Tsn(0), Tsn(0)],
                    db.reader_tsns
                        .lock()
                        .unwrap()
                        .values()
                        .cloned()
                        .collect::<Vec<_>>()
                );
                assert_eq!(PageID(0), reader2.header_page_id);
                assert_eq!(Tsn(0), reader2.tsn);

                {
                    let reader3 = db.reader().unwrap();
                    assert_eq!(
                        vec![Tsn(0), Tsn(0), Tsn(0)],
                        db.reader_tsns
                            .lock()
                            .unwrap()
                            .values()
                            .cloned()
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(PageID(0), reader3.header_page_id);
                    assert_eq!(Tsn(0), reader3.tsn);
                }
            }
        }
        assert_eq!(0, db.reader_tsns.lock().unwrap().len());

        // Writer transaction
        {
            let mut writer = db.writer().unwrap();
            assert_eq!(0, db.reader_tsns.lock().unwrap().len());
            assert_eq!(Tsn(1), writer.tsn);
            assert_eq!(PageID(0), writer.header_page_id);
            db.commit(&mut writer).unwrap();
        }

        // Reader after writer
        {
            let reader = db.reader().unwrap();
            assert_eq!(
                vec![Tsn(1)],
                db.reader_tsns
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            );
            assert_eq!(PageID(1), reader.header_page_id);
            assert_eq!(Tsn(1), reader.tsn);
        }
    }

    #[test]
    #[serial]
    fn test_copy_on_write_page_reuse() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let db = Lmdb::new(&db_path, 4096, VERBOSE).unwrap();
        // First transaction
        {
            let mut writer = db.writer().unwrap();

            // Check there are no free pages
            assert_eq!(0, writer.reusable_page_ids.len());

            // Check the free list root is page 2
            assert_eq!(PageID(2), writer.free_lists_tree_root_id);

            // Check the position root is page 3
            assert_eq!(PageID(3), writer.events_tree_root_id);

            // Allocate and insert free page ID.
            let free_page_id = writer.alloc_page_id();
            assert_eq!(PageID(5), free_page_id);
            writer
                .insert_freed_page_id(&db, writer.tsn, free_page_id)
                .unwrap();

            // Check the dirty page IDs
            assert_eq!(1, writer.dirty.len());
            assert_eq!(PageID(6), *writer.dirty.keys().collect::<Vec<_>>()[0]);

            // Check the freed page IDs
            assert_eq!(1, writer.freed_page_ids.len());
            assert_eq!(PageID(2), writer.freed_page_ids[0]);

            db.commit(&mut writer).unwrap();
        }

        // Second transaction
        {
            let mut writer = db.writer().unwrap();

            // Check there are two free pages
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((PageID(5), Tsn(1)), writer.reusable_page_ids[0]);
            assert_eq!((PageID(2), Tsn(1)), writer.reusable_page_ids[1]);

            // Check the free list root is page 2
            assert_eq!(PageID(6), writer.free_lists_tree_root_id);

            // Check the position root is page 3
            assert_eq!(PageID(3), writer.events_tree_root_id);

            // Allocate and insert free page ID.
            let free_page_id = writer.alloc_page_id();
            assert_eq!(PageID(5), free_page_id);
            writer
                .insert_freed_page_id(&db, writer.tsn, free_page_id)
                .unwrap();

            // Check the dirty page IDs
            assert_eq!(1, writer.dirty.len());
            assert_eq!(PageID(2), *writer.dirty.keys().collect::<Vec<_>>()[0]);

            // Check the freed page IDs
            assert_eq!(1, writer.freed_page_ids.len());
            assert_eq!(PageID(6), writer.freed_page_ids[0]);

            db.commit(&mut writer).unwrap();
        }

        // Third transaction
        {
            let mut writer = db.writer().unwrap();

            // Check there are two free pages
            assert_eq!(2, writer.reusable_page_ids.len());
            assert_eq!((PageID(5), Tsn(2)), writer.reusable_page_ids[0]);
            assert_eq!((PageID(6), Tsn(2)), writer.reusable_page_ids[1]);

            // Check the free list root is page 2
            assert_eq!(PageID(2), writer.free_lists_tree_root_id);

            // Check the position root is page 3
            assert_eq!(PageID(3), writer.events_tree_root_id);

            // Allocate and insert free page ID.
            let free_page_id = writer.alloc_page_id();
            assert_eq!(PageID(5), free_page_id);
            writer
                .insert_freed_page_id(&db, writer.tsn, free_page_id)
                .unwrap();

            // Check the dirty page IDs
            assert_eq!(1, writer.dirty.len());
            assert_eq!(PageID(6), *writer.dirty.keys().collect::<Vec<_>>()[0]);

            // Check the freed page IDs
            assert_eq!(1, writer.freed_page_ids.len());
            assert_eq!(PageID(2), writer.freed_page_ids[0]);

            db.commit(&mut writer).unwrap();
        }
    }

    // FreeListTree tests
    mod free_list_tree_tests {
        use super::*;
        use serial_test::serial;
        use tempfile::tempdir;

        // Helper function to create a test database with a specified page size
        fn construct_db(page_size: usize) -> (tempfile::TempDir, Lmdb) {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("lmdb-test.db");
            let db = Lmdb::new(&db_path, page_size, VERBOSE).unwrap();
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
            assert_eq!(PageID(5), header_node.next_page_id);

            // Construct a writer
            let tsn = Tsn(1001);

            let mut writer = Writer::new(
                header_page_id,
                tsn,
                header_node.next_page_id,
                header_node.free_lists_tree_root_id,
                header_node.event_tree_root_id,
                header_node.tags_tree_root_id,
                header_node.next_position,
                VERBOSE,
            );

            // Check the free list tree root ID
            let initial_root_id = writer.free_lists_tree_root_id;
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
            assert_eq!(expected_new_root_id, writer.free_lists_tree_root_id);
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
                        root_id: PageID(0),
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
                previous_root_id = writer.free_lists_tree_root_id;

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
                db.reader_tsns.lock().unwrap().insert(0, Tsn(0));
            }

            // Start a new writer to remove inserted freed page ID
            {
                writer = db.writer().unwrap();

                // Check there are no free pages (because we blocked them with a reader)
                assert_eq!(0, writer.reusable_page_ids.len());

                // Remember the initial root ID and next page ID
                let initial_root_id = writer.free_lists_tree_root_id;
                let next_page_id = writer.next_page_id;

                // Remove what was inserted
                writer
                    .remove_free_page_id(&db, inserted_tsn, inserted_page_id)
                    .unwrap();

                // Check the root page has been CoW-ed
                let expected_new_root_id = next_page_id;
                assert_eq!(expected_new_root_id, writer.free_lists_tree_root_id);
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
                            root_id: PageID(0),
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
            let mut tsn = Tsn(100);
            let mut writer = Writer::new(
                header_page_id,
                Tsn(header_node.tsn.0 + 1),
                header_node.next_page_id,
                header_node.free_lists_tree_root_id,
                header_node.event_tree_root_id,
                header_node.tags_tree_root_id,
                header_node.next_position,
                VERBOSE,
            );

            let mut has_split_leaf = false;
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();

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
                tsn = Tsn(tsn.0 + 1);

                // Check if we've split the leaf
                let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
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
            let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                writer.free_lists_tree_root_id,
                writer.events_tree_root_id,
                writer.tags_tree_root_id,
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
        fn test_insert_freed_page_ids_until_replace_internal_node_child_id() {
            let (_temp_dir, db) = construct_db(128);

            // Block inserted page IDs from being reused
            {
                db.reader_tsns.lock().unwrap().insert(0, Tsn(0));
            }

            let mut has_split_leaf = false;
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();

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

                // Check if we've split the leaf
                let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                match &root_page.node {
                    Node::FreeListInternal(_) => {
                        has_split_leaf = true;
                    }
                    _ => {}
                }

                if !has_split_leaf {
                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer
                        .insert_freed_page_id(&db, writer.tsn, page_id2)
                        .unwrap();
                    inserted.push((writer.tsn, page_id2));

                    // Check if we've split the leaf
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                    match &root_page.node {
                        Node::FreeListInternal(_) => {
                            has_split_leaf = true;
                        }
                        _ => {}
                    }
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
            let root_page = db.read_page(writer.free_lists_tree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                writer.free_lists_tree_root_id,
                writer.events_tree_root_id,
                writer.tags_tree_root_id,
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

            // We have split a leaf node, so now we have 8 active pages
            assert_eq!(8, active_page_ids.len());

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
            let (_temp_dir, mut db) = construct_db(128);

            // First, insert page IDs until we split a leaf
            if VERBOSE {
                println!("Inserting page IDs......");
            }
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.free_lists_tree_root_id;

                // Insert page IDs until we split a leaf
                let mut has_split_leaf = false;
                let mut tsn = writer.tsn;

                while !has_split_leaf {
                    // Increment TSN
                    tsn = Tsn(tsn.0 + 1);

                    // Allocate and insert the first page ID
                    let page_id1 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id1).unwrap();
                    inserted.push((tsn, page_id1));

                    // Allocate and insert second page ID
                    let page_id2 = writer.alloc_page_id();
                    writer.insert_freed_page_id(&mut db, tsn, page_id2).unwrap();
                    inserted.push((tsn, page_id2));

                    // Check if we've split the leaf
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
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
            if VERBOSE {
                println!();
                println!("Removing all inserted page IDs......");
            }

            {
                // Get latest header
                let (header_page_id, header_node) = db.get_latest_header().unwrap();

                // Create a new writer
                let mut writer = Writer::new(
                    header_page_id,
                    Tsn(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.free_lists_tree_root_id,
                    header_node.event_tree_root_id,
                    header_node.tags_tree_root_id,
                    header_node.next_position,
                    VERBOSE,
                );

                // Remember the initial root ID
                let old_root_id = writer.free_lists_tree_root_id;

                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    writer.remove_free_page_id(&db, *tsn, *page_id).unwrap();
                    if VERBOSE {
                        println!("Dirty pages: {:?}", writer.dirty.keys());
                    }
                }

                // Check the root page has been CoW-ed
                assert_ne!(old_root_id, writer.free_lists_tree_root_id);

                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&writer.free_lists_tree_root_id));

                let new_root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                assert_eq!(writer.free_lists_tree_root_id, new_root_page.page_id);

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
                            root_id: PageID(0),
                        }];
                        assert_eq!(expected_values, node.values);
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Audit page IDs
                let active_page_ids = vec![
                    db.header_page_id0,
                    db.header_page_id1,
                    writer.free_lists_tree_root_id,
                    writer.events_tree_root_id,
                    writer.tags_tree_root_id,
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
            let mut writer = Writer::new(
                header_page_id,
                Tsn(header_node.tsn.0 + 1),
                header_node.next_page_id,
                header_node.free_lists_tree_root_id,
                header_node.event_tree_root_id,
                header_node.tags_tree_root_id,
                header_node.next_position,
                VERBOSE,
            );

            // Start with TSN 100
            let mut tsn = Tsn(100);
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();
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
                tsn = Tsn(tsn.0 + 1);

                // Check if we've split an internal node
                let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
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
            let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
            let root_node = match &root_page.node {
                Node::FreeListInternal(node) => node,
                _ => panic!("Expected FreeListInternal node"),
            };

            // Collect active page IDs
            let mut active_page_ids = vec![
                db.header_page_id0,
                db.header_page_id1,
                writer.free_lists_tree_root_id,
                writer.events_tree_root_id,
                writer.tags_tree_root_id,
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

            // We should have 10 active pages
            assert_eq!(11, active_page_ids.len());

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
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();
            let previous_root_id;
            let previous_writer_tsn;

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Remember the initial root ID
                previous_root_id = writer.free_lists_tree_root_id;

                // Insert page IDs until we split an internal node
                let mut has_split_internal = false;
                let mut tsn = writer.tsn;

                while !has_split_internal {
                    // Increment TSN
                    tsn = Tsn(tsn.0 + 1);
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
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
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
                let mut writer = Writer::new(
                    header_page_id,
                    Tsn(header_node.tsn.0 + 1),
                    header_node.next_page_id,
                    header_node.free_lists_tree_root_id,
                    header_node.event_tree_root_id,
                    header_node.tags_tree_root_id,
                    header_node.next_position,
                    VERBOSE,
                );

                // Remember the initial root ID
                let old_root_id = writer.free_lists_tree_root_id;

                // Remove all inserted page IDs
                for (tsn, page_id) in inserted.iter() {
                    writer.remove_free_page_id(&db, *tsn, *page_id).unwrap();
                }

                // Check the root page has been CoW-ed
                assert_ne!(old_root_id, writer.free_lists_tree_root_id);
                assert_eq!(1, writer.dirty.len());
                assert!(writer.dirty.contains_key(&writer.free_lists_tree_root_id));

                let new_root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
                assert_eq!(writer.free_lists_tree_root_id, new_root_page.page_id);

                // Check old root ID is in freed page IDs
                let freed_page_ids: Vec<PageID> = writer.freed_page_ids.iter().cloned().collect();
                assert!(freed_page_ids.contains(&old_root_id));

                // There were 14 pages, and now we have 1. We have
                // freed page IDs for 7 old pages and 6 CoW pages.
                assert_eq!(13, writer.freed_page_ids.len());

                // Check keys and values of the new root page
                match &new_root_page.node {
                    Node::FreeListLeaf(node) => {
                        let expected_keys = vec![previous_writer_tsn];
                        assert_eq!(expected_keys, node.keys);

                        let expected_values = vec![FreeListLeafValue {
                            page_ids: vec![previous_root_id],
                            root_id: PageID(0),
                        }];
                        assert_eq!(expected_values, node.values);
                    }
                    _ => panic!("Expected FreeListLeaf node"),
                }

                // Audit page IDs
                let active_page_ids = vec![
                    db.header_page_id0,
                    db.header_page_id1,
                    writer.free_lists_tree_root_id,
                    writer.events_tree_root_id,
                    writer.tags_tree_root_id,
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
            let (_temp_dir, mut db) = construct_db(128);

            // First, insert page IDs until we split an internal node
            let mut inserted: Vec<(Tsn, PageID)> = Vec::new();

            {
                // Get a writer
                let mut writer = db.writer().unwrap();

                // Insert page IDs until we split an internal node
                let mut has_split_internal = false;
                let mut tsn = writer.tsn;

                while !has_split_internal {
                    // Increment TSN
                    tsn = Tsn(tsn.0 + 1);
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
                    let root_page = writer.dirty.get(&writer.free_lists_tree_root_id).unwrap();
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
            db.reader_tsns.lock().unwrap().insert(0, Tsn(0));

            // Remove each free page ID.
            for (page_id, tsn) in free_page_ids {
                let mut writer = db.writer().unwrap();
                writer.remove_free_page_id(&db, tsn, page_id).unwrap();
                db.commit(&mut writer).unwrap();
            }
        }
    }
}
