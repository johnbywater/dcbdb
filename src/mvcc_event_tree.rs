use crate::mvcc::{Lmdb, LmdbWriter, Result};
use crate::mvcc_common::{LmdbError, PageID};
use crate::mvcc_node_event::{EventInternalNode, EventLeafNode, EventRecord, Position};
use crate::mvcc_nodes::Node;
use crate::mvcc_page::Page;

/// Append an event to the root event leaf page.
///
/// This function obtains a mutable reference to a dirty copy of the root event
/// leaf page (using copy-on-write if necessary) and appends the provided
/// Position to the keys and the EventRecord to the values.
pub fn append_event(db: &Lmdb, writer: &mut LmdbWriter, event: EventRecord, position: Position) -> Result<()> {
    let verbose = db.verbose;
    if verbose {
        println!("Appending {position:?} for {event:?}");
        println!("Root is {:?}", writer.event_tree_root_id);
    }
    // Get the current root page id for the event tree
    let mut current_page_id: PageID = writer.event_tree_root_id;

    // Traverse the tree to find a leaf node
    let mut stack: Vec<PageID> = Vec::new();
    loop {
        let current_page_ref = writer.get_page_ref(db, current_page_id)?;
        if matches!(current_page_ref.node, Node::EventLeaf(_)) {
            break;
        }
        if let Node::EventInternal(internal_node) = &current_page_ref.node {
            if verbose {
                println!("{:?} is internal node", current_page_ref.page_id);
            }
            stack.push(current_page_id);
            current_page_id = *internal_node.child_ids.last().unwrap();
        } else {
            return Err(LmdbError::DatabaseCorrupted(
                "Expected EventInternal node".to_string(),
            ));
        }
    }
    if verbose {
        println!("{current_page_id:?} is leaf node");
    }

    // Make the leaf page dirty
    let dirty_page_id = { writer.get_dirty_page_id(current_page_id)? };
    let replacement_info: Option<(PageID, PageID)> = {
        if dirty_page_id != current_page_id {
            Some((current_page_id, dirty_page_id))
        } else {
            None
        }
    };
    // Get a mutable leaf node....
    let dirty_leaf_page = writer.get_mut_dirty(dirty_page_id)?;

    // Ensure it is an EventLeaf and append the data
    match &mut dirty_leaf_page.node {
        Node::EventLeaf(node) => {
            node.keys.push(position);
            node.values.push(event);
        }
        _ => {
            return Err(LmdbError::DatabaseCorrupted(
                "Expected EventLeaf node at event tree root".to_string(),
            ));
        }
    }

    // Check if the leaf needs splitting by estimating the serialized size
    let mut split_info: Option<(Position, PageID)> = None;

    if dirty_leaf_page.calc_serialized_size() > db.page_size {
        // Split the leaf node
        if let Node::EventLeaf(dirty_leaf_node) = &mut dirty_leaf_page.node {
            let (last_key, last_value) = dirty_leaf_node.pop_last_key_and_value()?;

            if verbose {
                println!(
                    "Split leaf {:?}: {:?}",
                    dirty_page_id,
                    dirty_leaf_node.clone()
                );
            }
            let new_leaf_node = EventLeafNode {
                keys: vec![last_key],
                values: vec![last_value],
                next_leaf_id: PageID(0),
            };

            let new_leaf_page_id = writer.alloc_page_id();
            let new_leaf_page = Page::new(new_leaf_page_id, Node::EventLeaf(new_leaf_node));

            // Check if the new leaf page needs splitting

            let serialized_size = new_leaf_page.calc_serialized_size();
            if serialized_size > db.page_size {
                return Err(LmdbError::DatabaseCorrupted(
                    "Overflow event data not implemented".to_string(),
                ));
            }

            if verbose {
                println!(
                    "Created new leaf {:?}: {:?}",
                    new_leaf_page_id, new_leaf_page.node
                );
            }
            writer.insert_dirty(new_leaf_page)?;

            // dirty_leaf_node.next_leaf_id = new_leaf_page_id;

            // Propagate the split up the tree
            if verbose {
                println!("Promoting {last_key:?} and {new_leaf_page_id:?}");
            }
            split_info = Some((last_key, new_leaf_page_id));
        } else {
            return Err(LmdbError::DatabaseCorrupted(
                "Expected EventLeaf node".to_string(),
            ));
        }
    }

    // Propagate splits and replacements up the stack
    let mut current_replacement_info = replacement_info;
    while let Some(parent_page_id) = stack.pop() {
        // Make the internal page dirty
        let dirty_page_id = { writer.get_dirty_page_id(parent_page_id)? };
        let parent_replacement_info: Option<(PageID, PageID)> = {
            if dirty_page_id != parent_page_id {
                Some((parent_page_id, dirty_page_id))
            } else {
                None
            }
        };
        // Get a mutable internal node....
        let dirty_internal_page = writer.get_mut_dirty(dirty_page_id)?;

        if let Node::EventInternal(dirty_internal_node) = &mut dirty_internal_page.node {
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
            return Err(LmdbError::DatabaseCorrupted(
                "Expected EventInternal node".to_string(),
            ));
        }

        if let Some((promoted_key, promoted_page_id)) = split_info {
            if let Node::EventInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                // Add the promoted key and page ID
                dirty_internal_node.append_promoted_key_and_page_id(promoted_key, promoted_page_id)?;

                if verbose {
                    println!(
                        "Appended promoted key {promoted_key:?} and child {promoted_page_id:?} in {dirty_page_id:?}: {dirty_internal_node:?}"
                    );
                }
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected EventInternal node".to_string(),
                ));
            }
        }

        // Check if the internal page needs splitting

        if dirty_internal_page.calc_serialized_size() > db.page_size {
            if let Node::EventInternal(dirty_internal_node) = &mut dirty_internal_page.node {
                if verbose {
                    println!("Splitting internal {dirty_page_id:?}...");
                }
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

                let new_internal_node = EventInternalNode {
                    keys: new_keys,
                    child_ids: new_child_ids,
                };

                // Ensure the new node also maintains the invariant
                assert_eq!(
                    new_internal_node.keys.len() + 1,
                    new_internal_node.child_ids.len()
                );

                // Create a new internal page.
                let new_internal_page_id = writer.alloc_page_id();
                let new_internal_page = Page::new(
                    new_internal_page_id,
                    Node::EventInternal(new_internal_node),
                );
                if verbose {
                    println!(
                        "Created internal {:?}: {:?}",
                        new_internal_page_id, new_internal_page.node
                    );
                }
                writer.insert_dirty(new_internal_page)?;

                split_info = Some((promoted_key, new_internal_page_id));
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected EventInternal node".to_string(),
                ));
            }
        } else {
            split_info = None;
        }
        current_replacement_info = parent_replacement_info;
    }

    if let Some((promoted_key, promoted_page_id)) = split_info {
        // Create a new root
        let new_internal_node = EventInternalNode {
            keys: vec![promoted_key],
            child_ids: vec![writer.event_tree_root_id, promoted_page_id],
        };

        let new_root_page_id = writer.alloc_page_id();
        let new_root_page =
            Page::new(new_root_page_id, Node::EventInternal(new_internal_node));
        if verbose {
            println!(
                "Created new internal root {:?}: {:?}",
                new_root_page_id, new_root_page.node
            );
        }
        writer.insert_dirty(new_root_page)?;

        writer.event_tree_root_id = new_root_page_id;
    } else if let Some((old_id, new_id)) = current_replacement_info {
        if writer.event_tree_root_id == old_id {
            writer.event_tree_root_id = new_id;
            if verbose {
                println!("Replaced root {old_id:?} with {new_id:?}");
            }
        } else {
            return Err(LmdbError::RootIDMismatch(old_id, new_id));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc_nodes::Node;
    use serial_test::serial;
    use tempfile::tempdir;
    use rand::random;

    static VERBOSE: bool = false;

    // Helper function to create a test database with a specified page size
    fn construct_db(page_size: usize) -> (tempfile::TempDir, Lmdb) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let db = Lmdb::new(&db_path, page_size, VERBOSE).unwrap();
        (temp_dir, db)
    }

    #[test]
    #[serial]
    fn test_append_event_to_empty_leaf_root() {
        // Setup a temporary database
        let (_temp_dir, mut db) = construct_db(128);

        // Start a writer
        let mut writer = db.writer().unwrap();

        // Issue a new position
        let position = writer.issue_position();

        // Create an event record
        let record = EventRecord {
            event_type: "UserCreated".to_string(),
            data: vec![1, 2, 3, 4],
            tags: vec!["users".to_string(), "creation".to_string()],
            position,
        };

        // Call append_event
        append_event(&db, &mut writer, record.clone(), position).unwrap();

        // Verify that the dirty root page contains the appended key/value
        let new_root_id = writer.event_tree_root_id;
        assert!(writer.dirty.contains_key(&new_root_id));
        let page = writer.dirty.get(&new_root_id).unwrap();
        match &page.node {
            Node::EventLeaf(node) => {
                assert_eq!(vec![position], node.keys);
                assert_eq!(vec![record.clone()], node.values);
            }
            _ => panic!("Expected EventLeaf node"),
        }

        // Commit the writer and verify persistence
        db.commit(&mut writer).unwrap();

        // Read back the latest header and the persisted root event leaf page
        let (_header_page_id, header) = db.get_latest_header().unwrap();
        let persisted_page = db.read_page(header.event_tree_root_id).unwrap();
        match &persisted_page.node {
            Node::EventLeaf(node) => {
                assert_eq!(vec![position], node.keys);
                assert_eq!(vec![record], node.values);
            }
            _ => panic!("Expected EventLeaf node after commit"),
        }
    }

    #[test]
    #[serial]
    fn test_insert_events_until_split_leaf() {
        // Setup a temporary database
        let (_temp_dir, mut db) = construct_db(256);

        // Start a writer
        let mut writer = db.writer().unwrap();

        let mut has_split_leaf = false;
        let mut appended: Vec<(Position, EventRecord)> = Vec::new();

        // Insert events until we split a leaf
        while !has_split_leaf {
            // Issue a new position and create a record
            let position = writer.issue_position();
            let record = EventRecord {
                event_type: "UserCreated".to_string(),
                data: (0..8).map(|_| random::<u8>()).collect(),
                tags: vec!["users".to_string(), "creation".to_string()],
                position,
            };
            appended.push((position, record.clone()));

            // Append the event
            append_event(&db, &mut writer, record, position).unwrap();

            // Check if we've split the leaf
            let root_page = writer.dirty.get(&writer.event_tree_root_id).unwrap();
            match &root_page.node {
                Node::EventInternal(_) => {
                    has_split_leaf = true;
                }
                _ => {}
            }
        }

        // Check keys and values of all pages
        let mut copy_inserted = appended.clone();

        // Get the root node
        let root_page = writer.dirty.get(&writer.event_tree_root_id).unwrap();
        let root_node = match &root_page.node {
            Node::EventInternal(node) => node,
            _ => panic!("Expected EventInternal node"),
        };

        // Check each child of the root
        for (i, &child_id) in root_node.child_ids.iter().enumerate() {
            let child_page = writer.dirty.get(&child_id).unwrap();
            assert_eq!(child_id, child_page.page_id);

            let child_node = match &child_page.node {
                Node::EventLeaf(node) => node,
                _ => panic!("Expected EventLeaf node"),
            };

            // Check that the keys are properly ordered
            if i > 0 {
                assert_eq!(root_node.keys[i - 1], child_node.keys[0]);
            }

            // Check each key and value in the child
            for (k, &key) in child_node.keys.iter().enumerate() {
                let record = &child_node.values[k];
                let (appended_position, appended_record) = copy_inserted.remove(0);
                assert_eq!(appended_position, key);
                assert_eq!(appended_record, record.clone());
            }
        }
    }


}
