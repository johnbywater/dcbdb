use crate::mvcc::{Lmdb, LmdbWriter, Result};
use crate::mvcc_common::{LmdbError, PageID};
use crate::mvcc_node_event::{EventRecord, Position};
use crate::mvcc_nodes::Node;

/// Append an event to the root event leaf page.
///
/// This function obtains a mutable reference to a dirty copy of the root event
/// leaf page (using copy-on-write if necessary) and appends the provided
/// Position to the keys and the EventRecord to the values.
pub fn append_event(db: &Lmdb, writer: &mut LmdbWriter, event: EventRecord, position: Position) -> Result<()> {
    // Get the current root page id for the event tree
    let root_page_id: PageID = writer.event_tree_root_id;

    // Ensure the page is deserialized and available
    writer.get_page_ref(db, root_page_id)?;

    // Make (or get) a dirty copy of the root page
    let dirty_page_id = writer.get_dirty_page_id(root_page_id)?;

    // Obtain a mutable reference to the dirty page
    let dirty_page = writer.get_mut_dirty(dirty_page_id)?;

    // Ensure it is an EventLeaf and append the data
    match &mut dirty_page.node {
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

    // If copy-on-write replaced the root page, update the writer's root id
    if dirty_page_id != root_page_id {
        writer.event_tree_root_id = dirty_page_id;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc_nodes::Node;
    use serial_test::serial;
    use tempfile::tempdir;

    #[test]
    #[serial]
    fn test_append_event_to_root_leaf() {
        // Setup a temporary database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let mut db = Lmdb::new(&db_path, 4096, false).unwrap();

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
                assert_eq!(vec![record], node.values);
            }
            _ => panic!("Expected EventLeaf node"),
        }
    }
}
