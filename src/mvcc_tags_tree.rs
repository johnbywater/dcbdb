use crate::mvcc::{Lmdb, LmdbReader, LmdbWriter, Result};
use crate::mvcc_common::{LmdbError, PageID, Position};
use crate::mvcc_node_tags::{TagHash, TagsInternalNode, TagsLeafNode, TagsLeafValue};
use crate::mvcc_nodes::Node;
use crate::mvcc_page::Page;

/// Insert a Position into the tags tree at the given TagHash key.
///
/// Behaves similarly to LmdbWriter::insert_freed_page_id, but operates on the
/// Tags tree (TagsLeafNode/TagsInternalNode) and maintains TagHash keys in
/// sorted order. If the TagHash already exists in the leaf and the value has no
/// subtree (root_id == PageID(0)), the Position is appended to the positions
/// vector. Otherwise a new key/value pair is inserted at the correct sorted
/// index.
pub fn insert_position(db: &Lmdb, writer: &mut LmdbWriter, tag: TagHash, pos: Position) -> Result<()> {
    let verbose = db.verbose;
    if verbose {
        println!("Inserting position {pos:?} for tag {:?}", tag);
        println!("Tags root is {:?}", writer.tags_tree_root_id);
    }

    // Start from the root of the tags tree
    let mut current_page_id: PageID = writer.tags_tree_root_id;

    // Traverse to the correct leaf, keeping track of parent ids and the child index taken at each step
    let mut stack: Vec<(PageID, usize)> = Vec::new();
    loop {
        let current_page_ref = writer.get_page_ref(db, current_page_id)?;
        match &current_page_ref.node {
            Node::TagsLeaf(_) => break,
            Node::TagsInternal(internal_node) => {
                if verbose {
                    println!("{:?} is TagsInternal node", current_page_ref.page_id);
                }
                // Decide child index using B-tree separator convention:
                // keys[i] is the minimum key in child i+1
                let child_idx = match internal_node.keys.binary_search(&tag) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                // Push (parent_page_id, child_idx taken)
                stack.push((current_page_id, child_idx));
                current_page_id = internal_node.child_ids[child_idx];
            }
            _ => {
                return Err(LmdbError::DatabaseCorrupted(
                    "Invalid node type in tags tree (expected TagsInternal/TagsLeaf)".to_string(),
                ));
            }
        }
    }

    if verbose {
        println!("{:?} is TagsLeaf node", current_page_id);
    }

    // Make the leaf page dirty (copy-on-write if needed)
    let dirty_leaf_page_id = writer.get_dirty_page_id(current_page_id)?;
    // If copy-on-write happened, we need to replace the child id in the parent at the recorded index
    let mut replacement_info: Option<(PageID, PageID, usize)> = None;
    if dirty_leaf_page_id != current_page_id {
        if let Some((_, child_idx)) = stack.last().cloned() {
            replacement_info = Some((current_page_id, dirty_leaf_page_id, child_idx));
        } else {
            // Replacing the root leaf
            replacement_info = Some((current_page_id, dirty_leaf_page_id, 0));
        }
    }

    // Get a mutable reference to the dirty leaf page
    let dirty_leaf_page = writer.get_mut_dirty(dirty_leaf_page_id)?;

    // Insert or append the position at the sorted index
    if let Node::TagsLeaf(leaf) = &mut dirty_leaf_page.node {
        match leaf.keys.binary_search(&tag) {
            Ok(i) => {
                // Append to existing value if no subtree
                if leaf.values[i].root_id == PageID(0) {
                    leaf.values[i].positions.push(pos);
                    if verbose {
                        println!("Appended position to existing tag at index {}", i);
                    }
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Per-tag subtree not implemented".to_string(),
                    ));
                }
            }
            Err(i) => {
                // Insert new key/value at the correct index
                leaf.keys.insert(i, tag);
                leaf.values.insert(
                    i,
                    TagsLeafValue {
                        root_id: PageID(0),
                        positions: vec![pos],
                    },
                );
                if verbose {
                    println!("Inserted new tag at index {}", i);
                }
            }
        }
    } else {
        return Err(LmdbError::DatabaseCorrupted(
            "Expected TagsLeaf node".to_string(),
        ));
    }

    // Track split information as (promoted_key, new_page_id, parent_child_idx_to_insert_after)
    let mut split_info: Option<(TagHash, PageID, usize)> = None;

    // Check if leaf overflows
    if dirty_leaf_page.calc_serialized_size() > db.page_size {
        if let Node::TagsLeaf(leaf) = &mut dirty_leaf_page.node {
            // Move the last (largest) key to a new right sibling
            let (last_key, last_value) = leaf.pop_last_key_and_value()?;
            if verbose {
                println!(
                    "Split TagsLeaf {:?}, moving last key {:?}",
                    dirty_leaf_page_id, last_key
                );
            }
            let new_leaf_node = TagsLeafNode {
                keys: vec![last_key],
                values: vec![last_value],
            };
            let new_leaf_page_id = writer.alloc_page_id();
            let new_leaf_page = Page::new(new_leaf_page_id, Node::TagsLeaf(new_leaf_node));

            // Check new page size sanity
            let sz = new_leaf_page.calc_serialized_size();
            if sz > db.page_size {
                return Err(LmdbError::DatabaseCorrupted(
                    "Overflow tag positions to subtree not implemented".to_string(),
                ));
            }
            writer.insert_dirty(new_leaf_page)?;

            // Determine where to insert in parent: to the right of the leaf we came from
            let parent_child_idx = if let Some((_, child_idx)) = stack.last().cloned() {
                child_idx
            } else {
                0
            };
            split_info = Some((last_key, new_leaf_page_id, parent_child_idx));
        }
    }

    // Propagate replacements and splits up the tree
    let mut current_replacement_info = replacement_info; // (old_id, new_id, idx)
    while let Some((parent_page_id, parent_child_idx)) = stack.pop() {
        // Make parent page dirty
        let dirty_parent_page_id = writer.get_dirty_page_id(parent_page_id)?;
        let parent_replacement_info = if dirty_parent_page_id != parent_page_id {
            // Need to replace this parent in its own parent later
            // The index to use for that will be fetched from the next stack entry in the next loop iteration
            Some((parent_page_id, dirty_parent_page_id, 0usize))
        } else {
            None
        };

        let parent_page = writer.get_mut_dirty(dirty_parent_page_id)?;
        // First, apply child replacement if needed
        if let Node::TagsInternal(internal) = &mut parent_page.node {
            if let Some((old_id, new_id, _)) = current_replacement_info.take() {
                let target_idx = parent_child_idx;
                if internal.child_ids[target_idx] == old_id {
                    internal.child_ids[target_idx] = new_id;
                    if verbose {
                        println!(
                            "Replaced child at idx {}: {:?} -> {:?} in {:?}",
                            target_idx, old_id, new_id, dirty_parent_page_id
                        );
                    }
                } else if let Some(found) = internal.child_ids.iter().position(|&cid| cid == old_id) {
                    internal.child_ids[found] = new_id;
                    if verbose {
                        println!(
                            "Replaced child by search at idx {}: {:?} -> {:?} in {:?}",
                            found, old_id, new_id, dirty_parent_page_id
                        );
                    }
                } else {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Parent did not contain expected child id".to_string(),
                    ));
                }
            } else if verbose {
                println!("No child replacement needed in {:?}", dirty_parent_page_id);
            }
        } else {
            return Err(LmdbError::DatabaseCorrupted(
                "Expected TagsInternal node".to_string(),
            ));
        }

        // Then, apply promoted split from below, if any
        if let Some((promoted_key, new_child_id, child_idx_from_below)) = split_info.take() {
            if let Node::TagsInternal(internal) = &mut parent_page.node {
                let insert_key_idx = child_idx_from_below;
                let insert_child_idx = child_idx_from_below + 1;
                internal.keys.insert(insert_key_idx, promoted_key);
                internal.child_ids.insert(insert_child_idx, new_child_id);
                if verbose {
                    println!(
                        "Inserted promoted key at {} and child at {} in {:?}",
                        insert_key_idx, insert_child_idx, dirty_parent_page_id
                    );
                }
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected TagsInternal node".to_string(),
                ));
            }
        }

        // Now check for internal overflow after any insertion
        let needs_split = parent_page.calc_serialized_size() > db.page_size;
        if needs_split {
            if let Node::TagsInternal(internal) = &mut parent_page.node {
                if verbose {
                    println!("Splitting TagsInternal {:?}...", dirty_parent_page_id);
                }
                if internal.keys.len() < 3 || internal.child_ids.len() < 4 {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Cannot split internal node with too few keys/children".to_string(),
                    ));
                }
                let (promote_up, new_keys, new_child_ids) = internal.split_off()?;
                let new_internal = TagsInternalNode { keys: new_keys, child_ids: new_child_ids };
                assert_eq!(internal.keys.len() + 1, internal.child_ids.len());
                assert_eq!(new_internal.keys.len() + 1, new_internal.child_ids.len());
                let new_internal_id = writer.alloc_page_id();
                let new_internal_page = Page::new(new_internal_id, Node::TagsInternal(new_internal));
                if verbose {
                    println!("Created new TagsInternal {:?}", new_internal_id);
                }
                writer.insert_dirty(new_internal_page)?;
                split_info = Some((promote_up, new_internal_id, parent_child_idx));
            } else {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected TagsInternal node".to_string(),
                ));
            }
        } else {
            split_info = None;
        }

        // Prepare replacement info for the next level up if this parent was copied-on-write
        if let Some((old_id, new_id, _)) = parent_replacement_info {
            current_replacement_info = Some((old_id, new_id, 0)); // index resolved at next iteration
        } else {
            current_replacement_info = None;
        }
    }

    // Apply root replacement if needed
    if let Some((old_id, new_id, _)) = current_replacement_info.take() {
        if writer.tags_tree_root_id == old_id {
            writer.tags_tree_root_id = new_id;
            if verbose {
                println!("Replaced Tags root {:?} -> {:?}", old_id, new_id);
            }
        } else {
            return Err(LmdbError::RootIDMismatch(old_id, new_id));
        }
    }

    // If we still have a split to propagate, create a new internal root
    if let Some((promoted_key, promoted_page_id, _)) = split_info.take() {
        let new_root_id = writer.alloc_page_id();
        let new_root = TagsInternalNode {
            keys: vec![promoted_key],
            child_ids: vec![writer.tags_tree_root_id, promoted_page_id],
        };
        let new_root_page = Page::new(new_root_id, Node::TagsInternal(new_root));
        if verbose {
            println!(
                "Created new TagsInternal root {:?}",
                new_root_id
            );
        }
        writer.insert_dirty(new_root_page)?;
        writer.tags_tree_root_id = new_root_id;
    }

    Ok(())
}

pub fn lookup_tag(db: &Lmdb, reader: &LmdbReader, tag: TagHash) -> Result<Vec<Position>> {
    let mut current_page_id: PageID = reader.tags_tree_root_id;
    loop {
        let page = db.read_page(current_page_id)?;
        match &page.node {
            Node::TagsInternal(internal) => {
                let idx = match internal.keys.binary_search(&tag) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                if idx >= internal.child_ids.len() {
                    return Err(LmdbError::DatabaseCorrupted(
                        "Child index out of bounds in tags tree".to_string(),
                    ));
                }
                current_page_id = internal.child_ids[idx];
            }
            Node::TagsLeaf(leaf) => match leaf.keys.binary_search(&tag) {
                Ok(i) => {
                    let val = &leaf.values[i];
                    if val.root_id == PageID(0) {
                        return Ok(val.positions.clone());
                    } else {
                        return Err(LmdbError::DatabaseCorrupted(
                            "Per-tag subtree not implemented".to_string(),
                        ));
                    }
                }
                Err(_) => return Ok(Vec::new()),
            },
            _ => {
                return Err(LmdbError::DatabaseCorrupted(
                    "Expected TagsInternal or TagsLeaf node in tags tree".to_string(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::Lmdb;
    use tempfile::{tempdir, TempDir};

    static VERBOSE: bool = false;

    fn construct_db(page_size: usize) -> (TempDir, Lmdb) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("lmdb-test.db");
        let db = Lmdb::new(&db_path, page_size, VERBOSE).unwrap();
        (temp_dir, db)
    }

    fn th(n: u64) -> TagHash {
        // simple helper to generate an 8-byte TagHash from a number
        n.to_le_bytes()
    }

    #[test]
    fn test_insert_position_into_empty_leaf_root() {
        let (_tmp, mut db) = construct_db(1024);
        let mut writer = db.writer().unwrap();
        let tag = th(10);
        let pos = writer.issue_position();
        insert_position(&db, &mut writer, tag, pos).unwrap();
        db.commit(&mut writer);

        let mut writer = db.writer().unwrap();
        // Verify by reading root page
        let page = db.read_page(writer.tags_tree_root_id).unwrap();
        match &page.node {
            Node::TagsLeaf(leaf) => {
                assert_eq!(leaf.keys.len(), 1);
                assert_eq!(leaf.keys[0], tag);
                assert_eq!(leaf.values[0].positions, vec![pos]);
            }
            _ => panic!("Expected TagsLeaf at root"),
        }
    }

    #[test]
    fn test_insert_positions_into_empty_leaf_root() {
        let (_tmp, mut db) = construct_db(1024);
        let mut writer = db.writer().unwrap();
        // Insert out of order
        let tags = [30u64, 10, 20, 20, 15];
        for &n in &tags {
            let pos = writer.issue_position();
            insert_position(&db, &mut writer, th(n), pos).unwrap();
        }
        // Verify sorted order and duplicate handling in leaf
        let page = writer.get_page_ref(&db, writer.tags_tree_root_id).unwrap();
        match &page.node {
            Node::TagsLeaf(leaf) => {
                // Extract unique sorted keys for expectation: 10,15,20,30
                assert_eq!(leaf.keys, vec![th(10), th(15), th(20), th(30)]);
                // 20 should have two positions
                let idx20 = leaf.keys.iter().position(|k| *k == th(20)).unwrap();
                assert!(leaf.values[idx20].positions.len() >= 2);
            }
            _ => panic!("Expected TagsLeaf at root"),
        }

        // Commit and verify lookup_tag on some keys
        db.commit(&mut writer).unwrap();
        let reader = db.reader().unwrap();
        let res_10 = lookup_tag(&db, &reader, th(10)).unwrap();
        assert!(!res_10.is_empty());
        let res_missing = lookup_tag(&db, &reader, th(999)).unwrap();
        assert!(res_missing.is_empty());
    }

    #[test]
    fn test_lookup_tag_returns_positions() {
        let (_tmp, mut db) = construct_db(1024);
        let mut writer = db.writer().unwrap();
        let t1 = th(42);
        let p1 = writer.issue_position();
        insert_position(&db, &mut writer, t1, p1).unwrap();
        let p2 = writer.issue_position();
        insert_position(&db, &mut writer, t1, p2).unwrap();
        let t2 = th(7);
        let p3 = writer.issue_position();
        insert_position(&db, &mut writer, t2, p3).unwrap();
        db.commit(&mut writer).unwrap();

        let reader = db.reader().unwrap();
        let vals = lookup_tag(&db, &reader, t1).unwrap();
        assert_eq!(vals, vec![p1, p2]);
        // non-existent
        let vals_none = lookup_tag(&db, &reader, th(1000)).unwrap();
        assert!(vals_none.is_empty());
    }
}
