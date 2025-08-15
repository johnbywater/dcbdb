use std::path::Path;

use crate::api::{DCBEvent, DCBSequencedEvent, DCBQuery};
use crate::mvcc_db::{Db, Result};
use crate::mvcc_event_tree::{event_tree_append, EventIterator};
use crate::mvcc_node_event::EventRecord;
use crate::mvcc_node_tags::TagHash;
use crate::mvcc_tags_tree::tags_tree_insert;

/// Open a database by calling Db::new
pub fn open_db<P: AsRef<Path>>(path: P, page_size: usize, verbose: bool) -> Result<Db> {
    Db::new(path.as_ref(), page_size, verbose)
}

/// Compute a TagHash ([u8; 8]) from a tag string using a stable 64-bit hash.
fn tag_to_hash(tag: &str) -> TagHash {
    // Build a 64-bit value by combining two crc32 hashes for stability and simplicity.
    let mut hasher1 = crc32fast::Hasher::new();
    hasher1.update(tag.as_bytes());
    let a = hasher1.finalize();

    let mut hasher2 = crc32fast::Hasher::new();
    hasher2.update(tag.as_bytes());
    hasher2.update(&[0x9E, 0x37, 0x79, 0xB9]);
    let b = hasher2.finalize();

    let value = ((a as u64) << 32) | (b as u64);
    value.to_le_bytes()
}

/// Append events unconditionally to the database.
///
/// For each event, this will:
/// - issue a position from the writer
/// - append an EventRecord to the event tree
/// - insert the position for each tag into the tags tree
/// Finally, it commits the writer.
pub fn unconditional_append(db: &mut Db, events: Vec<DCBEvent>) -> Result<()> {
    let mut writer = db.writer()?;

    for ev in events.into_iter() {
        let position = writer.issue_position();
        let record = EventRecord {
            event_type: ev.event_type,
            data: ev.data,
            tags: ev.tags,
        };
        // Clone tags so we can index them after moving record into event_tree_append
        let tags = record.tags.clone();
        event_tree_append(&db, &mut writer, record, position)?;
        for tag in tags.iter() {
            let tag_hash: TagHash = tag_to_hash(tag);
            tags_tree_insert(&db, &mut writer, tag_hash, position)?;
        }
    }

    db.commit(&mut writer)
}

/// Read all events from the database, returning them as DCBSequencedEvent instances.
/// Uses EventIterator to iterate over all (Position, EventRecord) pairs and includes
/// the position in the returned items.
pub fn read_all(db: &mut Db) -> Result<Vec<DCBSequencedEvent>> {
    let reader = db.reader()?;
    let mut iter = EventIterator::new(&db, reader, None);
    let mut out: Vec<DCBSequencedEvent> = Vec::new();

    loop {
        let batch = iter.next_batch(64)?;
        if batch.is_empty() {
            break;
        }
        for (pos, rec) in batch.into_iter() {
            out.push(DCBSequencedEvent {
                position: pos.0,
                event: DCBEvent {
                    event_type: rec.event_type,
                    data: rec.data,
                    tags: rec.tags,
                },
            });
        }
    }

    Ok(out)
}

/// Read events that match any of the items in the DCBQuery.
/// Matching logic:
/// - For each DCBQueryItem, an event matches if (item.types is empty or contains the event type)
///   AND every tag in item.tags is present in the event's tags.
/// - If the query has no items, all events are returned.
pub fn read(db: &mut Db, query: DCBQuery) -> Result<Vec<DCBSequencedEvent>> {
    let reader = db.reader()?;
    let mut iter = EventIterator::new(&db, reader, None);
    let mut out: Vec<DCBSequencedEvent> = Vec::new();

    let matches_item = |rec: &EventRecord| -> bool {
        if query.items.is_empty() {
            return true;
        }
        for item in &query.items {
            let type_ok = item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type);
            if !type_ok { continue; }
            let tags_ok = item.tags.iter().all(|t| rec.tags.iter().any(|et| et == t));
            if type_ok && tags_ok { return true; }
        }
        false
    };

    loop {
        let batch = iter.next_batch(64)?;
        if batch.is_empty() { break; }
        for (pos, rec) in batch.into_iter() {
            if matches_item(&rec) {
                out.push(DCBSequencedEvent {
                    position: pos.0,
                    event: DCBEvent {
                        event_type: rec.event_type,
                        data: rec.data,
                        tags: rec.tags,
                    },
                });
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::tempdir;
    use crate::api::{DCBQuery, DCBQueryItem};

    static VERBOSE: bool = false;

    #[test]
    #[serial]
    fn test_mvcc_api_roundtrip_append_and_read_all() {
        // Create temporary directory for DB file
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-api-test.db");

        // Build events with some reused tags; ensure no tag is used in more than five events
        let mut input: Vec<DCBEvent> = Vec::new();
        let shared_tags = vec![
            "alpha".to_string(),
            "beta".to_string(),
            "gamma".to_string(),
            "delta".to_string(),
            "epsilon".to_string(),
        ];
        for i in 0..10u8 {
            let t1 = shared_tags[(i % 5) as usize].clone();
            let t2 = shared_tags[((i + 2) % 5) as usize].clone();
            input.push(DCBEvent {
                event_type: format!("Type{}", i),
                data: vec![i, i + 1, i + 2],
                tags: vec![t1, t2],
            });
        }

        // Open DB
        let mut db = open_db(&db_path, 512, VERBOSE).unwrap();

        // Append unconditionally
        unconditional_append(&mut db, input.clone()).unwrap();

        // Read all
        let output = read_all(&mut db).unwrap();

        // Compare lengths
        assert_eq!(input.len(), output.len());
        // Compare items field-by-field (no PartialEq derive on DCBEvent)
        for (a, b) in input.iter().zip(output.iter()) {
            assert_eq!(a.event_type, b.event.event_type);
            assert_eq!(a.data, b.event.data);
            assert_eq!(a.tags, b.event.tags);
        }
        // Positions should be positive and strictly increasing
        let mut prev = 0u64;
        for s in output.iter() {
            assert!(s.position > prev);
            prev = s.position;
        }

        // Build a DCBQuery with one item per shared tag
        let mut query = DCBQuery { items: Vec::new() };
        for tag in &shared_tags {
            query.items.push(DCBQueryItem {
                types: Vec::new(),
                tags: vec![tag.clone()],
            });
        }

        // Read using the query
        let filtered = read(&mut db, query).unwrap();

        // The query should match all events (each event has at least one shared tag)
        assert_eq!(filtered.len(), input.len());

        // Compare to read_all output and to original input
        for ((a, b_all), b_q) in input.iter().zip(output.iter()).zip(filtered.iter()) {
            assert_eq!(a.event_type, b_q.event.event_type);
            assert_eq!(a.data, b_q.event.data);
            assert_eq!(a.tags, b_q.event.tags);
            // Same ordering and positions as read_all
            assert_eq!(b_all.position, b_q.position);
            assert_eq!(b_all.event.event_type, b_q.event.event_type);
        }

        // Positions should be positive and strictly increasing in filtered as well
        let mut prev = 0u64;
        for s in filtered.iter() {
            assert!(s.position > prev);
            prev = s.position;
        }
    }
}
