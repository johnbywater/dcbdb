use std::path::Path;

use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::dcbapi::{DCBAppendCondition, DCBEvent, DCBEventStore, DCBQuery, DCBSequencedEvent, DCBReadResponse, DCBError, DCBResult};
use crate::lmdb::{Lmdb, LmdbResult};
use crate::events_tree::{event_tree_append, event_tree_lookup, EventIterator};
use crate::events_tree_nodes::EventRecord;
use crate::tags_tree_nodes::TagHash;
use crate::tags_tree::{tags_tree_insert, tags_tree_iter};
use crate::common::Position;

static DEFAULT_PAGE_SIZE: usize = 4096;

// Map MVCC errors to API errors
fn map_mvcc_err<E: std::fmt::Display>(e: E) -> DCBError {
    DCBError::Corruption(format!("{}", e))
}

/// LMDB-backed EventStore implementing the DCBEventStore
pub struct EventStore {
    lmdb: Lmdb,
}

impl EventStore {
    /// Create a new EventStore at the given directory or file path.
    /// If a directory path is provided, a file named "dcb.db" will be used inside it.
    pub fn new<P: AsRef<Path>>(path: P) -> DCBResult<Self> {
        let p = path.as_ref();
        let file_path = if p.is_dir() { p.join("dcb.db") } else { p.to_path_buf() };
        let lmdb = Lmdb::new(&file_path, DEFAULT_PAGE_SIZE, false).map_err(map_mvcc_err)?;
        Ok(Self { lmdb })
    }
}

struct ReadResponse {
    events: Vec<DCBSequencedEvent>,
    idx: usize,
    head: Option<u64>,
}

impl Iterator for ReadResponse {
    type Item = DCBSequencedEvent;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.events.len() { return None; }
        let item = self.events[self.idx].clone();
        self.idx += 1;
        Some(item)
    }
}

impl DCBReadResponse for ReadResponse {
    fn head(&self) -> Option<u64> { self.head }
    fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>) {
        (self.events.clone(), self.head)
    }
    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        let batch = self.events[self.idx..].to_vec();
        self.idx = self.events.len();
        Ok(batch)
    }
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
pub fn unconditional_append(lmdb: &Lmdb, events: Vec<DCBEvent>) -> LmdbResult<u64> {
    let mut writer = lmdb.writer()?;
    let mut last_pos_u64: u64 = 0;

    for ev in events.into_iter() {
        let position = writer.issue_position();
        last_pos_u64 = position.0;
        let record = EventRecord {
            event_type: ev.event_type,
            data: ev.data,
            tags: ev.tags,
        };
        // Clone tags so we can index them after moving record into event_tree_append
        let tags = record.tags.clone();
        event_tree_append(&lmdb, &mut writer, record, position)?;
        for tag in tags.iter() {
            let tag_hash: TagHash = tag_to_hash(tag);
            tags_tree_insert(&lmdb, &mut writer, tag_hash, position)?;
        }
    }

    lmdb.commit(&mut writer)?;
    Ok(last_pos_u64)
}


/// Read events using the tags index by merging per-tag iterators, grouping by position,
/// filtering by tag and type matches, and then looking up the event record.
pub fn read_conditional(lmdb: &Lmdb, query: DCBQuery, after: Position, limit: Option<usize>) -> LmdbResult<Vec<DCBSequencedEvent>> {
    // Special case: explicit zero limit
    if let Some(0) = limit {
        return Ok(Vec::new());
    }

    // If no items, return all events with after/limit respected via sequential scan
    if query.items.is_empty() {
        let reader = lmdb.reader()?;
        let mut iter = EventIterator::new(&lmdb, reader.event_tree_root_id, Some(after));
        let mut out: Vec<DCBSequencedEvent> = Vec::new();
        'outer_all: loop {
            let batch = iter.next_batch(64)?;
            if batch.is_empty() { break; }
            for (pos, rec) in batch.into_iter() {
                out.push(DCBSequencedEvent {
                    position: pos.0,
                    event: DCBEvent { event_type: rec.event_type, data: rec.data, tags: rec.tags },
                });
                if let Some(lim) = limit { if out.len() >= lim { break 'outer_all; } }
            }
        }
        return Ok(out);
    }

    // All query items must have at least one tag to use the tag index path.
    let all_items_have_tags = query.items.iter().all(|it| !it.tags.is_empty());
    if !all_items_have_tags {
        // Fallback: sequentially scan all events and apply the same matching logic
        let reader = lmdb.reader()?;
        let mut iter = EventIterator::new(&lmdb, reader.event_tree_root_id, Some(after));
        let mut out: Vec<DCBSequencedEvent> = Vec::new();
        let matches_item = |rec: &EventRecord| -> bool {
            for item in &query.items {
                let type_ok = item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type);
                if !type_ok { continue; }
                let tags_ok = item.tags.iter().all(|t| rec.tags.iter().any(|et| et == t));
                if type_ok && tags_ok { return true; }
            }
            false
        };
        'outer_fallback: loop {
            let batch = iter.next_batch(64)?;
            if batch.is_empty() { break; }
            for (pos, rec) in batch.into_iter() {
                if matches_item(&rec) {
                    out.push(DCBSequencedEvent {
                        position: pos.0,
                        event: DCBEvent { event_type: rec.event_type, data: rec.data, tags: rec.tags },
                    });
                    if let Some(lim) = limit { if out.len() >= lim { break 'outer_fallback; } }
                }
            }
        }
        return Ok(out);
    }

    // Invert query: tag -> list of query item indices that require this tag
    let mut tag_qiis: HashMap<String, Vec<usize>> = HashMap::new();
    let mut qi_tags: Vec<HashSet<String>> = Vec::new();

    for (qiid, item) in query.items.iter().enumerate() {
        qi_tags.push(item.tags.iter().cloned().collect());
        for tag in &item.tags {
            tag_qiis.entry(tag.clone()).or_default().push(qiid);
        }
    }

    // Prepare per-tag iterators yielding (position, tag, qiids)
    struct PositionTagQiidIterator<I>
    where
        I: Iterator<Item = Position>,
    {
        inner: I,
        tag: String,
        qiids: Vec<usize>,
    }
    impl<I> PositionTagQiidIterator<I>
    where
        I: Iterator<Item = Position>,
    {
        fn new(inner: I, tag: String, qiids: Vec<usize>) -> Self {
            Self { inner, tag, qiids }
        }
    }
    impl<I> Iterator for PositionTagQiidIterator<I>
    where
        I: Iterator<Item = Position>,
    {
        type Item = (Position, String, Vec<usize>);
        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next().map(|p| (p, self.tag.clone(), self.qiids.clone()))
        }
    }

    let reader = lmdb.reader()?;
    let mut tag_iters: Vec<PositionTagQiidIterator<_>> = Vec::new();
    for (tag, qiids) in tag_qiis.iter() {
        let tag_hash: TagHash = tag_to_hash(tag);
        let positions_iter = tags_tree_iter(&lmdb, &reader, tag_hash, after)?; // yields positions for tag
        tag_iters.push(PositionTagQiidIterator::new(positions_iter, tag.clone(), qiids.clone()));
    }

    // Merge iterators ordered by position
    let merged = tag_iters.into_iter().kmerge_by(|a, b| a.0 < b.0);

    // Group by position, collecting tags and qiids
    struct GroupByPositionIterator<I>
    where
        I: Iterator<Item = (Position, String, Vec<usize>)>,
    {
        inner: I,
        current_pos: Option<Position>,
        tags: HashSet<String>,
        qiis: HashSet<usize>,
        finished: bool,
    }
    impl<I> GroupByPositionIterator<I>
    where
        I: Iterator<Item = (Position, String, Vec<usize>)>,
    {
        fn new(inner: I) -> Self {
            Self { inner, current_pos: None, tags: HashSet::new(), qiis: HashSet::new(), finished: false }
        }
    }
    impl<I> Iterator for GroupByPositionIterator<I>
    where
        I: Iterator<Item = (Position, String, Vec<usize>)>,
    {
        type Item = (Position, HashSet<String>, HashSet<usize>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.finished { return None; }
            for (pos, tag, qiids) in self.inner.by_ref() {
                if self.current_pos.is_none() {
                    self.current_pos = Some(pos);
                } else if self.current_pos.unwrap() != pos {
                    let out_pos = self.current_pos.unwrap();
                    let out_tags = std::mem::take(&mut self.tags);
                    let out_qiis = std::mem::take(&mut self.qiis);
                    self.current_pos = Some(pos);
                    self.tags.insert(tag);
                    for q in qiids { self.qiis.insert(q); }
                    return Some((out_pos, out_tags, out_qiis));
                }
                self.tags.insert(tag);
                for q in qiids { self.qiis.insert(q); }
            }
            if let Some(p) = self.current_pos.take() {
                self.finished = true;
                let out_tags = std::mem::take(&mut self.tags);
                let out_qiis = std::mem::take(&mut self.qiis);
                return Some((p, out_tags, out_qiis));
            }
            None
        }
    }

    let reader2 = lmdb.reader()?; // separate reader for event lookup if needed
    let mut out: Vec<DCBSequencedEvent> = Vec::new();
    for (pos, tags_present, qiis_present) in GroupByPositionIterator::new(merged) {
        // Find any query item whose required tag set is subset of tags_present
        let matching_qiis: Vec<usize> = qiis_present
            .iter()
            .copied()
            .filter(|&qii| qi_tags[qii].is_subset(&tags_present))
            .collect();
        if matching_qiis.is_empty() { continue; }

        // Lookup the event record at position
        let rec = event_tree_lookup(&lmdb, reader2.event_tree_root_id, pos)?;

        // Check type matching against any of the matching items
        let mut type_ok = false;
        'typecheck: for qii in matching_qiis.iter().copied() {
            let item = &query.items[qii];
            if item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type) {
                type_ok = true; break 'typecheck;
            }
        }
        if !type_ok { continue; }

        out.push(DCBSequencedEvent { position: pos.0, event: DCBEvent { event_type: rec.event_type, data: rec.data, tags: rec.tags } });
        if let Some(lim) = limit { if out.len() >= lim { break; } }
    }

    Ok(out)
}

impl DCBEventStore for EventStore {
    fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> DCBResult<Box<dyn DCBReadResponse + '_>> {
        let db = &self.lmdb;

        // Compute last committed position for unlimited head
        let (_, header) = db.get_latest_header().map_err(map_mvcc_err)?;
        let last_committed_position = header.next_position.0.saturating_sub(1);

        // Build query and after
        let q = query.unwrap_or(DCBQuery { items: vec![] });
        let after_pos = Position(after.unwrap_or(0));

        // Delegate to read_conditional
        let events = read_conditional(db, q, after_pos, limit).map_err(map_mvcc_err)?;

        // Compute head according to semantics
        let head = if limit.is_none() {
            if last_committed_position == 0 { None } else { Some(last_committed_position) }
        } else {
            events.last().map(|e| e.position)
        };

        Ok(Box::new(ReadResponse { events, idx: 0, head }))
    }

    fn head(&self) -> DCBResult<Option<u64>> {
        let db = &self.lmdb;
        let (_, header) = db.get_latest_header().map_err(map_mvcc_err)?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
    }

    fn append(&self, events: Vec<DCBEvent>, condition: Option<DCBAppendCondition>) -> DCBResult<u64> {
        let db = &self.lmdb;

        // Check condition using read_conditional (limit 1), starting after the provided position
        if let Some(cond) = condition {
            let after = Position(cond.after.unwrap_or(0));
            let found = read_conditional(db, cond.fail_if_events_match.clone(), after, Some(1))
                .map_err(map_mvcc_err)?;
            if !found.is_empty() {
                return Err(DCBError::IntegrityError);
            }
        }

        // If no events to append then return 0
        if events.is_empty() {
            return Ok(0);
        }

        // Append unconditionally via helper, which commits internally and returns last position
        let last = unconditional_append(db, events).map_err(map_mvcc_err)?;
        Ok(last)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::tempdir;
    use crate::dcbapi::{DCBQuery, DCBQueryItem, DCBAppendCondition, DCBError};

    static VERBOSE: bool = false;

    // Helper to produce a deterministic set of 10 events with shared tags and unique types
    fn standard_events() -> Vec<DCBEvent> {
        let shared_tags = vec![
            "alpha".to_string(),
            "beta".to_string(),
            "gamma".to_string(),
            "delta".to_string(),
            "epsilon".to_string(),
        ];
        let mut input: Vec<DCBEvent> = Vec::new();
        for i in 0..10u8 {
            let t1 = shared_tags[(i % 5) as usize].clone();
            let t2 = shared_tags[((i + 2) % 5) as usize].clone();
            input.push(DCBEvent {
                event_type: format!("Type{}", i),
                data: vec![i, i + 1, i + 2],
                tags: vec![t1, t2],
            });
        }
        input
    }

    // Create DB with the standard events; keep temp dir alive by returning it
    fn setup_db_with_standard_events() -> (tempfile::TempDir, Lmdb, Vec<DCBEvent>) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-api-test.db");
        let db = Lmdb::new(db_path.as_ref(), DEFAULT_PAGE_SIZE, VERBOSE).unwrap();
        let input = standard_events();
        let last = unconditional_append(&db, input.clone()).unwrap();
        // Verify last equals committed head
        let (_, header) = db.get_latest_header().unwrap();
        let head = header.next_position.0.saturating_sub(1);
        assert_eq!(last, head);
        (temp_dir, db, input)
    }

    #[test]
    #[serial]
    fn empty_query_after_and_limit() {
        let (_tmp, mut db, input) = setup_db_with_standard_events();

        // after = 0 -> all
        let all = read_conditional(&mut db, DCBQuery { items: vec![] }, Position(0), None).unwrap();
        assert_eq!(all.len(), input.len());
        assert!(all.windows(2).all(|w| w[0].position < w[1].position));

        // after = first -> tail
        let first = all[0].position;
        let tail = read_conditional(&mut db, DCBQuery { items: vec![] }, Position(first), None).unwrap();
        assert_eq!(tail.len(), input.len() - 1);

        // after = last -> empty
        let last = all.last().unwrap().position;
        let none = read_conditional(&mut db, DCBQuery { items: vec![] }, Position(last), None).unwrap();
        assert!(none.is_empty());

        // limits
        let lim0 = read_conditional(&mut db, DCBQuery { items: vec![] }, Position(0), Some(0)).unwrap();
        assert!(lim0.is_empty());
        let lim3 = read_conditional(&mut db, DCBQuery { items: vec![] }, Position(0), Some(3)).unwrap();
        assert_eq!(lim3.len(), 3);
        let lim20 = read_conditional(&mut db, DCBQuery { items: vec![] }, Position(0), Some(20)).unwrap();
        assert_eq!(lim20.len(), input.len());
    }

    #[test]
    #[serial]
    fn tags_only_single_tag_after_and_limit() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery { items: vec![DCBQueryItem { types: vec![], tags: vec!["alpha".to_string()] }] };
        let res = read_conditional(&mut db, qi.clone(), Position(0), None).unwrap();
        assert_eq!(res.len(), 4);
        assert!(res.iter().all(|e| e.event.tags.iter().any(|t| t == "alpha")));
        assert!(res.windows(2).all(|w| w[0].position < w[1].position));

        // after combinations
        let positions: Vec<u64> = res.iter().map(|e| e.position).collect();
        let after_first = read_conditional(&mut db, qi.clone(), Position(positions[0]), None).unwrap();
        assert_eq!(after_first.len(), positions.len() - 1);
        let after_last = read_conditional(&mut db, qi.clone(), Position(*positions.last().unwrap()), None).unwrap();
        assert!(after_last.is_empty());

        // limits
        let lim0 = read_conditional(&mut db, qi.clone(), Position(0), Some(0)).unwrap();
        assert!(lim0.is_empty());
        let lim1 = read_conditional(&mut db, qi.clone(), Position(0), Some(1)).unwrap();
        assert_eq!(lim1.len(), 1);
        let lim10 = read_conditional(&mut db, qi, Position(0), Some(10)).unwrap();
        assert_eq!(lim10.len(), 4);
    }

    #[test]
    #[serial]
    fn tags_only_multi_tag_and() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery { items: vec![DCBQueryItem { types: vec![], tags: vec!["alpha".to_string(), "gamma".to_string()] }] };
        let res = read_conditional(&mut db, qi, Position(0), None).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res.iter().all(|e| e.event.tags.iter().any(|t| t == "alpha")));
        assert!(res.iter().all(|e| e.event.tags.iter().any(|t| t == "gamma")));
    }

    #[test]
    #[serial]
    fn types_plus_tags_index_path() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery { items: vec![DCBQueryItem { types: vec!["Type0".to_string()], tags: vec!["alpha".to_string()] }] };
        let res = read_conditional(&mut db, qi, Position(0), None).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].event.event_type, "Type0");
        assert!(res[0].event.tags.iter().any(|t| t == "alpha"));
    }

    #[test]
    #[serial]
    fn or_semantics_and_deduplication() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let alpha_only = DCBQuery { items: vec![DCBQueryItem { types: vec![], tags: vec!["alpha".to_string()] }] };
        let alpha_positions: Vec<u64> = read_conditional(&mut db, alpha_only.clone(), Position(0), None).unwrap()
            .into_iter().map(|e| e.position).collect();

        // Overlapping items: alpha OR (alpha AND gamma) should deduplicate
        let query = DCBQuery { items: vec![
            DCBQueryItem { types: vec![], tags: vec!["alpha".to_string()] },
            DCBQueryItem { types: vec![], tags: vec!["alpha".to_string(), "gamma".to_string()] },
        ]};
        let res = read_conditional(&mut db, query, Position(0), None).unwrap();
        let res_positions: Vec<u64> = res.into_iter().map(|e| e.position).collect();
        assert_eq!(res_positions, alpha_positions);
    }

    #[test]
    #[serial]
    fn fallback_types_only_after_and_limit() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-fallback-types-only.db");
        let mut db = Lmdb::new(db_path.as_ref(), DEFAULT_PAGE_SIZE, VERBOSE).unwrap();

        // Use a smaller custom set to make counts obvious
        let events = vec![
            DCBEvent { event_type: "TypeA".to_string(), data: vec![1], tags: vec!["x".to_string()] },
            DCBEvent { event_type: "TypeB".to_string(), data: vec![2], tags: vec!["y".to_string()] },
            DCBEvent { event_type: "TypeA".to_string(), data: vec![3], tags: vec!["z".to_string()] },
        ];
        let last = unconditional_append(&db, events).unwrap();
        let (_, header) = db.get_latest_header().unwrap();
        let head = header.next_position.0.saturating_sub(1);
        assert_eq!(last, head);

        // Query item with no tags => forces fallback path; select TypeA only
        let qi = DCBQuery { items: vec![DCBQueryItem { types: vec!["TypeA".to_string()], tags: vec![] }] };
        let res = read_conditional(&mut db, qi.clone(), Position(0), None).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res.iter().all(|e| e.event.event_type == "TypeA"));

        // After skip first matching
        let first_pos = res[0].position;
        let res_after = read_conditional(&mut db, qi.clone(), Position(first_pos), None).unwrap();
        assert_eq!(res_after.len(), 1);

        // Limit 1
        let res_lim1 = read_conditional(&mut db, qi, Position(0), Some(1)).unwrap();
        assert_eq!(res_lim1.len(), 1);
    }

    #[test]
    #[serial]
    fn fallback_empty_item_matches_all() {
        let (_tmp, mut db, input) = setup_db_with_standard_events();
        // An empty item (no types, no tags) should match all events via fallback path
        let qi = DCBQuery { items: vec![DCBQueryItem { types: vec![], tags: vec![] }] };

        let all = read_conditional(&mut db, qi.clone(), Position(0), None).unwrap();
        assert_eq!(all.len(), input.len());

        // After and limit still apply
        let first = all[0].position;
        let tail = read_conditional(&mut db, qi.clone(), Position(first), None).unwrap();
        assert_eq!(tail.len(), input.len() - 1);
        let lim5 = read_conditional(&mut db, qi, Position(0), Some(5)).unwrap();
        assert_eq!(lim5.len(), 5);
    }

    #[test]
    #[serial]
    fn test_event_store() {
        let temp_dir = tempdir().unwrap();
        let store = EventStore::new(temp_dir.path()).unwrap();

        // Head is None on empty store
        assert_eq!(None, store.head().unwrap());

        // Append a couple of events
        let events = vec![
            DCBEvent { event_type: "TypeA".to_string(), data: vec![1], tags: vec!["foo".to_string()] },
            DCBEvent { event_type: "TypeB".to_string(), data: vec![2], tags: vec!["bar".to_string(), "foo".to_string()] },
        ];
        let last = store.append(events.clone(), None).unwrap();
        assert!(last > 0);
        assert_eq!(store.head().unwrap(), Some(last));

        // Read all
        let mut resp = store.read(None, None, None).unwrap();
        let (all, head) = resp.collect_with_head();
        assert_eq!(head, Some(last));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].event.event_type, "TypeA");
        assert_eq!(all[1].event.event_type, "TypeB");

        // Limit semantics: only first event returned and head equals that position
        let mut resp_lim1 = store.read(None, None, Some(1)).unwrap();
        let (only_one, head_lim1) = resp_lim1.collect_with_head();
        assert_eq!(only_one.len(), 1);
        assert_eq!(only_one[0].event.event_type, "TypeA");
        assert_eq!(head_lim1, Some(only_one[0].position));

        // Tag-filtered read ("foo")
        let query = DCBQuery { items: vec![DCBQueryItem { types: vec![], tags: vec!["foo".to_string()] }] };
        let mut resp2 = store.read(Some(query), None, None).unwrap();
        let out2 = resp2.next_batch().unwrap();
        assert_eq!(out2.len(), 2);
        assert!(out2.iter().all(|e| e.event.tags.iter().any(|t| t == "foo")));

        // After semantics: skip the first event
        let first_pos = all[0].position;
        let mut resp3 = store.read(None, Some(first_pos), None).unwrap();
        let out3 = resp3.next_batch().unwrap();
        assert_eq!(out3.len(), 1);
        assert_eq!(out3[0].event.event_type, "TypeB");

        // Append with a condition that should PASS: query matches existing 'foo' but after = last
        let cond_pass = DCBAppendCondition {
            fail_if_events_match: DCBQuery { items: vec![DCBQueryItem { types: vec![], tags: vec!["foo".to_string()] }] },
            after: Some(last),
        };
        let ok_last = store
            .append(vec![DCBEvent { event_type: "TypeC".to_string(), data: vec![3], tags: vec!["baz".to_string()] }], Some(cond_pass))
            .expect("append with passing condition should succeed");
        assert!(ok_last > last);
        assert_eq!(store.head().unwrap(), Some(ok_last));

        // Append with a condition that should FAIL: same query but after = 0
        let cond_fail = DCBAppendCondition {
            fail_if_events_match: DCBQuery { items: vec![DCBQueryItem { types: vec![], tags: vec!["foo".to_string()] }] },
            after: Some(0),
        };
        let before_head = store.head().unwrap();
        let res = store.append(vec![DCBEvent { event_type: "TypeD".to_string(), data: vec![4], tags: vec!["qux".to_string()] }], Some(cond_fail));
        match res {
            Err(DCBError::IntegrityError) => {}
            other => panic!("Expected IntegrityError, got {:?}", other),
        }
        // Ensure head unchanged after failed append
        assert_eq!(store.head().unwrap(), before_head);
    }
}
