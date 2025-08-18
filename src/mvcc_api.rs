use std::path::Path;

use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::api::{DCBAppendCondition, DCBEvent, DCBEventStoreAPI, DCBQuery, DCBSequencedEvent, DCBReadResponse, EventStoreError, Result as ApiResult};
use crate::mvcc_db::{Db, Reader, Result as MvccResult, Writer};
use crate::mvcc_event_tree::{event_tree_append, event_tree_lookup, EventIterator};
use crate::mvcc_node_event::EventRecord;
use crate::mvcc_node_tags::TagHash;
use crate::mvcc_tags_tree::{tags_tree_insert, tags_tree_iter};
use crate::mvcc_common::Position as MvccPosition;

// Map MVCC errors to API errors
fn map_mvcc_err<E: std::fmt::Display>(e: E) -> EventStoreError {
    EventStoreError::Corruption(format!("{}", e))
}

/// MVCC-backed EventStore implementing the DCBEventStoreAPI
pub struct EventStore {
    db: Db,
}

impl EventStore {
    /// Create a new MVCC EventStore at the given directory or file path.
    /// If a directory path is provided, a file named "mvcc.db" will be used inside it.
    pub fn new<P: AsRef<Path>>(path: P) -> ApiResult<Self> {
        let p = path.as_ref();
        let file_path = if p.is_dir() { p.join("mvcc.db") } else { p.to_path_buf() };
        let db = Db::new(&file_path, 512, false).map_err(map_mvcc_err)?;
        Ok(Self { db })
    }

    fn reader(&self) -> ApiResult<Reader> {
        self.db.reader().map_err(map_mvcc_err)
    }
    fn writer(&self) -> ApiResult<Writer> {
        self.db.writer().map_err(map_mvcc_err)
    }
    fn commit(&self, writer: &mut Writer) -> ApiResult<()> {
        self.db.commit(writer).map_err(map_mvcc_err)
    }
}

struct MVCCReadResponse {
    events: Vec<DCBSequencedEvent>,
    idx: usize,
    head: Option<u64>,
}

impl Iterator for MVCCReadResponse {
    type Item = DCBSequencedEvent;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.events.len() { return None; }
        let item = self.events[self.idx].clone();
        self.idx += 1;
        Some(item)
    }
}

impl DCBReadResponse for MVCCReadResponse {
    fn head(&self) -> Option<u64> { self.head }
    fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>) {
        (self.events.clone(), self.head)
    }
    fn next_batch(&mut self) -> ApiResult<Vec<DCBSequencedEvent>> {
        let batch = self.events[self.idx..].to_vec();
        self.idx = self.events.len();
        Ok(batch)
    }
}

/// Open a database by calling Db::new
pub fn open_db<P: AsRef<Path>>(path: P, page_size: usize, verbose: bool) -> MvccResult<Db> {
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
pub fn unconditional_append(db: &mut Db, events: Vec<DCBEvent>) -> MvccResult<()> {
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
pub fn read_all(db: &mut Db) -> MvccResult<Vec<DCBSequencedEvent>> {
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
pub fn read(db: &mut Db, query: DCBQuery) -> MvccResult<Vec<DCBSequencedEvent>> {
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

fn event_matches_query_items(rec: &EventRecord, query: &Option<DCBQuery>) -> bool {
    match query {
        None => true,
        Some(q) => {
            if q.items.is_empty() { return true; }
            for item in &q.items {
                let type_ok = item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type);
                if !type_ok { continue; }
                let tags_ok = item.tags.iter().all(|t| rec.tags.iter().any(|et| et == t));
                if type_ok && tags_ok { return true; }
            }
            false
        }
    }
}

impl DCBEventStoreAPI for EventStore {
    fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> ApiResult<Box<dyn DCBReadResponse + '_>> {
        // Special-case limit == 0
        if let Some(0) = limit { return Ok(Box::new(MVCCReadResponse { events: vec![], idx: 0, head: None })); }

        let db = &self.db;

        // Compute last committed position for unlimited head
        let (_, header) = db.get_latest_header().map_err(map_mvcc_err)?;
        let last_committed_position = header.next_position.0.saturating_sub(1);

        let reader = db.reader().map_err(map_mvcc_err)?;
        let after_pos = after.map(|a| MvccPosition(a));
        let mut iter = EventIterator::new(&db, reader, after_pos);
        let mut events: Vec<DCBSequencedEvent> = Vec::new();

        loop {
            let batch = iter.next_batch(128).map_err(map_mvcc_err)?;
            if batch.is_empty() { break; }
            for (pos, rec) in batch.into_iter() {
                if event_matches_query_items(&rec, &query) {
                    let se = DCBSequencedEvent {
                        position: pos.0,
                        event: DCBEvent { event_type: rec.event_type, data: rec.data, tags: rec.tags },
                    };
                    events.push(se);
                    if let Some(lim) = limit { if events.len() >= lim { break; } }
                }
            }
            if let Some(lim) = limit { if events.len() >= lim { break; } }
        }

        // Compute head according to semantics
        let head = if limit.is_none() {
            if last_committed_position == 0 { None } else { Some(last_committed_position) }
        } else {
            events.last().map(|e| e.position)
        };

        Ok(Box::new(MVCCReadResponse { events, idx: 0, head }))
    }

    fn head(&self) -> ApiResult<Option<u64>> {
        let db = &self.db;
        let (_, header) = db.get_latest_header().map_err(map_mvcc_err)?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
    }

    fn append(&self, events: Vec<DCBEvent>, condition: Option<DCBAppendCondition>) -> ApiResult<u64> {
        let db = &self.db;

        // Check condition
        if let Some(cond) = condition {
            // If query matches any existing event (after cond.after), fail
            let reader = self.reader()?;
            let after_pos = cond.after.map(MvccPosition);
            let mut iter = EventIterator::new(&db, reader, after_pos);
            let mut matched = false;
            'outer: loop {
                let batch = iter.next_batch(128).map_err(map_mvcc_err)?;
                if batch.is_empty() { break; }
                for (_pos, rec) in batch.into_iter() {
                    if event_matches_query_items(&rec, &Some(cond.fail_if_events_match.clone())) { matched = true; break 'outer; }
                }
            }
            if matched { return Err(EventStoreError::IntegrityError); }
        }

        let mut writer = self.writer()?;
        let mut last_pos: u64 = 0;
        for ev in events.into_iter() {
            let pos = writer.issue_position();
            let record = EventRecord { event_type: ev.event_type, data: ev.data, tags: ev.tags };
            let tags = record.tags.clone();
            event_tree_append(&db, &mut writer, record, pos).map_err(map_mvcc_err)?;
            for tag in tags.iter() {
                let tag_hash: TagHash = tag_to_hash(tag);
                tags_tree_insert(&db, &mut writer, tag_hash, pos).map_err(map_mvcc_err)?;
            }
            last_pos = pos.0;
        }
        self.commit(&mut writer)?;
        Ok(last_pos)
    }
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


/// Read events using the tags index by merging per-tag iterators, grouping by position,
/// filtering by tag and type matches, and then looking up the event record.
pub fn read_conditional(db: &mut Db, query: DCBQuery) -> MvccResult<Vec<DCBSequencedEvent>> {
    // If no items, return all events
    if query.items.is_empty() {
        return read_all(db);
    }

    // All query items must have at least one tag to use the tag index path.
    let all_items_have_tags = query.items.iter().all(|it| !it.tags.is_empty());
    if !all_items_have_tags {
        // Fallback: sequentially scan all events and apply the same matching as read()
        return read(db, query);
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
        I: Iterator<Item = MvccPosition>,
    {
        inner: I,
        tag: String,
        qiids: Vec<usize>,
    }
    impl<I> PositionTagQiidIterator<I>
    where
        I: Iterator<Item = MvccPosition>,
    {
        fn new(inner: I, tag: String, qiids: Vec<usize>) -> Self {
            Self { inner, tag, qiids }
        }
    }
    impl<I> Iterator for PositionTagQiidIterator<I>
    where
        I: Iterator<Item = MvccPosition>,
    {
        type Item = (MvccPosition, String, Vec<usize>);
        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next().map(|p| (p, self.tag.clone(), self.qiids.clone()))
        }
    }

    let reader = db.reader()?;
    let mut tag_iters: Vec<PositionTagQiidIterator<_>> = Vec::new();
    for (tag, qiids) in tag_qiis.iter() {
        let tag_hash: TagHash = tag_to_hash(tag);
        let positions_iter = tags_tree_iter(&db, &reader, tag_hash)?; // yields positions for tag
        tag_iters.push(PositionTagQiidIterator::new(positions_iter, tag.clone(), qiids.clone()));
    }

    // Merge iterators ordered by position
    let merged = tag_iters.into_iter().kmerge_by(|a, b| a.0 < b.0);

    // Group by position, collecting tags and qiids
    struct GroupByPositionIterator<I>
    where
        I: Iterator<Item = (MvccPosition, String, Vec<usize>)>,
    {
        inner: I,
        current_pos: Option<MvccPosition>,
        tags: HashSet<String>,
        qiis: HashSet<usize>,
        finished: bool,
    }
    impl<I> GroupByPositionIterator<I>
    where
        I: Iterator<Item = (MvccPosition, String, Vec<usize>)>,
    {
        fn new(inner: I) -> Self {
            Self { inner, current_pos: None, tags: HashSet::new(), qiis: HashSet::new(), finished: false }
        }
    }
    impl<I> Iterator for GroupByPositionIterator<I>
    where
        I: Iterator<Item = (MvccPosition, String, Vec<usize>)>,
    {
        type Item = (MvccPosition, HashSet<String>, HashSet<usize>);
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

    let reader2 = db.reader()?; // separate reader for event lookup if needed
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
        let rec = event_tree_lookup(&db, &reader2, pos)?;

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
    }

    Ok(out)
}


#[cfg(test)]
mod tests_conditional {
    use super::*;
    use serial_test::serial;
    use tempfile::tempdir;
    use crate::api::DCBQueryItem;

    static VERBOSE: bool = false;

    #[test]
    #[serial]
    fn test_read_conditional() {
        // Create temporary directory for DB file
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-api-test-conditional.db");

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

        // Query: single tag 'alpha' -> expect 4 events
        let query_alpha = DCBQuery {
            items: vec![DCBQueryItem { types: vec![], tags: vec!["alpha".to_string()] }],
        };
        let res_alpha = read_conditional(&mut db, query_alpha).unwrap();
        assert_eq!(4, res_alpha.len());
        let mut prev = 0u64;
        for s in &res_alpha {
            assert!(s.position > prev);
            prev = s.position;
            assert!(s.event.tags.iter().any(|t| t == "alpha"));
        }

        // Query: tags 'alpha' AND 'gamma' -> expect 2 events (i=0,5)
        let query_alpha_gamma = DCBQuery {
            items: vec![DCBQueryItem { types: vec![], tags: vec!["alpha".to_string(), "gamma".to_string()] }],
        };
        let res_alpha_gamma = read_conditional(&mut db, query_alpha_gamma).unwrap();
        assert_eq!(2, res_alpha_gamma.len());
        for s in &res_alpha_gamma {
            assert!(s.event.tags.iter().any(|t| t == "alpha"));
            assert!(s.event.tags.iter().any(|t| t == "gamma"));
        }

        // Query: type filter combined with tag
        let query_type0_alpha = DCBQuery {
            items: vec![DCBQueryItem { types: vec!["Type0".to_string()], tags: vec!["alpha".to_string()] }],
        };
        let res_type0_alpha = read_conditional(&mut db, query_type0_alpha).unwrap();
        assert_eq!(1, res_type0_alpha.len());
        assert_eq!("Type0", res_type0_alpha[0].event.event_type);
        assert!(res_type0_alpha[0].event.tags.iter().any(|t| t == "alpha"));

        // Query: OR semantics over items: alpha OR beta -> expect 8 (no overlaps)
        let query_alpha_or_beta = DCBQuery {
            items: vec![
                DCBQueryItem { types: vec![], tags: vec!["alpha".to_string()] },
                DCBQueryItem { types: vec![], tags: vec!["beta".to_string()] },
            ],
        };
        let res_alpha_or_beta = read_conditional(&mut db, query_alpha_or_beta).unwrap();
        assert_eq!(8, res_alpha_or_beta.len());
    }
}
