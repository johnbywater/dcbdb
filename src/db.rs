use std::path::Path;

use itertools::Itertools;
use std::collections::{HashMap, HashSet};

use crate::common::{PageID, Position};
use crate::dcb::{
    DCBAppendCondition, DCBError, DCBEvent, DCBEventStore, DCBQuery, DCBReadResponse, DCBResult,
    DCBSequencedEvent,
};
use crate::events_tree::{EventIterator, event_tree_append, event_tree_lookup};
use crate::events_tree_nodes::EventRecord;
use crate::mvcc::{Mvcc, Writer};
use crate::page::Page;
use crate::tags_tree::{TagsTreeIterator, tags_tree_insert};
use crate::tags_tree_nodes::TagHash;

pub static DEFAULT_PAGE_SIZE: usize = 4096;

/// EventStore implementing the DCBEventStore interface
pub struct UmaDB {
    mvcc: std::sync::Arc<Mvcc>,
}

impl UmaDB {
    /// Create a new EventStore at the given directory or file path.
    /// If a directory path is provided, a file named "uma.db" will be used inside it.
    pub fn new<P: AsRef<Path>>(path: P) -> DCBResult<Self> {
        let p = path.as_ref();
        let file_path = if p.is_dir() {
            p.join("uma.db")
        } else {
            p.to_path_buf()
        };
        let mvcc = Mvcc::new(&file_path, DEFAULT_PAGE_SIZE, false)?;
        Ok(Self {
            mvcc: std::sync::Arc::new(mvcc),
        })
    }

    pub fn from_arc(mvcc: std::sync::Arc<Mvcc>) -> Self {
        Self { mvcc }
    }

    /// Appends a batch of (events, condition) using a single writer/transaction.
    /// For each item, behaves like append():
    /// - If condition is Some and matches any events (considering uncommitted writes), returns Err(IntegrityError) for that item and continues.
    /// - If events is empty, returns Ok(0) for that item and continues.
    /// - Otherwise performs unconditional append and records Ok(last_position) for that item.
    ///
    /// At the end, commits the writer once. If commit fails, returns the commit error and discards per-item results.
    pub fn append_batch(
        &self,
        items: Vec<(Vec<DCBEvent>, Option<DCBAppendCondition>)>,
    ) -> DCBResult<Vec<DCBResult<u64>>> {
        // println!("Processing batch of {} items", items.len());

        let mvcc = &self.mvcc;
        let mut writer = mvcc.writer()?;
        let mut results: Vec<DCBResult<u64>> = Vec::with_capacity(items.len());

        for (events, condition) in items.into_iter() {
            // Check condition using read_conditional (limit 1), starting after the provided position
            if let Some(cond) = condition {
                let after = Position(cond.after.unwrap_or(0));
                let found = read_conditional(
                    mvcc,
                    &writer.dirty,
                    writer.events_tree_root_id,
                    writer.tags_tree_root_id,
                    cond.fail_if_events_match.clone(),
                    after,
                    Some(1),
                );
                match found {
                    Ok(found_vec) => {
                        if let Some(matched) = found_vec.first() {
                            let msg = format!(
                                "matching event: {:?}, condition: {:?}",
                                matched,
                                cond.fail_if_events_match.clone()
                            );
                            results.push(Err(DCBError::IntegrityError(msg)));
                            continue;
                        }
                    }
                    Err(e) => {
                        // Propagate read error for this item but continue with others
                        results.push(Err(e));
                        continue;
                    }
                }
            }

            if events.is_empty() {
                results.push(Ok(0));
                continue;
            }

            // Append unconditionally
            match unconditional_append(mvcc, &mut writer, events) {
                Ok(last) => results.push(Ok(last)),
                Err(e) => {
                    // Record error for this item and continue
                    results.push(Err(e));
                }
            }
        }

        // Single commit at the end of the batch
        mvcc.commit(&mut writer)?;
        Ok(results)
    }
}

impl DCBEventStore for UmaDB {
    fn read(
        &self,
        query: Option<DCBQuery>,
        after: Option<u64>,
        limit: Option<usize>,
        _subscribe: bool,
        _batch_size: Option<usize>,
    ) -> DCBResult<Box<dyn DCBReadResponse + '_>> {
        let mvcc = &self.mvcc;
        let reader = mvcc.reader()?;

        // Compute last committed position for unlimited head
        let last_committed_position = reader.next_position.0.saturating_sub(1);

        // Build query and after
        let q = query.unwrap_or(DCBQuery { items: vec![] });
        let after_pos = Position(after.unwrap_or(0));

        // Delegate to read_conditional
        let events = read_conditional(
            mvcc,
            &HashMap::new(),
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            q,
            after_pos,
            limit,
        )?;

        // Compute head according to semantics
        let head = if limit.is_none() {
            if last_committed_position == 0 {
                None
            } else {
                Some(last_committed_position)
            }
        } else {
            events.last().map(|e| e.position)
        };

        Ok(Box::new(ReadResponse {
            events,
            idx: 0,
            head,
        }))
    }

    fn head(&self) -> DCBResult<Option<u64>> {
        let db = &self.mvcc;
        let (_, header) = db.get_latest_header()?;
        let last = header.next_position.0.saturating_sub(1);
        if last == 0 { Ok(None) } else { Ok(Some(last)) }
    }

    fn append(
        &self,
        events: Vec<DCBEvent>,
        condition: Option<DCBAppendCondition>,
    ) -> DCBResult<u64> {
        // Preserve existing fast-path: if no events, do nothing and return 0 (avoid opening/committing a writer)
        if events.is_empty() {
            return Ok(0);
        }
        // Delegate to append_batch with a single item to reuse unified batching logic
        let mut results = self.append_batch(vec![(events, condition)])?;
        debug_assert_eq!(results.len(), 1);
        match results.remove(0) {
            Ok(pos) => Ok(pos),
            Err(e) => Err(e),
        }
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
        if self.idx >= self.events.len() {
            return None;
        }
        let item = self.events[self.idx].clone();
        self.idx += 1;
        Some(item)
    }
}

impl DCBReadResponse for ReadResponse {
    fn head(&self) -> Option<u64> {
        self.head
    }
    fn collect_with_head(&mut self) -> (Vec<DCBSequencedEvent>, Option<u64>) {
        (self.events.clone(), self.head)
    }
    fn next_batch(&mut self) -> DCBResult<Vec<DCBSequencedEvent>> {
        let batch = self.events[self.idx..].to_vec();
        self.idx = self.events.len();
        Ok(batch)
    }
}

/// Append events unconditionally to the database.
///
/// For each event, this will:
/// - issue a position from the writer
/// - append an EventRecord to the event tree
/// - insert the position for each tag into the tags tree
///
/// Caller is responsible for committing the writer.
pub fn unconditional_append(
    mvcc: &Mvcc,
    writer: &mut Writer,
    events: Vec<DCBEvent>,
) -> DCBResult<u64> {
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
        event_tree_append(mvcc, writer, record, position)?;
        for tag in tags.iter() {
            let tag_hash: TagHash = tag_to_hash(tag);
            tags_tree_insert(mvcc, writer, tag_hash, position)?;
        }
    }

    Ok(last_pos_u64)
}

/// Read events using the tags index by merging per-tag iterators, grouping by position,
/// filtering by tag and type matches, and then looking up the event record.
pub fn read_conditional(
    mvcc: &Mvcc,
    dirty: &HashMap<PageID, Page>,
    events_tree_root_id: PageID,
    tags_tree_root_id: PageID,
    query: DCBQuery,
    after: Position,
    limit: Option<usize>,
) -> DCBResult<Vec<DCBSequencedEvent>> {
    const SCAN_BATCH_SIZE: usize = 256;
    // Special case: explicit zero limit
    if let Some(0) = limit {
        return Ok(Vec::new());
    }

    // If no items, return all events with after/limit respected via sequential scan
    if query.items.is_empty() {
        let mut iter = EventIterator::new(mvcc, dirty, events_tree_root_id, Some(after));
        let mut out: Vec<DCBSequencedEvent> = Vec::new();
        'outer_all: loop {
            let batch = iter.next_batch(limit.unwrap_or(SCAN_BATCH_SIZE))?;
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
                if let Some(lim) = limit
                    && out.len() >= lim
                {
                    break 'outer_all;
                }
            }
        }
        return Ok(out);
    }

    // All query items must have at least one tag to use the tag index path.
    let all_items_have_tags = query.items.iter().all(|it| !it.tags.is_empty());
    if !all_items_have_tags {
        // Fallback: sequentially scan all events and apply the same matching logic
        let mut iter = EventIterator::new(mvcc, dirty, events_tree_root_id, Some(after));
        let mut out: Vec<DCBSequencedEvent> = Vec::new();
        let matches_item = |rec: &EventRecord| -> bool {
            for item in &query.items {
                let type_ok =
                    item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type);
                if !type_ok {
                    continue;
                }
                let tags_ok = item.tags.iter().all(|t| rec.tags.iter().any(|et| et == t));
                if type_ok && tags_ok {
                    return true;
                }
            }
            false
        };
        'outer_fallback: loop {
            let batch = iter.next_batch(SCAN_BATCH_SIZE)?;
            if batch.is_empty() {
                break;
            }
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
                    if let Some(lim) = limit
                        && out.len() >= lim
                    {
                        break 'outer_fallback;
                    }
                }
            }
        }
        return Ok(out);
    }

    // Invert query: tag -> list of query item indices that require this tag
    let mut tag_qiis: HashMap<String, Vec<usize>> = HashMap::with_capacity(query.items.len() * 2);
    let mut qi_tags: Vec<HashSet<String>> = Vec::with_capacity(query.items.len());

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
            self.inner
                .next()
                .map(|p| (p, self.tag.clone(), self.qiids.clone()))
        }
    }

    let mut tag_iters: Vec<PositionTagQiidIterator<_>> = Vec::new();
    for (tag, qiids) in tag_qiis.iter() {
        let tag_hash: TagHash = tag_to_hash(tag);
        let positions_iter = TagsTreeIterator::new(mvcc, dirty, tags_tree_root_id, tag_hash, after); // yields positions for tag
        tag_iters.push(PositionTagQiidIterator::new(
            positions_iter,
            tag.clone(),
            qiids.clone(),
        ));
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
            Self {
                inner,
                current_pos: None,
                tags: HashSet::new(),
                qiis: HashSet::new(),
                finished: false,
            }
        }
    }
    impl<I> Iterator for GroupByPositionIterator<I>
    where
        I: Iterator<Item = (Position, String, Vec<usize>)>,
    {
        type Item = (Position, HashSet<String>, HashSet<usize>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.finished {
                return None;
            }
            for (pos, tag, qiids) in self.inner.by_ref() {
                if self.current_pos.is_none() {
                    self.current_pos = Some(pos);
                } else if self.current_pos.unwrap() != pos {
                    let out_pos = self.current_pos.unwrap();
                    let out_tags = std::mem::take(&mut self.tags);
                    let out_qiis = std::mem::take(&mut self.qiis);
                    self.current_pos = Some(pos);
                    self.tags.insert(tag);
                    for q in qiids {
                        self.qiis.insert(q);
                    }
                    return Some((out_pos, out_tags, out_qiis));
                }
                self.tags.insert(tag);
                for q in qiids {
                    self.qiis.insert(q);
                }
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

    let mut out: Vec<DCBSequencedEvent> = Vec::new();
    for (pos, tags_present, qiis_present) in GroupByPositionIterator::new(merged) {
        // Find any query item whose required tag set is subset of tags_present
        let matching_qiis: Vec<usize> = qiis_present
            .iter()
            .copied()
            .filter(|&qii| qi_tags[qii].is_subset(&tags_present))
            .collect();
        if matching_qiis.is_empty() {
            continue;
        }

        // Lookup the event record at position
        let rec = event_tree_lookup(mvcc, dirty, events_tree_root_id, pos)?;

        // Check type and actual tag matching against any of the matching items to avoid hash-collision false positives
        let mut match_ok = false;
        'matchcheck: for qii in matching_qiis.iter().copied() {
            let item = &query.items[qii];
            // Type must match (or be unspecified)
            let type_ok = item.types.is_empty() || item.types.iter().any(|t| t == &rec.event_type);
            if !type_ok {
                continue;
            }
            // Verify actual event tags contain all item tags (guards against tag-hash collisions)
            let tags_ok = item.tags.iter().all(|t| rec.tags.iter().any(|et| et == t));
            if tags_ok {
                match_ok = true;
                break 'matchcheck;
            }
        }
        if !match_ok {
            continue;
        }

        out.push(DCBSequencedEvent {
            position: pos.0,
            event: DCBEvent {
                event_type: rec.event_type,
                data: rec.data,
                tags: rec.tags,
            },
        });
        if let Some(lim) = limit
            && out.len() >= lim
        {
            break;
        }
    }

    Ok(out)
}
/// Compute a TagHash ([u8; 8]) from a tag string using a stable 64-bit hash.
#[inline(always)]
pub fn tag_to_hash(tag: &str) -> TagHash {
    const SALT: [u8; 4] = [0x9E, 0x37, 0x79, 0xB9];
    // Build a 64-bit value by combining two crc32 hashes for stability and simplicity.
    let mut hasher1 = crc32fast::Hasher::new();
    hasher1.update(tag.as_bytes());
    let a = hasher1.finalize();

    let mut hasher2 = crc32fast::Hasher::new();
    // Note: Benchmark (benches/tag_hash_bench.rs) shows two update() calls
    // are consistently faster than concatenating bytes+salt into a buffer
    // and calling update() once, because concatenation requires allocation
    // and copying. Keeping the two calls avoids extra work and is at least
    // as fast across sizes from 0..8192 bytes.
    hasher2.update(tag.as_bytes());
    hasher2.update(&SALT);
    let b = hasher2.finalize();

    let value = ((a as u64) << 32) | (b as u64);
    value.to_le_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dcb::{
        DCBAppendCondition, DCBError, DCBEvent, DCBEventStore, DCBQuery, DCBQueryItem,
    };
    use crate::page::Page;
    use serial_test::serial;
    use std::collections::HashMap;
    use tempfile::tempdir;

    // Backward-compatible wrapper for tests: call new read_conditional with an empty dirty map
    fn read_conditional(
        mvcc: &Mvcc,
        events_tree_root_id: PageID,
        tags_tree_root_id: PageID,
        query: DCBQuery,
        after: Position,
        limit: Option<usize>,
    ) -> DCBResult<Vec<DCBSequencedEvent>> {
        super::read_conditional(
            mvcc,
            &HashMap::<PageID, Page>::new(),
            events_tree_root_id,
            tags_tree_root_id,
            query,
            after,
            limit,
        )
    }

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
    fn setup_db_with_standard_events() -> (tempfile::TempDir, Mvcc, Vec<DCBEvent>) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-api-test.db");
        let db = Mvcc::new(db_path.as_ref(), DEFAULT_PAGE_SIZE, VERBOSE).unwrap();
        let input = standard_events();
        let mut writer = db.writer().unwrap();
        let last = unconditional_append(&db, &mut writer, input.clone()).unwrap();
        db.commit(&mut writer).unwrap();
        // Verify last equals committed head
        let (_, header) = db.get_latest_header().unwrap();
        let head = header.next_position.0.saturating_sub(1);
        assert_eq!(last, head);
        (temp_dir, db, input)
    }

    #[test]
    #[serial]
    fn empty_query_after_and_limit() {
        let (_tmp, mut mvcc, input) = setup_db_with_standard_events();

        // after = 0 -> all
        let reader = mvcc.reader().unwrap();

        let all = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Position(0),
            None,
        )
        .unwrap();
        assert_eq!(all.len(), input.len());
        assert!(all.windows(2).all(|w| w[0].position < w[1].position));

        // after = first -> tail
        let first = all[0].position;
        let tail = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Position(first),
            None,
        )
        .unwrap();
        assert_eq!(tail.len(), input.len() - 1);

        // after = last -> empty
        let last = all.last().unwrap().position;
        let none = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Position(last),
            None,
        )
        .unwrap();
        assert!(none.is_empty());

        // limits
        let lim0 = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Position(0),
            Some(0),
        )
        .unwrap();
        assert!(lim0.is_empty());
        let lim3 = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Position(0),
            Some(3),
        )
        .unwrap();
        assert_eq!(lim3.len(), 3);
        let lim20 = read_conditional(
            &mut mvcc,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            DCBQuery { items: vec![] },
            Position(0),
            Some(20),
        )
        .unwrap();
        assert_eq!(lim20.len(), input.len());
    }

    #[test]
    #[serial]
    fn tags_only_single_tag_after_and_limit() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(0),
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 4);
        assert!(
            res.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "alpha"))
        );
        assert!(res.windows(2).all(|w| w[0].position < w[1].position));

        // after combinations
        let positions: Vec<u64> = res.iter().map(|e| e.position).collect();
        let after_first = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(positions[0]),
            None,
        )
        .unwrap();
        assert_eq!(after_first.len(), positions.len() - 1);
        let after_last = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(*positions.last().unwrap()),
            None,
        )
        .unwrap();
        assert!(after_last.is_empty());

        // limits
        let lim0 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(0),
            Some(0),
        )
        .unwrap();
        assert!(lim0.is_empty());
        let lim1 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(0),
            Some(1),
        )
        .unwrap();
        assert_eq!(lim1.len(), 1);
        let lim10 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Position(0),
            Some(10),
        )
        .unwrap();
        assert_eq!(lim10.len(), 4);
    }

    #[test]
    #[serial]
    fn tags_only_multi_tag_and() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string(), "gamma".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Position(0),
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 2);
        assert!(
            res.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "alpha"))
        );
        assert!(
            res.iter()
                .all(|e| e.event.tags.iter().any(|t| t == "gamma"))
        );
    }

    #[test]
    #[serial]
    fn types_plus_tags_index_path() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["Type0".to_string()],
                tags: vec!["alpha".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Position(0),
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].event.event_type, "Type0");
        assert!(res[0].event.tags.iter().any(|t| t == "alpha"));
    }

    #[test]
    #[serial]
    fn or_semantics_and_deduplication() {
        let (_tmp, mut db, _input) = setup_db_with_standard_events();
        let alpha_only = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["alpha".to_string()],
            }],
        };
        let reader = db.reader().unwrap();
        let alpha_positions: Vec<u64> = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            alpha_only.clone(),
            Position(0),
            None,
        )
        .unwrap()
        .into_iter()
        .map(|e| e.position)
        .collect();

        // Overlapping items: alpha OR (alpha AND gamma) should deduplicate
        let query = DCBQuery {
            items: vec![
                DCBQueryItem {
                    types: vec![],
                    tags: vec!["alpha".to_string()],
                },
                DCBQueryItem {
                    types: vec![],
                    tags: vec!["alpha".to_string(), "gamma".to_string()],
                },
            ],
        };
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            query,
            Position(0),
            None,
        )
        .unwrap();
        let res_positions: Vec<u64> = res.into_iter().map(|e| e.position).collect();
        assert_eq!(res_positions, alpha_positions);
    }

    #[test]
    #[serial]
    fn fallback_types_only_after_and_limit() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("mvcc-fallback-types-only.db");
        let mut db = Mvcc::new(db_path.as_ref(), DEFAULT_PAGE_SIZE, VERBOSE).unwrap();

        // Use a smaller custom set to make counts obvious
        let events = vec![
            DCBEvent {
                event_type: "TypeA".to_string(),
                data: vec![1],
                tags: vec!["x".to_string()],
            },
            DCBEvent {
                event_type: "TypeB".to_string(),
                data: vec![2],
                tags: vec!["y".to_string()],
            },
            DCBEvent {
                event_type: "TypeA".to_string(),
                data: vec![3],
                tags: vec!["z".to_string()],
            },
        ];
        let mut writer = db.writer().unwrap();
        let last = unconditional_append(&db, &mut writer, events).unwrap();
        db.commit(&mut writer).unwrap();
        let (_, header) = db.get_latest_header().unwrap();
        let head = header.next_position.0.saturating_sub(1);
        assert_eq!(last, head);

        // Query item with no tags => forces fallback path; select TypeA only
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["TypeA".to_string()],
                tags: vec![],
            }],
        };
        let reader = db.reader().unwrap();
        let res = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(0),
            None,
        )
        .unwrap();
        assert_eq!(res.len(), 2);
        assert!(res.iter().all(|e| e.event.event_type == "TypeA"));

        // After skip first matching
        let first_pos = res[0].position;
        let res_after = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(first_pos),
            None,
        )
        .unwrap();
        assert_eq!(res_after.len(), 1);

        // Limit 1
        let res_lim1 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Position(0),
            Some(1),
        )
        .unwrap();
        assert_eq!(res_lim1.len(), 1);
    }

    #[test]
    #[serial]
    fn fallback_empty_item_matches_all() {
        let (_tmp, mut db, input) = setup_db_with_standard_events();
        // An empty item (no types, no tags) should match all events via fallback path
        let qi = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec![],
            }],
        };

        let reader = db.reader().unwrap();
        let all = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(0),
            None,
        )
        .unwrap();
        assert_eq!(all.len(), input.len());

        // After and limit still apply
        let first = all[0].position;
        let tail = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi.clone(),
            Position(first),
            None,
        )
        .unwrap();
        assert_eq!(tail.len(), input.len() - 1);
        let lim5 = read_conditional(
            &mut db,
            reader.events_tree_root_id,
            reader.tags_tree_root_id,
            qi,
            Position(0),
            Some(5),
        )
        .unwrap();
        assert_eq!(lim5.len(), 5);
    }

    #[test]
    #[serial]
    fn test_event_store() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Head is None on empty store
        assert_eq!(None, store.head().unwrap());

        // Append a couple of events
        let events = vec![
            DCBEvent {
                event_type: "TypeA".to_string(),
                data: vec![1],
                tags: vec!["foo".to_string()],
            },
            DCBEvent {
                event_type: "TypeB".to_string(),
                data: vec![2],
                tags: vec!["bar".to_string(), "foo".to_string()],
            },
        ];
        let last = store.append(events.clone(), None).unwrap();
        assert!(last > 0);
        assert_eq!(store.head().unwrap(), Some(last));

        // Read all
        let mut resp = store.read(None, None, None, false, None).unwrap();
        let (all, head) = resp.collect_with_head();
        assert_eq!(head, Some(last));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].event.event_type, "TypeA");
        assert_eq!(all[1].event.event_type, "TypeB");

        // Limit semantics: only first event returned and head equals that position
        let mut resp_lim1 = store.read(None, None, Some(1), false, None).unwrap();
        let (only_one, head_lim1) = resp_lim1.collect_with_head();
        assert_eq!(only_one.len(), 1);
        assert_eq!(only_one[0].event.event_type, "TypeA");
        assert_eq!(head_lim1, Some(only_one[0].position));

        // Tag-filtered read ("foo")
        let query = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["foo".to_string()],
            }],
        };
        let mut resp2 = store.read(Some(query), None, None, false, None).unwrap();
        let out2 = resp2.next_batch().unwrap();
        assert_eq!(out2.len(), 2);
        assert!(out2.iter().all(|e| e.event.tags.iter().any(|t| t == "foo")));

        // After semantics: skip the first event
        let first_pos = all[0].position;
        let mut resp3 = store.read(None, Some(first_pos), None, false, None).unwrap();
        let out3 = resp3.next_batch().unwrap();
        assert_eq!(out3.len(), 1);
        assert_eq!(out3[0].event.event_type, "TypeB");

        // Append with a condition that should PASS: query matches existing 'foo' but after = last
        let cond_pass = DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec!["foo".to_string()],
                }],
            },
            after: Some(last),
        };
        let ok_last = store
            .append(
                vec![DCBEvent {
                    event_type: "TypeC".to_string(),
                    data: vec![3],
                    tags: vec!["baz".to_string()],
                }],
                Some(cond_pass),
            )
            .expect("append with passing condition should succeed");
        assert!(ok_last > last);
        assert_eq!(store.head().unwrap(), Some(ok_last));

        // Append with a condition that should FAIL: same query but after = 0
        let cond_fail = DCBAppendCondition {
            fail_if_events_match: DCBQuery {
                items: vec![DCBQueryItem {
                    types: vec![],
                    tags: vec!["foo".to_string()],
                }],
            },
            after: Some(0),
        };
        let before_head = store.head().unwrap();
        let res = store.append(
            vec![DCBEvent {
                event_type: "TypeD".to_string(),
                data: vec![4],
                tags: vec!["qux".to_string()],
            }],
            Some(cond_fail),
        );
        match res {
            Err(DCBError::IntegrityError(_)) => {}
            other => panic!("Expected IntegrityError, got {:?}", other),
        }
        // Ensure head unchanged after failed append
        assert_eq!(store.head().unwrap(), before_head);
    }

    #[test]
    fn test_append_batch_mixed_conditions() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        let e1 = DCBEvent {
            event_type: "A".into(),
            data: b"1".to_vec(),
            tags: vec!["t1".into()],
        };
        let e2 = DCBEvent {
            event_type: "B".into(),
            data: b"2".to_vec(),
            tags: vec!["t2".into()],
        };
        let e3 = DCBEvent {
            event_type: "C".into(),
            data: b"3".to_vec(),
            tags: vec!["t3".into()],
        };

        // Batch: first succeeds, second fails due to condition matching any event, third succeeds (after high position)
        let items = vec![
            (vec![e1.clone()], None),
            (
                vec![e2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: DCBQuery::default(),
                    after: None,
                }),
            ),
            (
                vec![e3.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: DCBQuery::default(),
                    after: Some(10),
                }),
            ),
        ];

        let results = store.append_batch(items).unwrap();

        assert_eq!(results.len(), 3);
        // First item should succeed with last position 1
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            Err(e) => panic!("unexpected error for first item: {:?}", e),
        }
        // Second item should fail integrity
        match &results[1] {
            Ok(pos) => panic!("expected integrity error, got Ok({})", pos),
            Err(e) => assert!(matches!(e, DCBError::IntegrityError(_))),
        }
        // Third item should succeed with last position 2 (since second didn't append)
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            Err(e) => panic!("unexpected error for third item: {:?}", e),
        }

        // Verify committed state: only e1 and e3 should be present, head is 2
        let (events, head) = store.read_with_head(None, None, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.data, e1.data);
        assert_eq!(events[1].event.data, e3.data);
        assert_eq!(head, Some(2));
    }

    #[test]
    fn test_append_batch_dirty_visibility_with_tags() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Item1 introduces tag "x"; Item2 condition queries tag "x" and must see it via dirty tags tree; Item3 uses after to ignore it
        let e1 = DCBEvent {
            event_type: "T".into(),
            data: b"one".to_vec(),
            tags: vec!["x".into()],
        };
        let e2 = DCBEvent {
            event_type: "T".into(),
            data: b"two".to_vec(),
            tags: vec!["y".into()],
        };
        let e3 = DCBEvent {
            event_type: "T".into(),
            data: b"three".to_vec(),
            tags: vec!["z".into()],
        };

        let query_tag_x = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec![],
                tags: vec!["x".into()],
            }],
        };

        let items = vec![
            // 1) Append e1 (tag x)
            (vec![e1.clone()], None),
            // 2) Attempt append e2, but fail if any events with tag x exist after None (i.e., from the start); should fail due to e1 in dirty pages
            (
                vec![e2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: query_tag_x.clone(),
                    after: None,
                }),
            ),
            // 3) Append e3 with condition that ignores position 1 by using after=Some(1); should pass
            (
                vec![e3.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: query_tag_x.clone(),
                    after: Some(1),
                }),
            ),
        ];

        let results = store.append_batch(items).unwrap();

        assert_eq!(results.len(), 3);
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            Err(e) => panic!("unexpected error for first item: {:?}", e),
        }
        match &results[1] {
            Ok(pos) => panic!("expected integrity error, got Ok({})", pos),
            Err(e) => assert!(matches!(e, DCBError::IntegrityError(_))),
        }
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            Err(e) => panic!("unexpected error for third item: {:?}", e),
        }

        // Verify committed state and tag index behavior
        let (events, head) = store.read_with_head(None, None, None).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.data, e1.data);
        assert_eq!(events[1].event.data, e3.data);
        assert_eq!(head, Some(2));

        // Query by tag x returns only the first event
        let (tagx_events, _) = store
            .read_with_head(Some(query_tag_x.clone()), None, None)
            .unwrap();
        assert_eq!(tagx_events.len(), 1);
        assert_eq!(tagx_events[0].event.data, e1.data);
    }

    #[test]
    fn test_append_batch_dirty_visibility_with_types_small_and_big_overflow() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Prepare events
        let small = DCBEvent {
            event_type: "S".into(),
            data: b"sm".to_vec(),
            tags: vec!["tS".into()],
        };
        // Large data to ensure it spills into event overflow pages
        let big_data_len = super::DEFAULT_PAGE_SIZE * 3; // 3 pages worth to be safe
        let big = DCBEvent {
            event_type: "B".into(),
            data: vec![0xAB; big_data_len],
            tags: vec!["tB".into()],
        };
        let filler1 = DCBEvent {
            event_type: "X".into(),
            data: b"x".to_vec(),
            tags: vec![],
        };
        let filler2 = DCBEvent {
            event_type: "Y".into(),
            data: b"y".to_vec(),
            tags: vec![],
        };
        let final_ok = DCBEvent {
            event_type: "C".into(),
            data: b"c".to_vec(),
            tags: vec![],
        };

        // Queries by type only (no tags) to force fallback path over events tree (which reads from dirty pages)
        let q_type_s = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["S".into()],
                tags: vec![],
            }],
        };
        let q_type_b = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["B".into()],
                tags: vec![],
            }],
        };

        let items = vec![
            // 1) Append small S
            (vec![small.clone()], None),
            // 2) Should fail because type S exists in dirty pages (after None)
            (
                vec![filler1.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_type_s.clone(),
                    after: None,
                }),
            ),
            // 3) Append big B (overflow)
            (vec![big.clone()], None),
            // 4) Should fail because type B exists in dirty pages (after None)
            (
                vec![filler2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_type_b.clone(),
                    after: None,
                }),
            ),
            // 5) Should succeed because after=Some(2) ignores positions <= 2 (small at 1, big at 2)
            (
                vec![final_ok.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_type_b.clone(),
                    after: Some(2),
                }),
            ),
        ];

        let results = store.append_batch(items).unwrap();
        assert_eq!(results.len(), 5);
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            other => panic!("unexpected for item0: {:?}", other),
        }
        match &results[1] {
            Err(DCBError::IntegrityError(_)) => {}
            other => panic!("expected IntegrityError for item1, got {:?}", other),
        }
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            other => panic!("unexpected for item2: {:?}", other),
        }
        match &results[3] {
            Err(DCBError::IntegrityError(_)) => {}
            other => panic!("expected IntegrityError for item3, got {:?}", other),
        }
        match &results[4] {
            Ok(pos) => assert_eq!(*pos, 3),
            other => panic!("unexpected for item4: {:?}", other),
        }

        // Verify committed state: we should have small (pos1), big (pos2), final_ok (pos3)
        let (events, head) = store.read_with_head(None, None, None).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.event_type, small.event_type);
        assert_eq!(events[1].event.event_type, big.event_type);
        assert_eq!(events[2].event.event_type, final_ok.event_type);
        assert_eq!(head, Some(3));

        // Check type queries and large data integrity
        let (small_by_type, _) = store
            .read_with_head(Some(q_type_s.clone()), None, None)
            .unwrap();
        assert_eq!(small_by_type.len(), 1);
        assert_eq!(small_by_type[0].event.event_type, "S");

        let (big_by_type, _) = store
            .read_with_head(Some(q_type_b.clone()), None, None)
            .unwrap();
        assert_eq!(big_by_type.len(), 1);
        assert_eq!(big_by_type[0].event.event_type, "B");
        assert_eq!(big_by_type[0].event.data.len(), big_data_len);
        assert!(big_by_type[0].event.data.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_append_batch_dirty_visibility_with_tags_and_types_small_and_big_overflow() {
        let temp_dir = tempdir().unwrap();
        let store = UmaDB::new(temp_dir.path()).unwrap();

        // Small inline event: type "S" with tag "x"
        let small = DCBEvent {
            event_type: "S".into(),
            data: b"sm".to_vec(),
            tags: vec!["x".into()],
        };
        // Big overflow event: type "B" with tag "y" and large payload to exercise overflow pages
        let big_data_len = super::DEFAULT_PAGE_SIZE * 3; // ensure multiple overflow pages
        let big = DCBEvent {
            event_type: "B".into(),
            data: vec![0xCD; big_data_len],
            tags: vec!["y".into()],
        };
        // Fillers that will be conditioned out
        let filler1 = DCBEvent {
            event_type: "X".into(),
            data: b"x".to_vec(),
            tags: vec![],
        };
        let filler2 = DCBEvent {
            event_type: "Y".into(),
            data: b"y".to_vec(),
            tags: vec![],
        };
        let final_ok = DCBEvent {
            event_type: "C".into(),
            data: b"c".to_vec(),
            tags: vec![],
        };

        // Conditions combining tags and types so the tags index is used and the type filter applies after lookup
        let q_s_and_x = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["S".into()],
                tags: vec!["x".into()],
            }],
        };
        let q_b_and_y = DCBQuery {
            items: vec![DCBQueryItem {
                types: vec!["B".into()],
                tags: vec!["y".into()],
            }],
        };

        let items = vec![
            // 1) Append small S@x
            (vec![small.clone()], None),
            // 2) Should fail because S@x exists in dirty pages (tags path + type filter)
            (
                vec![filler1.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_s_and_x.clone(),
                    after: None,
                }),
            ),
            // 3) Append big B@y (overflow)
            (vec![big.clone()], None),
            // 4) Should fail because B@y exists in dirty pages (tags path + type filter and overflow read)
            (
                vec![filler2.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_b_and_y.clone(),
                    after: None,
                }),
            ),
            // 5) Should succeed because after=Some(2) ignores positions <= 2 (small at 1, big at 2)
            (
                vec![final_ok.clone()],
                Some(DCBAppendCondition {
                    fail_if_events_match: q_b_and_y.clone(),
                    after: Some(2),
                }),
            ),
        ];

        let results = store.append_batch(items).unwrap();
        assert_eq!(results.len(), 5);
        match &results[0] {
            Ok(pos) => assert_eq!(*pos, 1),
            other => panic!("unexpected for item0: {:?}", other),
        }
        match &results[1] {
            Err(DCBError::IntegrityError(_)) => {}
            other => panic!("expected IntegrityError for item1, got {:?}", other),
        }
        match &results[2] {
            Ok(pos) => assert_eq!(*pos, 2),
            other => panic!("unexpected for item2: {:?}", other),
        }
        match &results[3] {
            Err(DCBError::IntegrityError(_)) => {}
            other => panic!("expected IntegrityError for item3, got {:?}", other),
        }
        match &results[4] {
            Ok(pos) => assert_eq!(*pos, 3),
            other => panic!("unexpected for item4: {:?}", other),
        }

        // Verify committed state and order
        let (events, head) = store.read_with_head(None, None, None).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.event_type, small.event_type);
        assert_eq!(events[1].event.event_type, big.event_type);
        assert_eq!(events[2].event.event_type, final_ok.event_type);
        assert_eq!(head, Some(3));

        // Query by combined type+tag should return exactly one for each
        let (small_combined, _) = store
            .read_with_head(Some(q_s_and_x.clone()), None, None)
            .unwrap();
        assert_eq!(small_combined.len(), 1);
        assert_eq!(small_combined[0].event.event_type, "S");
        assert!(small_combined[0].event.tags.iter().any(|t| t == "x"));

        let (big_combined, _) = store
            .read_with_head(Some(q_b_and_y.clone()), None, None)
            .unwrap();
        assert_eq!(big_combined.len(), 1);
        assert_eq!(big_combined[0].event.event_type, "B");
        assert!(big_combined[0].event.tags.iter().any(|t| t == "y"));
        assert_eq!(big_combined[0].event.data.len(), big_data_len);
        assert!(big_combined[0].event.data.iter().all(|&b| b == 0xCD));
    }
}
