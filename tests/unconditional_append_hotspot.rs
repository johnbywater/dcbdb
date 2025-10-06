use dcbdb::dcbapi::{DCBEvent, DCBEventStore};
use dcbdb::event_store::EventStore;
use std::env;
use std::time::Instant;
use tempfile::tempdir;

// This test is intended for profiling hotspots when appending events.
// It can append one event per call or many events per call, to profile both per-call overhead
// and per-event work inside the unconditional append path.
//
// How to run (ignored by default):
//   cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//
// To adjust how many total events are appended:
//   APPEND_BATCH=500000 cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//
// To control the number of events per append call (default 1):
//   EVENTS_PER_CALL=100 cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//
// You can use a profiler around the single test process, e.g.:
//   Linux perf:   perf record -- cargo test --test unconditional_append_hotspot -- --ignored --nocapture
//   macOS (DTrace/instruments): instruments -t Time\ Profiler target/debug/deps/unconditional_append_hotspot-*
//   flamegraph:  cargo flamegraph --test unconditional_append_hotspot -- --ignored --nocapture
#[test]
#[ignore]
fn profile_unconditional_append_hotspots() {
    // Number of events is configurable via env var to allow quick iteration when profiling
    let batch: usize = env::var("APPEND_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000000);

    // Number of tags per event (1 or 2 is realistic). Using 2 exercises more indexing work.
    let tags_per_event: usize = env::var("TAGS_PER_EVENT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    // Number of events per append call; default to 1 to preserve previous behavior
    let events_per_call: usize = env::var("EVENTS_PER_CALL")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(100);

    let tmp = tempdir().expect("create temp dir");
    let store = EventStore::new(tmp.path()).expect("open event store");

    // Time the entire loop of batched appends.
    let start = Instant::now();
    let mut last: u64 = 0;

    let mut appended_calls: usize = 0;

    let data_size: usize = env::var("DATA_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    let mut i: usize = 0;
    while i < batch {
        let take = (batch - i).min(events_per_call);
        let mut events: Vec<DCBEvent> = Vec::with_capacity(take);
        for j in 0..take {
            let idx = i + j;
            let payload = vec![(idx as u8).wrapping_mul(31); data_size];

            // Generate 1 or 2 tags per event. Tag content is varied to avoid overly uniform hashing.
            let mut tags = Vec::with_capacity(tags_per_event);
            tags.push(format!("tag-{}", idx));
            if tags_per_event > 1 {
                tags.push(format!("group-{}", idx % 10));
            }

            let event = DCBEvent {
                event_type: format!("Type{}", idx % 16),
                data: payload,
                tags,
            };
            events.push(event);
        }
        last = store.append(events, None).expect("append ok");
        appended_calls += 1;
        i += take;
    }

    let elapsed = start.elapsed();

    let avg_per_call_us = (elapsed.as_secs_f64() * 1e6) / (appended_calls as f64);
    let avg_per_event_us = (elapsed.as_secs_f64() * 1e6) / (batch as f64);

    // Print some stats so --nocapture shows useful info during profiling sessions.
    eprintln!(
        "unconditional_append: appended {} events in {} calls ({} events/call); last position {}; elapsed = {:.3?} (avg {:.3} µs/call, {:.3} µs/event)",
        batch,
        appended_calls,
        events_per_call,
        last,
        elapsed,
        avg_per_call_us,
        avg_per_event_us,
    );

    // Keep tempdir until end of test to ensure DB flush completes before directory removal.
    drop(store);
    drop(tmp);
}
