use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use tempfile::tempdir;
use std::sync::Arc;
use std::net::SocketAddr;
use tokio::runtime::Runtime;
use dcbsd::{Event, EventStore, Query, QueryItem};
use dcbsd::server::EventStoreServer;
use dcbsd::client::{EventStoreClient, EventStoreClientSync};

fn setup_event_store(num_events: usize) -> (tempfile::TempDir, EventStore) {
    let temp_dir = tempdir().unwrap();
    let event_store = EventStore::open(temp_dir.path()).unwrap();

    // Append events for benchmarking
    for i in 0..num_events {
        let event = Event {
            event_type: format!("type{}", i % 3), // Create a few different types
            data: format!("data for event {}", i).into_bytes(),
            tags: vec![format!("tag{}", i % 5)], // Create a few different tags
        };
        event_store.append(vec![event], None).unwrap();
    }

    (temp_dir, event_store)
}

fn setup_grpc_client(num_events: usize) -> (tempfile::TempDir, EventStoreClientSync, Runtime) {
    // Create a temporary directory and event store
    let temp_dir = tempdir().unwrap();
    let event_store = EventStore::open(temp_dir.path()).unwrap();
    let event_store_server = EventStoreServer::with_event_store(Arc::new(event_store));

    // Create a runtime for the gRPC server
    let rt = Runtime::new().unwrap();

    // Start the gRPC server
    let server_addr = "127.0.0.1:50052".parse::<SocketAddr>().unwrap();
    let server_future = tonic::transport::Server::builder()
        .add_service(event_store_server.into_service())
        .serve(server_addr);

    // Spawn the server in the background
    rt.spawn(server_future);

    // Connect to the server
    let client = rt.block_on(async {
        EventStoreClient::connect("http://127.0.0.1:50052").await.unwrap()
    });

    let client_sync = EventStoreClientSync::new(client);

    // Append events for benchmarking
    for i in 0..num_events {
        let event = Event {
            event_type: format!("type{}", i % 3), // Create a few different types
            data: format!("data for event {}", i).into_bytes(),
            tags: vec![format!("tag{}", i % 5)], // Create a few different tags
        };
        client_sync.append(vec![event], None).unwrap();
    }

    (temp_dir, client_sync, rt)
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_operations_direct");

    // Test with different numbers of events in the store
    for num_events in [10, 100, 1000].iter() {
        let (_temp_dir, event_store) = setup_event_store(*num_events);

        // Benchmark reading all events
        group.bench_with_input(
            BenchmarkId::new("read_all", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    event_store.read(None, None, None).unwrap();
                });
            }
        );

        // Benchmark reading with a type filter
        group.bench_with_input(
            BenchmarkId::new("read_with_type_filter", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    let query = Query {
                        items: vec![QueryItem {
                            types: vec!["type1".to_string()],
                            tags: vec![],
                        }],
                    };
                    event_store.read(Some(query), None, None).unwrap();
                });
            }
        );

        // Benchmark reading with a tag filter
        group.bench_with_input(
            BenchmarkId::new("read_with_tag_filter", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    let query = Query {
                        items: vec![QueryItem {
                            types: vec![],
                            tags: vec!["tag2".to_string()],
                        }],
                    };
                    event_store.read(Some(query), None, None).unwrap();
                });
            }
        );

        // Benchmark reading with pagination
        group.bench_with_input(
            BenchmarkId::new("read_with_pagination", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    event_store.read(None, Some(5), Some(10)).unwrap();
                });
            }
        );
    }

    group.finish();
}

fn bench_read_grpc(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_operations_grpc");

    // Test with different numbers of events in the store
    for num_events in [10, 100, 1000].iter() {
        let (_temp_dir, client_sync, _rt) = setup_grpc_client(*num_events);

        // Benchmark reading all events
        group.bench_with_input(
            BenchmarkId::new("read_all", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    client_sync.read(None, None, None).unwrap();
                });
            }
        );

        // Benchmark reading with a type filter
        group.bench_with_input(
            BenchmarkId::new("read_with_type_filter", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    let query = Query {
                        items: vec![QueryItem {
                            types: vec!["type1".to_string()],
                            tags: vec![],
                        }],
                    };
                    client_sync.read(Some(query), None, None).unwrap();
                });
            }
        );

        // Benchmark reading with a tag filter
        group.bench_with_input(
            BenchmarkId::new("read_with_tag_filter", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    let query = Query {
                        items: vec![QueryItem {
                            types: vec![],
                            tags: vec!["tag2".to_string()],
                        }],
                    };
                    client_sync.read(Some(query), None, None).unwrap();
                });
            }
        );

        // Benchmark reading with pagination
        group.bench_with_input(
            BenchmarkId::new("read_with_pagination", num_events), 
            num_events,
            |b, _| {
                b.iter(|| {
                    client_sync.read(None, Some(5), Some(10)).unwrap();
                });
            }
        );
    }

    group.finish();
}

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_operations_direct");

    // Test with different batch sizes
    for batch_size in [1, 10, 100].iter() {
        let (_temp_dir, event_store) = setup_event_store(100); // Start with 100 events

        // Create events for the batch
        let events: Vec<Event> = (0..*batch_size)
            .map(|i| Event {
                event_type: format!("type{}", i % 3),
                data: format!("data for event {}", i).into_bytes(),
                tags: vec![format!("tag{}", i % 5)],
            })
            .collect();

        // Benchmark appending events
        group.bench_with_input(
            BenchmarkId::new("append_batch", batch_size), 
            batch_size,
            |b, _| {
                b.iter(|| {
                    let events_clone = events.clone();
                    event_store.append(events_clone, None).unwrap();
                });
            }
        );

        // Benchmark appending events with a condition
        group.bench_with_input(
            BenchmarkId::new("append_with_condition", batch_size), 
            batch_size,
            |b, _| {
                b.iter(|| {
                    let events_clone = events.clone();
                    let query = Query {
                        items: vec![QueryItem {
                            types: vec!["non_existent_type".to_string()],
                            tags: vec![],
                        }],
                    };
                    let condition = dcbsd::AppendCondition {
                        fail_if_events_match: query,
                        after: None,
                    };
                    event_store.append(events_clone, Some(condition)).unwrap();
                });
            }
        );
    }

    group.finish();
}

fn bench_append_grpc(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_operations_grpc");

    // Test with different batch sizes
    for batch_size in [1, 10, 100].iter() {
        let (_temp_dir, client_sync, _rt) = setup_grpc_client(100); // Start with 100 events

        // Create events for the batch
        let events: Vec<Event> = (0..*batch_size)
            .map(|i| Event {
                event_type: format!("type{}", i % 3),
                data: format!("data for event {}", i).into_bytes(),
                tags: vec![format!("tag{}", i % 5)],
            })
            .collect();

        // Benchmark appending events
        group.bench_with_input(
            BenchmarkId::new("append_batch", batch_size), 
            batch_size,
            |b, _| {
                b.iter(|| {
                    let events_clone = events.clone();
                    client_sync.append(events_clone, None).unwrap();
                });
            }
        );

        // Benchmark appending events with a condition
        group.bench_with_input(
            BenchmarkId::new("append_with_condition", batch_size), 
            batch_size,
            |b, _| {
                b.iter(|| {
                    let events_clone = events.clone();
                    let query = Query {
                        items: vec![QueryItem {
                            types: vec!["non_existent_type".to_string()],
                            tags: vec![],
                        }],
                    };
                    let condition = dcbsd::AppendCondition {
                        fail_if_events_match: query,
                        after: None,
                    };
                    client_sync.append(events_clone, Some(condition)).unwrap();
                });
            }
        );
    }

    group.finish();
}

criterion_group!(benches, bench_read, bench_read_grpc, bench_append, bench_append_grpc);
criterion_main!(benches);
