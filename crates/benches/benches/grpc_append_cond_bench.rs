use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use std::hint::black_box;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use tokio::sync::{oneshot};
use umadb_client::AsyncUmaDBClient;
use umadb_core::db::UmaDB;
use umadb_dcb::{
    DCBAppendCondition, DCBEvent, DCBEventStoreAsync, DCBEventStoreSync, DCBQuery, DCBQueryItem,
};
use umadb_server::start_server;

fn init_db_with_events(num_events: usize) -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    // Populate the database using the local EventStore (fast, in-process)
    let store = UmaDB::new(&path).expect("create event store");

    // Prepare events and append in moderate batches to avoid huge allocations
    let batch_size = 1000usize.min(num_events.max(1));
    let mut remaining = num_events;
    while remaining > 0 {
        let current = remaining.min(batch_size);
        let mut events = Vec::with_capacity(current);
        for i in 0..current {
            let ev = DCBEvent {
                event_type: "bench-init".to_string(),
                data: format!("init-{}", i).into_bytes(),
                tags: vec!["init".to_string()],
                uuid: None,
            };
            events.push(ev);
        }
        store.append(events, None).expect("append to store");
        remaining -= current;
    }

    (dir, path)
}

pub fn grpc_append_cond_benchmark(c: &mut Criterion) {
    for &threads in &[1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024] {

        // Initialize DB and server with some events so head() is not None (not required, but realistic)
        let initial_events = 10_000usize;
        let (_tmp_dir, db_path) = init_db_with_events(initial_events);

        // Find a free localhost port
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        drop(listener);

        let addr_http = format!("http://{}", addr);

        // Start the gRPC server in a background thread, with shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let db_path_clone = db_path.clone();
        let addr_clone = addr.clone();

        let parallelism = std::thread::available_parallelism();
        let server_threads = parallelism
            .map(|n| n.get())
            .unwrap_or(1);

        let server_thread = thread::spawn(move || {
            let rt = RtBuilder::new_multi_thread()
                .worker_threads(server_threads)
                .enable_all()
                .build()
                .expect("build tokio rt for server");
            rt.block_on(async move {
                start_server(db_path_clone, &addr_clone, shutdown_rx)
                    .await
                    .expect("start server");
            });
        });

        // Wait until the server is actually accepting connections (avoid race with startup)
        {
            use std::time::Duration;
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if std::net::TcpStream::connect(&addr).is_ok() {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    panic!("server did not start listening within timeout at {}", addr);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        let mut group = c.benchmark_group("grpc_append_cond");
        group.sample_size(100);

        // Number of events appended per iteration by a single writer client
        let events_per_iter = 1usize;

        // Build a single Tokio runtime with fixed number of worker threads
        let rt = RtBuilder::new_multi_thread()
            .worker_threads(server_threads)
            .enable_all()
            .build()
            .expect("build tokio rt (writers)");

        // Establish a single gRPC connection
        let client = Arc::new(
            rt.block_on(AsyncUmaDBClient::connect(&addr_http, None))
                .expect("connect client"),
        );

        let after = rt
            .block_on(async { client.head().await.expect("head ok") })
            .unwrap_or(0);


        // Report throughput as the total across all concurrent appends
        group.throughput(Throughput::Elements(
            (events_per_iter as u64) * (threads as u64),
        ));

        // Prepare per-writer state (tag, last_pos)
        let mut tags: Vec<String> = Vec::with_capacity(threads);
        for i in 0..threads {
            tags.push(format!("writer-{}", i));
        }
        let tags = Arc::new(tags);

        // Initialize last_pos for each writer with current head position
        let client = client.clone();
        let rt_handle = rt.handle().clone();
        group.bench_function(BenchmarkId::from_parameter(threads), move |b| {
            let tags = tags.clone();
            b.iter(|| {
                // Spawn threads number of concurrent append tasks using the single client
                rt_handle.block_on(async {
                    let futs = (0..threads).map(|i| {
                        let client = client.clone();
                        let tag = tags[i].clone();
                        async move {
                            // Build event for this writer
                            let events: Vec<DCBEvent> = (0..events_per_iter)
                                .map(|j| DCBEvent {
                                    event_type: "bench-append-cond".to_string(),
                                    data: format!("data-{}", j).into_bytes(),
                                    tags: vec![tag.clone()],
                                    uuid: None,
                                })
                                .collect();

                            // Build condition: fail if any events with this tag exist after `after`
                            let condition = DCBAppendCondition {
                                fail_if_events_match: DCBQuery {
                                    items: vec![DCBQueryItem {
                                        types: vec![],
                                        tags: vec!["init".to_string()],
                                    }],
                                },
                                after: Some(after),
                            };

                            let _ = black_box(client
                                .append(black_box(events), Some(condition))
                                .await
                                .expect("append events"));
                        }
                    });
                    let _ = join_all(futs).await;
                });
            });
        });

        group.finish();

        // Shutdown server
        let _ = shutdown_tx.send(());
        let _ = server_thread.join();

    }

}

criterion_group!(benches, grpc_append_cond_benchmark);
criterion_main!(benches);
