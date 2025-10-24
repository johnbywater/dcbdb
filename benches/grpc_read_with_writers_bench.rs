use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput, black_box};
use umadb::dcb::{DCBEvent, DCBEventStore};
use umadb::db::UmaDB;
use umadb::grpc::{AsyncUmaDBClient, start_grpc_server_with_shutdown};
use std::net::TcpListener;
use std::thread;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use tokio::sync::oneshot;
// use futures::StreamExt;
use futures::future::join_all;

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
                event_type: "bench".to_string(),
                data: format!("event-{}", i).into_bytes(),
                tags: vec!["tag1".to_string()],
            };
            events.push(ev);
        }
        store.append(events, None).expect("append to store");
        remaining -= current;
    }

    (dir, path)
}

pub fn grpc_read_with_writers_benchmark(c: &mut Criterion) {
    const TOTAL_EVENTS: usize = 10_000;
    const READ_BATCH_SIZE: usize = 1000;
    const WRITER_COUNT: usize = 4;
    const WRITER_EVENTS_PER_APPEND: usize = 1; // small continuous appends

    // Initialize DB and server with some events
    let (_tmp_dir, db_path) = init_db_with_events(TOTAL_EVENTS);

    // Find a free localhost port
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to ephemeral port");
    let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
    drop(listener);

    let addr_http = format!("http://{}", addr);

    // Start the gRPC server in a background thread, with shutdown channel
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let db_path_clone = db_path.clone();
    let addr_clone = addr.clone();

    let server_thread = thread::spawn(move || {
        let server_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let rt = RtBuilder::new_multi_thread()
            .worker_threads(server_threads)
            .enable_all()
            .build()
            .expect("build tokio rt for server");
        rt.block_on(async move {
            start_grpc_server_with_shutdown(db_path_clone, &addr_clone, shutdown_rx)
                .await
                .expect("start server");
        });
    });

    // Wait until the server is actually accepting connections (avoid race with startup)
    {
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

    // Start background writers that continuously append while reads are benchmarked
    let writers_running = Arc::new(AtomicBool::new(true));
    let writers_rt = RtBuilder::new_multi_thread()
        .worker_threads(WRITER_COUNT)
        .enable_all()
        .build()
        .expect("build tokio rt (writers)");

    // Create independent writer clients
    let mut writer_clients: Vec<Arc<AsyncUmaDBClient>> = Vec::with_capacity(WRITER_COUNT);
    for _ in 0..WRITER_COUNT {
        let c = writers_rt
            .block_on(AsyncUmaDBClient::connect_optimized_url(&addr_http))
            .expect("connect writer client");
        writer_clients.push(Arc::new(c));
    }

    // Prebuild a tiny event batch for each append
    let writer_batch: Vec<DCBEvent> = (0..WRITER_EVENTS_PER_APPEND)
        .map(|i| DCBEvent {
            event_type: "writer".to_string(),
            data: format!("w-{}", i).into_bytes(),
            tags: vec!["w".to_string()],
        })
        .collect();
    let writer_batch = Arc::new(writer_batch);

    // Spawn continuous writer tasks
    let mut writer_handles = Vec::with_capacity(WRITER_COUNT);
    for i in 0..WRITER_COUNT {
        let client = writer_clients[i].clone();
        let running = writers_running.clone();
        let batch = writer_batch.clone();
        let handle = writers_rt.spawn(async move {
            while running.load(Ordering::Relaxed) {
                // ignore result intentionally; if an error occurs, just break
                let _ = client.append(batch.as_ref().clone(), None).await;
                // Yield a bit to avoid starving the system
                tokio::task::yield_now().await;
            }
        });
        writer_handles.push(handle);
    }

    let mut group = c.benchmark_group("grpc_read_4writers");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &threads in &[1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024] {
        // Report throughput as the total across all runtime worker threads
        group.throughput(Throughput::Elements((TOTAL_EVENTS as u64) * (threads as u64)));

        // Build a Tokio runtime and multiple persistent clients (one per concurrent reader)
        let rt = RtBuilder::new_multi_thread()
            .worker_threads(threads + 10)
            .enable_all()
            .build()
            .expect("build tokio rt (client)");

        // Establish independent gRPC connections upfront to avoid contention on a single HTTP/2 channel
        let mut clients: Vec<Arc<AsyncUmaDBClient>> = Vec::with_capacity(threads);
        for _ in 0..threads {
            let c = rt
                .block_on(AsyncUmaDBClient::connect_optimized_url(&addr_http))
                .expect("connect client");
            clients.push(Arc::new(c));
        }
        let clients = Arc::new(clients);

        group.bench_function(BenchmarkId::from_parameter(threads), move |b| {
            let clients = clients.clone();
            b.iter(|| {
                rt.block_on(async {
                    // Spawn `threads` concurrent read futures and await them all
                    let futs = (0..threads).map(|i| {
                        let client = clients[i].clone();
                        async move {
                            let mut resp = client
                                .read(None, None, Some(TOTAL_EVENTS), false, Some(READ_BATCH_SIZE))
                                .await
                                .expect("start read response");
                            let mut count = 0usize;
                            loop {
                                let batch = resp.next_batch().await.expect("next_batch ok");
                                if batch.is_empty() { break; }
                                for item in batch.into_iter() {
                                    let _evt = black_box(item);
                                    count += 1;
                                }
                            }
                            assert_eq!(count, TOTAL_EVENTS, "expected to read all preloaded events");
                        }
                    });
                    let _ = join_all(futs).await;
                });
            });
        });
    }

    group.finish();

    // Stop writers and shutdown server
    writers_running.store(false, Ordering::Relaxed);
    // Wait briefly to let writer tasks exit
    writers_rt.block_on(async {
        for h in writer_handles {
            let _ = h.await;
        }
    });

    let _ = shutdown_tx.send(());
    let _ = server_thread.join();
}

criterion_group!(benches, grpc_read_with_writers_benchmark);
criterion_main!(benches);
