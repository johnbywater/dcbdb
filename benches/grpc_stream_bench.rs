use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput, black_box};
use dcbdb::dcbapi::{DCBEvent, DCBEventStore};
use dcbdb::event_store::EventStore;
use dcbdb::grpc::{GrpcEventStoreClient, start_grpc_server_with_shutdown};
use std::net::TcpListener;
use std::thread;
use tempfile::tempdir;
use tokio::runtime::Builder as RtBuilder;
use tokio::sync::oneshot;
use futures::StreamExt;

fn init_db_with_events(num_events: usize) -> (tempfile::TempDir, String) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().to_str().unwrap().to_string();

    // Populate the database using the local EventStore (fast, in-process)
    let store = EventStore::new(&path).expect("create event store");

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

fn run_bench_for_threads(thread_count: usize, addr_http: String, total_events: usize) {
    // Build a runtime with the specified worker threads and perform a single full-stream read
    let rt = RtBuilder::new_multi_thread()
        .worker_threads(thread_count)
        .enable_all()
        .build()
        .expect("build tokio rt");

    rt.block_on(async move {
        let client = GrpcEventStoreClient::connect(addr_http).await.expect("connect client");
        // subscribe=false, batch_size=None to just stream existing events
        let mut stream = client
            .read(None, None, None, false, None)
            .await
            .expect("start read stream");

        let mut count = 0usize;
        while let Some(item) = stream.next().await {
            let _evt = item.expect("stream item ok");
            count += 1;
        }
        assert_eq!(count, total_events, "expected to read all preloaded events");
    });
}

pub fn grpc_stream_benchmark(c: &mut Criterion) {
    // Initialize DB and server with 10_000 events
    let total_events = 10_000usize;
    let (_tmp_dir, db_path) = init_db_with_events(total_events);

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

    // Give the server a brief moment to start listening
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut group = c.benchmark_group("grpc_stream_read");

    for &threads in &[1usize, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024] {
        // Report throughput as the total across all runtime worker threads
        group.throughput(Throughput::Elements((total_events as u64) * (threads as u64)));
        group.bench_function(BenchmarkId::from_parameter(threads), |b| {
            // Ensure any optimizations don't elide the work
            b.iter(|| {
                run_bench_for_threads(black_box(threads), addr_http.clone(), total_events);
            });
        });
    }

    group.finish();

    // Shutdown server
    let _ = shutdown_tx.send(());
    let _ = server_thread.join();
}

criterion_group!(benches, grpc_stream_benchmark);
criterion_main!(benches);
