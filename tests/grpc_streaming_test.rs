use dcbdb::dcbapi::{DCBEvent, DCBEventStore};
use dcbdb::grpc::{start_grpc_server_with_shutdown, GrpcEventStoreClient};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn grpc_streams_many_batches_for_large_reads() {
    // Arrange: start a gRPC server backed by a temporary directory
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50071"; // test port
    let addr_http = format!("http://{}", addr);

    // Channel to shutdown the server
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn a thread to run the server on a Tokio runtime
    let server_handle = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            // Ignore result on shutdown
            let _ = start_grpc_server_with_shutdown(db_path, &addr, shutdown_rx).await;
        });
    });

    // Give the server a brief moment to start listening
    thread::sleep(Duration::from_millis(200));

    // Connect the client (connect is async, create a runtime just for that)
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt
        .block_on(async { GrpcEventStoreClient::connect(addr_http).await })
        .expect("client connect");

    // Append 1000 events
    let events: Vec<DCBEvent> = (0..1000)
        .map(|i| DCBEvent {
            event_type: "TestEvent".to_string(),
            data: format!("data-{i}").into_bytes(),
            tags: vec!["grpc-test".to_string()],
        })
        .collect();

    let last_pos = client.append(events, None).expect("append 1000 events");
    assert!(last_pos >= 1000, "last position should be >= 1000, got {}", last_pos);

    // Act: read with a small limit to force multiple batches
    let mut response = client
        .read(None, None, None)
        .expect("read response");

    let mut total = 0usize;
    let mut batches = 0usize;

    loop {
        let batch = response.next_batch().expect("next batch");
        if batch.is_empty() {
            break;
        }
        total += batch.len();
        batches += 1;
    }

    // Assert: we read all 1000 events in multiple batches
    assert_eq!(1000, total, "should read exactly 1000 events");
    assert!(batches > 1, "should have streamed many batches, got {}", batches);

    // Cleanup: shutdown server
    let _ = shutdown_tx.send(());
    // Give it a moment to shutdown cleanly
    thread::sleep(Duration::from_millis(100));
    let _ = server_handle.join();
}
