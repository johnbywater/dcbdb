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
        .read(None, None, None, false)
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


#[test]
fn grpc_does_not_stream_past_starting_head() {
    // Arrange: start a gRPC server backed by a temporary directory
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50072"; // another test port
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

    // Append initial 300 events
    let initial_events: Vec<DCBEvent> = (0..300)
        .map(|i| DCBEvent {
            event_type: "TestEvent".to_string(),
            data: format!("data-{i}").into_bytes(),
            tags: vec!["grpc-boundary".to_string()],
        })
        .collect();
    let last_pos_initial = client.append(initial_events, None).expect("append initial events");
    assert!(last_pos_initial >= 300, "last position should be >= 300, got {}", last_pos_initial);

    // Start a streaming read with no limit to get multiple batches and capture starting head semantics
    let mut response = client
        .read(None, None, None, false)
        .expect("read response");

    // Consume the first batch to ensure the stream has begun
    let first_batch = response.next_batch().expect("first batch");
    assert!(!first_batch.is_empty(), "first batch should not be empty");

    // Append 50 more events AFTER the read has started
    let new_events: Vec<DCBEvent> = (0..50)
        .map(|i| DCBEvent {
            event_type: "TestEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-boundary".to_string()],
        })
        .collect();
    let _ = client.append(new_events, None).expect("append new events during read");

    // Continue reading remaining batches
    let mut total = first_batch.len();
    let mut batches = 1usize;
    while let Ok(batch) = response.next_batch() {
        if batch.is_empty() { break; }
        total += batch.len();
        batches += 1;
    }

    // The total read should equal the head at the time the read started (300)
    assert_eq!(300, total, "should read exactly initial 300 events, not including newly appended ones");

    // The head reported by the stream should be the starting head (300)
    assert_eq!(Some(300), response.head(), "stream head should be the starting head");

    // There should be multiple batches
    assert!(batches > 1, "should have streamed multiple batches, got {}", batches);

    // Cleanup: shutdown server
    let _ = shutdown_tx.send(());
    thread::sleep(Duration::from_millis(100));
    let _ = server_handle.join();
}


#[test]
fn grpc_subscription_catch_up_and_continue() {
    // Arrange: start a gRPC server backed by a temporary directory
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let addr = "127.0.0.1:50073"; // subscription test port
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

    // Append initial events
    let initial_count = 40usize;
    let initial_events: Vec<DCBEvent> = (0..initial_count as u64)
        .map(|i| DCBEvent {
            event_type: "SubTestEvent".to_string(),
            data: format!("init-{i}").into_bytes(),
            tags: vec!["grpc-sub".to_string()],
        })
        .collect();
    let last_pos_initial = client.append(initial_events, None).expect("append initial events");
    assert!(last_pos_initial >= initial_count as u64, "last position should be >= {initial_count}, got {}", last_pos_initial);

    // Start a subscription that should catch up existing events and then block
    let mut response = client
        .read(None, None, None, true)
        .expect("subscription read response");

    // Collect exactly the initial events from the subscription without blocking afterwards
    let mut collected_initial = Vec::new();
    while collected_initial.len() < initial_count {
        if let Some(ev) = response.next() {
            println!("subscription received already recorded event: {ev:?}");
            collected_initial.push(ev);
        } else {
            panic!("subscription ended unexpectedly while catching up initial events");
        }
    }
    assert_eq!(initial_count, collected_initial.len(), "should receive all initially written events via subscription");
    assert!(collected_initial.iter().all(|e| e.event.tags.iter().any(|t| t == "grpc-sub")), "all initial events should have grpc-sub tag");

    // Append more events after the subscription has caught up
    println!("appending new events");
    let new_count = 25usize;
    let new_events: Vec<DCBEvent> = (0..new_count as u64)
        .map(|i| DCBEvent {
            event_type: "SubTestEvent2".to_string(),
            data: format!("new-{i}").into_bytes(),
            tags: vec!["grpc-sub".to_string()],
        })
        .collect();
    let _ = client.append(new_events, None).expect("append new events during subscription");

    // Continue iterating the subscription to receive the newly appended events
    let mut collected_new = Vec::new();
    while collected_new.len() < new_count {
        if let Some(ev) = response.next() {
            println!("subscription received subsequent event: {ev:?}");

            collected_new.push(ev);
        } else {
            panic!("subscription ended unexpectedly before receiving newly appended events");
        }
    }
    assert_eq!(new_count, collected_new.len(), "should receive all newly appended events via subscription");
    assert!(collected_new.iter().all(|e| e.event.tags.iter().any(|t| t == "grpc-sub")), "all new events should have grpc-sub tag");

    // Cleanup: shutdown server (do not drain subscription further; server will be stopped)
    let _ = shutdown_tx.send(());
    thread::sleep(Duration::from_millis(100));
    let _ = server_handle.join();
}
