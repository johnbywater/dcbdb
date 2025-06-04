use tempfile::tempdir;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use dcbsd::{Event, EventStore, EventStoreApi};
use dcbsd::server::{EventStoreServer, proto::event_store_service_server::EventStoreServiceServer};
use dcbsd::client::{EventStoreClient, EventStoreClientSync};

// Import the run_event_store_test function from the event_store_test module
mod event_store_test;
use event_store_test::run_event_store_test;

#[test]
fn test_grpc_event_store() {
    let temp_dir = tempdir().unwrap();
    let event_store = EventStore::open(temp_dir.path()).unwrap();
    let event_store_server = EventStoreServer::with_event_store(Arc::new(event_store));

    // Create a runtime for the gRPC server
    let rt = Runtime::new().unwrap();

    // Start the gRPC server
    let server_addr = "127.0.0.1:50051".parse::<SocketAddr>().unwrap();
    let server_future = tonic::transport::Server::builder()
        .add_service(event_store_server.into_service())
        .serve(server_addr);

    // Spawn the server in the background
    rt.spawn(server_future);

    // Connect to the server
    let client = rt.block_on(async {
        EventStoreClient::connect("http://127.0.0.1:50051").await.unwrap()
    });

    let client_sync = EventStoreClientSync::new(client);

    // Run the test
    run_event_store_test(&client_sync);
}
