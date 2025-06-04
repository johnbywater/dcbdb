use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

use dcbsd::EventStore;
use dcbsd::server::EventStoreServer;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("Usage: {} <data_dir> <server_addr>", args[0]);
        println!("Example: {} ./data 127.0.0.1:50051", args[0]);
        return;
    }

    let data_dir = PathBuf::from(&args[1]);
    let server_addr = &args[2];

    println!("Starting EventStore gRPC server");
    println!("Data directory: {}", data_dir.display());
    println!("Server address: {}", server_addr);

    // Create the event store
    let event_store = match EventStore::open(&data_dir) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Failed to open event store: {}", e);
            return;
        }
    };

    // Create the gRPC server
    let event_store_server = EventStoreServer::with_event_store(Arc::new(event_store));

    // Create a runtime for the gRPC server
    let rt = Runtime::new().unwrap();

    // Start the gRPC server
    let server_addr = match server_addr.parse() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Failed to parse server address: {}", e);
            return;
        }
    };

    let server_future = tonic::transport::Server::builder()
        .add_service(event_store_server.into_service())
        .serve(server_addr);

    println!("Server started. Press Ctrl+C to stop.");

    // Run the server
    rt.block_on(server_future).unwrap();
}
