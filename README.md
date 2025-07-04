# DCBDB - Event Store for Dynamic Consistency Boundaries

DCBDB is an event store designed for dynamic consistency boundaries with a gRPC interface. It provides a robust foundation for event-driven architectures where consistency boundaries may shift based on business requirements.

## Quick Start

1. Build the project:
   ```bash
   cargo build --release
   ```

2. Start the gRPC server:
   ```bash
   cargo run --bin grpc_server -- --path /path/to/event-store
   ```

3. In another terminal, run the example client:
   ```bash
   cargo run --bin example_client
   ```

This will start a server, connect to it with the example client, append an event, and read it back.

## Building the Project

To build the project, you need to have Rust and Cargo installed. If you don't have them installed, you can get them from [rustup.rs](https://rustup.rs/).

Once you have Rust and Cargo installed, you can build the project with:

```bash
cargo build --release
```

This will create the executable in `target/release/`.

## Running the gRPC Server

The gRPC server can be started using the `grpc_server` binary. You can run it directly after building:

```bash
./target/release/grpc_server --path /path/to/event-store --address 127.0.0.1:50051
```

Or you can use `cargo run`:

```bash
cargo run --bin grpc_server -- --path /path/to/event-store --address 127.0.0.1:50051
```

### Command-line Options

The gRPC server accepts the following command-line options:

- `-p, --path <PATH>`: Path to the event store directory (required)
- `-a, --address <ADDR>`: Address to listen on (default: "127.0.0.1:50051")
- `-h, --help`: Print help information
- `-V, --version`: Print version information

## Interacting with the gRPC Server

You can interact with the gRPC server using any gRPC client. The server implements the following methods:

- `Read`: Read events from the event store
- `Append`: Append events to the event store

### Using the Example Client

DCBDB includes an example client that you can use to interact with the gRPC server. You can run it with:

```bash
cargo run --bin example_client -- --address http://127.0.0.1:50051
```

The example client:
1. Connects to the gRPC server
2. Appends an event to the event store
3. Reads events from the event store using a query
4. Displays the events that were read

### Using the Rust Client in Your Own Code

DCBDB provides a Rust client that you can use to interact with the gRPC server in your own code. Here's an example of how to use it:

```rust
use dcbdb::api::{DCBEvent, DCBEventStoreAPI, DCBQuery, DCBQueryItem};
use dcbdb::grpc::GrpcEventStoreClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the gRPC server
    let client = GrpcEventStoreClient::connect("http://127.0.0.1:50051").await?;

    // Append an event
    let event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
    };
    let position = client.append(vec![event], None)?;
    println!("Appended event at position: {}", position);

    // Read events
    let query = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["example".to_string()],
            tags: vec!["tag1".to_string()],
        }],
    };
    let response = client.read(Some(query), None, None)?;

    // Iterate through the events
    for event in response {
        println!("Event at position {}: {:?}", event.position, event.event);
    }

    Ok(())
}
```

### Using Other gRPC Clients

You can also use other gRPC clients to interact with the server. The protocol definition is in the `proto/event_store.proto` file.

## Additional Information

- The event store is stored in the directory specified by the `--path` option.
- The server listens on the address specified by the `--address` option.
- The server uses the gRPC protocol for communication.
- The server is implemented in Rust and uses the Tokio runtime for asynchronous I/O.
