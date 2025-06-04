# EventStore gRPC Server CLI Documentation

## Overview

The EventStore CLI provides a command-line interface for running a gRPC server that manages an event store. The event store is a persistent storage system for events, supporting operations like appending events and querying events with various filters.

## Installation

Ensure you have Rust and Cargo installed on your system. You also need the Protocol Buffers compiler (`protoc`) installed:

- **macOS**: `brew install protobuf`
- **Linux**: `apt-get install protobuf-compiler` or equivalent for your distribution
- **Windows**: Download from [Protocol Buffers Releases](https://github.com/protocolbuffers/protobuf/releases)

Then build the project:

```bash
cargo build --release
```

## Usage

```
dcbsd <data_dir> <server_addr>
```

### Arguments

- `<data_dir>`: Directory where event store data will be stored
  - This directory will be created if it doesn't exist
  - All event data will be persisted in this location
  - Example: `./data`

- `<server_addr>`: Address and port where the gRPC server will listen
  - Format: `<ip_address>:<port>`
  - Example: `127.0.0.1:50051`

### Examples

Start the server with data stored in the `./data` directory and listening on localhost port 50051:

```bash
dcbsd ./data 127.0.0.1:50051
```

Start the server with data stored in a custom directory and listening on all interfaces:

```bash
dcbsd /path/to/event/data 0.0.0.0:8080
```

## Server Operation

Once started, the server will:

1. Initialize the event store in the specified data directory
2. Start listening for gRPC connections on the specified address
3. Process client requests for appending and reading events
4. Continue running until terminated with Ctrl+C

### Data Storage

The EventStore uses a simple but robust storage format:

- Events are stored in a Write-Ahead Log (WAL) file named `eventstore.wal`
- Each event record in the WAL contains:
  - Length of the serialized event (4 bytes)
  - Serialized event data (variable length)
  - CRC32 checksum (4 bytes) for data integrity
- The server maintains event positions as 1-based sequential numbers
- The data directory is created if it doesn't exist when the server starts
- The WAL file is recreated on each server start (no persistence between restarts in the current version)

### Performance Considerations

- The current implementation loads all events into memory when querying
- For production use with large event volumes, consider:
  - Limiting query results with the `limit` parameter
  - Using more specific queries to reduce the number of events processed
  - Running the server on hardware with sufficient memory for your event volume

## Client Interaction

Clients can interact with the server using gRPC. The server implements the following operations:

- `Read`: Query events with optional filters, pagination, and limits
- `Append`: Add new events to the store with optional consistency conditions

### EventStore API

The EventStore provides a simple yet powerful API for event sourcing:

#### Events

Events are the core data structure and consist of:
- `event_type`: A string identifying the type of event
- `tags`: A list of strings for categorizing and filtering events
- `data`: Binary data containing the event payload

#### Reading Events

The `Read` operation supports:
- Filtering events by type and tags
- Pagination using the `after` parameter
- Limiting the number of results
- Complex queries with multiple conditions (AND/OR)

#### Appending Events

The `Append` operation supports:
- Adding multiple events in a single transaction
- Conditional appends using the `AppendCondition` to ensure consistency
- Optimistic concurrency control using the `after` parameter

#### Example Client Code

Here's an example of how to use the Rust client to interact with the EventStore server:

```
// Connect to the gRPC server
let client = EventStoreClient::connect("http://127.0.0.1:50051").await?;

// Read events with a filter
let query = Query {
    items: vec![QueryItem {
        types: vec!["UserRegistered".to_string()],
        tags: vec!["user123".to_string()],
    }],
};
let (events, head) = client.read(Some(query), None, None).await?;

// Append a new event
let event = Event {
    event_type: "UserUpdated".to_string(),
    tags: vec!["user123".to_string()],
    data: b"{\"name\":\"John Doe\"}".to_vec(),
};
let position = client.append(vec![event], None).await?;
```

## Error Handling

The server will display error messages if:
- The data directory cannot be accessed or created
- The server address is invalid or cannot be bound
- The gRPC server encounters runtime errors

## Logging

The server logs basic information to the console, including:
- Server startup confirmation
- Data directory location
- Server address and port
- Any errors encountered during operation

## Stopping the Server

To stop the server, press Ctrl+C in the terminal where it's running.

## Troubleshooting

If you encounter issues:

1. Ensure the data directory is writable
2. Verify the server address is not already in use
3. Check that the protoc compiler is correctly installed
4. Ensure all dependencies are properly installed with `cargo build`

## Security Considerations

The current implementation has several security considerations to be aware of:

- **No Authentication**: The gRPC server does not implement authentication. In production, consider:
  - Running behind a reverse proxy that handles authentication
  - Deploying in a secure network environment
  - Implementing TLS (see below)

- **No Encryption**: Communication is not encrypted by default. For sensitive data:
  - Configure TLS for the gRPC server
  - Use network-level encryption (VPN, etc.)

- **No Authorization**: All clients have full access to all events. Consider:
  - Implementing application-level authorization
  - Segregating different event types into different server instances

- **Data Persistence**: The WAL file contains all events in serialized form:
  - Ensure proper file system permissions on the data directory
  - Consider encrypting the data directory for sensitive information
  - Implement backup procedures appropriate for your data sensitivity

For production deployments, it's recommended to address these security considerations based on your specific requirements and threat model.
