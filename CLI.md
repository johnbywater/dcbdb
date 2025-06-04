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

## Client Interaction

Clients can interact with the server using gRPC. The server implements the following operations:

- `Read`: Query events with optional filters, pagination, and limits
- `Append`: Add new events to the store with optional consistency conditions

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