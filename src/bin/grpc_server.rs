use std::path::PathBuf;
use clap::Parser;
use dcbdb::grpc;
use tokio::signal;

#[derive(Parser)]
#[command(author, version, about = "DCBDB gRPC Server", long_about = None)]
struct Args {
    /// Path to the database directory
    #[arg(short, long, value_name = "PATH")]
    path: PathBuf,

    /// Address to listen on (e.g., "127.0.0.1:50051")
    #[arg(short, long, value_name = "ADDR", default_value = "127.0.0.1:50051")]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Starting gRPC server for database at {:?}", args.path);
    println!("Listening on {}", args.address);
    println!("Press Ctrl+C to shutdown gracefully");

    // Create a shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Set up a signal handler for SIGINT (Ctrl+C)
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("Received Ctrl+C, initiating graceful shutdown...");
        shutdown_tx.send(()).expect("Failed to send shutdown signal");
    });

    // Start the gRPC server with shutdown capability
    grpc::start_grpc_server_with_shutdown(args.path, &args.address, shutdown_rx).await?;

    println!("Server has been gracefully shut down");
    Ok(())
}
