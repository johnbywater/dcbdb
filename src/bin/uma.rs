use clap::Parser;
use std::path::PathBuf;
use tokio::signal;
use umadb::grpc;

#[derive(Parser)]
#[command(author, version, about = "UmaDB gRPC Server", long_about = None)]
struct Args {
    /// Path to the database directory
    #[arg(short, long, value_name = "PATH")]
    path: PathBuf,

    /// Address to listen on (e.g., "127.0.0.1:50051")
    #[arg(short, long, value_name = "ADDR", default_value = "127.0.0.1:50051")]
    address: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // console_subscriber::init();
    let args = Args::parse();

    println!("Starting gRPC server for database at {:?}", args.path);
    println!("Listening on {}", args.address);
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    println!(
        "Tokio runtime flavor: multi_thread; worker threads (default) = {}",
        workers
    );
    println!("Press Ctrl+C to shutdown gracefully");

    // Create a shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Set up a signal handler for SIGINT (Ctrl+C)
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("Received Ctrl+C, initiating graceful shutdown...");
        shutdown_tx
            .send(())
            .expect("Failed to send shutdown signal");
    });

    // Start the gRPC server with shutdown capability
    grpc::start_server(args.path, &args.address, shutdown_rx).await?;

    println!("Server has been gracefully shut down");
    Ok(())
}
