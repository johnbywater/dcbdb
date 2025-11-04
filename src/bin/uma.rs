use clap::Parser;
use std::path::PathBuf;
use tokio::signal;
use tokio::sync::oneshot;

use umadb::grpc::{start_server, start_server_secure_from_files};

/// UmaDB server binary
#[derive(Debug, Parser)]
#[command(name = "uma", about = "Run UmaDB gRPC server (with optional TLS)", version)]
struct Cli {
    /// Database directory or file path. If a directory is provided, uma.db file will be created inside it
    #[arg(long, short, value_name = "PATH")]
    path: PathBuf,

    /// Address to listen on, e.g. 0.0.0.0:50051
    #[arg(long, short, default_value = "0.0.0.0:50051", value_name = "ADDR")]
    addr: String,

    /// Path to PEM-encoded server certificate chain (enable TLS when used with --key)
    #[arg(long, short, value_name = "FILE")]
    cert: Option<PathBuf>,

    /// Path to PEM-encoded private key (enable TLS when used with --cert)
    #[arg(long, short, value_name = "FILE")]
    key: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Prepare shutdown trigger
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    // Spawn a task that waits for Ctrl-C and triggers shutdown
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        let _ = shutdown_tx.send(());
    });

    let db_path = cli.path;
    let addr = cli.addr;

    match (cli.cert, cli.key) {
        (Some(cert), Some(key)) => {
            start_server_secure_from_files(db_path, &addr, shutdown_rx, cert, key).await?
        }
        (None, None) => {
            start_server(db_path, &addr, shutdown_rx).await?
        }
        _ => {
            eprintln!("Error: --cert and --key must be provided together");
            std::process::exit(2);
        }
    }

    Ok(())
}
