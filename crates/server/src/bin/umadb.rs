use clap::Parser;
use tokio::signal;
use tokio::sync::oneshot;
use umadb_server::{start_server, start_server_secure_from_files};

#[derive(Parser, Debug)]
#[command(about = "UmaDB gRPC server", version)]
struct Args {
    /// Listen address, e.g. 127.0.0.1:50051
    #[arg(long = "listen")]
    listen: String,

    /// Path to database file or folder
    #[arg(long = "db-path")]
    db_path: String,

    /// Optional TLS server certificate (PEM)
    #[arg(long = "tls-cert", required = false)]
    cert: Option<String>,

    /// Optional TLS server private key (PEM)
    #[arg(long = "tls-key", required = false)]
    key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Prepare shutdown trigger
    let (tx, rx) = oneshot::channel::<()>();

    // Spawn a task that waits for Ctrl-C and triggers shutdown
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        let _ = tx.send(());
    });

    match (args.cert, args.key) {
        (Some(cert), Some(key)) => {
            start_server_secure_from_files(args.db_path, &args.listen, rx, cert, key).await?
        }
        (None, None) => start_server(args.db_path, &args.listen, rx).await?,
        _ => {
            eprintln!("Both --cert and --key must be provided for TLS");
            std::process::exit(2);
        }
    }

    Ok(())
}
