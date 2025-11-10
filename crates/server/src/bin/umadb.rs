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

    /// Optional TLS server certificate (PEM). Can also be set via UMADB_TLS_CERT environment variable
    #[arg(long = "tls-cert", required = false)]
    cert: Option<String>,

    /// Optional TLS server private key (PEM). Can also be set via UMADB_TLS_KEY environment variable
    #[arg(long = "tls-key", required = false)]
    key: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build a custom runtime with increased blocking thread pool for high concurrency
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        )
        .max_blocking_threads(2048)
        .enable_all()
        .build()?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Read TLS cert and key from environment variables if not provided via CLI
    let cert = args.cert.or_else(|| std::env::var("UMADB_TLS_CERT").ok());
    let key = args.key.or_else(|| std::env::var("UMADB_TLS_KEY").ok());

    // Prepare shutdown trigger
    let (tx, rx) = oneshot::channel::<()>();

    // Spawn a task that waits for Ctrl-C and triggers shutdown
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        let _ = tx.send(());
    });

    match (cert, key) {
        (Some(cert), Some(key)) => {
            start_server_secure_from_files(args.db_path, &args.listen, rx, cert, key).await?
        }
        (None, None) => start_server(args.db_path, &args.listen, rx).await?,
        _ => {
            eprintln!(
                "Both --tls-cert and --tls-key (or UMADB_TLS_CERT and UMADB_TLS_KEY) must be provided for TLS"
            );
            std::process::exit(2);
        }
    }

    Ok(())
}
