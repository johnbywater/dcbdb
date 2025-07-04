use std::path::PathBuf;
use clap::Parser;
use dcbsd::grpc;

#[derive(Parser)]
#[command(author, version, about = "DCBSD gRPC Server", long_about = None)]
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
    
    grpc::start_grpc_server(args.path, &args.address).await?;
    
    Ok(())
}