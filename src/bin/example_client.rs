use dcbsd::api::{DCBEvent, DCBEventStoreAPI, DCBQuery, DCBQueryItem};
use dcbsd::grpc::GrpcEventStoreClient;
use clap::Parser;

#[derive(Parser)]
#[command(author, version, about = "DCBSD Example Client", long_about = None)]
struct Args {
    /// Address of the gRPC server (e.g., "http://127.0.0.1:50051")
    #[arg(short, long, value_name = "ADDR", default_value = "http://127.0.0.1:50051")]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    println!("Connecting to gRPC server at {}", args.address);
    
    // Connect to the gRPC server
    let client = GrpcEventStoreClient::connect(args.address).await?;
    println!("Connected successfully!");

    // Append an event
    let event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
    };
    
    println!("Appending event...");
    let position = client.append(vec![event], None)?;
    println!("Appended event at position: {}", position);

    // Read events
    let query = DCBQuery {
        items: vec![DCBQueryItem {
            types: vec!["example".to_string()],
            tags: vec!["tag1".to_string()],
        }],
    };
    
    println!("Reading events...");
    let response = client.read(Some(query), None, None)?;
    
    // Iterate through the events
    println!("Events:");
    for event in response {
        println!("  Position {}: Type={}, Tags={:?}, Data={:?}", 
            event.position, 
            event.event.event_type, 
            event.event.tags, 
            String::from_utf8_lossy(&event.event.data));
    }

    println!("Example completed successfully!");
    Ok(())
}