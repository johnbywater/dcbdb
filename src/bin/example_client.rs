use std::cmp::max;
use clap::Parser;
use dcbdb::dcbapi::{DCBEvent, DCBEventStore};
// use dcbdb::dcbapi::{DCBEvent, DCBEventStore, DCBQuery, DCBQueryItem};
use dcbdb::grpc::GrpcEventStoreClient;
use std::time::Instant;

#[derive(Parser)]
#[command(author, version, about = "DCBDB Example Client", long_about = None)]
struct Args {
    /// Address of the gRPC server (e.g., "http://127.0.0.1:50051")
    #[arg(
        short,
        long,
        value_name = "ADDR",
        default_value = "http://127.0.0.1:50051"
    )]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Connecting to gRPC server at {}...", args.address);

    // Connect to the gRPC server
    let t_connect = Instant::now();
    let client = GrpcEventStoreClient::connect(args.address).await?;
    let connect_elapsed = t_connect.elapsed();
    println!("Connected client to server");

    // Append an event
    let event = DCBEvent {
        event_type: "example".to_string(),
        tags: vec!["tag1".to_string(), "tag2".to_string()],
        data: b"Hello, world!".to_vec(),
    };

    println!("Appending event...");
    let events_to_append = vec![event.clone(), event.clone(), event.clone(), event.clone(), event.clone()];
    let events_len = events_to_append.len();
    let t_append = Instant::now();
    let position = client.append(events_to_append, None)?;
    let append_elapsed = t_append.elapsed();
    let append_eps = if append_elapsed.as_secs_f64() > 0.0 {
        events_len as f64 / append_elapsed.as_secs_f64()
    } else {
        f64::INFINITY
    };
    println!("Appended {events_len} events, last position: {position}");

    // Read events
    // let query = DCBQuery {
    //     items: vec![DCBQueryItem {
    //         types: vec!["example".to_string()],
    //         tags: vec!["tag1".to_string()],
    //     }],
    // };

    let tail: usize = 1000;
    println!("Reading last {tail} events...");
    let t_read = Instant::now();
    let read_after_position = max(client.head().unwrap().unwrap().saturating_sub(tail as u64), 0);
    let response = client.read(None, Some(read_after_position), Some(tail))?;

    // Iterate through the events
    // println!("Events:");
    let mut ev_count = 0usize;
    let mut total_bytes = 0usize;
    for event in response {
        ev_count += 1;
        total_bytes += event.event.data.len();
        // println!(
        //     "  Position {}: Type={}, Tags={:?}, Data={:?}",
        //     event.position,
        //     event.event.event_type,
        //     event.event.tags,
        //     String::from_utf8_lossy(&event.event.data)
        // );
    }
    let read_elapsed = t_read.elapsed();
    let read_eps = if read_elapsed.as_secs_f64() > 0.0 {
        ev_count as f64 / read_elapsed.as_secs_f64()
    } else {
        f64::INFINITY
    };
    println!("Read {ev_count} events, {total_bytes} bytes");

    println!("--- Timing summary ---");
    println!("Connect: {:?}", connect_elapsed);
    println!("Append {events_len}: {:?} ({:.2} events/s)", append_elapsed, append_eps);
    println!("Read {ev_count}:   {:?} ({:.2} events/s)", read_elapsed, read_eps);
    Ok(())
}
