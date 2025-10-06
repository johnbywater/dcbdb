use clap::Parser;
use dcbdb::dcbapi::{DCBEvent, DCBEventStore};
use dcbdb::event_store::EventStore;

/// Utility to generate a database with 1,000,000 events.
/// Each event has:
/// - a unique tag: tag-0 .. tag-999999
/// - a type cycling through: type-0 .. type-9
/// - data equal to a constant-length zero-padded representation of the tag: "tag-XXXXXX"
#[derive(Parser, Debug)]
#[command(author, version, about = "Generate a DCBDB with 1,000,000 events", long_about = None)]
struct Args {
    /// Path to the event store directory or file.
    /// If a directory is provided, a file named "dcb.db" will be created inside it.
    #[arg(short, long, value_name = "PATH")]
    path: String,

    /// Batch size for appends (tuning knob for performance).
    #[arg(short = 'b', long, value_name = "N", default_value_t = 10_000)]
    batch_size: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!(
        "Generating database at '{}' with 1,000,000 events (batch size = {})",
        args.path, args.batch_size
    );

    let store = EventStore::new(&args.path)?; // creates/opens the store

    // Constants
    const TOTAL: usize = 1_000_000;
    const PAD_WIDTH: usize = 6; // zero-padded width for 0..999999

    let mut next_index: usize = 0;
    let mut last_pos: u64 = 0;

    while next_index < TOTAL {
        let remaining = TOTAL - next_index;
        let take = remaining.min(args.batch_size);

        let mut batch: Vec<DCBEvent> = Vec::with_capacity(take);
        for i in next_index..next_index + take {
            let tag_str = format!("tag-{}", i);
            // Data is the constant-length zero-padded representation of the tag.
            // We interpret this as the tag string with the numeric part zero-padded to 6 digits.
            // Example: for i=42 => data = b"tag-000042".
            let data_str = format!("tag-{i:0PAD_WIDTH$}", PAD_WIDTH = PAD_WIDTH);
            let event = DCBEvent {
                event_type: format!("type-{}", i % 10),
                data: data_str.into_bytes(),
                tags: vec![tag_str],
            };
            batch.push(event);
        }

        last_pos = store.append(batch, None)?;
        next_index += take;

        if next_index % 100_000 == 0 || next_index == TOTAL {
            println!("Progress: {}/{} events appended (last position = {})", next_index, TOTAL, last_pos);
        }
    }

    println!(
        "Completed. Appended {} events. Final position = {}",
        TOTAL, last_pos
    );

    Ok(())
}
