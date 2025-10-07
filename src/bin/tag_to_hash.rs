fn main() {
    let mut args = std::env::args().skip(1);
    let tag = match (args.next(), args.next()) {
        (Some(tag), None) => tag,
        _ => {
            eprintln!("Usage: tag_to_hash <TAG>");
            std::process::exit(1);
        }
    };

    let hash = dcbdb::event_store::tag_to_hash(&tag);
    println!("{:?}", hash);
}
