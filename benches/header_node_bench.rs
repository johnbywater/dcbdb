use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use umadb::header_node::HeaderNode;
use umadb::common::{PageID, Position, Tsn};

fn build_sample_header() -> HeaderNode {
    HeaderNode {
        tsn: Tsn(42),
        next_page_id: PageID(123),
        free_lists_tree_root_id: PageID(456),
        events_tree_root_id: PageID(789),
        tags_tree_root_id: PageID(321),
        next_position: Position(9876543210),
    }
}

pub fn header_node_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("header_node");

    // Known constant sizes for header serialization
    let header_size_bytes: u64 = 48;
    group.throughput(Throughput::Bytes(header_size_bytes));

    // Benchmark serialization
    group.bench_function(BenchmarkId::new("serialize", header_size_bytes), |b| {
        let header = build_sample_header();
        b.iter(|| {
            let bytes = header.serialize();
            black_box(bytes)
        })
    });

    // Prepare serialized bytes once for deserialization benchmark
    let serialized = build_sample_header().serialize();

    // Benchmark deserialization
    group.bench_function(BenchmarkId::new("deserialize", header_size_bytes), |b| {
        b.iter_batched(
            || serialized.clone(),
            |bytes| {
                let node = HeaderNode::from_slice(black_box(&bytes)).expect("valid header bytes");
                black_box(node)
            },
            BatchSize::SmallInput,
        )
    });

    // Benchmark serialize + deserialize round trip
    group.bench_function(BenchmarkId::new("round_trip", header_size_bytes), |b| {
        let header = build_sample_header();
        b.iter(|| {
            let bytes = header.serialize();
            let node = HeaderNode::from_slice(&bytes).unwrap();
            black_box(node)
        })
    });

    group.finish();
}

criterion_group!(benches, header_node_benchmarks);
criterion_main!(benches);
