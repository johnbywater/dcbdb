use criterion::{black_box, criterion_group, criterion_main,  BenchmarkId, Criterion, Throughput};
use umadb::common::{PageID, Position, Tsn};
use umadb::header_node::HeaderNode;

// Build the sample header once, outside of the measured benchmark closures
static HEADER: HeaderNode = HeaderNode {
    tsn: Tsn(42),
    next_page_id: PageID(123),
    free_lists_tree_root_id: PageID(456),
    events_tree_root_id: PageID(789),
    tags_tree_root_id: PageID(321),
    next_position: Position(9876543210),
};

pub fn header_node_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("header_node");

    // Known constant sizes for header serialization
    let header_size_bytes: u64 = 48;
    group.throughput(Throughput::Bytes(header_size_bytes));

    // Benchmark serialization (alloc + encode)
    group.bench_function(BenchmarkId::new("serialize", header_size_bytes), |b| {
        b.iter(|| {
            let bytes = black_box(&HEADER).serialize();
            black_box(bytes)
        })
    });

    // Allocation-only: separate the Vec allocation cost
    group.bench_function(BenchmarkId::new("alloc_only", header_size_bytes), |b| {
        b.iter(|| {
            let v = black_box(Vec::<u8>::with_capacity(48));
            black_box(v)
        })
    });

    // Allocation-free serialization into a fixed stack buffer
    group.bench_function(BenchmarkId::new("serialize_into_stack", header_size_bytes), |b| {
        let mut buf = [0u8; 48];
        b.iter(|| {
            black_box(&HEADER).serialize_into(black_box(&mut buf));
            black_box(&buf);
        })
    });

    // Prepare serialized bytes once for deserialization benchmark (outside iter)
    let serialized = HEADER.serialize();

    // Benchmark deserialization reusing the same bytes each iteration (pure from_slice; no cloning/allocation)
    group.bench_function(BenchmarkId::new("deserialize_reuse", header_size_bytes), |b| {
        b.iter(|| {
            let node = HeaderNode::from_slice(black_box(&serialized)).expect("valid header bytes");
            black_box(node)
        })
    });

    // Benchmark deserialization immediately after a fresh serialize (setup does serialize; measure only from_slice)
    group.bench_function(BenchmarkId::new("deserialize_after_serialize", header_size_bytes), |b| {
        b.iter_batched(
            || HEADER.serialize(),
            |bytes| {
                let node = HeaderNode::from_slice(black_box(&bytes)).expect("valid header bytes");
                black_box(node)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Allocation-free round trip using a reusable stack buffer
    group.bench_function(BenchmarkId::new("round_trip_no_alloc", header_size_bytes), |b| {
        let mut buf = [0u8; 48];
        b.iter(|| {
            let header = black_box(&HEADER);
            header.serialize_into(black_box(&mut buf));
            let node = HeaderNode::from_slice(black_box(&buf)).unwrap();
            black_box(node)
        })
    });

    // Benchmark serialize + deserialize round trip (alloc + encode + decode)
    group.bench_function(BenchmarkId::new("round_trip", header_size_bytes), |b| {
        b.iter(|| {
            let bytes = black_box(&HEADER).serialize();
            // Black-box the bytes to prevent the compiler from fusing serialize+deserialize
            let node = HeaderNode::from_slice(black_box(&bytes)).unwrap();
            black_box(node)
        })
    });

    group.finish();
}

criterion_group!(benches, header_node_benchmarks);
criterion_main!(benches);
