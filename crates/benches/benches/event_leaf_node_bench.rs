use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use umadb_benches::bench_api::{BenchEventLeafInline, BenchEventLeafOverflow};

pub fn event_leaf_node_benchmark(c: &mut Criterion) {
    // Inline values: small payloads fully in the leaf
    let mut inline = c.benchmark_group("event_leaf_inline");
    inline.sample_size(100);

    // for &keys in &[1usize, 8, 64, 256, 1024, 4096] {
    // for &keys in &[1usize, 8, 64, 256] {
    for &keys in &[64] {
        // 64 bytes payload, 2 tags per value
        let mut bench = BenchEventLeafInline::new(keys, 64, 2);
        // Ensure buffer populated once for the deserialize-only bench
        let _ = bench.serialize();

        inline.throughput(Throughput::Elements(keys as u64));

        inline.bench_function(BenchmarkId::new("serialize", keys), |b| {
            b.iter(|| {
                // measure just serialization into pre-allocated buffer
                black_box(bench.serialize());
            })
        });

        inline.bench_function(BenchmarkId::new("deserialize", keys), |b| {
            b.iter(|| {
                // parse from the existing buffer
                let _ = black_box(bench.deserialize_check()).expect("deserialize ok");
            })
        });
    }
    inline.finish();

    // Overflow values: large payloads kept out-of-line; only metadata lives in the leaf
    let mut overflow = c.benchmark_group("event_leaf_overflow");
    overflow.sample_size(100);

    // for &keys in &[1usize, 8, 64, 256, 1024, 4096] {
    // for &keys in &[1usize, 8, 64, 256] {
    for &keys in &[64] {
        // 128KiB logical data_len, 3 tags per value
        let mut bench = BenchEventLeafOverflow::new(keys, 128 * 1024, 3);
        // serialize once to have a buffer for deserialize bench
        let _ = bench.serialize();

        overflow.throughput(Throughput::Elements(keys as u64));

        overflow.bench_function(BenchmarkId::new("serialize", keys), |b| {
            b.iter(|| {
                black_box(bench.serialize());
            })
        });

        overflow.bench_function(BenchmarkId::new("deserialize", keys), |b| {
            b.iter(|| {
                let _ = black_box(bench.deserialize_check()).expect("deserialize ok");
            })
        });
    }
    overflow.finish();
}

criterion_group!(benches, event_leaf_node_benchmark);
criterion_main!(benches);
