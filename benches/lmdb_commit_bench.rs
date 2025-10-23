use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::tempdir;
use umadb::bench_api::BenchDb;

pub fn lmdb_commit_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("lmdb_commit");

    // Create a temporary database once per benchmark group
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("umadb.commit.bench");
    let page_size = 4096usize;
    let db = BenchDb::new(&db_path, page_size).expect("BenchDb::new");

    // Empty commit (no dirty pages)
    group.bench_function(BenchmarkId::new("commit_empty", page_size), |b| {
        b.iter(|| {
            db.commit_empty().expect("commit_empty");
        })
    });

    // Commit with varying numbers of dirty pages
    for &n in &[1usize, 10, 100] {
        group.bench_function(BenchmarkId::new("commit_with_dirty", n), |b| {
            b.iter(|| {
                db.commit_with_dirty(n).expect("commit_with_dirty");
            })
        });
    }

    group.finish();
}

criterion_group!(benches, lmdb_commit_benchmarks);
criterion_main!(benches);
