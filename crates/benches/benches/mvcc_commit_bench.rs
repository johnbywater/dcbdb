use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::cell::RefCell;
use tempfile::tempdir;
use umadb_benches::bench_api::BenchDb;

pub fn mvcc_commit_benchmarks(c: &mut Criterion) {
    let page_size = 4096usize;
    let mut group = c.benchmark_group("mvcc_commit");

    let db = fresh_db(page_size);

    // Benchmark: commit_empty
    group.bench_function(BenchmarkId::new("commit_empty", page_size), |b| {
        b.iter(|| {
            db.commit_empty().unwrap();
        })
    });

    // Setup once: persistent DB and writer reused across iterations
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("umadb.bench");
    let db = BenchDb::new(&db_path, page_size).unwrap();
    let writer = RefCell::new(db.writer());

    for _ in 0..100 {
        let mut w = writer.borrow_mut();
        db.insert_dirty_pages(&mut w, 100).unwrap();
        db.commit_with_dirty(&mut w).unwrap();
    }

    // Benchmark: commit_with_dirty for N in {1, 10, 100}
    for &n in &[1usize, 10, 100] {
        group.bench_function(BenchmarkId::new("commit_with_dirty_reuse_db", n), |b| {
            b.iter_batched_ref(
                || {
                    // Reset dirty pages before each commit
                    let mut w = writer.borrow_mut();
                    db.insert_dirty_pages(&mut w, n).unwrap();
                },
                |_| {
                    // Time only the commit
                    let mut w = writer.borrow_mut();
                    db.commit_with_dirty(&mut w).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Helper: create a fresh temporary BenchDb instance for each benchmark iteration.
fn fresh_db(page_size: usize) -> BenchDb {
    let dir = tempdir().expect("tempdir");
    let db_path = dir.path().join("umadb.commit.bench");
    BenchDb::new(&db_path, page_size).expect("BenchDb::new")
}

// =============================================
// ================ CRITERION CONFIG ===========
// =============================================

fn bench_config() -> Criterion {
    Criterion::default()
        .sample_size(200) // More samples for stability
        .warm_up_time(std::time::Duration::from_secs(2))
        .measurement_time(std::time::Duration::from_secs(5))
        .noise_threshold(0.05)
        .configure_from_args() // Allow CLI overrides (e.g., --sample-size)
}

criterion_group! {
    name = benches;
    config = bench_config();
    targets = mvcc_commit_benchmarks
}

criterion_main!(benches);
