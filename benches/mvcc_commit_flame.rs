use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::tempdir;
use umadb::bench_api::BenchDb;

pub fn mvcc_commit_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_commit_flame");

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

    // Additionally, generate flamegraphs explicitly using pprof so SVGs are guaranteed.
    generate_flamegraphs(&db, page_size).expect("failed to generate flamegraphs");
}

use pprof::ProfilerGuard;
use std::fs::File;
use std::time::{Duration, Instant};

fn profile_to_svg<F>(name: &str, mut work: F) -> std::io::Result<()>
where
    F: FnMut(),
{
    let guard = ProfilerGuard::new(100).expect("ProfilerGuard");

    // Run the workload for a short, fixed duration to collect samples
    let deadline = Instant::now() + Duration::from_millis(300);
    while Instant::now() < deadline {
        work();
    }

    // Build and write the flamegraph
    if let Ok(report) = guard.report().build() {
        let out_dir = std::path::Path::new("target/flamegraphs");
        let _ = std::fs::create_dir_all(out_dir);
        let path = out_dir.join(format!("{}.svg", name));
        let mut opts = pprof::flamegraph::Options::default();
        let file = File::create(&path)?;
        report
            .flamegraph_with_options(file, &mut opts)
            .expect("write flamegraph");
    }

    Ok(())
}

fn generate_flamegraphs(db: &BenchDb, page_size: usize) -> std::io::Result<()> {
    // commit_empty
    profile_to_svg(&format!("mvcc_commit_empty_{}", page_size), || {
        db.commit_empty().expect("commit_empty");
    })?;

    // commit_with_dirty for N in {1,10,100}
    for &n in &[1usize, 10, 100] {
        profile_to_svg(&format!("mvcc_commit_with_dirty_{}", n), || {
            db.commit_with_dirty(n).expect("commit_with_dirty");
        })?;
    }

    Ok(())
}

// Configure Criterion to use pprof to emit Flamegraph SVGs at a predictable location.
// use pprof::flamegraph::Options as FlameOptions;
use std::path::PathBuf;

fn flame_config() -> Criterion {
    // Ensure the output directory exists and ask pprof to place flamegraphs there, if supported.
    let out_dir = PathBuf::from("target/flamegraphs");
    let _ = std::fs::create_dir_all(&out_dir);

    // TODO: Can't do this because at time of writing, pprof 0.15 still targets Criterion 0.5.x. (not 0.7.0)
    // // Configure default flamegraph options; if the pprof backend honors output paths,
    // // it will emit into Criterion’s dir unless otherwise specified here.
    // Criterion::default().with_profiler(PProfProfiler::new(
    //     100,
    //     Output::Flamegraph(Some(FlameOptions::default())),
    // ))
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = flame_config();
    targets = mvcc_commit_benchmarks
}

criterion_main!(benches);
