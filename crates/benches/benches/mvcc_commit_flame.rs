// cargo bench --bench mvcc_commit_flame --features flamegraphs

fn main() -> std::io::Result<()> {
    use pprof::ProfilerGuard;
    use std::fs::File;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;
    use umadb_benches::bench_api::BenchDb;

    fn fresh_db(page_size: usize) -> BenchDb {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("umadb.commit.flame");
        BenchDb::new(&db_path, page_size).expect("BenchDb::new")
    }

    fn profile_to_svg<F>(name: &str, mut work: F) -> std::io::Result<()>
    where
        F: FnMut(),
    {
        let guard = ProfilerGuard::new(100).expect("ProfilerGuard");
        let deadline = Instant::now() + Duration::from_millis(300);
        while Instant::now() < deadline {
            work();
        }

        if let Ok(report) = guard.report().build() {
            let out_dir = std::path::Path::new("../../target/flamegraphs");
            std::fs::create_dir_all(out_dir)?;
            let path = out_dir.join(format!("{name}.svg"));
            let mut opts = pprof::flamegraph::Options::default();
            let file = File::create(&path)?;
            report
                .flamegraph_with_options(file, &mut opts)
                .expect("write flamegraph");
            println!("✅ Wrote {}", path.canonicalize()?.display());
        }
        Ok(())
    }

    fn generate_flamegraphs() -> std::io::Result<()> {
        let page_size = 4096usize;
        let db = fresh_db(page_size);
        let mut w = db.writer();

        profile_to_svg(&format!("mvcc_commit_empty_{page_size}"), || {
            db.commit_empty().expect("commit_empty");
        })?;

        for &n in &[1usize, 10, 100] {
            db.insert_dirty_pages(&mut w, n).unwrap();
            profile_to_svg(&format!("mvcc_commit_with_dirty_{n}"), || {
                db.commit_with_dirty(&mut w).unwrap();
            })?;
        }

        Ok(())
    }

    generate_flamegraphs()
}
