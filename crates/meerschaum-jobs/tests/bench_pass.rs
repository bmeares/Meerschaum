//! Micro-benchmark (ignored by default): CPU time of one idle supervisor pass over 50 retained,
//! completed jobs (list, restart check, status / PID / result reads).
//!
//!     cargo test -p meerschaum-jobs --test bench_pass -- --ignored --nocapture
//!
//! Uses only API that exists before and after the process-scan change, so the same file runs
//! against either tree.
#![cfg(target_os = "linux")]

use meerschaum_jobs::JobsContext;
use meerschaum_jobs::executor::{check_restart_jobs, get_jobs};

fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &stat[stat.rfind(')').unwrap() + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // utime and stime are fields 14 and 15 (indices 11 and 12 after the command name).
    (fields[11].parse::<f64>().unwrap() + fields[12].parse::<f64>().unwrap()) / 100.0
}

#[test]
#[ignore = "benchmark: run with --ignored --nocapture"]
fn idle_pass_over_fifty_completed_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    for i in 0..50 {
        let jobdir = dir.path().join("jobs").join(format!("done-{i}"));
        std::fs::create_dir_all(&jobdir).unwrap();
        std::fs::write(
            jobdir.join("properties.json"),
            r#"{"label": "show version", "target": {"name": "entry", "module": "meerschaum._internal.entry", "args": [["show", "version"]], "kw": {}}, "process": {"began": "2026-01-01T00:00:00", "ended": "2026-01-01T00:00:05"}, "result": [true, "Success"]}"#,
        )
        .unwrap();
    }
    let passes = 10;
    let (cpu0, wall0) = (cpu_seconds(), std::time::Instant::now());
    for _ in 0..passes {
        let mut jobs = get_jobs(&ctx, true);
        assert_eq!(jobs.len(), 50);
        let _ = check_restart_jobs(&mut jobs);
        for (_, job) in jobs.iter_mut() {
            let _ = (job.status(), job.pid(), job.result(), format!("{job}"));
        }
    }
    let (cpu, wall) = (cpu_seconds() - cpu0, wall0.elapsed().as_secs_f64());
    println!(
        "BENCH 50 completed jobs: {:.1} ms CPU and {:.1} ms wall per pass ({passes} passes)",
        cpu * 1000.0 / passes as f64,
        wall * 1000.0 / passes as f64
    );
}
