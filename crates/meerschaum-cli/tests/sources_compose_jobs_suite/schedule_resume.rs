//! A job process (`MRSM_DAEMON_ID` set) records its schedule per root and resumes it after a
//! restart; a second root and a foreground run (no job name) are independent.

use std::sync::atomic::Ordering;

use meerschaum_cli::dispatch::Runtime;
use meerschaum_cli::native::jobs::{run_schedule, stop_requested_flag};
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_jobs::scheduler::RootScheduler;

fn job_env(key: &str) -> Option<String> {
    (key == "MRSM_DAEMON_ID").then(|| "resumable".to_string())
}

fn no_env(_: &str) -> Option<String> {
    None
}

/// The tests share the process's stop flag: they take turns.
static TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Run the schedule once (the action raises the stop flag) in a runtime rooted at `root`.
fn run_once(root: &std::path::Path, env: fn(&str) -> Option<String>, schedule: &str) -> usize {
    let mut out = Vec::new();
    let mut rt = Runtime::new(DateContext::system(), 80, &mut out, env);
    let mut opts = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
    ]);
    opts.allow_writes = true;
    opts.home = root.to_path_buf();
    rt.config = Some(Config::new(opts).unwrap());
    let stop = stop_requested_flag();
    stop.store(false, Ordering::SeqCst);
    let mut runs = 0;
    let result = run_schedule(&mut rt, schedule, |_| {
        runs += 1;
        stop_requested_flag().store(true, Ordering::SeqCst);
        SuccessTuple::success()
    });
    assert!(result.success, "{result:?}");
    stop.store(false, Ordering::SeqCst);
    runs
}

#[test]
fn schedules_resume_per_root() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    for dir in [&a, &b] {
        std::fs::create_dir_all(dir.path().join("plugins")).unwrap();
    }
    assert_eq!(run_once(a.path(), job_env, "every 1 second"), 1);
    let state = RootScheduler::for_root(a.path())
        .state("resumable")
        .unwrap();
    assert_eq!(state.schedule, "every 1 second");
    let first_next = state.next_fire.expect("the next fire is recorded");
    assert!(state.last_fire.is_some());
    // The other root never saw it.
    assert!(
        RootScheduler::for_root(b.path())
            .state("resumable")
            .is_none()
    );

    // A "restart" 1+ seconds later: the recorded next fire is in the past, so it is not resumed
    // but replaced after the next fire; with a long interval it is resumed unchanged.
    assert_eq!(run_once(b.path(), job_env, "every 1 hour"), 1);
    let hour = RootScheduler::for_root(b.path())
        .state("resumable")
        .unwrap();
    let recorded = hour.next_fire.unwrap();
    let started = std::time::Instant::now();
    // Second run resumes `recorded` (about an hour away), so it must wait; stop it quickly.
    let handle = std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(600));
        stop_requested_flag().store(true, Ordering::SeqCst);
    });
    let runs = run_once_without_reset(b.path());
    handle.join().unwrap();
    assert_eq!(runs, 0, "a resumed far-future fire must not run early");
    assert!(started.elapsed().as_secs() < 30);
    assert_eq!(
        RootScheduler::for_root(b.path())
            .state("resumable")
            .unwrap()
            .next_fire,
        Some(recorded)
    );
    assert!(first_next > state.last_fire.unwrap());

    // A foreground run (no job name) records nothing.
    let c = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(c.path().join("plugins")).unwrap();
    assert_eq!(run_once(c.path(), no_env, "every 1 second"), 1);
    assert!(
        RootScheduler::for_root(c.path())
            .state("resumable")
            .is_none()
    );
}

fn run_once_without_reset(root: &std::path::Path) -> usize {
    let mut out = Vec::new();
    let mut rt = Runtime::new(DateContext::system(), 80, &mut out, job_env);
    let mut opts = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
    ]);
    opts.allow_writes = true;
    opts.home = root.to_path_buf();
    rt.config = Some(Config::new(opts).unwrap());
    stop_requested_flag().store(false, Ordering::SeqCst);
    let mut runs = 0;
    let _ = run_schedule(&mut rt, "every 1 hour", |_| {
        runs += 1;
        SuccessTuple::success()
    });
    stop_requested_flag().store(false, Ordering::SeqCst);
    runs
}

/// A job restarted between two fires resumes the recorded next fire and runs it once: the fire
/// after it is the schedule's next one, not the occurrence the restarted trigger began with
/// (RC32 found a nightly backup that ran twice in a row after an upgrade restart), one interval
/// after the resumed fire (RC33 found a `daily` job whose next run was counted from the restart).
#[test]
fn a_resumed_fire_runs_once_and_the_next_one_follows_it() {
    let _turn = TURN.lock().unwrap_or_else(|e| e.into_inner());
    use chrono::{SubsecRound, TimeDelta, Utc};
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("plugins")).unwrap();
    let schedule = "every 1 hour";
    let scheduler = RootScheduler::for_root(root.path());
    // Schedule state keeps microseconds.
    let now = Utc::now().fixed_offset().trunc_subsecs(6);
    // The previous process fired an hour ago and recorded the next fire, two seconds from now.
    let resumed = now + TimeDelta::seconds(2);
    scheduler
        .record_fire(
            "resumable",
            schedule,
            now - TimeDelta::hours(1),
            Some(resumed),
        )
        .unwrap();

    let mut out = Vec::new();
    let mut rt = Runtime::new(DateContext::system(), 80, &mut out, job_env);
    let mut opts = ConfigOptions::with_env(vec![
        (
            "MRSM_ROOT_DIR".into(),
            root.path().to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.path().join("plugins").to_string_lossy().into_owned(),
        ),
    ]);
    opts.allow_writes = true;
    opts.home = root.path().to_path_buf();
    rt.config = Some(Config::new(opts).unwrap());
    stop_requested_flag().store(false, Ordering::SeqCst);
    let stopper = std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(6));
        stop_requested_flag().store(true, Ordering::SeqCst);
    });
    let mut fires = Vec::new();
    let result = run_schedule(&mut rt, schedule, |_| {
        fires.push(Utc::now());
        SuccessTuple::success()
    });
    stopper.join().unwrap();
    stop_requested_flag().store(false, Ordering::SeqCst);
    assert!(result.success, "{result:?}");
    assert_eq!(
        fires.len(),
        1,
        "the resumed fire ran {} times: {fires:?}",
        fires.len()
    );
    let state = scheduler.state("resumable").unwrap();
    let next = state.next_fire.expect("the next fire is recorded");
    assert_eq!(
        next,
        resumed + TimeDelta::hours(1),
        "the fire after a resumed one keeps its cadence, not the restart's"
    );
}
