//! The API server's restart monitor (v4's `start_check_jobs_thread`): stopped `--restart` jobs
//! come back unless they were stopped by hand.

#![cfg(target_os = "linux")]

mod common;

use std::time::Duration;

use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::job::{Job, JobOptions};
use meerschaum_jobs::{JobsContext, Status, monitor};

fn context(root: &std::path::Path) -> JobsContext {
    let mut ctx = common::sh_context(root);
    ctx.config.startup.enabled = false;
    ctx
}

fn job(ctx: &JobsContext, name: &str, restart: bool) -> Job {
    let mut props = Map::new();
    props.insert("restart".into(), Value::Bool(restart));
    Job::new(
        name,
        Some(vec!["-c".into(), "echo lap".into()]),
        JobOptions {
            properties: Some(props),
            ..Default::default()
        },
        ctx,
    )
    .unwrap()
}

/// `process.began` as written on disk (a `Job` keeps the properties it read in memory).
fn began(job: &mut Job) -> String {
    let path = job.daemon().paths().properties.clone();
    meerschaum_jobs::properties::read_properties(&path)
        .unwrap_or_default()
        .get("process")
        .and_then(|p| p.get("began"))
        .map(Value::py_str)
        .unwrap_or_default()
}

fn wait_done(job: &mut Job) {
    assert!(
        common::wait_for(30.0, || job.status() == Status::Stopped
            && job
                .daemon()
                .properties()
                .get("result")
                .is_some_and(|r| !r.is_null())),
        "job {} did not finish",
        job.name
    );
}

#[test]
fn a_check_restarts_jobs_that_were_not_stopped_by_hand() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = context(dir.path());
    let mut restarter = job(&ctx, "restarter", true);
    let mut once = job(&ctx, "once", false);
    let mut stopped = job(&ctx, "stopped", true);
    for job in [&mut restarter, &mut once, &mut stopped] {
        assert!(job.start().success);
        wait_done(job);
    }
    // A manual stop records `stop_time`; the monitor leaves that job alone.
    assert!(stopped.stop(None).success);
    assert!(stopped.stop_time().is_some());
    let before = (began(&mut restarter), began(&mut once), began(&mut stopped));

    let result = monitor::check_once(&ctx).expect("the check-jobs lock is free");
    assert!(result.success, "{}", result.message);
    assert!(
        common::wait_for(30.0, || began(&mut restarter) != before.0),
        "the restart job was not restarted"
    );
    wait_done(&mut restarter);
    assert_eq!(
        began(&mut once),
        before.1,
        "a job without --restart was restarted"
    );
    assert_eq!(
        began(&mut stopped),
        before.2,
        "a manually stopped job was restarted"
    );
}

#[test]
fn the_monitor_keeps_restarting_until_it_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = context(dir.path());
    let mut restarter = job(&ctx, "looping", true);
    assert!(restarter.start().success);
    wait_done(&mut restarter);
    let first = began(&mut restarter);

    let running = monitor::spawn(ctx.clone(), Duration::from_millis(100)).unwrap();
    assert!(
        common::wait_for(30.0, || began(&mut restarter) != first),
        "the monitor did not restart the job"
    );
    running.stop();
    // Once stopped, the monitor starts nothing more.
    wait_done(&mut restarter);
    let last = began(&mut restarter);
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(began(&mut restarter), last);
}

#[test]
fn a_held_lock_skips_the_check() {
    use nix::fcntl::{Flock, FlockArg};
    let dir = tempfile::tempdir().unwrap();
    let ctx = context(dir.path());
    std::fs::create_dir_all(ctx.paths.check_jobs_lock.parent().unwrap()).unwrap();
    let file = std::fs::File::create(&ctx.paths.check_jobs_lock).unwrap();
    let _held = Flock::lock(file, FlockArg::LockExclusiveNonblock).unwrap();
    assert!(monitor::check_once(&ctx).is_none());
}
