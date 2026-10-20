//! The startup admission gate (D23) across real processes: file-lock slots shared by unrelated
//! processes, jobs held back by the detached supervisor, released by the worker's first-lap
//! signal, stopped while waiting, and the bulk-start pressure and jitter limits.
#![cfg(unix)]

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::job::{Job, JobOptions};
use meerschaum_jobs::startup::{self, StartKind, StartupPolicy};
use meerschaum_jobs::{JobsContext, Status};

fn gate_context(root: &Path, slots: usize, slot_timeout: f64) -> JobsContext {
    let mut ctx = common::sh_context(root);
    ctx.config.startup.enabled = true;
    ctx.config.startup.max_concurrent = Some(slots);
    ctx.config.startup.slot_timeout_seconds = slot_timeout;
    ctx.config.startup.jitter_seconds = 0.0;
    ctx
}

fn sh_job(ctx: &JobsContext, name: &str, script: &str, kind: StartKind) -> Job {
    let mut job = Job::new(
        name,
        Some(vec!["-c".into(), script.into()]),
        JobOptions::default(),
        ctx,
    )
    .unwrap();
    job.set_start_kind(kind);
    job
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

fn stamp(job: &mut Job, key: &str) -> chrono::NaiveDateTime {
    let props = job.daemon().properties();
    let text = props
        .get("process")
        .and_then(|p| p.get(key))
        .map(Value::py_str)
        .unwrap_or_else(|| panic!("job {} has no process.{key}", job.name));
    chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S"))
        .unwrap()
}

fn seconds_between(a: chrono::NaiveDateTime, b: chrono::NaiveDateTime) -> f64 {
    (b - a).num_microseconds().unwrap() as f64 / 1e6
}

// --- slots shared by unrelated processes --------------------------------------------------------

const HELPER_ENV: &str = "W18_JB_SLOT_HELPER_DIR";

/// Run as a child of [`slots_exclude_unrelated_processes`]: take the slot, hold it for 300 ms
/// and append `<start> <end>` (epoch milliseconds) to the shared log.
#[test]
fn slot_helper_process() {
    let Ok(dir) = std::env::var(HELPER_ENV) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let policy = StartupPolicy {
        kind: StartKind::Single,
        jitter_seconds: 0.0,
        max_load: 0.0,
        min_free_memory_percent: 0.0,
        max_concurrent: 1,
        slot_timeout_seconds: 60.0,
        settle_seconds: 0.0,
        dir: dir.join("slots"),
    };
    let stop = AtomicBool::new(false);
    let admitted = startup::admit(&policy, "helper", &stop, &mut |_| {}).expect("admitted");
    assert_eq!(admitted.slot(), Some(0));
    let millis = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    };
    let began = millis();
    std::thread::sleep(Duration::from_millis(300));
    let ended = millis();
    drop(admitted);
    use std::io::Write;
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("holds.log"))
        .unwrap();
    writeln!(log, "{began} {ended}").unwrap();
}

#[test]
fn slots_exclude_unrelated_processes() {
    if std::env::var(HELPER_ENV).is_ok() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let children: Vec<_> = (0..4)
        .map(|_| {
            Command::new(&exe)
                .args(["--exact", "slot_helper_process", "--test-threads=1"])
                .env(HELPER_ENV, dir.path())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    let text = fs::read_to_string(dir.path().join("holds.log")).unwrap();
    let mut holds: Vec<(u128, u128)> = text
        .lines()
        .map(|l| {
            let (a, b) = l.split_once(' ').unwrap();
            (a.parse().unwrap(), b.parse().unwrap())
        })
        .collect();
    assert_eq!(holds.len(), 4, "{text}");
    holds.sort();
    for pair in holds.windows(2) {
        assert!(
            pair[1].0 >= pair[0].1,
            "two processes held the only slot at once: {holds:?}"
        );
    }
}

#[test]
fn a_killed_holder_frees_its_slot() {
    let dir = tempfile::tempdir().unwrap();
    let slots = dir.path().join("slots");
    fs::create_dir_all(&slots).unwrap();
    // `flock` holds the slot from another process, which dies with the kill below.
    if Command::new("flock").arg("--version").output().is_err() {
        eprintln!("skipped: no flock(1)");
        return;
    }
    let mut holder = Command::new("flock")
        .arg(startup::slot_path(&slots, 0))
        .args(["sleep", "30"])
        .spawn()
        .unwrap();
    assert!(
        common::wait_for(5.0, || startup::try_acquire_slot(&slots, 1, "x").is_none()),
        "the other process never took the slot"
    );
    // flock(1) forks `sleep`: kill both through the process group's leader and its child.
    let _ = Command::new("pkill")
        .args(["-KILL", "-P", &holder.id().to_string()])
        .status();
    holder.kill().unwrap();
    let _ = holder.wait();
    assert!(
        common::wait_for(5.0, || startup::try_acquire_slot(&slots, 1, "x").is_some()),
        "a dead process still holds the slot"
    );
}

// --- the gate in the detached supervisor --------------------------------------------------------

#[test]
fn jobs_start_one_at_a_time_with_one_slot() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = gate_context(dir.path(), 1, 60.0);
    let mut jobs: Vec<Job> = (0..3)
        .map(|i| sh_job(&ctx, &format!("slot{i}"), "sleep 0.7", StartKind::Single))
        .collect();
    for job in &mut jobs {
        assert!(job.start().success);
    }
    for job in &mut jobs {
        wait_done(job);
    }
    let mut spans: Vec<_> = jobs
        .iter_mut()
        .map(|j| (stamp(j, "began"), stamp(j, "ended")))
        .collect();
    spans.sort();
    for pair in spans.windows(2) {
        // The next worker starts only after the previous one finished its (only) lap.
        assert!(
            seconds_between(pair[0].0, pair[1].0) >= 0.6,
            "jobs overlapped: {spans:?}"
        );
    }
}

#[test]
fn a_worker_frees_its_slot_when_its_first_lap_is_done() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = gate_context(dir.path(), 1, 60.0);
    // The worker is a direct child of the supervisor: its first lap is signalled by creating
    // the release file (`signal_first_lap_done` does exactly this for the real worker).
    let mut first = sh_job(
        &ctx,
        "lapper",
        r#"sleep 0.3; : > "${MRSM_JOB_STARTUP_RELEASE#*:}"; sleep 4"#,
        StartKind::Single,
    );
    let mut second = sh_job(&ctx, "follower", "sleep 0.1", StartKind::Single);
    assert!(first.start().success);
    assert!(common::wait_for(10.0, || first
        .daemon()
        .properties()
        .get("process")
        .and_then(|p| p.get("began"))
        .is_some()));
    assert!(second.start().success);
    wait_done(&mut second);
    // The follower ran while the first job was still in its second "lap".
    assert_eq!(
        first.status(),
        Status::Running,
        "the first job ended too early"
    );
    let gap = seconds_between(stamp(&mut first, "began"), stamp(&mut second, "began"));
    assert!(
        (0.25..3.5).contains(&gap),
        "follower started {gap} s after the first"
    );
    assert!(first.stop(None).success);
}

#[test]
fn a_job_stopped_while_waiting_never_runs() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = gate_context(dir.path(), 1, 120.0);
    let marker = dir.path().join("ran");
    let mut holder = sh_job(&ctx, "holder", "sleep 30", StartKind::Single);
    assert!(holder.start().success);
    assert!(common::wait_for(10.0, || holder.is_running()
        && holder
            .daemon()
            .properties()
            .get("process")
            .and_then(|p| p.get("began"))
            .is_some()));
    let mut waiter = sh_job(
        &ctx,
        "waiter",
        &format!("touch '{}'", marker.display()),
        StartKind::Single,
    );
    assert!(waiter.start().success);
    // Waiting at the gate counts as running (the supervisor is the job), and has not begun.
    assert!(common::wait_for(5.0, || waiter.status() == Status::Running));
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(waiter.status(), Status::Running);
    assert!(
        waiter
            .daemon()
            .properties()
            .get("process")
            .is_some_and(|p| p.get("began").is_none())
    );
    let stopped = waiter.stop(Some(meerschaum_jobs::Seconds::Int(10)));
    assert!(stopped.success, "{stopped}");
    assert!(common::wait_for(10.0, || waiter.status() == Status::Stopped));
    assert!(!marker.exists(), "the worker ran although it was stopped");
    let result = waiter.result();
    assert!(result.success, "{result:?}");
    assert!(
        result.message.contains("Stopped daemon 'waiter'"),
        "{result:?}"
    );
    let logs = waiter.get_logs().unwrap_or_default();
    assert!(
        logs.contains("Startup: waiting because every startup slot is busy"),
        "{logs}"
    );
    assert!(logs.contains("stopped before the job started"), "{logs}");
    assert!(holder.stop(None).success);
}

#[cfg(target_os = "linux")]
#[test]
fn a_bulk_start_waits_for_pressure_then_starts_anyway_and_no_stagger_skips_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = gate_context(dir.path(), 4, 1.2);
    // No host ever has 100% of its memory available: the pressure check never clears.
    ctx.config.startup.min_free_memory_percent = 100.0;
    ctx.config.startup.max_load = 0.0;

    let mut bulk = sh_job(&ctx, "bulk", "echo ok", StartKind::Bulk);
    let launched = chrono::Utc::now().naive_utc();
    assert!(bulk.start().success);
    wait_done(&mut bulk);
    let waited = seconds_between(launched, stamp(&mut bulk, "began"));
    assert!(waited >= 1.0, "a bulk start did not wait: {waited}");
    let logs = bulk.get_logs().unwrap_or_default();
    assert!(logs.contains("available, below 100%"), "{logs}");
    assert!(logs.contains("gave up waiting after 1 s"), "{logs}");

    // A single start ignores pressure; `--no-stagger` ignores the whole gate.
    for (name, kind) in [
        ("single", StartKind::Single),
        ("nostagger", StartKind::None),
    ] {
        let mut job = sh_job(&ctx, name, "echo ok", kind);
        let launched = chrono::Utc::now().naive_utc();
        assert!(job.start().success);
        wait_done(&mut job);
        let waited = seconds_between(launched, stamp(&mut job, "began"));
        assert!(waited < 1.0, "{name} waited {waited} s");
    }
}

#[test]
fn per_job_overrides_and_the_off_switch() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = gate_context(dir.path(), 1, 60.0);
    let mut job = sh_job(&ctx, "plain", "true", StartKind::Bulk);
    let policy = job.daemon().startup_policy(StartKind::Bulk).unwrap();
    assert_eq!(policy.max_concurrent, 1);
    assert_eq!(policy.dir, dir.path().join("jobs").join(".startup"));

    let mut over = Map::new();
    let mut startup = Map::new();
    startup.insert("jitter_seconds".into(), Value::Int(3));
    startup.insert("max_concurrent".into(), Value::Int(5));
    over.insert("startup".into(), Value::Map(startup));
    let mut tuned = Job::new(
        "tuned",
        Some(vec!["-c".into(), "true".into()]),
        JobOptions {
            properties: Some(over),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    let policy = tuned.daemon().startup_policy(StartKind::Bulk).unwrap();
    assert_eq!(policy.jitter_seconds, 3.0);
    assert_eq!(policy.max_concurrent, 5);
    assert!(tuned.daemon().startup_policy(StartKind::None).is_none());

    ctx.config.startup.enabled = false;
    let mut off = sh_job(&ctx, "off", "true", StartKind::Bulk);
    assert!(off.daemon().startup_policy(StartKind::Bulk).is_none());
}

#[cfg(target_os = "linux")]
#[test]
fn an_automatic_restart_is_a_bulk_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = gate_context(dir.path(), 2, 1.2);
    ctx.config.startup.min_free_memory_percent = 100.0;
    ctx.config.startup.max_load = 0.0;
    let mut props = Map::new();
    props.insert("restart".into(), Value::Bool(true));
    let mut job = Job::new(
        "restarter",
        Some(vec!["-c".into(), "echo hi; exit 1".into()]),
        JobOptions {
            properties: Some(props),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    // The explicit start is a single one: no pressure wait.
    assert_eq!(job.start_kind(), StartKind::Single);
    assert!(job.start().success);
    wait_done(&mut job);
    let first_began = stamp(&mut job, "began");
    assert!(job.restarts());
    // `check_restart` is what `restart jobs` calls: a bulk start for the duration of the start,
    // after which the job's own kind is back.
    let before = chrono::Utc::now().naive_utc();
    let restarted = job.check_restart();
    assert!(restarted.success, "{restarted}");
    assert_eq!(job.start_kind(), StartKind::Single);
    // Read the file, not the daemon's cached properties (they still hold the first run).
    let began_on_disk = |job: &mut Job| {
        job.daemon()
            .read_properties()
            .and_then(|p| p.get("process")?.get("began").map(Value::py_str))
            .and_then(|t| chrono::NaiveDateTime::parse_from_str(&t, "%Y-%m-%dT%H:%M:%S%.f").ok())
    };
    let advanced = common::wait_for(30.0, || {
        began_on_disk(&mut job).is_some_and(|began| began > first_began)
    });
    assert!(advanced, "the restart never began: {:?}", job.get_logs());
    let waited = seconds_between(before, began_on_disk(&mut job).unwrap());
    assert!(
        waited >= 1.0,
        "the restart did not wait like a bulk start: {waited}"
    );
}

/// A worker which is not doing anything (an idle server, a plugin loop sleeping between laps)
/// does not hold the slot for the whole timeout.
#[cfg(target_os = "linux")]
#[test]
fn an_idle_worker_frees_its_slot_after_it_settles() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = gate_context(dir.path(), 1, 120.0);
    ctx.config.startup.settle_seconds = 2.0;
    let mut idle = sh_job(&ctx, "idler", "sleep 40", StartKind::Single);
    assert!(idle.start().success);
    assert!(common::wait_for(10.0, || idle
        .daemon()
        .read_properties()
        .and_then(|p| p.get("process")?.get("began").cloned())
        .is_some()));
    let mut next = sh_job(&ctx, "after-idle", "echo ok", StartKind::Single);
    assert!(next.start().success);
    wait_done(&mut next);
    let gap = seconds_between(stamp(&mut idle, "began"), stamp(&mut next, "began"));
    assert!(
        (1.5..15.0).contains(&gap),
        "the next job started {gap} s later"
    );
    assert_eq!(idle.status(), Status::Running);
    let logs = idle.get_logs().unwrap_or_default();
    assert!(logs.contains("the job is idle"), "{logs}");
    assert!(idle.stop(None).success);
}

/// A worker which is busy (here through a child process: the whole tree counts) keeps its slot.
#[cfg(target_os = "linux")]
#[test]
fn a_busy_worker_keeps_its_slot() {
    if Command::new("timeout").arg("--version").output().is_err() {
        eprintln!("skipped: no timeout(1)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = gate_context(dir.path(), 1, 120.0);
    ctx.config.startup.settle_seconds = 2.0;
    let mut busy = sh_job(
        &ctx,
        "burner",
        "timeout 7 sh -c 'while :; do :; done'",
        StartKind::Single,
    );
    assert!(busy.start().success);
    assert!(common::wait_for(10.0, || busy
        .daemon()
        .read_properties()
        .and_then(|p| p.get("process")?.get("began").cloned())
        .is_some()));
    let mut next = sh_job(&ctx, "after-busy", "echo ok", StartKind::Single);
    assert!(next.start().success);
    wait_done(&mut next);
    wait_done(&mut busy);
    let gap = seconds_between(stamp(&mut busy, "began"), stamp(&mut next, "began"));
    assert!(gap >= 6.0, "the busy job's slot was freed after {gap} s");
    let logs = busy.get_logs().unwrap_or_default();
    assert!(!logs.contains("the job is idle"), "{logs}");
}
