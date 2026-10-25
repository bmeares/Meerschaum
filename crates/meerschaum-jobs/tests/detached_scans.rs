//! Job status is derived from what was recorded, not from scanning the process table: completed
//! jobs never cause a scan, a job whose PID file was lost is found by one shared scan which then
//! restores the file, and restart policies behave as before.
//!
//! The scan counter is process-wide, so the tests of this file run one at a time.
#![cfg(target_os = "linux")]

mod common;

use std::path::Path;
use std::process::{Child, Command};
use std::sync::Mutex;

use chrono::Utc;
use meerschaum_jobs::executor::{check_restart_jobs, get_jobs};
use meerschaum_jobs::process::cmdline_scans;
use meerschaum_jobs::{Daemon, Job, JobOptions, JobsContext, Status};

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn ago(seconds: i64) -> String {
    (Utc::now() - chrono::Duration::seconds(seconds))
        .naive_utc()
        .format("%Y-%m-%dT%H:%M:%S%.6f")
        .to_string()
}

/// A job directory with the recorded metadata of a run (`None` leaves the key out) and no PID
/// file: what v4 and v5 leave behind for a job which completed.
fn write_job(
    root: &Path,
    name: &str,
    sysargs: &[&str],
    began: Option<String>,
    ended: Option<String>,
    launched: Option<String>,
) {
    let dir = root.join("jobs").join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let mut process = serde_json::Map::new();
    for (key, value) in [("began", began), ("ended", ended), ("launched", launched)] {
        if let Some(value) = value {
            process.insert(key.into(), value.into());
        }
    }
    let ended_run = process.contains_key("ended");
    let props = serde_json::json!({
        "label": sysargs.join(" "),
        "target": {
            "name": "entry",
            "module": "meerschaum._internal.entry",
            "args": [sysargs],
            "kw": {},
        },
        "process": process,
        "result": if ended_run { serde_json::json!([true, "Success"]) } else { serde_json::Value::Null },
    });
    std::fs::write(dir.join("properties.json"), props.to_string()).unwrap();
}

/// A process whose command line looks like a job's supervisor for `jobs_dir`.
struct StandIn(Child);

impl StandIn {
    fn spawn(name: &str, jobs_dir: &Path) -> StandIn {
        StandIn(
            Command::new("sh")
                .args(["-c", "sleep 120; true", "supervisor"])
                .arg(format!("daemon_id='{name}'"))
                .arg("--jobs-dir")
                .arg(jobs_dir)
                .spawn()
                .unwrap(),
        )
    }

    fn pid(&self) -> i32 {
        self.0.id() as i32
    }
}

impl Drop for StandIn {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// One supervisor pass: list the jobs, check the restart policies, and read what a listing and a
/// `show jobs` read (status, PID, result, timestamps, formatting).
fn pass(ctx: &JobsContext) -> usize {
    let mut jobs = get_jobs(ctx, true);
    let count = jobs.len();
    let _ = check_restart_jobs(&mut jobs);
    for (_, job) in jobs.iter_mut() {
        let _ = job.status();
        let _ = job.pid();
        let _ = job.result();
        let _ = job.began();
        let _ = job.ended();
        let _ = job.paused();
        let _ = job.restarts();
        let _ = format!("{job:?} {job}");
        let _ = job.label();
    }
    count
}

#[test]
fn fifty_completed_jobs_never_scan_the_process_table() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    for i in 0..50 {
        write_job(
            dir.path(),
            &format!("done-{i}"),
            &["show", "version"],
            Some(ago(120)),
            Some(ago(60)),
            None,
        );
    }
    let before = cmdline_scans();
    for _ in 0..10 {
        assert_eq!(pass(&ctx), 50);
    }
    // Opening, constructing and reading each one by hand, too.
    for i in 0..50 {
        let name = format!("done-{i}");
        let mut daemon = Daemon::open(&name, &ctx).unwrap();
        assert_eq!(daemon.status(), Status::Stopped);
        assert_eq!(daemon.pid(), None);
        let _ = daemon.properties();
        let mut job = Job::open(&name, &ctx).unwrap();
        assert_eq!(job.status(), Status::Stopped);
        assert!(!job.check_restart().message.is_empty());
    }
    assert_eq!(
        cmdline_scans() - before,
        0,
        "completed jobs enumerated the process table"
    );
    // A job which never began has nothing to look for either.
    write_job(dir.path(), "fresh", &["show", "version"], None, None, None);
    let mut fresh = Job::open("fresh", &ctx).unwrap();
    assert_eq!(fresh.status(), Status::Stopped);
    assert_eq!(cmdline_scans() - before, 0);
}

#[test]
fn a_lost_pid_file_costs_one_scan_per_pass_and_then_none() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let jobs_dir = dir.path().join("jobs");
    for i in 0..20 {
        write_job(
            dir.path(),
            &format!("done-{i}"),
            &["show", "version"],
            Some(ago(120)),
            Some(ago(60)),
            None,
        );
    }
    // A live job which lost its PID file, and one whose recorded `began` is an hour before the
    // process with its name started (a reused PID or a leftover command line).
    let lost = StandIn::spawn("lost", &jobs_dir);
    let reused = StandIn::spawn("reused", &jobs_dir);
    write_job(dir.path(), "lost", &["sleep"], Some(ago(0)), None, None);
    write_job(
        dir.path(),
        "reused",
        &["sleep"],
        Some(ago(3600)),
        None,
        None,
    );
    // (the command lines must be visible: `sh` has exec'ed its `-c` script by now)
    assert!(common::wait_for(10.0, || {
        std::fs::read(format!("/proc/{}/cmdline", lost.pid()))
            .map(|c| String::from_utf8_lossy(&c).contains("daemon_id='lost'"))
            .unwrap_or(false)
            && std::fs::read(format!("/proc/{}/cmdline", reused.pid()))
                .map(|c| String::from_utf8_lossy(&c).contains("daemon_id='reused'"))
                .unwrap_or(false)
    }));

    let before = cmdline_scans();
    assert_eq!(pass(&ctx), 22);
    assert_eq!(
        cmdline_scans() - before,
        1,
        "one shared scan serves every job of the pass"
    );
    let mut job = Job::open("lost", &ctx).unwrap();
    assert_eq!(job.status(), Status::Running);
    // The marker is on the supervisor's command line; the job is its worker (here: the `sleep`
    // the stand-in shell runs).
    let worker = meerschaum_jobs::process::children(lost.pid()).unwrap()[0];
    assert_eq!(job.pid(), Some(worker));
    // The PID file was written again, so later reads are cheap.
    assert_eq!(
        std::fs::read_to_string(dir.path().join("jobs/lost/process.pid"))
            .unwrap()
            .trim(),
        worker.to_string()
    );
    // The process which started long after the recorded `began` is not the job's.
    let mut reused_job = Job::open("reused", &ctx).unwrap();
    assert_eq!(reused_job.status(), Status::Stopped);
    assert_eq!(reused_job.pid(), None);
    assert!(!dir.path().join("jobs/reused/process.pid").exists());

    let after_first = cmdline_scans();
    for _ in 0..10 {
        assert_eq!(pass(&ctx), 22);
    }
    assert_eq!(
        cmdline_scans() - after_first,
        0,
        "later passes read the PID file or the remembered miss"
    );
    assert_eq!(job.status(), Status::Running);
}

#[test]
fn a_job_which_is_not_found_is_not_searched_for_again_until_something_changes() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    write_job(dir.path(), "gone", &["sleep"], Some(ago(30)), None, None);
    let before = cmdline_scans();
    let mut job = Job::open("gone", &ctx).unwrap();
    for _ in 0..20 {
        assert_eq!(job.status(), Status::Stopped);
    }
    assert_eq!(cmdline_scans() - before, 1);
    // Recording a new launch (even from another process) is a change: look again.
    let at = cmdline_scans();
    write_job(
        dir.path(),
        "gone",
        &["sleep"],
        Some(ago(30)),
        None,
        Some(ago(0)),
    );
    assert_eq!(job.status(), Status::Stopped);
    assert!(cmdline_scans() > at);
    // Stopping through Meerschaum forgets the remembered miss too.
    let mut daemon = Daemon::open("gone", &ctx).unwrap();
    let at = cmdline_scans();
    let _ = daemon.kill(Some(meerschaum_jobs::Seconds::Float(0.2)));
    assert!(cmdline_scans() > at, "a kill searches fresh");
}

#[test]
fn a_job_being_relaunched_is_not_taken_for_a_finished_one() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let jobs_dir = dir.path().join("jobs");
    // A restart job which completed, and was launched again a moment ago: its supervisor
    // exists, but it has not written its PID file nor recorded `began` yet.
    let supervisor = StandIn::spawn("relaunch", &jobs_dir);
    write_job(
        dir.path(),
        "relaunch",
        &["sleep", "--restart"],
        Some(ago(60)),
        Some(ago(30)),
        Some(ago(1)),
    );
    assert!(common::wait_for(10.0, || {
        std::fs::read(format!("/proc/{}/cmdline", supervisor.pid()))
            .map(|c| String::from_utf8_lossy(&c).contains("daemon_id='relaunch'"))
            .unwrap_or(false)
    }));
    let mut job = Job::open("relaunch", &ctx).unwrap();
    assert_eq!(job.status(), Status::Running);
    let checked = job.check_restart();
    assert!(checked.message.contains("is running"), "{checked}");
    // Not launched a second time, and the supervisor's PID is not written down yet (`began` is
    // older than `launched`: the process found may be the launcher).
    assert!(!dir.path().join("jobs/relaunch/process.pid").exists());
}

fn sh_job(ctx: &JobsContext, name: &str, script: &str) -> Job {
    Job::new(
        name,
        Some(vec!["-c".into(), script.into(), "--restart".into()]),
        JobOptions::default(),
        ctx,
    )
    .unwrap()
}

#[test]
fn restart_policies_are_unchanged() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());

    // Stopped on its own (or externally) and not by the user: restarted.
    let mut job = sh_job(&ctx, "ends", "echo hi");
    assert!(job.start().success);
    assert!(common::wait_for(20.0, || job.status() == Status::Stopped
        && job.ended().is_some()));
    let first_began = job.began().map(|d| d.isoformat());
    assert!(job.restarts());
    let restarted = job.check_restart();
    assert!(restarted.success, "{restarted}");
    assert!(restarted.message.starts_with("Started"), "{restarted}");
    // (a fresh handle each time: a handle keeps the timestamps it has read)
    assert!(common::wait_for(20.0, || {
        let mut now = Job::open("ends", &ctx).unwrap();
        now.ended().is_some()
            && now.began().map(|d| d.isoformat()) > first_began
            && now.status() == Status::Stopped
    }));

    // Stopped by the user: it stays stopped.
    let mut manual = sh_job(&ctx, "manual", "sleep 30");
    assert!(manual.start().success);
    assert!(common::wait_for(20.0, || manual.status() == Status::Running));
    assert!(manual.stop(Some(meerschaum_jobs::Seconds::Int(10))).success);
    assert!(common::wait_for(20.0, || manual.status() == Status::Stopped));
    let checked = manual.check_restart();
    assert!(checked.message.contains("manually stopped"), "{checked}");
    assert_eq!(manual.status(), Status::Stopped);

    // A running one is left alone, and `check_restart_jobs` reports like before.
    let mut running = sh_job(&ctx, "running", "sleep 30");
    assert!(running.start().success);
    assert!(common::wait_for(20.0, || running.status() == Status::Running));
    assert!(running.check_restart().message.contains("is running"));
    let mut jobs = get_jobs(&ctx, true);
    let summary = check_restart_jobs(&mut jobs);
    assert!(summary.success, "{summary}");
    assert!(
        running
            .stop(Some(meerschaum_jobs::Seconds::Int(10)))
            .success
    );
}

/// Stops a job (and so its supervisor and worker) when the test ends, even by a failed assertion.
struct StopOnDrop(JobsContext, &'static str);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if let Ok(mut job) = Job::open(self.1, &self.0) {
            let _ = job.stop(Some(meerschaum_jobs::Seconds::Int(5)));
            let _ = job.daemon().kill(Some(meerschaum_jobs::Seconds::Int(5)));
        }
        // Wait for the supervisor to leave (it writes into the root until it exits).
        let _ = common::wait_for(10.0, || {
            meerschaum_jobs::detached::find_pids(
                self.1,
                &self.0.paths.jobs_dir,
                &Default::default(),
                None,
                true,
            )
            .is_empty()
        });
    }
}

fn pid_file(root: &Path, name: &str) -> Option<i32> {
    std::fs::read_to_string(root.join("jobs").join(name).join("process.pid"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[test]
fn a_restored_pid_file_names_the_worker_and_stop_pause_resume_reach_it() {
    use meerschaum_jobs::process::{ProcState, proc_state};
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let _stop = StopOnDrop(ctx.clone(), "real");
    // A real supervisor with a real worker (a looping shell: it survives being stopped).
    let mut job = Job::new(
        "real",
        Some(vec!["-c".into(), "while :; do sleep 0.1; done".into()]),
        JobOptions::default(),
        &ctx,
    )
    .unwrap();
    assert!(job.start().success);
    assert!(common::wait_for(20.0, || pid_file(dir.path(), "real")
        .is_some()
        && job.began().is_some()));
    let worker = pid_file(dir.path(), "real").unwrap();
    let supervisor = meerschaum_jobs::detached::find_pids(
        "real",
        &ctx.paths.jobs_dir,
        &Default::default(),
        None,
        true,
    )
    .first()
    .map(|f| f.pid)
    .expect("the supervisor's command line carries the marker");
    assert_ne!(worker, supervisor);

    // Lose the PID file: the next status read finds the job and writes the worker's PID back.
    std::fs::remove_file(dir.path().join("jobs/real/process.pid")).unwrap();
    meerschaum_jobs::detached::reset();
    let mut again = Job::open("real", &ctx).unwrap();
    assert_eq!(again.status(), Status::Running);
    assert_eq!(again.pid(), Some(worker));
    assert_eq!(pid_file(dir.path(), "real"), Some(worker));

    // Pausing reaches the worker, not the supervisor.
    let paused = again.pause(Some(meerschaum_jobs::Seconds::Int(10)));
    assert!(paused.success, "{paused}");
    assert_eq!(proc_state(worker), ProcState::Stopped);
    assert_ne!(proc_state(supervisor), ProcState::Stopped);
    assert_eq!(again.status(), Status::Paused);
    let resumed = again
        .daemon()
        .resume(Some(meerschaum_jobs::Seconds::Int(10)), None);
    assert!(resumed.success, "{resumed}");
    assert_eq!(proc_state(worker), ProcState::Alive);
    assert_eq!(again.status(), Status::Running);

    // Stopping ends the worker, and the supervisor removes the file when it leaves.
    let stopped = again.stop(Some(meerschaum_jobs::Seconds::Int(10)));
    assert!(stopped.success, "{stopped}");
    assert!(common::wait_for(20.0, || proc_state(worker)
        == ProcState::Gone
        && !dir.path().join("jobs/real/process.pid").exists()));
    assert_eq!(Job::open("real", &ctx).unwrap().status(), Status::Stopped);
}

#[test]
fn a_pid_file_of_a_completed_job_is_not_trusted() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    // An unrelated live process whose PID a stale file names (a reused PID).
    let other = StandIn::spawn("unrelated", &dir.path().join("elsewhere"));
    write_job(
        dir.path(),
        "finished",
        &["show", "version"],
        Some(ago(120)),
        Some(ago(60)),
        None,
    );
    let pid_path = dir.path().join("jobs/finished/process.pid");
    std::fs::write(&pid_path, other.pid().to_string()).unwrap();
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(300);
    std::fs::File::options()
        .write(true)
        .open(&pid_path)
        .unwrap()
        .set_modified(old)
        .unwrap();
    let mut job = Job::open("finished", &ctx).unwrap();
    assert_eq!(job.status(), Status::Stopped);
    assert_eq!(job.pid(), None);
    assert!(!pid_path.exists(), "the stale file is removed");
    // A file written after `ended` belongs to a newer run and stands.
    std::fs::write(&pid_path, other.pid().to_string()).unwrap();
    assert_eq!(job.status(), Status::Running);
    assert_eq!(job.pid(), Some(other.pid()));
}

#[test]
fn a_failed_launch_leaves_nothing_to_search_for() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = common::sh_context(dir.path());
    ctx.launcher.supervisor = vec!["/nonexistent/mrsm-job-supervisor".into()];
    let mut job = Job::new(
        "never",
        Some(vec!["-c".into(), "true".into()]),
        JobOptions::default(),
        &ctx,
    )
    .unwrap();
    let started = job.start();
    assert!(!started.success, "{started}");
    let props = std::fs::read_to_string(dir.path().join("jobs/never/properties.json")).unwrap();
    assert!(!props.contains("launched"), "{props}");
    let before = cmdline_scans();
    for _ in 0..5 {
        assert_eq!(job.status(), Status::Stopped);
    }
    assert_eq!(
        cmdline_scans() - before,
        0,
        "a job which never started is not searched for"
    );
}

#[test]
fn a_handle_with_a_remembered_miss_sees_a_relaunch_by_another_handle() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let _stop = StopOnDrop(ctx.clone(), "relaunched");
    // Began, never ended, no process: the miss is remembered (for 60 s).
    write_job(
        dir.path(),
        "relaunched",
        &["-c", "sleep 30"],
        Some(ago(30)),
        None,
        None,
    );
    let mut first = Job::open("relaunched", &ctx).unwrap();
    assert_eq!(first.status(), Status::Stopped);
    assert_eq!(first.status(), Status::Stopped);
    // Another handle (another process would do the same: the record is in the file) starts it.
    let mut second = Job::open("relaunched", &ctx).unwrap();
    let started = second.start();
    assert!(started.success, "{started}");
    // The first handle's next read sees it: `launched` changed what was remembered.
    assert!(common::wait_for(20.0, || first.status() == Status::Running));
    assert!(second.stop(Some(meerschaum_jobs::Seconds::Int(10))).success);
}

#[test]
fn a_job_can_be_started_again_right_after_it_was_stopped() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let _stop = StopOnDrop(ctx.clone(), "again");
    let mut job = Job::new(
        "again",
        // A background child keeps the supervisor's output pipes open after the
        // worker is gone, so the supervisor outlives its worker by a while (`mrsm sh sleep`
        // behaves like this).
        Some(vec!["-c".into(), "sleep 5 & wait".into()]),
        JobOptions::default(),
        &ctx,
    )
    .unwrap();
    for round in 0..2 {
        let started = job.start();
        assert!(started.success, "round {round}: {started}");
        assert!(
            started.message.starts_with("Started"),
            "round {round}: {started}"
        );
        assert!(common::wait_for(20.0, || job.status() == Status::Running));
        let first = pid_file(dir.path(), "again");
        let stopped = job.stop(Some(meerschaum_jobs::Seconds::Int(10)));
        assert!(stopped.success, "round {round}: {stopped}");
        // `Stopped` means stopped: it reads so at once, and no supervisor lingers.
        assert_eq!(job.status(), Status::Stopped, "round {round}");
        assert!(
            meerschaum_jobs::detached::find_pids(
                "again",
                &ctx.paths.jobs_dir,
                &Default::default(),
                None,
                true
            )
            .is_empty(),
            "round {round}: a supervisor is left"
        );
        assert!(first.is_some());
    }
    // Immediately started again, it really runs again.
    let started = job.start();
    assert!(started.message.starts_with("Started"), "{started}");
    assert!(common::wait_for(20.0, || job.status() == Status::Running));
}

#[test]
fn without_children_files_a_job_which_lost_its_pid_file_still_reads_running() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let dir = tempfile::tempdir().unwrap();
    let jobs_dir = dir.path().join("jobs");
    let supervisor = StandIn::spawn("nokids", &jobs_dir);
    write_job(
        dir.path(),
        "nokids",
        &["sleep", "--restart"],
        Some(ago(0)),
        None,
        None,
    );
    assert!(common::wait_for(10.0, || {
        std::fs::read(format!("/proc/{}/cmdline", supervisor.pid()))
            .map(|c| String::from_utf8_lossy(&c).contains("daemon_id='nokids'"))
            .unwrap_or(false)
    }));

    // A kernel which does not expose `children`: the supervisor stands in as running (so the
    // restart check does not launch the job a second time), and no PID file is invented.
    let blind = common::sh_context(dir.path()).with_children_reader(|_| None);
    let mut job = Job::open("nokids", &blind).unwrap();
    assert_eq!(job.status(), Status::Running);
    assert_eq!(job.pid(), Some(supervisor.pid()));
    let checked = job.check_restart();
    assert!(checked.message.contains("is running"), "{checked}");
    assert!(!dir.path().join("jobs/nokids/process.pid").exists());

    // A readable, empty list is a supervisor without a worker: finishing, so stopped.
    meerschaum_jobs::detached::reset();
    let finishing = common::sh_context(dir.path()).with_children_reader(|_| Some(Vec::new()));
    let mut job = Job::open("nokids", &finishing).unwrap();
    assert_eq!(job.status(), Status::Stopped);
}

/// A stand-in for a Python (v4) daemon: no `--jobs-dir`, the marker in its command line.
fn python_daemon(daemon_id: &str, root: &Path) -> Child {
    Command::new("sh")
        .args(["-c", "sleep 30; :", &format!("daemon_id='{daemon_id}'")])
        .env("MRSM_ROOT_DIR", root)
        .spawn()
        .unwrap()
}

#[test]
fn a_python_daemon_matches_only_the_root_it_was_started_with() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    meerschaum_jobs::detached::reset();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let name = format!("w_root_scope_{}", std::process::id());
    let mut in_a = python_daemon(&name, a.path());
    let mut in_b = python_daemon(&name, b.path());
    // Both are in the process table before the scan.
    assert!(common::wait_for(5.0, || {
        meerschaum_jobs::process::process_cmdlines()
            .iter()
            .filter(|(_, args)| args.iter().any(|x| x.contains(&name)))
            .count()
            >= 2
    }));
    let limits = meerschaum_jobs::detached::Limits::default();
    let found = |root: &Path| -> Vec<i32> {
        meerschaum_jobs::detached::find_pids(&name, &root.join("jobs"), &limits, None, true)
            .into_iter()
            .map(|f| f.pid)
            .collect()
    };
    let found_a = found(a.path());
    let found_b = found(b.path());
    let _ = in_a.kill();
    let _ = in_b.kill();
    let _ = in_a.wait();
    let _ = in_b.wait();
    assert_eq!(found_a, vec![in_a.id() as i32]);
    assert_eq!(found_b, vec![in_b.id() as i32]);
}
