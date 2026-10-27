//! Live supervision: jobs started through the detached supervisor (`/bin/sh <sysargs>` as the
//! worker), then paused, resumed, stopped, killed, fed input and deleted.
#![cfg(unix)]

mod common;

use std::fs;
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::job::{Job, JobOptions, MonitorControl, MonitorOptions};
use meerschaum_jobs::{Seconds, Status};

fn sh_job(ctx: &meerschaum_jobs::JobsContext, name: &str, script: &str, opts: JobOptions) -> Job {
    Job::new(name, Some(vec!["-c".into(), script.into()]), opts, ctx).unwrap()
}

fn wait_stopped(job: &mut Job) {
    assert!(
        common::wait_for(20.0, || job.status() == Status::Stopped
            && job
                .daemon()
                .properties()
                .get("result")
                .is_some_and(|r| !r.is_null())),
        "job did not finish"
    );
}

#[test]
fn completed_job_layout_and_logs() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(
        &ctx,
        "done",
        "echo hello; echo oops >&2; sleep 0.3",
        JobOptions::default(),
    );
    let started = job.start();
    assert!(started.success, "{started}");
    assert_eq!(
        started.message,
        "Started Job(\"done\", \"-c 'echo hello; echo oops >&2; sleep 0.3'\")."
    );
    wait_stopped(&mut job);

    let r = job.result();
    assert_eq!((r.success, r.message.as_str()), (true, "Success"));
    let jobdir = dir.path().join("jobs/done");
    assert!(!jobdir.join("process.pid").exists());
    use std::os::unix::fs::FileTypeExt;
    assert!(
        fs::metadata(jobdir.join("input.stdin"))
            .unwrap()
            .file_type()
            .is_fifo()
    );
    let props = fs::read_to_string(jobdir.join("properties.json")).unwrap();
    let props = Value::from_json_str(&props).unwrap();
    let keys: Vec<&str> = props.as_map().unwrap().keys().map(String::as_str).collect();
    // `process` is created by `run()` recording `process.launched` (as v4.1.5 does), before the
    // supervisor records `result`.
    assert_eq!(
        keys,
        ["mrsm_execution", "label", "target", "process", "result"]
    );
    assert_eq!(
        props
            .get("mrsm_execution")
            .unwrap()
            .get("kind")
            .and_then(Value::as_str),
        Some("persistent_job"),
    );
    assert_eq!(
        props.get("target").unwrap().json_dumps(),
        r#"{"name": "entry", "module": "meerschaum._internal.entry", "args": [["-c", "echo hello; echo oops >&2; sleep 0.3"]], "kw": {}}"#
    );
    let process: Vec<&str> = props
        .get("process")
        .unwrap()
        .as_map()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(process, ["launched", "began", "ended"]);
    assert!(job.began().is_some() && job.ended().is_some() && job.paused().is_none());

    // Both streams pass through their own interceptor: each starts with a newline.
    let logs = common::normalize_log_timestamps(&job.get_logs().unwrap());
    let ts = "2000-01-01 00:00 | ";
    let a = format!("\n{ts}hello\n{ts}");
    let b = format!("\n{ts}oops\n{ts}");
    assert!(
        logs == format!("{a}{b}") || logs == format!("{b}{a}"),
        "{logs:?}"
    );
    assert!(dir.path().join("logs/done.log.0").exists());
}

#[test]
fn failures_and_reported_results() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut failed = sh_job(&ctx, "failed", "exit 3", JobOptions::default());
    assert!(failed.start().success);
    let mut reported = sh_job(
        &ctx,
        "reported",
        r#"printf '[false, "custom message"]' > "$MRSM_JOB_RESULT_PATH""#,
        JobOptions::default(),
    );
    assert!(reported.start().success);
    let mut env = IndexMap::new();
    env.insert("FOO".to_string(), "bar".to_string());
    let mut with_env = sh_job(
        &ctx,
        "with-env",
        r#"echo "FOO=$FOO ID=$MRSM_DAEMON_ID LINES=$LINES"; sleep 0.2"#,
        JobOptions {
            env: Some(env),
            ..Default::default()
        },
    );
    assert!(with_env.start().success);

    wait_stopped(&mut failed);
    wait_stopped(&mut reported);
    wait_stopped(&mut with_env);
    assert_eq!(failed.result().message, "Returned exit code: 3");
    assert!(!failed.result().success);
    assert_eq!(reported.result().message, "custom message");
    assert!(!dir.path().join("jobs/reported/.result.json").exists());
    let logs = with_env.get_logs().unwrap();
    assert!(logs.contains("FOO=bar ID=with-env LINES=40"), "{logs:?}");
    assert_eq!(with_env.env().get("FOO").map(String::as_str), Some("bar"));
}

#[test]
fn pause_resume_stop_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    // `exec` so that SIGINT reaches the process doing the work (a non-interactive `sh` defers
    // SIGINT while it waits on a foreground child, and `stop` would fall back to killing it).
    let script = "echo started; exec sleep 30";
    let mut job = sh_job(&ctx, "looper", script, JobOptions::default());
    assert!(job.start().success);
    assert!(common::wait_for(10.0, || job.status() == Status::Running
        && job.daemon().paths().pid.exists()));
    let pid = job.pid().unwrap();
    assert!(job.start().message.ends_with("is already running."));

    let paused = job.pause(None);
    assert!(paused.success, "{paused}");
    assert_eq!(job.status(), Status::Paused);
    assert!(job.paused().is_some());
    let stop = job.daemon().read_stop_file().unwrap();
    assert_eq!(stop.get("action").and_then(Value::as_str), Some("pause"));
    assert!(
        stop.get("stop_time")
            .and_then(Value::as_str)
            .unwrap()
            .ends_with("+00:00")
    );

    // `start()` resumes a paused job.
    let resumed = job.start();
    assert!(resumed.success, "{resumed}");
    assert_eq!(job.status(), Status::Running);
    assert_eq!(job.pid(), Some(pid));
    assert!(job.daemon().read_stop_file().is_none());

    let stopped = job.stop(None);
    assert!(stopped.success, "{stopped}");
    assert_eq!(stopped.message, format!("Stopped {job}."));
    wait_stopped(&mut job);
    assert_eq!(
        job.daemon()
            .read_stop_file()
            .unwrap()
            .get("action")
            .and_then(Value::as_str),
        Some("quit")
    );
    assert!(job.stop_time().is_some());
    assert_eq!(job.stop(None).message, format!("{job} is not running."));

    let restarted = job.restart(None);
    assert!(restarted.success, "{restarted}");
    assert!(common::wait_for(10.0, || job.status() == Status::Running));
    assert!(job.restart(Some(Seconds::Int(4))).success);
    assert!(common::wait_for(10.0, || job.status() == Status::Running));
    assert!(job.delete().success);
    assert!(!dir.path().join("jobs/looper").exists());
}

#[test]
fn stopping_a_paused_job_kills_it() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(
        &ctx,
        "frozen",
        "while true; do sleep 0.05; done",
        JobOptions::default(),
    );
    assert!(job.start().success);
    assert!(common::wait_for(10.0, || job.daemon().paths().pid.exists()));
    assert!(job.pause(None).success);
    let stopped = job.stop(Some(Seconds::Int(5)));
    assert!(stopped.success, "{stopped}");
    assert!(common::wait_for(10.0, || job.status() == Status::Stopped));
    // PARITY: like a SIGKILLed Python daemon, no result and no `ended` are recorded.
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert_eq!(job.result().message, "No result available.");
    assert!(job.ended().is_none());
    assert_eq!(
        job.daemon()
            .read_stop_file()
            .unwrap()
            .get("action")
            .and_then(Value::as_str),
        Some("kill")
    );
}

#[test]
fn stdin_and_monitor_logs() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(
        &ctx,
        "reader",
        "echo waiting; read line; echo \"got $line\"; sleep 0.2",
        JobOptions::default(),
    );
    assert!(job.start().success);
    assert!(common::wait_for(10.0, || job
        .get_logs()
        .unwrap_or_default()
        .contains("waiting")));
    job.write_stdin("hi there\n").unwrap();

    let lines = Arc::new(Mutex::new(Vec::<String>::new()));
    let results = Arc::new(Mutex::new(Vec::new()));
    let (l2, r2) = (lines.clone(), results.clone());
    let monitored = job.monitor_logs(
        move |line| {
            l2.lock().unwrap().push(line.to_string());
            MonitorControl::Continue
        },
        MonitorOptions {
            stop_on_exit: true,
            strip_timestamps: true,
            accept_input: false,
            on_stop: Some(Box::new(move |r| r2.lock().unwrap().push(r.clone()))),
            ..Default::default()
        },
    );
    assert!(monitored.success);
    let text = lines.lock().unwrap().concat();
    assert!(
        text.contains("waiting\n") && text.contains("got hi there\n"),
        "{text:?}"
    );
    assert!(!text.contains(" | "), "{text:?}");
    assert_eq!(results.lock().unwrap().len(), 1);
    assert!(results.lock().unwrap()[0].success);
}

#[test]
fn log_rotation_and_refresh_ticks() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut logs = Map::new();
    logs.insert("max_file_size".into(), Value::Int(200));
    logs.insert("num_files_to_keep".into(), Value::Int(2));
    logs.insert("refresh_files_seconds".into(), Value::Float(0.2));
    let mut props = Map::new();
    props.insert("logs".into(), Value::Map(logs));
    let mut job = sh_job(
        &ctx,
        "chatty",
        "for i in 1 2 3 4 5 6 7 8 9 10 11 12; do echo \"line $i of output\"; sleep 0.1; done",
        JobOptions {
            properties: Some(props),
            ..Default::default()
        },
    );
    assert!(job.start().success);
    wait_stopped(&mut job);
    let mut subfiles: Vec<String> = fs::read_dir(dir.path().join("logs"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    subfiles.sort();
    assert_eq!(subfiles.len(), 2, "{subfiles:?}");
    assert!(subfiles.iter().all(|f| f.starts_with("chatty.log.")));
    assert!(!subfiles.contains(&"chatty.log.0".to_string()));
    // Python reinstalls the interceptors on every refresh tick: the next chunk after a tick
    // starts with an extra newline (a blank, timestamped line).
    let text = common::normalize_log_timestamps(&job.get_logs().unwrap());
    let blank = "\n2000-01-01 00:00 | \n2000-01-01 00:00 | line";
    assert!(text.matches(blank).count() >= 2, "{text:?}");
}

#[test]
fn delete_after_completion_removes_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(
        &ctx,
        "ephemeral",
        "echo bye",
        JobOptions {
            delete_after_completion: true,
            ..Default::default()
        },
    );
    assert!(job.start().success);
    assert!(common::wait_for(20.0, || !dir
        .path()
        .join("jobs/ephemeral")
        .exists()));
    assert!(common::wait_for(5.0, || fs::read_dir(
        dir.path().join("logs")
    )
    .map(|d| d.count() == 0)
    .unwrap_or(true)));
}

#[test]
fn executors() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(&ctx, "listed", "true", JobOptions::default());
    assert!(job.start().success);
    wait_stopped(&mut job);
    let jobs = meerschaum_jobs::get_jobs(&ctx, false);
    assert_eq!(jobs.keys().collect::<Vec<_>>(), ["listed"]);
    let local = meerschaum_jobs::executor::make_executor("local", &ctx);
    assert!(local.get_job_exists("listed"));
    assert_eq!(local.get_job_status("listed"), "stopped");
    let meta = local.get_job_metadata("listed");
    assert_eq!(meta.get("restart"), Some(&Value::Bool(false)));

    let mut api_job = Job::new(
        "remote",
        Some(vec!["show".into(), "version".into()]),
        JobOptions {
            executor_keys: Some("api:main".into()),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    // No jobs client is injected into this context: an explicit error, never a silent success.
    let r = api_job.start();
    assert!(!r.success);
    assert_eq!(
        r.message,
        "No jobs client is available for the 'api:main' executor."
    );
    // The systemd executor runs `systemctl` through the context's runner; this context uses a
    // fake, so the host's services are never touched.
    let fake = std::sync::Arc::new(meerschaum_jobs::FakeSystemctl::new());
    let ctx = ctx.with_process_runner(fake.clone());
    let systemd = meerschaum_jobs::executor::make_executor("systemd", &ctx);
    let stopped = systemd.stop_job("x");
    // The unit was never loaded, so it is already stopped (v4 reports success then).
    assert!(stopped.success);
    // A scratch root's units are scoped by a hash of the root (`mrsm-<hash>.x.service`).
    let service = meerschaum_jobs::systemd::SystemdExecutor::new(ctx.clone()).get_service_name("x");
    assert!(
        service.starts_with("mrsm-") && service.ends_with(".x.service"),
        "{service}"
    );
    assert_eq!(
        fake.rendered_calls().first().cloned(),
        Some(format!("kill -s SIGINT {service}"))
    );
}

/// The supervisor hands the worker its heartbeat file (`MRSM_JOB_LAPS_PATH`, in the job's
/// directory), and `Job::health` reads what the worker records there.
#[test]
fn a_worker_records_its_laps_and_the_job_reports_its_health() {
    use meerschaum_jobs::health::{HealthState, Heartbeat, LAPS_FILE_NAME, read_record};
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    // The worker shows where its heartbeats go, then waits to be stopped.
    let mut job = sh_job(
        &ctx,
        "laps",
        "echo \"laps=$MRSM_JOB_LAPS_PATH\"; sleep 30",
        JobOptions::default(),
    );
    // Not running, nothing recorded.
    assert_eq!(job.health().state, HealthState::Stopped);
    assert!(job.start().success);
    let path = dir.path().join("jobs").join("laps").join(LAPS_FILE_NAME);
    let expected = format!("laps={}", path.display());
    assert!(
        common::wait_for(20.0, || job
            .get_logs()
            .is_some_and(|logs| logs.contains(&expected))),
        "{:?}",
        job.get_logs()
    );
    // Running, without a heartbeat (a job that does not lap): unknown.
    assert!(common::wait_for(10.0, || job.status() == Status::Running));
    let unknown = job.health();
    assert_eq!(unknown.state, HealthState::Unknown);
    assert_eq!(unknown.message, "no laps recorded");

    // What a looping worker writes (here from the test, at the path the worker was given).
    {
        let mut heartbeat = Heartbeat::at(Some(path.clone()), Some(60.0), "sync");
        heartbeat.lap_started();
        heartbeat.lap_finished(true, "Synced 3 pipes.");
    }
    let healthy = job.health();
    assert_eq!(healthy.state, HealthState::Healthy, "{healthy:?}");
    assert_eq!(healthy.message, "last good sync just now");
    assert_eq!(healthy.expected_interval_seconds, Some(60.0));
    {
        let mut heartbeat = Heartbeat::at(Some(path.clone()), Some(60.0), "sync");
        heartbeat.lap_started();
        heartbeat.lap_finished(false, "Failed to sync.");
        heartbeat.lap_started();
        heartbeat.lap_finished(false, "Failed to sync.");
    }
    let failing = job.health();
    assert_eq!(failing.state, HealthState::Failing);
    assert_eq!(failing.consecutive_failures, 2);
    assert!(
        failing.message.starts_with("last 2 laps failed"),
        "{failing:?}"
    );
    assert_eq!(read_record(&path).unwrap().laps, 3);

    assert!(job.stop(None).success);
    wait_stopped(&mut job);
    let stopped = job.health();
    assert_eq!(stopped.state, HealthState::Stopped);
    assert_eq!(stopped.consecutive_failures, 2);
    // Deleting the job deletes its heartbeats with it.
    assert!(job.delete().success);
    assert!(!path.exists());
}
