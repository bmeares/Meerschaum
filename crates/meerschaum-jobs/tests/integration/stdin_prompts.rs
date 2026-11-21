//! A job's prompts: the supervisor keeps `input.stdin.block` while any process of the job waits
//! for input (`crate::stdin_watch`), so `is_blocking_on_stdin()` and the monitors answer it.
//! Real jobs through the detached supervisor (`/bin/sh <sysargs>` as the worker).

use crate::common;
use std::sync::{Arc, Mutex};

use meerschaum_jobs::Status;
use meerschaum_jobs::job::{Job, JobOptions, MonitorControl, MonitorOptions};

fn sh_job(ctx: &meerschaum_jobs::JobsContext, name: &str, script: &str) -> Job {
    Job::new(
        name,
        Some(vec!["-c".into(), script.into()]),
        JobOptions::default(),
        ctx,
    )
    .unwrap()
}

fn marker(root: &std::path::Path, name: &str) -> std::path::PathBuf {
    root.join("jobs").join(name).join("input.stdin.block")
}

fn wait_blocking(job: &mut Job) {
    assert!(
        common::wait_for(15.0, || job.is_blocking_on_stdin()),
        "the job never waited for input: {:?}",
        job.get_logs()
    );
}

fn wait_finished(job: &mut Job) -> String {
    assert!(
        common::wait_for(20.0, || job.status() == Status::Stopped
            && job
                .daemon()
                .properties()
                .get("result")
                .is_some_and(|r| !r.is_null())),
        "job did not finish: {:?}",
        job.get_logs()
    );
    job.get_logs().unwrap_or_default()
}

fn python3() -> bool {
    std::process::Command::new("python3")
        .args(["-c", "pass"])
        .status()
        .is_ok_and(|s| s.success())
}

/// A job whose first act is a silent `read` (nothing printed) waits, and is answered.
#[test]
fn a_silent_shell_read_is_seen_and_answered() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(&ctx, "silent", "read line; echo \"got [$line]\"");
    assert!(job.start().success);
    wait_blocking(&mut job);
    assert!(marker(dir.path(), "silent").exists());
    job.write_stdin("hi there\n").unwrap();
    let logs = wait_finished(&mut job);
    assert!(logs.contains("got [hi there]"), "{logs:?}");
    assert!(!marker(dir.path(), "silent").exists());
    assert!(!job.is_blocking_on_stdin());
}

/// `monitor_logs(accept_input=True)` (what `attach` and the websocket do) answers a silent read
/// and then a second prompt, with nothing printed before the first.
#[test]
fn the_monitor_answers_two_prompts_in_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(
        &ctx,
        "two",
        "read a; echo \"first [$a]\"; read b; echo \"second [$b]\"",
    );
    assert!(job.start().success);
    let answers = Arc::new(Mutex::new(vec!["two\n".to_string(), "one\n".to_string()]));
    let asked = Arc::new(Mutex::new(0usize));
    let lines = Arc::new(Mutex::new(String::new()));
    let (a2, k2, l2) = (answers.clone(), asked.clone(), lines.clone());
    let monitored = job.monitor_logs(
        move |line| {
            l2.lock().unwrap().push_str(line);
            MonitorControl::Continue
        },
        MonitorOptions {
            stop_on_exit: true,
            strip_timestamps: true,
            accept_input: true,
            input: Some(Box::new(move || {
                *k2.lock().unwrap() += 1;
                a2.lock().unwrap().pop()
            })),
            ..Default::default()
        },
    );
    assert!(monitored.success);
    let text = lines.lock().unwrap().clone();
    assert!(
        text.contains("first [one]") && text.contains("second [two]"),
        "{text:?}"
    );
    // Each prompt was asked once: the marker went away as soon as an answer was read.
    assert_eq!(*asked.lock().unwrap(), 2);
}

/// A Python `input()` and a read by a child of the job's process are seen too.
#[test]
fn a_python_input_and_a_childs_read_are_seen() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut child = sh_job(
        &ctx,
        "child",
        "sh -c 'read x; echo \"child got [$x]\"'; echo done",
    );
    assert!(child.start().success);
    wait_blocking(&mut child);
    child.write_stdin("ok\n").unwrap();
    let logs = wait_finished(&mut child);
    assert!(logs.contains("child got [ok]"), "{logs:?}");

    if !python3() {
        eprintln!("python3 is not available: skipping the Python input().");
        return;
    }
    let mut py = sh_job(
        &ctx,
        "py",
        "python3 -c 'print(\"python got\", repr(input()), flush=True)'",
    );
    assert!(py.start().success);
    wait_blocking(&mut py);
    py.write_stdin("answer\n").unwrap();
    let logs = wait_finished(&mut py);
    assert!(logs.contains("python got 'answer'"), "{logs:?}");
}

/// A job which ends while it waits (nobody attached) leaves no marker behind, and a job that
/// never reads its input is never reported as waiting.
#[test]
fn a_job_that_ends_while_waiting_waits_no_more() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = common::sh_context(dir.path());
    let mut job = sh_job(&ctx, "ends", "read line");
    assert!(job.start().success);
    wait_blocking(&mut job);
    assert!(job.stop(None).success);
    let _ = wait_finished(&mut job);
    assert!(!marker(dir.path(), "ends").exists());
    assert!(!job.is_blocking_on_stdin());

    let mut busy = sh_job(&ctx, "busy", "echo working; sleep 3; echo done");
    assert!(busy.start().success);
    let mut ever = false;
    let _ = common::wait_for(2.5, || {
        ever |= busy.is_blocking_on_stdin() || marker(dir.path(), "busy").exists();
        false
    });
    assert!(!ever, "a job that never reads was reported waiting");
    let _ = wait_finished(&mut busy);
}
