//! Jobs created by Rust are read by the Python 4.1 `Job` class with the same state (and Python
//! can stop a Rust-supervised job).
//!
//! Needs the parity reference interpreter (`scripts/parity/setup_reference.sh`) and the
//! `test_root/venvs/mrsm` venv (Python's job code imports `dill` and `psutil` from it); the test
//! is skipped with a notice when either is missing.
#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use meerschaum_core::value::Value;
use meerschaum_jobs::job::{Job, JobOptions};
use meerschaum_jobs::{JobsContext, Status};

fn reference_python() -> Option<(PathBuf, PathBuf)> {
    let repo = common::repo_root();
    let python = repo.join(".venv-ref").join("bin").join("python");
    let mrsm_venv = repo.join("test_root").join("venvs").join("mrsm");
    (python.exists() && mrsm_venv.join("lib").exists()).then_some((python, mrsm_venv))
}

fn python_view(python: &Path, base: &Path, args: &[&str]) -> Value {
    let script = common::fixtures_dir().join("read_jobs.py");
    let out = Command::new(python)
        .arg("-s")
        .arg(&script)
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", base.join("userhome"))
        .env("MRSM_ROOT_DIR", base.join("root"))
        .env("MRSM_PLUGINS_DIR", base.join("plugins"))
        .env("MRSM_VENVS_DIR", base.join("venvs"))
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("PYTHONHASHSEED", "0")
        .current_dir(base.join("work"))
        .output()
        .expect("python");
    assert!(
        out.status.success(),
        "read_jobs.py failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    Value::from_json_str(&String::from_utf8_lossy(&out.stdout)).expect("JSON")
}

fn rust_view(job: &mut Job) -> Vec<(&'static str, Value)> {
    let opt = |v: Option<String>| v.map(Value::Str).unwrap_or(Value::Null);
    let r = job.result();
    vec![
        ("status", Value::Str(job.status().as_str().into())),
        (
            "pid",
            job.pid()
                .map(|p| Value::Int(p as i64))
                .unwrap_or(Value::Null),
        ),
        (
            "sysargs",
            Value::List(job.sysargs().into_iter().map(Value::Str).collect()),
        ),
        ("executor_keys", Value::Str(job.executor_keys.clone())),
        (
            "result",
            Value::List(vec![Value::Bool(r.success), Value::Str(r.message)]),
        ),
        ("logs", opt(job.get_logs())),
        ("label", Value::Str(job.label())),
        ("daemon_label", Value::Str(job.daemon().label().to_string())),
        ("restart", Value::Bool(job.restarts())),
        ("began", opt(job.began().map(|d| d.isoformat()))),
        ("paused", opt(job.paused().map(|d| d.isoformat()))),
        ("ended", opt(job.ended().map(|d| d.isoformat()))),
        ("stop_time", opt(job.stop_time().map(|d| d.isoformat()))),
        ("hidden", Value::Bool(job.hidden())),
        ("exists", Value::Bool(job.exists())),
        (
            "env",
            Value::Map(
                job.env()
                    .into_iter()
                    .map(|(k, v)| (k, Value::Str(v)))
                    .collect(),
            ),
        ),
        (
            "delete_after_completion",
            Value::Bool(job.delete_after_completion()),
        ),
        (
            "is_blocking_on_stdin",
            Value::Bool(job.is_blocking_on_stdin()),
        ),
        ("str", Value::Str(job.to_string())),
        ("properties", Value::Map(job.daemon().properties())),
    ]
}

fn sh_job(ctx: &JobsContext, name: &str, script: &str, opts: JobOptions) -> Job {
    let mut job = Job::new(name, Some(vec!["-c".into(), script.into()]), opts, ctx).unwrap();
    assert!(job.start().success);
    job
}

fn finished(job: &mut Job) -> bool {
    job.status() == Status::Stopped
        && job
            .daemon()
            .properties()
            .get("result")
            .is_some_and(|r| !r.is_null())
}

#[test]
fn python_reads_rust_jobs() {
    let Some((python, mrsm_venv)) = reference_python() else {
        eprintln!("skipping: no .venv-ref reference interpreter or test_root/venvs/mrsm");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    for sub in ["root", "plugins", "userhome", "venvs", "work"] {
        std::fs::create_dir_all(base.join(sub)).unwrap();
    }
    std::os::unix::fs::symlink(&mrsm_venv, base.join("venvs").join("mrsm")).unwrap();
    let ctx = common::sh_context(&base.join("root"));

    let mut env = indexmap::IndexMap::new();
    env.insert("FOO".to_string(), "bar".to_string());
    let mut jobs = [
        sh_job(
            &ctx,
            "rs-done",
            "echo hello; sleep 0.2",
            JobOptions::default(),
        ),
        sh_job(
            &ctx,
            "rs-failed",
            "echo failing >&2; exit 3",
            JobOptions::default(),
        ),
        sh_job(
            &ctx,
            "rs-env",
            "echo \"FOO=$FOO\"",
            JobOptions {
                env: Some(env),
                ..Default::default()
            },
        ),
        sh_job(
            &ctx,
            "rs-running",
            "echo up; exec sleep 60",
            JobOptions::default(),
        ),
        sh_job(
            &ctx,
            "rs-paused",
            "echo up; exec sleep 60",
            JobOptions::default(),
        ),
        sh_job(
            &ctx,
            "rs-stopped",
            "echo up; exec sleep 60",
            JobOptions::default(),
        ),
        sh_job(
            &ctx,
            "rs-python-stops",
            "echo up; exec sleep 60",
            JobOptions::default(),
        ),
    ];
    for job in jobs.iter_mut().take(3) {
        assert!(common::wait_for(20.0, || finished(job)), "{job}");
    }
    for job in jobs.iter_mut().skip(3) {
        assert!(common::wait_for(20.0, || job
            .get_logs()
            .unwrap_or_default()
            .contains("up")));
    }
    assert!(jobs[4].pause(None).success);
    assert!(jobs[5].stop(None).success);
    assert!(common::wait_for(20.0, || finished(&mut jobs[5])));

    let names = [
        "rs-done",
        "rs-failed",
        "rs-env",
        "rs-running",
        "rs-paused",
        "rs-stopped",
    ];
    let mut args = vec!["--stop", "rs-python-stops"];
    args.extend(names);
    let py = python_view(&python, base, &args);

    // Python stopped a Rust-supervised job with SIGINT; the supervisor recorded the result.
    let stopped = py.get_path(&["stopped", "rs-python-stops"]).unwrap();
    assert_eq!(
        stopped.as_list().unwrap()[0],
        Value::Bool(true),
        "{stopped:?}"
    );
    assert!(common::wait_for(20.0, || finished(&mut jobs[6])));
    assert_eq!(jobs[6].result().message, "KeyboardInterrupt");
    assert_eq!(
        jobs[6]
            .daemon()
            .read_stop_file()
            .unwrap()
            .get("action")
            .and_then(Value::as_str),
        Some("quit")
    );

    let mut mismatches = Vec::new();
    for name in names {
        let expected = py.get_path(&["jobs", name]).unwrap();
        let mut job = Job::open(name, &ctx).unwrap();
        for (key, value) in rust_view(&mut job) {
            if Some(&value) != expected.get(key) {
                mismatches.push(format!(
                    "{name}.{key}: rust {value:?} != python {:?}",
                    expected.get(key)
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));

    // Python left a `pickle.pkl` behind; Rust still reads the same job.
    assert!(base.join("root/jobs/rs-done/pickle.pkl").exists());
    let mut done = Job::open("rs-done", &ctx).unwrap();
    assert_eq!(done.result().message, "Success");

    for name in ["rs-running", "rs-paused"] {
        let mut job = Job::open(name, &ctx).unwrap();
        assert!(job.stop(Some(meerschaum_jobs::Seconds::Int(5))).success);
    }
    for name in names.iter().chain(["rs-python-stops"].iter()) {
        let mut job = Job::open(name, &ctx).unwrap();
        assert!(job.delete().success, "{name}");
    }
}
