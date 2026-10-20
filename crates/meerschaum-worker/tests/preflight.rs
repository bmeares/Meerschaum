//! A host whose Python cannot serve as a worker fails fast, once and clearly, and never hangs
//! (wave 18, FX2). The interpreters here are fake shell wrappers around the system `python3`
//! (`-S -s`: no site-packages, so no real `meerschaum` is ever found), and the `meerschaum` they
//! find is a stub package written to a temporary `PYTHONPATH`.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use meerschaum_worker::WorkerError;
use meerschaum_worker::pool::{PoolConfig, WorkerPool};
use meerschaum_worker::protocol::Msg;
use meerschaum_worker::worker::{CallOptions, Worker, WorkerOptions};

fn system_python() -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|d| d.join("python3"))
            .find(|p| p.is_file())
    })
}

macro_rules! require_python3 {
    () => {
        match system_python() {
            Some(p) => p,
            None => {
                eprintln!("skipped: no python3 on PATH");
                return;
            }
        }
    };
}

fn write_script(path: &Path, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A fake interpreter that records every start in `<dir>/starts` and runs the system python
/// without site-packages. `socket_branch` replaces the run when the arguments carry `--socket`
/// (the real worker start, after the preflight).
fn fake_python(dir: &Path, python: &Path, socket_branch: Option<&str>) -> PathBuf {
    let path = dir.join("fake-python");
    let socket = socket_branch
        .map(|b| format!("case \"$*\" in *--socket*) {b};; esac\n"))
        .unwrap_or_default();
    write_script(
        &path,
        &format!(
            "echo x >> '{starts}'\n{socket}exec '{python}' -S -s \"$@\"",
            starts = dir.join("starts").display(),
            python = python.display(),
        ),
    );
    path
}

fn starts(dir: &Path) -> usize {
    std::fs::read_to_string(dir.join("starts"))
        .map(|s| s.lines().count())
        .unwrap_or(0)
}

/// A stub `meerschaum` package in `<dir>/site`: a `config/_version.py`, and a `worker` module
/// when `protocol` is given.
fn stub_package(dir: &Path, version: &str, protocol: Option<u32>) -> PathBuf {
    let site = dir.join("site");
    let package = site.join("meerschaum");
    std::fs::create_dir_all(package.join("config")).unwrap();
    std::fs::write(
        package.join("__init__.py"),
        "raise SystemExit('the stub package must never be imported')\n",
    )
    .unwrap();
    std::fs::write(
        package.join("config").join("_version.py"),
        format!("__version__ = \"{version}\"\n"),
    )
    .unwrap();
    if let Some(protocol) = protocol {
        std::fs::create_dir_all(package.join("worker")).unwrap();
        std::fs::write(
            package.join("worker").join("__init__.py"),
            format!("PROTOCOL_VERSION = {protocol}\n"),
        )
        .unwrap();
    }
    site
}

fn options(python: PathBuf, pythonpath: Option<&Path>) -> WorkerOptions {
    let empty = std::env::temp_dir().join("w18fx2-no-such-dir");
    WorkerOptions {
        python,
        env: vec![(
            "PYTHONPATH".into(),
            pythonpath.unwrap_or(&empty).display().to_string(),
        )],
        quiet: true,
        startup_timeout: Duration::from_secs(2),
        preflight_timeout: Duration::from_secs(2),
        ..WorkerOptions::default()
    }
}

fn refused(result: Result<Worker, WorkerError>) -> String {
    match result {
        Err(WorkerError::Refused(message)) => message,
        Err(other) => panic!("expected a refusal, got: {other}"),
        Ok(_) => panic!("expected a refusal, but a worker started"),
    }
}

#[tokio::test]
async fn an_interpreter_without_meerschaum_is_one_clear_message() {
    let python = require_python3!();
    let dir = tempfile::tempdir().unwrap();
    let fake = fake_python(dir.path(), &python, None);
    let opts = options(fake, None);
    let t = Instant::now();
    let message = refused(Worker::spawn(&opts).await);
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    let version = meerschaum_core::python_guard::expected_version();
    assert!(
        message.contains("cannot import the `meerschaum` package"),
        "{message}"
    );
    // Names the interpreter (as the process sees itself: `sys.executable`, behind the wrapper),
    // the release it needs and the fix.
    assert!(message.contains(&python.display().to_string()), "{message}");
    assert!(
        message.contains(&format!("pip install 'meerschaum=={version}'")),
        "{message}"
    );
    assert!(message.contains("MRSM_PYTHON"), "{message}");
    // No worker was started: the interpreter ran once (the preflight), with no `--socket`.
    assert_eq!(starts(dir.path()), 1);
}

#[tokio::test]
async fn a_stale_v4_meerschaum_is_refused_with_its_version_and_path_and_asked_once() {
    let python = require_python3!();
    let dir = tempfile::tempdir().unwrap();
    let site = stub_package(dir.path(), "4.1.4", None);
    let fake = fake_python(dir.path(), &python, None);
    let opts = options(fake.clone(), Some(&site));
    let t = Instant::now();
    let message = refused(Worker::spawn(&opts).await);
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert!(message.contains("4.1.4"), "the found version: {message}");
    assert!(
        message.contains(&site.join("meerschaum").display().to_string()),
        "the found path: {message}"
    );
    assert!(
        message.contains("pip install 'meerschaum=="),
        "the fix: {message}"
    );
    assert!(message.contains("MRSM_PYTHON"), "{message}");
    assert_eq!(starts(dir.path()), 1);

    // Everyone after gets the same message at once, without starting the interpreter again.
    let t = Instant::now();
    for _ in 0..5 {
        assert_eq!(refused(Worker::spawn(&opts).await), message);
    }
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    assert_eq!(starts(dir.path()), 1, "the refusal is remembered");
}

#[tokio::test]
async fn a_pool_does_not_back_off_or_wait_on_a_refused_interpreter() {
    let python = require_python3!();
    let dir = tempfile::tempdir().unwrap();
    let site = stub_package(dir.path(), "4.1.4", None);
    let fake = fake_python(dir.path(), &python, None);
    // The default backoff (250 ms, doubling to 30 s) is what made N calls wait minutes.
    let pool = WorkerPool::new(PoolConfig {
        worker: options(fake, Some(&site)),
        ..PoolConfig::default()
    });
    let t = Instant::now();
    for _ in 0..12 {
        let error = pool
            .call("ping", Msg::new(), vec![], CallOptions::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(error, WorkerError::Refused(_)), "{error}");
        assert!(error.to_string().contains("4.1.4"), "{error}");
    }
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert_eq!(pool.stats().spawned, 0);
    assert_eq!(starts(dir.path()), 1);
}

#[tokio::test]
async fn an_interpreter_that_hangs_at_start_is_declared_hung_within_the_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let fake = dir.path().join("hangs");
    write_script(&fake, "exec sleep 1000");
    let mut opts = options(fake.clone(), None);
    opts.preflight_timeout = Duration::from_secs(1);
    let t = Instant::now();
    let message = refused(Worker::spawn(&opts).await);
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert!(message.contains(&fake.display().to_string()), "{message}");
    assert!(message.contains("did not answer"), "{message}");
    assert!(message.contains("MRSM_PYTHON"), "{message}");
}

#[tokio::test]
async fn an_interpreter_that_cannot_be_started_names_itself() {
    let opts = options(PathBuf::from("/w18fx2/no/such/python"), None);
    let message = refused(Worker::spawn(&opts).await);
    assert!(message.contains("/w18fx2/no/such/python"), "{message}");
    assert!(message.contains("could not be started"), "{message}");
    assert!(message.contains("MRSM_PYTHON"), "{message}");
}

#[tokio::test]
async fn a_worker_that_passes_the_check_but_never_connects_is_stopped_within_the_timeout() {
    let python = require_python3!();
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let version = meerschaum_core::python_guard::expected_version();
    let site = stub_package(
        dir.path(),
        &version,
        Some(meerschaum_core::python_guard::WORKER_PROTOCOL_VERSION),
    );
    let fake = fake_python(
        dir.path(),
        &python,
        Some(&format!(
            "echo $$ > '{}'; exec sleep 1000",
            pid_file.display()
        )),
    );
    let opts = options(fake.clone(), Some(&site));
    let t = Instant::now();
    let message = refused(Worker::spawn(&opts).await);
    let took = t.elapsed();
    assert!(
        took >= Duration::from_secs(2) && took < Duration::from_secs(8),
        "{took:?}"
    );
    assert!(message.contains(&fake.display().to_string()), "{message}");
    assert!(message.contains("did not connect within 2 s"), "{message}");
    // The version and path the preflight found are in it.
    assert!(
        message.contains(&format!("meerschaum {version} at")),
        "{message}"
    );
    assert!(message.contains("MRSM_PYTHON"), "{message}");
    // Nothing was left running.
    let pid: u32 = std::fs::read_to_string(&pid_file)
        .expect("the worker was started")
        .trim()
        .parse()
        .unwrap();
    let proc = PathBuf::from(format!("/proc/{pid}"));
    let gone = (0..40).any(|_| {
        std::thread::sleep(Duration::from_millis(50));
        !proc.exists()
    });
    assert!(
        gone || !Path::new("/proc/self").exists(),
        "the silent worker was not killed"
    );
    // And it is not tried again at once.
    let t = Instant::now();
    assert_eq!(refused(Worker::spawn(&opts).await), message);
    assert!(t.elapsed() < Duration::from_secs(1));
}
