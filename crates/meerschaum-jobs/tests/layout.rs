//! Jobs created by Python 4.1 (`tests/fixtures/jobs.json`, from `gen_jobs.py`) are read by Rust
//! with the same state, and the `RotatingFile`, interceptor and systemd outputs match.
#![cfg(unix)]

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::{Child, Command};

use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::daemon::Daemon;
use meerschaum_jobs::job::Job;
use meerschaum_jobs::rotating::{Interceptor, RotatingFile, RotatingFileOptions};
use meerschaum_jobs::systemd::SystemdExecutor;
use meerschaum_jobs::{JobsContext, Status};

/// A process standing in for a job's daemon (`{PID}`), stopped for paused jobs.
struct Placeholder {
    child: Child,
}

impl Placeholder {
    fn spawn(paused: bool) -> Placeholder {
        let child = Command::new("sleep").arg("60").spawn().expect("sleep");
        if paused {
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(child.id() as i32),
                nix::sys::signal::Signal::SIGSTOP,
            )
            .unwrap();
            assert!(common::wait_for(5.0, || {
                meerschaum_jobs::process::proc_state(child.id() as i32)
                    == meerschaum_jobs::process::ProcState::Stopped
            }));
        }
        Placeholder { child }
    }
}

impl Drop for Placeholder {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Write a scenario's files under `root`; returns the job name and the placeholder process.
fn materialize(root: &Path, files: &Map, status: &str) -> (String, Option<Placeholder>) {
    let mut name = String::new();
    let mut placeholder = None;
    for (rel, entry) in files {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        if let Some(n) = rel.strip_prefix("jobs/") {
            name = n.split('/').next().unwrap().to_string();
        }
        let typ = entry.get("type").and_then(Value::as_str).unwrap();
        match typ {
            "fifo" => meerschaum_jobs::stdin::ensure_fifo(&path).unwrap(),
            "pickle" => fs::write(&path, b"").unwrap(),
            "pid" => {
                let p = Placeholder::spawn(status == "paused");
                fs::write(&path, p.child.id().to_string()).unwrap();
                placeholder = Some(p);
            }
            _ => fs::write(&path, entry.get("text").and_then(Value::as_str).unwrap()).unwrap(),
        }
    }
    (name, placeholder)
}

fn opt_str(v: Option<String>) -> Value {
    v.map(Value::Str).unwrap_or(Value::Null)
}

fn rust_state(job: &mut Job) -> BTreeMap<&'static str, Value> {
    let mut s = BTreeMap::new();
    s.insert("status", Value::Str(job.status().as_str().into()));
    s.insert(
        "pid",
        if job.pid().is_some() {
            Value::Str("{PID}".into())
        } else {
            Value::Null
        },
    );
    s.insert(
        "sysargs",
        Value::List(job.sysargs().into_iter().map(Value::Str).collect()),
    );
    s.insert("executor_keys", Value::Str(job.executor_keys.clone()));
    let r = job.result();
    s.insert(
        "result",
        Value::List(vec![Value::Bool(r.success), Value::Str(r.message)]),
    );
    s.insert("logs", opt_str(job.get_logs()));
    s.insert("label", Value::Str(job.label()));
    s.insert("daemon_label", Value::Str(job.daemon().label().to_string()));
    s.insert("restart", Value::Bool(job.restarts()));
    s.insert("began", opt_str(job.began().map(|d| d.isoformat())));
    s.insert("paused", opt_str(job.paused().map(|d| d.isoformat())));
    s.insert("ended", opt_str(job.ended().map(|d| d.isoformat())));
    s.insert("stop_time", opt_str(job.stop_time().map(|d| d.isoformat())));
    s.insert("hidden", Value::Bool(job.hidden()));
    s.insert("exists", Value::Bool(job.exists()));
    s.insert(
        "env",
        Value::Map(
            job.env()
                .into_iter()
                .map(|(k, v)| (k, Value::Str(v)))
                .collect(),
        ),
    );
    s.insert(
        "delete_after_completion",
        Value::Bool(job.delete_after_completion()),
    );
    s.insert(
        "is_blocking_on_stdin",
        Value::Bool(job.is_blocking_on_stdin()),
    );
    s.insert("str", Value::Str(job.to_string()));
    s.insert("properties", Value::Map(job.daemon().properties()));
    s
}

#[test]
fn python_jobs_are_read_identically() {
    let fixture = common::load_fixture("jobs.json");
    let scenarios = fixture.get("scenarios").and_then(Value::as_map).unwrap();
    assert!(scenarios.len() >= 8);
    for (scenario, data) in scenarios {
        let dir = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(dir.path());
        let expected = data.get("state").and_then(Value::as_map).unwrap();
        let status = expected.get("status").and_then(Value::as_str).unwrap();
        let files = data.get("files").and_then(Value::as_map).unwrap();
        let (name, _placeholder) = materialize(dir.path(), files, status);

        // The files themselves: the snapshot round-trips through Rust's properties writer.
        let props_rel = format!("jobs/{name}/properties.json");
        let raw = files[props_rel.as_str()]
            .get("text")
            .and_then(Value::as_str)
            .unwrap();
        let parsed = Value::from_json_str(raw).unwrap();
        assert_eq!(
            meerschaum_jobs::properties::dumps(&parsed),
            raw,
            "{scenario}"
        );

        let mut job = Job::open(&name, &ctx).unwrap();
        let actual = rust_state(&mut job);
        for (key, value) in &actual {
            assert_eq!(
                Some(value),
                expected.get(*key),
                "scenario '{scenario}', key '{key}'"
            );
        }
    }
}

#[test]
fn readlines_persists_the_offset() {
    let fixture = common::load_fixture("jobs.json");
    let case = fixture.get("readlines").unwrap();
    let name = case.get("name").and_then(Value::as_str).unwrap();
    let files = fixture
        .get_path(&["scenarios", name, "files"])
        .and_then(Value::as_map)
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    materialize(dir.path(), files, "stopped");
    let mut daemon = Daemon::open(name, &ctx).unwrap();
    let lines: Vec<Value> = daemon.readlines().into_iter().map(Value::Str).collect();
    assert_eq!(Some(&Value::List(lines)), case.get("lines"));
    let offset =
        fs::read_to_string(dir.path().join("logs").join(format!(".{name}.log.offset"))).unwrap();
    assert_eq!(
        Some(offset.as_str()),
        case.get("offset").and_then(Value::as_str)
    );
    let again: Vec<Value> = daemon.readlines().into_iter().map(Value::Str).collect();
    assert_eq!(Some(&Value::List(again)), case.get("again"));
}

#[test]
fn delete_removes_the_layout() {
    let fixture = common::load_fixture("jobs.json");
    let case = fixture.get("deleted").unwrap();
    let name = case.get("name").and_then(Value::as_str).unwrap();
    let files = fixture
        .get_path(&["scenarios", name, "files"])
        .and_then(Value::as_map)
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    materialize(dir.path(), files, "stopped");
    // A previous `readlines()` leaves an offset file; `delete` removes it too.
    Daemon::open(name, &ctx).unwrap().readlines();
    let mut job = Job::open(name, &ctx).unwrap();
    let result = job.delete();
    assert_eq!(
        Value::List(vec![
            Value::Bool(result.success),
            Value::Str(result.message)
        ]),
        *case.get("result").unwrap()
    );
    assert!(!dir.path().join("jobs").join(name).exists());
    let leftover: Vec<_> = fs::read_dir(dir.path().join("logs"))
        .unwrap()
        .flatten()
        .collect();
    assert!(leftover.is_empty(), "{leftover:?}");
    assert!(!job.exists());
}

fn files_in(dir: &Path) -> Value {
    let mut names: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    Value::Map(
        names
            .into_iter()
            .map(|n| {
                let text = fs::read_to_string(dir.join(&n)).unwrap();
                (n, Value::Str(text))
            })
            .collect(),
    )
}

#[test]
fn rotating_file_sessions_match() {
    let fixture = common::load_fixture("jobs.json");
    for session in fixture.get("rotating").and_then(Value::as_list).unwrap() {
        let name = session.get("name").and_then(Value::as_str).unwrap();
        let kw = session.get("kwargs").unwrap();
        let opts = RotatingFileOptions {
            num_files_to_keep: kw.get("num_files_to_keep").and_then(Value::as_i64).unwrap(),
            max_file_size: kw.get("max_file_size").and_then(Value::as_i64).unwrap(),
            write_timestamps: kw.get("write_timestamps").is_some_and(Value::truthy),
            timestamp_format: "%Y-%m-%d %H:%M".into(),
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("{name}.log"));
        let fresh = || {
            RotatingFile::new(&path, opts.clone())
                .unwrap()
                .with_fixed_prefix("2000-01-01 00:00 | ")
        };
        let mut rf = fresh();
        for (i, step) in session
            .get("ops")
            .and_then(Value::as_list)
            .unwrap()
            .iter()
            .enumerate()
        {
            let op = step.get("op").and_then(Value::as_list).unwrap();
            let kind = op[0].as_str().unwrap();
            let value = match kind {
                "write" => {
                    rf.write(op[1].as_str().unwrap()).unwrap();
                    Value::Null
                }
                "read" => Value::Str(rf.read()),
                "readlines" => Value::List(rf.readlines().into_iter().map(Value::Str).collect()),
                "seek" => {
                    rf.seek(op[1].as_i64().unwrap() as u64);
                    Value::Null
                }
                "fresh_read" => Value::Str(fresh().read()),
                "fresh_readlines" => {
                    Value::List(fresh().readlines().into_iter().map(Value::Str).collect())
                }
                "close" => {
                    rf.close();
                    Value::Null
                }
                other => panic!("unknown op {other}"),
            };
            let ctx = format!("session '{name}', op #{i} {op:?}");
            assert_eq!(Some(&value), step.get("value"), "{ctx}: value");
            let (ix, pos) = rf.cursor();
            assert_eq!(
                Some(&Value::List(vec![Value::Int(ix), Value::Int(pos as i64)])),
                step.get("cursor"),
                "{ctx}: cursor"
            );
            assert_eq!(
                Some(&files_in(dir.path())),
                step.get("files"),
                "{ctx}: files"
            );
        }
    }
}

#[test]
fn interceptor_transform_matches() {
    let fixture = common::load_fixture("jobs.json");
    for case in fixture.get("interceptor").and_then(Value::as_list).unwrap() {
        let mut i = Interceptor::new();
        let mut out = Vec::new();
        for chunk in case.get("chunks").and_then(Value::as_list).unwrap() {
            out.extend(i.transform(chunk.as_str().unwrap().as_bytes(), "2000-01-01 00:00 | "));
        }
        assert_eq!(
            Some(String::from_utf8(out).unwrap().as_str()),
            case.get("output").and_then(Value::as_str)
        );
    }
}

#[test]
fn strip_timestamp_matches() {
    let fixture = common::load_fixture("jobs.json");
    for case in fixture
        .get("strip_timestamp")
        .and_then(Value::as_list)
        .unwrap()
    {
        let line = case.get("line").and_then(Value::as_str).unwrap();
        assert_eq!(
            meerschaum_jobs::pytime::strip_timestamp_from_line(line, "%Y-%m-%d %H:%M"),
            case.get("stripped").and_then(Value::as_str).unwrap(),
            "{line:?}"
        );
    }
}

#[test]
fn systemd_units_match() {
    let fixture = common::load_fixture("jobs.json");
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().to_string_lossy().to_string();
    let mut ctx = JobsContext::for_root(dir.path().join("root"));
    ctx.paths.systemd_user_dir = dir.path().join("userhome/.config/systemd/user");
    let executor = SystemdExecutor::new(ctx.clone());
    let process_env: Vec<(String, String)> = vec![
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("HOME".into(), format!("{tmp}/userhome")),
        ("MRSM_ROOT_DIR".into(), format!("{tmp}/root")),
        ("MRSM_PLUGINS_DIR".into(), format!("{tmp}/plugins")),
        ("MRSM_VENVS_DIR".into(), format!("{tmp}/venvs")),
        ("MRSM_NO_AUTO_INSTALL".into(), "1".into()),
        ("PYTHONHASHSEED".into(), "0".into()),
    ];
    let norm = |s: String| s.replace(&tmp, "{TMP}");
    for case in fixture.get("systemd").and_then(Value::as_list).unwrap() {
        let name = case.get("name").and_then(Value::as_str).unwrap();
        let sysargs: Vec<String> = case
            .get("sysargs")
            .and_then(Value::as_list)
            .unwrap()
            .iter()
            .map(Value::py_str)
            .collect();
        if let Some(props) = case.get("properties").and_then(Value::as_map) {
            let mut hidden = executor
                .get_hidden_job(name, Some(sysargs.clone()), Some(props.clone()))
                .unwrap();
            hidden.set_externally_managed();
            assert!(hidden.daemon().write_properties().success);
        }
        let expect = |k: &str| case.get(k).and_then(Value::as_str).unwrap().to_string();
        assert_eq!(executor.get_service_name(name), expect("service_name"));
        let posix = |p: std::path::PathBuf| norm(p.to_string_lossy().to_string());
        assert_eq!(
            posix(executor.get_service_file_path(name)),
            expect("service_file_path")
        );
        assert_eq!(
            posix(executor.get_service_symlink_file_path(name)),
            expect("symlink_path")
        );
        assert_eq!(
            posix(executor.get_service_logs_path(name)),
            expect("logs_path")
        );
        assert_eq!(posix(executor.get_socket_path(name)), expect("socket_path"));
        assert_eq!(posix(executor.get_result_path(name)), expect("result_path"));
        // v5 pins the workers' interpreter with `Environment=MRSM_PYTHON=...` (tested in
        // `systemd_executor.rs`) and spaces restarts with `RestartSec=5` (D23, PARITY.md
        // W18-JB-001); v4 has neither line.
        let unit = norm(executor.get_service_file_text(name, &sysargs, "{PYTHON}", &process_env))
            .split('\n')
            .filter(|l| !l.starts_with("Environment=MRSM_PYTHON=") && *l != "RestartSec=5")
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(unit, expect("unit"), "{name}");
        assert_eq!(norm(executor.get_socket_file_text(name)), expect("socket"));
        let parsed: Vec<Value> = meerschaum_jobs::systemd::parse_exec_start_sysargs(&unit)
            .into_iter()
            .map(Value::Str)
            .collect();
        assert_eq!(Some(&Value::List(parsed)), case.get("parsed_sysargs"));
    }
}

#[test]
fn placeholder_statuses() {
    // Sanity check of the status mapping the scenarios rely on.
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    let jobs_dir = dir.path().join("jobs").join("p");
    fs::create_dir_all(&jobs_dir).unwrap();
    fs::write(jobs_dir.join("properties.json"), r#"{"label": "x"}"#).unwrap();
    let p = Placeholder::spawn(true);
    fs::write(jobs_dir.join("process.pid"), p.child.id().to_string()).unwrap();
    assert_eq!(Daemon::open("p", &ctx).unwrap().status(), Status::Paused);
    drop(p);
    assert!(common::wait_for(5.0, || Daemon::open("p", &ctx)
        .unwrap()
        .status()
        == Status::Stopped));
    assert!(!jobs_dir.join("process.pid").exists());
}
