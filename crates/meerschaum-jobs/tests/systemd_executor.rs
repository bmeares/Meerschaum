//! The `systemd` executor against the released `SystemdExecutor` (`systemd_executor.json`, from
//! `tests/fixtures/gen_systemd_units.py`) with a fake `systemctl`: command traces, files, status,
//! PID, timestamps, results, plus the behavior v4 has no fixture for (listing, ownership, logs).
//!
//! Nothing here runs `systemctl`: the context's process runner is [`FakeSystemctl`]. The one real
//! smoke test is `real_systemctl_smoke`, ignored unless `MRSM_TEST_SYSTEMD_SMOKE` is set; it only
//! asks `systemctl --user` for the state of a unit that does not exist.

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::config::Seconds;
use meerschaum_jobs::executor::{get_jobs, get_jobs_for, make_executor};
use meerschaum_jobs::runner::FakeService;
use meerschaum_jobs::systemd::SystemdExecutor;
use meerschaum_jobs::{FakeSystemctl, Job, JobExecutor, JobOptions, JobsContext, Launcher};

struct Fixture {
    _dir: tempfile::TempDir,
    tmp: String,
    fake: Arc<FakeSystemctl>,
    ctx: JobsContext,
    process_env: Vec<(String, String)>,
}

impl Fixture {
    fn new() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir
            .path()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let fake = Arc::new(FakeSystemctl::new());
        let mut ctx = JobsContext::for_root(Path::new(&tmp).join("root"));
        ctx.paths.systemd_user_dir = Path::new(&tmp).join("userhome/.config/systemd/user");
        ctx.config.timeout_seconds = Seconds::Float(0.3);
        ctx.config.check_timeout_interval_seconds = Seconds::Float(0.05);
        let ctx = ctx
            .with_launcher(Launcher {
                supervisor: vec!["unused".into()],
                program: vec!["{PYTHON}".into()],
            })
            .with_process_runner(fake.clone());
        // What the reference generator's child process exported.
        let config =
            r#"{"jobs": {"timeout_seconds": 0.3, "check_timeout_interval_seconds": 0.05}}"#;
        let process_env = vec![
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
            ("HOME".to_string(), format!("{tmp}/userhome")),
            ("MRSM_ROOT_DIR".to_string(), format!("{tmp}/root")),
            ("MRSM_PLUGINS_DIR".to_string(), format!("{tmp}/plugins")),
            ("MRSM_VENVS_DIR".to_string(), format!("{tmp}/venvs")),
            ("MRSM_NO_AUTO_INSTALL".to_string(), "1".to_string()),
            ("MRSM_CONFIG".to_string(), config.to_string()),
            ("PYTHONHASHSEED".to_string(), "0".to_string()),
            ("TZ".to_string(), "UTC".to_string()),
        ];
        Fixture {
            _dir: dir,
            tmp,
            fake,
            ctx,
            process_env,
        }
    }

    fn executor(&self) -> SystemdExecutor {
        SystemdExecutor::new(self.ctx.clone()).with_process_env(self.process_env.clone())
    }

    fn norm(&self, s: &str) -> String {
        // `ExecStart=` quotes the program like `shlex.join` does (the placeholder has braces).
        // `Environment=MRSM_PYTHON=...` is v5's addition to the unit (it pins the workers'
        // interpreter, see `the_unit_pins_the_workers_interpreter`); v4 has no such line.
        let without_pin: Vec<&str> = s
            .split('\n')
            .filter(|l| !l.starts_with("Environment=MRSM_PYTHON="))
            .collect();
        without_pin
            .join("\n")
            .replace(&self.tmp, "{TMP}")
            .replace("ExecStart='{PYTHON}'", "ExecStart={PYTHON}")
    }

    /// The files under the executor's directories, like the generator's `render_tree`.
    fn tree(&self) -> BTreeMap<String, Value> {
        let mut files = BTreeMap::new();
        let home = Path::new(&self.tmp).join("userhome/.config/systemd/user");
        let roots = [
            (Path::new(&self.tmp).join("root/systemd"), "root/systemd"),
            (Path::new(&self.tmp).join("root/jobs"), "root/jobs"),
            (home, "home/systemd/user"),
        ];
        for (base, label) in roots {
            self.walk(&base, &base, label, &mut files);
        }
        files
    }

    fn walk(&self, base: &Path, dir: &Path, label: &str, out: &mut BTreeMap<String, Value>) {
        use std::os::unix::fs::FileTypeExt;
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            let rel = format!(
                "{label}/{}",
                path.strip_prefix(base).unwrap().to_string_lossy()
            );
            let mut desc = Map::new();
            let ft = meta.file_type();
            if ft.is_symlink() {
                desc.insert("type".into(), Value::Str("symlink".into()));
                let target = std::fs::read_link(&path).unwrap();
                desc.insert(
                    "target".into(),
                    Value::Str(self.norm(&target.to_string_lossy())),
                );
                desc.insert("dangling".into(), Value::Bool(!path.exists()));
            } else if ft.is_dir() {
                self.walk(base, &path, label, out);
                continue;
            } else if ft.is_fifo() {
                desc.insert("type".into(), Value::Str("fifo".into()));
            } else {
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                if path.file_name().is_some_and(|n| n == "properties.json") {
                    desc.insert("type".into(), Value::Str("json".into()));
                    desc.insert("text".into(), Value::Str(text));
                } else {
                    desc.insert("type".into(), Value::Str("text".into()));
                    desc.insert("text".into(), Value::Str(self.norm(&text)));
                }
            }
            out.insert(rel, Value::Map(desc));
        }
    }
}

fn fixture() -> Value {
    common::load_fixture("systemd_executor.json")
}

fn strings(v: &Value) -> Vec<String> {
    v.as_list()
        .unwrap()
        .iter()
        .map(Value::py_str)
        .collect::<Vec<_>>()
}

fn tuple(v: &Value) -> (bool, String) {
    let l = v.as_list().unwrap();
    (l[0].truthy(), l[1].py_str())
}

fn props_without_target(text: &str) -> Value {
    let mut v = Value::from_json_str(text).unwrap();
    if let Some(m) = v.as_map_mut() {
        m.shift_remove("target");
    }
    v
}

#[test]
fn create_matches_the_reference() {
    let fx = fixture();
    for case in fx.get("create").and_then(Value::as_list).unwrap() {
        let f = Fixture::new();
        let ex = f.executor();
        let name = case.get("name").and_then(Value::as_str).unwrap();
        let sysargs = strings(case.get("sysargs").unwrap());
        let props = case.get("properties").and_then(Value::as_map).cloned();
        let result = ex.create_job(name, &sysargs, props.as_ref());
        let (ok, msg) = tuple(case.get("result").unwrap());
        assert_eq!(
            (result.success, result.message.clone()),
            (ok, msg),
            "{name}"
        );
        assert_eq!(
            f.fake.rendered_calls(),
            strings(case.get("commands").unwrap()),
            "{name}"
        );

        let expected = case.get("files").and_then(Value::as_map).unwrap();
        let actual = f.tree();
        for (path, want) in expected {
            let want_type = want.get("type").and_then(Value::as_str).unwrap();
            if want_type == "pickle" {
                // Python's `pickle.pkl` is never written or read by Rust.
                continue;
            }
            let got = actual
                .get(path)
                .unwrap_or_else(|| panic!("{name}: missing {path}; have {:?}", actual.keys()));
            assert_eq!(got.get("type"), want.get("type"), "{name}: {path}");
            match want_type {
                "symlink" => {
                    assert_eq!(got.get("target"), want.get("target"), "{name}: {path}");
                    assert_eq!(got.get("dangling"), want.get("dangling"), "{name}: {path}");
                }
                "json" => {
                    // Rust also records the job's `target` (see `Daemon::setup`).
                    let got_props =
                        props_without_target(got.get("text").and_then(Value::as_str).unwrap());
                    let want_props =
                        props_without_target(want.get("text").and_then(Value::as_str).unwrap());
                    assert_eq!(got_props, want_props, "{name}: {path}");
                }
                "text" => assert_eq!(got.get("text"), want.get("text"), "{name}: {path}"),
                _ => {}
            }
        }
        // No file the reference did not write (but the pickle, which Rust cannot).
        for path in actual.keys() {
            assert!(expected.contains_key(path), "{name}: extra file {path}");
        }
    }
}

#[test]
fn create_failure_accounting_matches() {
    let fx = fixture();
    for case in fx.get("create_failures").and_then(Value::as_list).unwrap() {
        let f = Fixture::new();
        for sub in strings(case.get("fail").unwrap()) {
            f.fake.fail_on(&sub);
        }
        let result = f.executor().create_job(
            "failing",
            &["show".to_string(), "version".to_string()],
            None,
        );
        let (ok, msg) = tuple(case.get("result").unwrap());
        assert_eq!((result.success, result.message), (ok, msg));
        assert_eq!(
            f.fake.rendered_calls(),
            strings(case.get("commands").unwrap())
        );
    }
}

fn setup_lifecycle(f: &Fixture, name: &str, props: Option<&Map>) {
    let r = f.executor().create_job(
        name,
        &["sync".into(), "pipes".into(), "--loop".into()],
        props,
    );
    assert!(r.success, "{}", r.message);
    f.fake.clear_calls();
}

fn service(state: &str, pid: i32, stopped: bool) -> FakeService {
    FakeService {
        active_state: state.into(),
        main_pid: pid,
        stopped,
        enabled: true,
        ..FakeService::default()
    }
}

/// Only the commands which change a service (the status polling around them varies).
fn mutating(calls: &[String]) -> Vec<String> {
    calls
        .iter()
        .filter(|c| !c.starts_with("is-active") && !c.starts_with("show"))
        .cloned()
        .collect()
}

#[test]
fn lifecycle_commands_match_the_reference() {
    let fx = fixture();
    for case in fx.get("lifecycle").and_then(Value::as_list).unwrap() {
        let scenario = case.get("scenario").and_then(Value::as_str).unwrap();
        let f = Fixture::new();
        let ex = f.executor();
        let (name, delete_props) = match scenario {
            "delete_after_completion" => {
                let mut m = Map::new();
                m.insert("delete_after_completion".into(), Value::Bool(true));
                ("cleanup", Some(m))
            }
            _ => ("lifecycle", None),
        };
        setup_lifecycle(&f, name, delete_props.as_ref());
        let svc = format!("mrsm-{name}.service");
        f.fake.stop_on_sigint(false);
        let result = match scenario {
            "start_inactive" => {
                f.fake.set_service(&svc, service("inactive", 0, false));
                ex.start_job(name)
            }
            "start_paused" => {
                f.fake.set_service(&svc, service("active", 4242, true));
                ex.start_job(name)
            }
            "stop_graceful" => {
                f.fake.set_service(&svc, service("active", 4242, false));
                f.fake.script_is_active(&svc, &["active", "inactive"]);
                ex.stop_job(name)
            }
            "stop_already_stopped" => {
                f.fake.set_service(&svc, service("inactive", 0, false));
                ex.stop_job(name)
            }
            "stop_timeout" => {
                f.fake.set_service(&svc, service("active", 4242, false));
                ex.stop_job(name)
            }
            "pause" => {
                f.fake.set_service(&svc, service("active", 4242, false));
                ex.pause_job(name)
            }
            "delete" | "delete_after_completion" => {
                f.fake.set_service(&svc, service("inactive", 0, false));
                ex.delete_job(name)
            }
            other => panic!("unknown scenario {other}"),
        };
        let (ok, msg) = tuple(case.get("result").unwrap());
        assert_eq!(
            (result.success, result.message.clone()),
            (ok, msg),
            "{scenario}"
        );
        let want = strings(case.get("commands").unwrap());
        let got = f.fake.rendered_calls();
        if scenario == "stop_timeout" {
            assert_eq!(mutating(&got), mutating(&want), "{scenario}");
            assert!(got.last().unwrap().starts_with("stop "), "{scenario}");
        } else {
            assert_eq!(got, want, "{scenario}");
        }
        if let Some(files) = case.get("files").and_then(Value::as_map) {
            // The reference leaves a dangling `mrsm-<name>.service` link behind (a v4 bug: its
            // `exists()` check is false for a dangling link, and a later create fails with
            // `FileExistsError`); Rust removes it. Everything else is identical.
            let actual = f.tree();
            let ref_paths: Vec<&String> = files
                .keys()
                .filter(|p| {
                    !(p.starts_with("home/systemd/user/")
                        && files[*p].get("dangling") == Some(&Value::Bool(true)))
                })
                .collect();
            let got_paths: Vec<&String> = actual.keys().collect();
            assert_eq!(got_paths, ref_paths, "{scenario}");
            assert!(
                !actual.keys().any(|p| p.starts_with("home/systemd/user/")),
                "{scenario}: the unit link must be removed"
            );
        }
    }
}

#[test]
fn a_deleted_job_can_be_created_again() {
    let f = Fixture::new();
    let ex = f.executor();
    setup_lifecycle(&f, "again", None);
    f.fake.stop_on_sigint(true);
    assert!(ex.delete_job("again").success);
    assert!(!ex.get_job_exists("again"));
    // v4 fails here: the dangling link makes `symlink_to` raise `FileExistsError`.
    let again = ex.create_job("again", &["show".into(), "version".into()], None);
    assert!(again.success, "{}", again.message);
    assert!(ex.get_job_exists("again"));
}

#[test]
fn status_pid_timestamps_results_match() {
    let fx = fixture();
    let f = Fixture::new();
    let ex = f.executor();
    setup_lifecycle(&f, "meta", None);
    let svc = "mrsm-meta.service";

    for row in fx.get("status").and_then(Value::as_list).unwrap() {
        let pid: i32 = row
            .get("pid")
            .and_then(Value::as_str)
            .unwrap()
            .parse()
            .unwrap();
        let is_active = row.get("is_active").and_then(Value::as_str).unwrap();
        let process = row.get("process").and_then(Value::as_str);
        let mut s = service(is_active, pid, process == Some("stopped"));
        s.process_gone = process == Some("gone");
        f.fake.set_service(svc, s);
        assert_eq!(
            ex.get_job_status("meta"),
            row.get("status").and_then(Value::as_str).unwrap(),
            "{row:?}"
        );
    }

    for row in fx.get("timestamps").and_then(Value::as_list).unwrap() {
        let output = row.get("output").and_then(Value::as_str).unwrap();
        let (prop, value) = output.split_once('=').unwrap();
        let mut s = service("active", 0, false);
        if prop.starts_with("Active") {
            s.active_enter = value.into();
        } else {
            s.inactive_enter = value.into();
        }
        f.fake.set_service(svc, s);
        let got = if prop.starts_with("Active") {
            ex.get_job_began("meta")
        } else {
            ex.get_job_ended("meta")
        };
        let want = row.get("value").and_then(Value::as_str).map(str::to_string);
        assert_eq!(got, want, "{output}");
    }

    f.fake.set_service(svc, service("active", 4242, false));
    assert_eq!(ex.get_job_pid("meta"), Some(4242));
    f.fake.set_service(svc, service("inactive", 0, false));
    assert_eq!(ex.get_job_pid("meta"), None);

    // Metadata of a stopped restart job.
    let mut meta = ex.get_job_metadata("meta");
    let want = fx.get("metadata").and_then(Value::as_map).unwrap();
    let daemon = meta.get_mut("daemon").and_then(Value::as_map_mut).unwrap();
    let props = daemon
        .get_mut("properties")
        .and_then(Value::as_map_mut)
        .unwrap();
    props.shift_remove("label");
    props.shift_remove("target");
    assert_eq!(&Value::Map(meta.clone()), &Value::Map(want.clone()));
    assert_eq!(
        strings(fx.get("sysargs").unwrap()),
        ex.get_job_metadata("meta")
            .get("sysargs")
            .and_then(Value::as_list)
            .unwrap()
            .iter()
            .map(Value::py_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(fx.get("restart"), Some(&Value::Bool(true)));
    assert!(ex.get_job_names().contains(&"meta".to_string()));
    assert!(ex.get_job_exists("meta") && !ex.get_job_exists("missing"));

    // Result file.
    let result = fx.get("result").unwrap();
    let path = ex.get_result_path("meta");
    let r = ex.read_job_result("meta");
    assert_eq!(
        (r.success, r.message),
        tuple(result.get("missing").unwrap())
    );
    std::fs::write(&path, r#"[true, "done"]"#).unwrap();
    let r = ex.read_job_result("meta");
    assert_eq!(
        (r.success, r.message),
        tuple(result.get("present").unwrap())
    );
    std::fs::write(&path, "not json").unwrap();
    let r = ex.read_job_result("meta");
    assert_eq!((r.success, r.message), tuple(result.get("broken").unwrap()));
}

#[test]
fn sister_jobs_are_externally_managed_and_listed_through_the_executor() {
    let f = Fixture::new();
    let ex = f.executor();
    setup_lifecycle(&f, "worker one", None);
    assert!(ex.get_job_names().contains(&"worker one".to_string()));
    assert!(
        ex.get_service_symlink_file_path("worker one")
            .to_string_lossy()
            .ends_with("mrsm-worker-one.service")
    );

    // The sister job lives under `jobs/` but is hidden from local listings.
    assert!(!get_jobs(&f.ctx, true).contains_key("worker one"));
    let listing = get_jobs_for(&f.ctx, Some("systemd"), false, false).unwrap();
    let job = &listing.jobs["worker one"];
    assert_eq!(job.executor_keys, "systemd");

    // A local job of the same name is reported once, the systemd job wins.
    let mut ctx = f.ctx.clone();
    ctx.config.executor = Some("systemd".into());
    let mut local = Job::new(
        "worker one",
        Some(vec!["show".into(), "version".into()]),
        JobOptions {
            executor_keys: Some("local".into()),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    // `Job::start` of a local job would spawn a process; write the properties instead.
    assert!(local.daemon().setup(true).success);
    let combined = get_jobs_for(&ctx, None, false, true).unwrap();
    // The externally managed sister and the local job share a directory: the sister's marker
    // keeps it out of the local half, so there is nothing to warn about.
    assert!(combined.jobs.contains_key("worker one"));
    assert_eq!(combined.jobs["worker one"].executor_keys, "systemd");
    assert!(combined.warnings.is_empty());
}

#[test]
fn jobs_start_stop_and_report_through_the_job_api() {
    let f = Fixture::new();
    let mut ctx = f.ctx.clone();
    ctx.config.executor = Some("systemd".into());
    let mut job = Job::new(
        "api job",
        Some(vec!["sync".into(), "pipes".into(), "--loop".into()]),
        JobOptions::default(),
        &ctx,
    )
    .unwrap();
    assert_eq!(job.executor_keys, "systemd");
    // Creates the unit (the job does not exist yet).
    let started = job.start();
    assert!(started.success, "{}", started.message);
    assert_eq!(started.message, "Started job 'api job' via systemd.");
    let calls = f.fake.rendered_calls();
    assert!(calls.contains(&"enable mrsm-api-job.service".to_string()));
    assert!(job.exists());
    assert_eq!(job.status().as_str(), "running");
    assert!(job.pid().is_some());
    assert!(job.restarts());
    assert!(job.began().is_some());

    f.fake.stop_on_sigint(false);
    let paused = job.pause(None);
    assert!(paused.success, "{}", paused.message);
    assert_eq!(job.status().as_str(), "paused");
    assert!(job.paused().is_some() || job.stop_time().is_some());
    // Resume: `start` on an existing, paused job continues it.
    f.fake.clear_calls();
    assert!(job.start().success);
    assert_eq!(
        f.fake.rendered_calls().last().unwrap(),
        "kill -s SIGCONT mrsm-api-job.service"
    );

    f.fake.stop_on_sigint(true);
    assert!(job.stop(None).success);
    assert_eq!(job.status().as_str(), "stopped");
    assert!(job.ended().is_some());
    assert!(job.delete().success);
    assert!(!job.exists());
}

#[test]
fn unavailable_systemctl_is_an_error() {
    let f = Fixture::new();
    f.fake.set_unavailable(true);
    let ex = f.executor();
    let created = ex.create_job("down", &["show".into(), "version".into()], None);
    assert!(!created.success);
    assert_eq!(created.message, "Failed to reload systemd.");
    let started = ex.start_job("down");
    assert!(!started.success);
    assert!(
        started
            .message
            .contains("systemctl --user start mrsm-down.service")
    );
    assert!(started.message.contains("No such file"));
    assert_eq!(ex.get_job_status("down"), "stopped");
}

#[test]
fn logs_and_monitoring_read_the_service_log() {
    let f = Fixture::new();
    let ex = f.executor();
    setup_lifecycle(&f, "logger", None);
    let svc = "mrsm-logger.service";
    f.fake.set_service(svc, service("active", 4242, false));
    let mut log = ex.get_job_rotating_file("logger").unwrap();
    log.write("first line\nsecond line\n").unwrap();
    assert_eq!(ex.get_logs("logger").unwrap(), "first line\nsecond line\n");

    // The job stops with a result: the monitor drains the log, reports it and ends.
    let result_path = ex.get_result_path("logger");
    std::fs::write(&result_path, r#"[true, "all done"]"#).unwrap();
    f.fake.set_service(svc, service("inactive", 0, false));
    let mut lines: Vec<String> = Vec::new();
    let stop = std::cell::RefCell::new(None);
    let outcome = ex.monitor_logs(
        "logger",
        &mut |line: &str| {
            lines.push(line.to_string());
            meerschaum_jobs::MonitorControl::Continue
        },
        meerschaum_jobs::MonitorOptions {
            stop_on_exit: true,
            accept_input: false,
            poll_interval: Some(std::time::Duration::from_millis(20)),
            on_stop: Some(Box::new(|r| {
                *stop.borrow_mut() = Some(r.clone());
            })),
            ..Default::default()
        },
    );
    assert!(outcome.success);
    assert_eq!(lines.concat(), "first line\nsecond line\n");
    let stopped = stop.borrow().clone().unwrap();
    assert_eq!(
        (stopped.success, stopped.message.as_str()),
        (true, "all done")
    );
}

#[test]
fn stdin_blocking_marker_is_the_socket_block_file() {
    let f = Fixture::new();
    let ex = f.executor();
    setup_lifecycle(&f, "asker", None);
    assert!(!ex.get_job_is_blocking_on_stdin("asker"));
    let socket = ex.get_socket_path("asker");
    let mut block = socket.into_os_string();
    block.push(".block");
    std::fs::write(&block, "").unwrap();
    assert!(ex.get_job_is_blocking_on_stdin("asker"));
}

#[test]
fn make_executor_builds_the_systemd_executor_over_the_context_runner() {
    let f = Fixture::new();
    let ex = make_executor("systemd", &f.ctx);
    assert_eq!(ex.executor_keys(), "systemd");
    assert_eq!(ex.get_job_status("nothing"), "stopped");
    assert_eq!(
        f.fake.rendered_calls(),
        vec!["is-active mrsm-nothing.service".to_string()]
    );
}

#[test]
#[ignore = "talks to the host's `systemctl --user`; set MRSM_TEST_SYSTEMD_SMOKE=1 and pass --ignored"]
fn real_systemctl_smoke() {
    if std::env::var("MRSM_TEST_SYSTEMD_SMOKE").is_err() {
        return;
    }
    // Read-only: ask for the state of a unit which does not exist.
    let dir = tempfile::tempdir().unwrap();
    let ctx = JobsContext::for_root(dir.path());
    let ex = SystemdExecutor::new(ctx);
    assert_eq!(ex.get_job_status("mrsm-v5-smoke-does-not-exist"), "stopped");
}

#[test]
fn the_unit_pins_the_workers_interpreter() {
    let f = Fixture::new();
    let bin = Path::new(&f.tmp).join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let python = bin.join("python3");
    std::fs::write(&python, "#!/bin/sh\n").unwrap();
    let other = bin.join("my-python");
    std::fs::write(&other, "#!/bin/sh\n").unwrap();
    let unit = |env: Vec<(&str, String)>| {
        let ex = SystemdExecutor::new(f.ctx.clone());
        let env: Vec<(String, String)> = env.into_iter().map(|(k, v)| (k.into(), v)).collect();
        ex.get_service_file_text("pinned", &["show".into(), "version".into()], "mrsm", &env)
    };
    let line = |text: &str| {
        text.lines()
            .find(|l| l.starts_with("Environment=MRSM_PYTHON="))
            .map(str::to_string)
    };

    // `python3` on the creating process's PATH.
    let text = unit(vec![("PATH", format!("{}:/nonexistent", bin.display()))]);
    assert_eq!(
        line(&text),
        Some(format!("Environment=MRSM_PYTHON={}", python.display()))
    );
    // `MRSM_PYTHON`, absolute.
    let text = unit(vec![("MRSM_PYTHON", other.display().to_string())]);
    assert_eq!(
        line(&text),
        Some(format!("Environment=MRSM_PYTHON={}", other.display()))
    );
    // `MRSM_PYTHON` as a bare command is resolved against PATH.
    let text = unit(vec![
        ("MRSM_PYTHON", "my-python".into()),
        ("PATH", bin.display().to_string()),
    ]);
    assert_eq!(
        line(&text),
        Some(format!("Environment=MRSM_PYTHON={}", other.display()))
    );
    // Nothing resolves: nothing is pinned (and nothing is invented).
    assert_eq!(line(&unit(vec![("PATH", "/nonexistent".into())])), None);
    assert_eq!(
        line(&unit(vec![("MRSM_PYTHON", "/nonexistent/python".into())])),
        None
    );
    // `create_job` hands the warning to the caller (nothing is printed by the library), and only
    // when no interpreter resolves.
    let silent = SystemdExecutor::new(f.ctx.clone())
        .with_process_env(vec![("MRSM_PYTHON".into(), other.display().to_string())]);
    assert!(
        silent
            .create_job("quiet", &["show".into(), "version".into()], None)
            .success
    );
    assert!(silent.take_warnings().is_empty());
    let bare = SystemdExecutor::new(f.ctx.clone())
        .with_process_env(vec![("PATH".into(), "/nonexistent".into())]);
    assert!(
        bare.create_job("bare", &["show".into(), "version".into()], None)
            .success
    );
    let warnings = bare.take_warnings();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("'bare'") && warnings[0].contains("MRSM_PYTHON"));
    assert!(bare.take_warnings().is_empty(), "drained");
    // The warning names the job and the variable.
    let warning = meerschaum_jobs::systemd::unpinned_python_warning("pinned");
    assert!(
        warning.contains("'pinned'") && warning.contains("MRSM_PYTHON"),
        "{warning}"
    );
    // `create_job` writes it into the unit file.
    let ex = SystemdExecutor::new(f.ctx.clone())
        .with_process_env(vec![("MRSM_PYTHON".into(), other.display().to_string())]);
    assert!(
        ex.create_job("pinned", &["show".into(), "version".into()], None)
            .success
    );
    let written = std::fs::read_to_string(ex.get_service_file_path("pinned")).unwrap();
    assert_eq!(
        line(&written),
        Some(format!("Environment=MRSM_PYTHON={}", other.display()))
    );
}
