//! rc15 (Q31, Q23, Q26): `start pipeline NAME` / `stop pipeline NAME` run a saved Pipeline's
//! background job; action chains serialize under the hidden `chain` action while v4's
//! `start pipeline ... -P {...}` jobs on disk keep running; `--loop` repeats any action; and a
//! looping job records its laps (job health).
//!
//! Every job here runs the debug `mrsm-cli`, so it is a `local` job (a development build never
//! chooses `systemd` by itself) under a scratch root.
use serde_json::{Value, json};
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

struct Env {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
}

impl Env {
    fn new() -> Self {
        let base = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap()
            .join(".cache/mrsm-pipeline-job-tests");
        std::fs::create_dir_all(&base).unwrap();
        let tmp = tempfile::tempdir_in(base).unwrap();
        let root = tmp.path().join("root");
        let plugins = tmp.path().join("plugins");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&plugins).unwrap();
        Self {
            _tmp: tmp,
            root,
            plugins,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_") || ["TMUX", "TMUX_PANE", "RUST_LOG"].contains(&key.as_str())
            {
                command.env_remove(key);
            }
        }
        command
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env("MRSM_NOASK", "true")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:local"},"formatting":{"ansi":false,"unicode":false},"shell":{"updates":{"check":false}},"jobs":{"startup":{"enabled":false}}}"#,
            )
            .args(args)
            .output()
            .unwrap()
    }

    fn text(output: &Output) -> String {
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    fn ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        let text = Self::text(&output);
        assert!(output.status.success(), "{args:?}\n{text}");
        text
    }

    fn fails(&self, args: &[&str]) -> String {
        let output = self.run(args);
        let text = Self::text(&output);
        assert!(!output.status.success(), "{args:?} succeeded:\n{text}");
        text
    }

    /// Run `args`, which must exit by itself within `seconds` (and succeed): a command that
    /// turned into a foreground scheduler or loop is killed and fails the test.
    fn ok_within(&self, seconds: u64, args: &[&str]) -> String {
        let out_path = self._tmp.path().join("ok_within.out");
        let out = std::fs::File::create(&out_path).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_") || ["TMUX", "TMUX_PANE", "RUST_LOG"].contains(&key.as_str())
            {
                command.env_remove(key);
            }
        }
        let mut child = command
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env("MRSM_NOASK", "true")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:local"},"formatting":{"ansi":false,"unicode":false},"shell":{"updates":{"check":false}},"jobs":{"startup":{"enabled":false}}}"#,
            )
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "{args:?} still ran after {seconds} s:\n{}",
                    std::fs::read_to_string(&out_path).unwrap_or_default()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let text = std::fs::read_to_string(&out_path).unwrap_or_default();
        assert!(status.success(), "{args:?}\n{text}");
        text
    }

    /// Register the `sql:local` pipes `(metric, schedule)` and the Pipeline `name` over them.
    fn pipeline(&self, name: &str, members: &[(&str, Option<&str>)]) {
        let mut targets = Vec::new();
        for (metric, schedule) in members {
            let mut params = json!({"columns": {"id": "id"}, "sql": "SELECT 1 AS id"});
            if let Some(schedule) = schedule {
                params["schedule"] = json!(schedule);
            }
            self.ok(&[
                "register",
                "pipes",
                "-c",
                "sql:local",
                "-m",
                metric,
                "-i",
                "sql:local",
                "--params",
                &params.to_string(),
                "-y",
            ]);
            targets.push(format!("sql:local|sql:local|{metric}|None"));
        }
        self.ok(&[
            "register",
            "pipelines",
            name,
            "--params",
            &json!({ "targets": targets }).to_string(),
            "-i",
            "sql:local",
            "-y",
        ]);
    }

    fn job_dir(&self, name: &str) -> PathBuf {
        self.root.join("jobs").join(name)
    }

    /// The job's sysargs as Rust records them (`target.args[0]`).
    fn job_sysargs(&self, name: &str) -> Vec<String> {
        let text = std::fs::read_to_string(self.job_dir(name).join("properties.json")).unwrap();
        let props: Value = serde_json::from_str(&text).unwrap();
        props["target"]["args"][0]
            .as_array()
            .unwrap_or_else(|| panic!("no target in {props}"))
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }

    fn laps(&self, name: &str) -> Option<Value> {
        let text = std::fs::read_to_string(self.job_dir(name).join("laps.json")).ok()?;
        serde_json::from_str(&text).ok()
    }

    fn logs(&self, name: &str) -> String {
        Self::text(&self.run(&["show", "logs", name, "--nopretty"]))
    }

    fn wait_for(&self, what: &str, seconds: u64, mut f: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline {
            if f() {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("timed out waiting for {what}");
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        // Leave no worker behind, whatever failed.
        let _ = self.run(&["stop", "jobs", "-y", "-f"]);
        let _ = self.run(&["delete", "jobs", "-y", "-f"]);
    }
}

#[test]
fn start_and_stop_pipeline_run_its_background_job() {
    let env = Env::new();
    env.ok(&[
        "register",
        "pipes",
        "-c",
        "sql:local",
        "-m",
        "readings",
        "-i",
        "sql:local",
        "--params",
        &json!({"columns": {"id": "id"}, "sql": "SELECT 1 AS id"}).to_string(),
        "-y",
    ]);
    env.ok(&[
        "register",
        "pipelines",
        "daily",
        "--params",
        &json!({"targets": ["sql:local|sql:local|readings|None"]}).to_string(),
        "-i",
        "sql:local",
        "-y",
    ]);

    // A name that is not saved lists the saved ones; no name asks for one.
    let unknown = env.fails(&["start", "pipeline", "nope", "-i", "sql:local"]);
    assert!(
        unknown.contains("Pipeline 'nope' is not saved on 'sql:local'."),
        "{unknown}"
    );
    assert!(unknown.contains("Saved Pipelines: daily."), "{unknown}");
    let missing = env.fails(&["start", "pipeline", "-i", "sql:local"]);
    assert!(missing.contains("Name the Pipeline to start"), "{missing}");
    assert!(!env.job_dir("pipeline-daily").exists());

    // Without a background job, `start pipeline` creates `pipeline-daily`.
    let created = env.ok(&[
        "start",
        "pipeline",
        "daily",
        "-i",
        "sql:local",
        "--min-seconds",
        "3600",
    ]);
    assert!(
        created.contains("Created job 'pipeline-daily' for Pipeline 'daily'"),
        "{created}"
    );
    assert_eq!(
        env.job_sysargs("pipeline-daily"),
        [
            "sync",
            "pipelines",
            "daily",
            "-i",
            "sql:local",
            "--loop",
            "--min-seconds",
            "3600.0"
        ]
    );
    // The job's first lap syncs the Pipeline and records a good lap.
    env.wait_for("the first lap", 120, || {
        env.laps("pipeline-daily")
            .is_some_and(|laps| !laps["last_success"].is_null())
    });
    let laps = env.laps("pipeline-daily").unwrap();
    assert_eq!(laps["noun"], "sync", "{laps}");
    assert_eq!(laps["interval_seconds"], 3600.0, "{laps}");
    let shown = env.ok(&["show", "jobs", "pipeline-daily"]);
    assert!(shown.contains("last good sync"), "{shown}");

    // A name that is not saved (and has no job) stops nothing, like `start`.
    let mixed = env.fails(&["stop", "pipelines", "daily", "nope", "-i", "sql:local"]);
    assert!(
        mixed.contains("Pipeline 'nope' is not saved on 'sql:local'."),
        "{mixed}"
    );
    assert!(mixed.contains("Saved Pipelines: daily."), "{mixed}");
    // `stop pipeline` stops it; `start pipeline` starts the same job again.
    let stopped = env.ok(&["stop", "pipelines", "daily", "-i", "sql:local"]);
    assert!(
        stopped.contains("Stopped job 'pipeline-daily'"),
        "{stopped}"
    );
    let again = env.ok(&["start", "pipeline", "daily", "-i", "sql:local"]);
    assert!(
        again.contains("Started job 'pipeline-daily' for Pipeline 'daily'"),
        "{again}"
    );
    assert!(!again.contains("Created job"), "{again}");
    let jobs: Vec<String> = std::fs::read_dir(env.root.join("jobs"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with('.'))
        .collect();
    assert_eq!(jobs, ["pipeline-daily"]);
    env.ok(&["stop", "pipeline", "daily", "-i", "sql:local"]);
    let nothing = env.ok(&["stop", "pipeline", "daily", "-i", "sql:local"]);
    assert!(nothing.contains("No background job"), "{nothing}");
}

#[test]
fn chains_run_from_either_serialized_form() {
    let env = Env::new();
    // A chain started in the background is stored under the hidden `chain` action.
    env.ok(&[
        "show",
        "version",
        "+",
        "show",
        "version",
        ":",
        "x2",
        "--min-seconds",
        "0",
        "-d",
        "--name",
        "chained",
        "-y",
    ]);
    let sysargs = env.job_sysargs("chained");
    assert_eq!(sysargs[0], "chain", "{sysargs:?}");
    env.wait_for("the chained job", 60, || {
        env.logs("chained")
            .contains("Ran the action chain 2 times.")
    });
    let logs = env.logs("chained");
    assert_eq!(logs.matches(meerschaum_cli::VERSION).count(), 4, "{logs}");
    // `show jobs` prints the chain as it was typed.
    let shown = env.ok(&["show", "jobs", "chained"]);
    assert!(shown.contains("show version"), "{shown}");
    assert!(shown.contains("+ show version"), "{shown}");
    assert!(!shown.contains("sub_args_line"), "{shown}");

    // A job Meerschaum 4.1 wrote (`start pipeline ... -P {...}`; Python records only the
    // label) still runs as a chain.
    let legacy = env.job_dir("legacy");
    std::fs::create_dir_all(&legacy).unwrap();
    let label = r#"start pipeline x2 --min-seconds 0 -P '{"sub_args_line":"show version","patch_args":null}'"#;
    std::fs::write(
        legacy.join("properties.json"),
        json!({"label": label, "result": null}).to_string(),
    )
    .unwrap();
    let shown = env.ok(&["show", "jobs", "legacy"]);
    assert!(shown.contains("show version"), "{shown}");
    assert!(!shown.contains("sub_args_line"), "{shown}");
    env.ok(&["start", "job", "legacy", "-y"]);
    env.wait_for("the v4 chain job", 60, || {
        env.logs("legacy").contains("Ran the action chain 2 times.")
    });
    assert!(!env.logs("legacy").contains("deprecated"));
}

#[test]
fn any_action_loops_and_reports_its_laps() {
    let env = Env::new();
    env.ok(&[
        "show",
        "version",
        "--loop",
        "--min-seconds",
        "0.2",
        "-d",
        "--name",
        "looping",
        "-y",
    ]);
    env.wait_for("three laps", 60, || {
        env.laps("looping")
            .is_some_and(|laps| laps["laps"].as_u64().unwrap_or(0) >= 3)
    });
    let logs = env.logs("looping");
    assert!(logs.matches(meerschaum_cli::VERSION).count() >= 3, "{logs}");
    assert!(logs.contains("Sleeping for 0.2 seconds."), "{logs}");
    let laps = env.laps("looping").unwrap();
    assert_eq!(laps["consecutive_failures"], 0, "{laps}");
    assert_eq!(laps["noun"], "run", "{laps}");
    let shown = env.ok(&["show", "jobs", "looping"]);
    assert!(shown.contains("last good run"), "{shown}");
    env.ok(&["stop", "job", "looping", "-y"]);

    // A failing action backs off and reports its failures.
    env.ok(&[
        "show",
        "config",
        "synthetic_missing_key",
        "--loop",
        "--min-seconds",
        "0",
        "-d",
        "--name",
        "failing",
        "-y",
    ]);
    env.wait_for("a failed lap", 60, || {
        env.laps("failing")
            .is_some_and(|laps| laps["consecutive_failures"].as_u64().unwrap_or(0) >= 1)
    });
    let laps = env.laps("failing").unwrap();
    assert!(laps["last_success"].is_null(), "{laps}");
    let shown = env.ok(&["show", "jobs", "failing"]);
    assert!(shown.contains("failing: last"), "{shown}");
    env.wait_for("the cooldown message", 30, || {
        env.logs("failing").contains("failed. Trying again in")
    });
    env.ok(&["stop", "job", "failing", "-y"]);
}

#[test]
fn start_pipeline_gives_its_schedule_to_the_job_and_prefers_scheduled_jobs() {
    let env = Env::new();
    env.pipeline("daily", &[("readings", None)]);

    // `-s` is the new job's schedule: the command creates the job and returns (it used to run
    // `start pipeline` itself on the schedule, in the foreground).
    let created = env.ok_within(
        60,
        &[
            "start",
            "pipeline",
            "daily",
            "-i",
            "sql:local",
            "-s",
            "every 5 minutes",
            "--name",
            "sched-daily",
        ],
    );
    assert_eq!(
        created.matches("Created job 'sched-daily'").count(),
        1,
        "{created}"
    );
    let sysargs = env.job_sysargs("sched-daily");
    assert_eq!(
        sysargs,
        [
            "sync",
            "pipelines",
            "daily",
            "-i",
            "sql:local",
            "--schedule",
            "every 5 minutes"
        ]
    );
    // Stopping with the same flags stops it once.
    let stopped = env.ok_within(
        60,
        &[
            "stop",
            "pipeline",
            "daily",
            "-i",
            "sql:local",
            "-s",
            "every 5 minutes",
        ],
    );
    assert!(stopped.contains("Stopped job 'sched-daily'"), "{stopped}");

    // A loop job too (`--name` is free): starting it next to nothing running is quiet; a new
    // job's default pace is a lap a minute.
    let looping = env.ok_within(
        60,
        &[
            "start",
            "pipeline",
            "daily",
            "-i",
            "sql:local",
            "--name",
            "loop-daily",
        ],
    );
    assert!(looping.contains("Created job 'loop-daily'"), "{looping}");
    assert!(!looping.contains("sync more than once"), "{looping}");
    assert_eq!(
        env.job_sysargs("loop-daily"),
        [
            "sync",
            "pipelines",
            "daily",
            "-i",
            "sql:local",
            "--loop",
            "--min-seconds",
            "60"
        ]
    );
    env.ok(&["stop", "job", "loop-daily", "-y"]);

    // With a scheduled job, `start pipeline` starts that one and leaves the loop job alone.
    let started = env.ok_within(60, &["start", "pipeline", "daily", "-i", "sql:local"]);
    assert!(
        started.contains("Started job 'sched-daily' for Pipeline 'daily'"),
        "{started}"
    );
    assert!(!started.contains("Started job 'loop-daily'"), "{started}");
    assert!(started.contains("left 'loop-daily' as it was"), "{started}");
    // Both running: the pipes sync twice, which `start pipeline` says.
    env.ok(&["start", "job", "loop-daily", "-y"]);
    let both = env.ok_within(60, &["start", "pipeline", "daily", "-i", "sql:local"]);
    assert!(
        both.contains("all sync Pipeline 'daily', so its pipes sync more than once"),
        "{both}"
    );
    // `stop pipeline` stops every job of the Pipeline.
    let stopped = env.ok(&["stop", "pipeline", "daily", "-i", "sql:local"]);
    assert!(stopped.contains("Stopped job 'sched-daily'"), "{stopped}");
    assert!(stopped.contains("Stopped job 'loop-daily'"), "{stopped}");
}

#[test]
fn a_pipeline_job_follows_its_members_schedules_and_edits() {
    let env = Env::new();
    env.pipeline("hourly", &[("readings", Some("every 1 hour"))]);
    env.ok(&[
        "start",
        "pipeline",
        "hourly",
        "-i",
        "sql:local",
        "--min-seconds",
        "1",
    ]);
    env.wait_for("the first lap", 120, || {
        env.laps("pipeline-hourly")
            .is_some_and(|laps| !laps["last_success"].is_null())
    });
    // The engine's continuous mode: the member syncs when its schedule is due, so laps a
    // second apart do not each run (and record) the whole graph.
    std::thread::sleep(Duration::from_secs(6));
    let logs = env.logs("pipeline-hourly");
    assert_eq!(logs.matches("Run: hourly-").count(), 1, "{logs}");
    let laps = env.laps("pipeline-hourly").unwrap();
    assert_eq!(laps["interval_seconds"], 3600.0, "{laps}");
    let shown = env.ok(&["show", "jobs", "pipeline-hourly"]);
    assert!(shown.contains("last good sync"), "{shown}");

    // An edit takes effect without restarting the job: a new member without a schedule syncs
    // on every lap.
    env.ok(&[
        "register",
        "pipes",
        "-c",
        "sql:local",
        "-m",
        "plain",
        "-i",
        "sql:local",
        "--params",
        &json!({"columns": {"id": "id"}, "sql": "SELECT 2 AS id"}).to_string(),
        "-y",
    ]);
    let saved: Value = serde_json::from_str(
        env.ok(&[
            "show",
            "pipelines",
            "hourly",
            "-i",
            "sql:local",
            "--nopretty",
        ])
        .lines()
        .skip_while(|line| !line.starts_with('['))
        .collect::<Vec<_>>()
        .join("\n")
        .as_str(),
    )
    .unwrap();
    let revision = saved[0]["revision"].as_u64().unwrap();
    env.ok(&[
        "edit",
        "pipelines",
        "hourly",
        "-i",
        "sql:local",
        "--params",
        &json!({
            "revision": revision,
            "targets": ["sql:local|sql:local|plain|None", "sql:local|sql:local|readings|None"],
        })
        .to_string(),
        "-y",
    ]);
    env.wait_for("the edited Pipeline's members", 120, || {
        let logs = env.logs("pipeline-hourly");
        logs.contains("Pipeline 'hourly' was edited: reloading its members.")
            && logs.matches("Run: hourly-").count() >= 3
    });
    env.ok(&["stop", "pipeline", "hourly", "-i", "sql:local"]);
}
