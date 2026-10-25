//! Native plugin executors and streaming connectors through the `mrsm` binary:
//!
//! - the SDK example `native_executor` declares the executor type `procs`; `-e procs:<label>`
//!   creates, lists, logs, stops and deletes jobs that run as local `mrsm` processes, and a method
//!   the plugin leaves out (`pause_job`) is reported as not supported;
//! - the SDK example `native_stream` declares the streaming connector `ticker` and a plugin-level
//!   `subscribe`; `sync pipes --listen` lands their batches, as they arrive, in a SQLite pipe and
//!   acknowledges each committed batch;
//! - the shell's `executor procs:<label>` command and a compose project whose `config` sets
//!   `meerschaum:executor` run their jobs on the plugin's executor.
//!
//! Unix only (the example executor detaches jobs with `sh`). Every run uses a temporary root and
//! plugins directory and a scrubbed environment; all data is synthetic.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use meerschaum_connector::DataQuery;
use meerschaum_connector_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};
use meerschaum_core::Pipe;

fn library(name: &str) -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "meerschaum-plugin", "--examples"])
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
    });
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    fn new(examples: &[&str]) -> Home {
        let home = Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        };
        std::fs::create_dir_all(home.root.path().join("sqlite")).unwrap();
        for name in examples {
            let dir = home.plugins.path().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join("plugin.toml"),
                format!("name = \"{name}\"\nversion = \"1.0.0\"\n"),
            )
            .unwrap();
            std::fs::copy(
                library(name),
                dir.join(meerschaum_plugin::discovery::library_name(name)),
            )
            .unwrap();
        }
        home
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NOASK", "true")
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:local"},"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false}}"#,
            );
        command
    }

    fn run(&self, args: &[&str]) -> String {
        let Output { stdout, stderr, .. } = self.command().args(args).output().unwrap();
        format!(
            "{}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        )
    }

    fn job_dir(&self, label: &str, name: &str) -> PathBuf {
        self.root.path().join("procs_jobs").join(label).join(name)
    }
}

/// Run the interactive shell with `lines` piped on stdin.
fn shell(home: &Home, lines: &str) -> String {
    let mut child = home
        .command()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        use std::io::Write;
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(lines.as_bytes()).unwrap();
    }
    let Output { stdout, stderr, .. } = child.wait_with_output().unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    )
}

fn job_pid(dir: &Path) -> u32 {
    std::fs::read_to_string(dir.join("pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn pid_alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, rest)| !rest.starts_with('Z'))
        })
        .unwrap_or_else(|_| {
            Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success())
        })
}

/// Kills a job process left behind by a failed assertion.
struct Reaper(Option<u32>);
impl Drop for Reaper {
    fn drop(&mut self) {
        if let Some(pid) = self.0
            && pid_alive(pid)
        {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
    }
}

fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
    let until = Instant::now() + deadline;
    while Instant::now() < until {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    done()
}

#[test]
fn procs_executor_runs_lists_logs_stops_and_deletes_jobs() {
    let home = Home::new(&["native_executor"]);

    // An unknown type is still rejected by the argument parser (v4's message).
    let invalid = home.run(&["show", "jobs", "-e", "nope:test"]);
    assert!(
        invalid.contains("Invalid exectutor keys 'nope:test'."),
        "{invalid}"
    );

    let created = home.run(&[
        "show",
        "version",
        "-s",
        "every 1 seconds",
        "-d",
        "-e",
        "procs:test",
        "--name",
        "ticktock",
        "-y",
    ]);
    assert!(
        created.contains("Successfully started job 'ticktock'."),
        "{created}"
    );
    let dir = home.job_dir("test", "ticktock");
    let pid: u32 = std::fs::read_to_string(dir.join("pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let _reaper = Reaper(Some(pid));
    assert!(pid_alive(pid));
    // The executor receives the sysargs without `-d` and `-e`.
    let job: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("job.json")).unwrap()).unwrap();
    let sysargs: Vec<String> = serde_json::from_value(job["sysargs"].clone()).unwrap();
    assert_eq!(&sysargs[..4], ["show", "version", "-s", "every 1 seconds"]);
    assert!(
        !sysargs.iter().any(|a| a == "-e" || a == "-d"),
        "{sysargs:?}"
    );

    let listed = home.run(&["show", "jobs", "-e", "procs:test"]);
    assert!(listed.contains("Jobs on Executor 'procs:test'"), "{listed}");
    assert!(listed.contains("ticktock"), "{listed}");
    assert!(listed.contains("running"), "{listed}");

    assert!(
        wait_for(Duration::from_secs(60), || {
            home.run(&["show", "logs", "ticktock", "-e", "procs:test", "--nopretty"])
                .matches("Meerschaum v")
                .count()
                >= 2
        }),
        "the job never logged:\n{}",
        home.run(&["show", "logs", "ticktock", "-e", "procs:test", "--nopretty"])
    );

    let paused = home.run(&["pause", "jobs", "ticktock", "-e", "procs:test", "-y"]);
    assert!(
        paused.contains("`pause_job` is not supported by the 'procs:test' executor."),
        "{paused}"
    );
    assert!(
        paused.contains("Failed to pause job 'ticktock'."),
        "{paused}"
    );
    assert!(pid_alive(pid));

    let stopped = home.run(&["stop", "jobs", "ticktock", "-e", "procs:test", "-y"]);
    assert!(stopped.contains("Stopped 1 job."), "{stopped}");
    assert!(wait_for(Duration::from_secs(10), || !pid_alive(pid)));
    let listed = home.run(&["show", "jobs", "-e", "procs:test"]);
    assert!(listed.contains("stopped"), "{listed}");

    // Restart through `start jobs`, then delete (which stops it first).
    let restarted = home.run(&["start", "jobs", "ticktock", "-e", "procs:test", "-y"]);
    assert!(
        restarted.contains("Successfully started job 'ticktock'."),
        "{restarted}"
    );
    let pid: u32 = std::fs::read_to_string(dir.join("pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let _reaper = Reaper(Some(pid));
    let deleted = home.run(&["delete", "jobs", "ticktock", "-e", "procs:test", "-y"]);
    assert!(deleted.contains("Deleted job 'ticktock'."), "{deleted}");
    assert!(!dir.exists());
    assert!(wait_for(Duration::from_secs(10), || !pid_alive(pid)));
    let empty = home.run(&["show", "jobs", "-e", "procs:test"]);
    assert!(empty.contains("No jobs to show."), "{empty}");
    // The local executor never saw the job.
    assert!(!home.root.path().join("jobs").join("ticktock").exists());

    // A failing executor read is reported, never silently shown as a stopped job.
    let broken = home.job_dir("test", "broken");
    std::fs::create_dir_all(&broken).unwrap();
    std::fs::write(broken.join("job.json"), "not json").unwrap();
    for args in [
        &["show", "jobs", "-e", "procs:test"][..],
        &["stop", "jobs", "broken", "-e", "procs:test", "-y"][..],
    ] {
        let output = home.run(args);
        assert!(output.contains("corrupt job.json"), "{args:?}:\n{output}");
    }
}

#[test]
fn shell_executor_command_selects_the_plugin_executor() {
    let home = Home::new(&["native_executor"]);
    let output = shell(
        &home,
        "executor procs:sh\n\
         show version -s \"every 1 seconds\" -d --name shelljob -y\n\
         show jobs\n\
         exit\n",
    );
    let dir = home.job_dir("sh", "shelljob");
    let _reaper = Reaper(dir.join("pid").exists().then(|| job_pid(&dir)));
    assert!(
        output.contains("Default executor for the current shell: procs:sh"),
        "{output}"
    );
    assert!(output.contains("Jobs on Executor 'procs:sh'"), "{output}");
    assert!(dir.join("job.json").is_file(), "{output}");
    assert!(!home.root.path().join("jobs").join("shelljob").exists());
    let deleted = home.run(&["delete", "jobs", "shelljob", "-e", "procs:sh", "-y"]);
    assert!(deleted.contains("Deleted job 'shelljob'."), "{deleted}");
}

#[test]
fn compose_runs_jobs_on_the_configured_plugin_executor() {
    let home = Home::new(&["native_executor"]);
    let project = home.root.path().join("project");
    std::fs::create_dir_all(project.join("root")).unwrap();
    std::fs::write(
        project.join("mrsm-compose.yaml"),
        format!(
            "project_name: nativeexec\nroot_dir: ./root\nplugins_dir: [{}]\n\
             config:\n  meerschaum:\n    instance: sql:local\n    executor: procs:cmp\n\
             jobs:\n  composed: show version -s 'every 1 seconds'\n",
            home.plugins.path().display()
        ),
    )
    .unwrap();
    let compose = |args: &[&str]| {
        let Output { stdout, stderr, .. } = home
            .command()
            .current_dir(&project)
            .arg("compose")
            .args(args)
            .output()
            .unwrap();
        format!(
            "{}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        )
    };
    let up = compose(&["up", "--no-stagger"]);
    // The project's root holds the executor's state (`$MRSM_ROOT_DIR/procs_jobs/<label>`).
    let dir = project.join("root/procs_jobs/cmp/composed");
    assert!(
        wait_for(Duration::from_secs(30), || dir.join("pid").is_file()),
        "{up}"
    );
    let pid = job_pid(&dir);
    let _reaper = Reaper(Some(pid));
    assert!(pid_alive(pid), "{up}");
    assert!(!project.join("root/jobs/composed").exists(), "{up}");
    let ps = compose(&["ps", "--nopretty"]);
    assert!(ps.contains("composed"), "{ps}");
    let down = compose(&["down"]);
    assert!(
        wait_for(Duration::from_secs(15), || !pid_alive(pid)),
        "{down}"
    );
    compose(&["down", "-v", "-y"]);
}

fn sqlite(rt: &tokio::runtime::Runtime, root: &Path) -> SqlConnector {
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            root.join("sqlite/mrsm_local.db")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    };
    rt.block_on(SqlConnector::connect(
        "local",
        ConnectionSpec::from_attributes(&attrs).unwrap(),
    ))
    .unwrap()
}

fn rowcount(rt: &tokio::runtime::Runtime, conn: &SqlConnector, pipe: &Pipe) -> i64 {
    rt.block_on(conn.get_pipe_rowcount(pipe, &DataQuery::default()))
        .ok()
        .flatten()
        .unwrap_or(0)
}

fn stop(child: &mut Child) {
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn listen_lands_native_stream_batches_as_they_arrive() {
    let home = Home::new(&["native_stream"]);
    let acks = home.root.path().join("acks.log");
    let kwargs_log = home.root.path().join("kwargs.log");
    // Id 4 is filtered: an empty batch whose token must still be acknowledged.
    for (connector, fetch) in [
        (
            "ticker:test",
            r#"{"count": 7, "interval_ms": 400, "skip": [4]}"#,
        ),
        (
            "plugin:native_stream",
            r#"{"count": 4, "interval_ms": 100}"#,
        ),
    ] {
        let params = format!(
            r#"{{"fetch": {fetch}, "columns": {{"datetime": "id"}}, "dtypes": {{"id": "int", "value": "float"}}}}"#
        );
        let registered = home.run(&[
            "register",
            "pipes",
            "-c",
            connector,
            "-m",
            "readings",
            "-i",
            "sql:local",
            "--params",
            &params,
        ]);
        assert!(
            registered.contains("Successfully registered"),
            "{registered}"
        );
    }

    let log_path = home.root.path().join("listen.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut child = home
        .command()
        .env("NATIVE_STREAM_ACK_LOG", &acks)
        .env("NATIVE_STREAM_KWARGS_LOG", &kwargs_log)
        .args([
            "sync",
            "pipes",
            "-c",
            "ticker:test",
            "plugin:native_stream",
            "-i",
            "sql:local",
            "--listen",
            "--chunksize",
            "3",
        ])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let conn = sqlite(&rt, home.root.path());
    let pipe = |connector: &str| {
        Pipe::builder(connector, "readings")
            .instance("sql:local")
            .build()
            .unwrap()
    };
    let (ticker, channel) = (pipe("ticker:test"), pipe("plugin:native_stream"));
    let mut counts = std::collections::BTreeSet::new();
    let landed = wait_for(Duration::from_secs(90), || {
        let n = rowcount(&rt, &conn, &ticker);
        counts.insert(n);
        n >= 6 && rowcount(&rt, &conn, &channel) >= 4
    });
    let (rows, channel_rows) = (
        rowcount(&rt, &conn, &ticker),
        rowcount(&rt, &conn, &channel),
    );
    // Acknowledgements follow the commits; give the last one a moment.
    wait_for(Duration::from_secs(5), || {
        std::fs::read_to_string(&acks)
            .unwrap_or_default()
            .lines()
            .count()
            >= 7
    });
    stop(&mut child);
    let output = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(landed, "rows: {rows} / {channel_rows}\n{output}");
    assert_eq!(rows, 6, "{output}");
    assert_eq!(channel_rows, 4, "{output}");
    assert!(output.contains("Listening for new data"), "{output}");
    // Batches landed over time, not in one flush at the end.
    let partial: Vec<&i64> = counts.iter().filter(|n| **n > 0 && **n < 6).collect();
    assert!(!partial.is_empty(), "counts seen: {counts:?}");

    let data = rt
        .block_on(conn.get_pipe_data(&ticker, &DataQuery::default()))
        .unwrap()
        .unwrap();
    assert_eq!(data.height(), 6);
    let mut acked: Vec<u64> = std::fs::read_to_string(&acks)
        .unwrap()
        .lines()
        .map(|l| l.parse().unwrap())
        .collect();
    acked.sort_unstable();
    acked.dedup();
    assert_eq!(acked, vec![1, 2, 3, 4, 5, 6, 7]);

    // `subscribe` received the sync's keyword arguments (as Python `subscribe` sources do).
    let kwargs: Vec<serde_json::Value> = std::fs::read_to_string(&kwargs_log)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(!kwargs.is_empty());
    for kw in &kwargs {
        assert_eq!(kw["chunksize"], serde_json::json!(3), "{kw}");
        assert_eq!(kw["begin"], serde_json::json!(""), "{kw}");
        assert!(kw.get("check_existing").is_some(), "{kw}");
    }
}
