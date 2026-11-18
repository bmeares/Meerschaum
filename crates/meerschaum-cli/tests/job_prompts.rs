//! A job's prompts answered end to end: `mrsm attach job` in a real pseudo-terminal (the job's
//! supervisor keeps `input.stdin.block` while the job waits, see `meerschaum_jobs::stdin_watch`),
//! and a loopback `mrsm start api` reporting `is_blocking_on_stdin` and sending `MRSM_STDIN` over
//! the job log websocket.
//!
//! Temporary roots and plugin directories, the root's own SQLite instance (`sql:local`), the
//! `local` executor, no inherited `MRSM_*`; every job is deleted (its processes killed) on drop.
#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        let cache = PathBuf::from(std::env::var("HOME").expect("HOME"))
            .join(".cache")
            .join("mrsm-job-prompts");
        std::fs::create_dir_all(&cache).unwrap();
        Home {
            root: tempfile::tempdir_in(&cache).unwrap(),
            plugins: tempfile::tempdir_in(&cache).unwrap(),
        }
    }

    fn env(&self) -> Vec<(String, String)> {
        let p = |path: &Path| path.display().to_string();
        vec![
            ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
            ("HOME".into(), p(self.root.path())),
            ("TERM".into(), "xterm-256color".into()),
            ("LANG".into(), "C.UTF-8".into()),
            ("MRSM_ROOT_DIR".into(), p(self.root.path())),
            ("MRSM_PLUGINS_DIR".into(), p(self.plugins.path())),
            ("MRSM_PYTHON".into(), p(&self.root.path().join("no-python"))),
            ("MRSM_NO_UPDATE_CHECK".into(), "1".into()),
            (
                "MRSM_CONFIG".into(),
                r#"{"meerschaum":{"instance":"sql:local","executor":"local"},"formatting":{"ansi":false,"unicode":false},"shell":{"updates":{"check":false}},"system":{"cli":{"check_updates":false}}}"#
                    .into(),
            ),
        ]
    }

    fn command(&self) -> Command {
        let mut command = Command::new(BIN);
        command.env_clear();
        for (k, v) in self.env() {
            command.env(k, v);
        }
        command
    }

    /// `mrsm <args>` with `stdin` as its input.
    fn run(&self, args: &[&str], stdin: &str) -> Output {
        let mut child = self
            .command()
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }

    /// A job running `/bin/sh -c <script>` (through `mrsm sh <file>`), created and started.
    fn sh_job(&self, name: &str, script: &str) {
        let path = self.root.path().join(format!("{name}.sh"));
        std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let out = self.run(
            &[
                "sh",
                &path.to_string_lossy(),
                "-d",
                "--name",
                name,
                "--noask",
            ],
            "",
        );
        assert!(out.status.success(), "{}", text(&out));
    }

    fn job_dir(&self, name: &str) -> PathBuf {
        self.root.path().join("jobs").join(name)
    }

    fn waiting(&self, name: &str) -> bool {
        self.job_dir(name).join("input.stdin.block").exists()
    }

    fn result(&self, name: &str) -> Option<String> {
        let props = std::fs::read_to_string(self.job_dir(name).join("properties.json")).ok()?;
        let value: serde_json::Value = serde_json::from_str(&props).ok()?;
        let result = value.get("result")?;
        (!result.is_null()).then(|| result.to_string())
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        let _ = self
            .command()
            .args(["delete", "jobs", "-y", "-f"])
            .stdin(Stdio::null())
            .output();
    }
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn wait_until(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `mrsm attach job <name>` in a pseudo-terminal.
struct Attached {
    rx: Receiver<Vec<u8>>,
    raw: Vec<u8>,
    writer: Box<dyn Write + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    _master: Box<dyn portable_pty::MasterPty + Send>,
}

impl Attached {
    fn spawn(home: &Home, name: &str) -> Attached {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 30,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new(BIN);
        cmd.env_clear();
        for (k, v) in home.env() {
            cmd.env(k, v);
        }
        cmd.args(["attach", "job", name]);
        let child = pair.slave.spawn_command(cmd).unwrap();
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let writer = pair.master.take_writer().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
                    break;
                }
            }
        });
        Attached {
            rx,
            raw: Vec::new(),
            writer,
            child,
            _master: pair.master,
        }
    }

    fn screen(&mut self) -> String {
        while let Ok(chunk) = self.rx.try_recv() {
            self.raw.extend(chunk);
        }
        String::from_utf8_lossy(&self.raw).into_owned()
    }

    fn wait_for(&mut self, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.screen().contains(needle) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {needle:?} in {:?}",
                self.screen()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn type_line(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\r").as_bytes())
            .unwrap();
        self.writer.flush().unwrap();
    }

    /// `attach` ends by itself when the job stops.
    fn wait_exit(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "attach did not end: {:?}",
                self.screen()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Meerschaum's own prompt (`pause jobs` without names asks before pausing every running job,
/// itself included) is answered through `attach`: the question shows, `n` reaches the job.
#[test]
fn attach_answers_meerschaums_own_prompt() {
    let home = Home::new();
    let out = home.run(&["pause", "jobs", "-d", "--name", "asks"], "y\n");
    assert!(out.status.success(), "{}", text(&out));
    wait_until(30, "the job to wait for input", || home.waiting("asks"));
    let mut attached = Attached::spawn(&home, "asks");
    attached.wait_for("Pause all running jobs? [y/N]");
    attached.type_line("n");
    attached.wait_for("No jobs were paused.");
    attached.wait_exit();
    let result = home.result("asks").unwrap_or_default();
    assert!(result.contains("No jobs were paused."), "{result}");
    assert!(!home.waiting("asks"));
}

/// A silent `read` (the job printed nothing) and then a second prompt are answered through
/// `attach`.
#[test]
fn attach_answers_a_silent_read_and_a_second_prompt() {
    let home = Home::new();
    home.sh_job(
        "reads",
        "read a; echo \"first [$a]\"; read b; echo \"second [$b]\"",
    );
    wait_until(30, "the job to wait for input", || home.waiting("reads"));
    let mut attached = Attached::spawn(&home, "reads");
    std::thread::sleep(Duration::from_millis(500));
    attached.type_line("one");
    attached.wait_for("first [one]");
    attached.type_line("two");
    attached.wait_for("second [two]");
    attached.wait_exit();
    assert!(home.result("reads").is_some_and(|r| r.contains("true")));
}

fn free_port() -> u16 {
    loop {
        let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = socket.local_addr().unwrap().port();
        if ![3000, 5432, 6379, 8000, 8989].contains(&port) {
            return port;
        }
    }
}

struct Server {
    child: Child,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &format!("-{}", self.child.id())])
            .stderr(Stdio::null())
            .status();
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn get(port: u16, path: &str) -> String {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim().to_string())
        .unwrap_or_default()
}

/// The server answers `is_blocking_on_stdin` from the marker and asks the websocket client for
/// input (`MRSM_STDIN`); the client's answer reaches the job, which then finishes.
#[test]
fn the_server_reports_the_wait_and_forwards_the_websocket_answer() {
    let home = Home::new();
    let port = free_port();
    let mut command = home.command();
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let log = std::fs::File::create(home.root.path().join("server.log")).unwrap();
    let child = command
        .args([
            "--noask",
            "start",
            "api",
            "-i",
            "sql:local",
            "--host",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "--no-dash",
            "--no-auth",
            "-w",
            "1",
        ])
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let _server = Server { child };
    wait_until(60, "the server", || {
        std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
            && get(port, "/healthcheck").contains("true")
    });

    home.sh_job("asks", "read a; echo \"got [$a]\"");
    assert_eq!(get(port, "/jobs/missing/is_blocking_on_stdin"), "false");
    wait_until(30, "is_blocking_on_stdin", || {
        get(port, "/jobs/asks/is_blocking_on_stdin") == "true"
    });

    let conn = meerschaum_connector_api::APIConnector::from_uri(
        &format!("http://127.0.0.1:{port}"),
        Some("tprjobprompts"),
    )
    .unwrap();
    let lines = Arc::new(Mutex::new(String::new()));
    let asked = Arc::new(Mutex::new(0usize));
    let result = Arc::new(Mutex::new(None));
    let (l2, a2, r2) = (lines.clone(), asked.clone(), result.clone());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(60),
                conn.monitor_logs(
                    "asks",
                    meerschaum_connector_api::client::LogsMonitor {
                        on_line: Box::new(move |line| l2.lock().unwrap().push_str(&line)),
                        on_input: Some(Box::new(move || {
                            *a2.lock().unwrap() += 1;
                            "hello\n".to_string()
                        })),
                        on_stop: Some(Box::new(move |r| *r2.lock().unwrap() = Some(r))),
                        stop_on_exit: true,
                        strip_timestamps: true,
                    },
                ),
            )
            .await
        })
        .expect("the websocket did not end")
        .unwrap();
    assert_eq!(*asked.lock().unwrap(), 1);
    let lines = lines.lock().unwrap().clone();
    assert!(lines.contains("got [hello]"), "{lines:?}");
    let result = result.lock().unwrap().clone().expect("a result");
    assert!(result.success, "{result:?}");
    assert_eq!(get(port, "/jobs/asks/is_blocking_on_stdin"), "false");
}

/// An isolated team's job runs inside the team's filesystem sandbox, with its job directory out
/// of its reach: its prompt is seen (the supervisor, outside the sandbox, keeps the marker) and
/// answered, and the sandbox is not widened (the job still cannot write in its directory).
#[test]
fn a_sandboxed_team_jobs_prompt_is_seen_and_answered() {
    use meerschaum_core::team_scope::{TeamScope, write_scope_record};
    use meerschaum_core::value::{Map, Value};
    use meerschaum_jobs::job::{Job, JobOptions};
    let support = meerschaum_core::team_scope::sandbox::kernel_support();
    let home = Home::new();
    let root = home.root.path().canonicalize().unwrap();
    std::fs::create_dir_all(root.join("config")).unwrap();
    write_scope_record(
        &root,
        &TeamScope {
            team: "wetlands".into(),
            team_id: Some("id-tpr-prompts".into()),
            ..TeamScope::default()
        },
    )
    .unwrap();
    let mut ctx =
        meerschaum_jobs::JobsContext::for_root(&root).with_launcher(meerschaum_jobs::Launcher {
            supervisor: vec![
                BIN.to_string(),
                meerschaum_jobs::supervisor::SUPERVISOR_ARG.to_string(),
            ],
            program: vec![BIN.to_string()],
        });
    ctx.config.startup.enabled = false;
    let mut props = Map::new();
    props.insert("team".into(), Value::Str("wetlands".into()));
    let job_dir = root.join("jobs").join("team-asks");
    // The script lives in the team's temporary directory, which its sandbox may read.
    let tmp = meerschaum_core::team_scope::sandbox::team_tmp_dir(&root, "id-tpr-prompts").unwrap();
    std::fs::create_dir_all(&tmp).unwrap();
    let script = tmp.join("asks.sh");
    std::fs::write(
        &script,
        format!(
            "if touch '{}/escaped' 2>/dev/null; then echo writable; else echo read-only; fi\n\
             read a\necho \"got [$a]\"\n",
            job_dir.display()
        ),
    )
    .unwrap();
    let script = script.to_string_lossy().into_owned();
    let mut job = Job::new(
        "team-asks",
        Some(vec!["sh".into(), "bash".into(), script]),
        JobOptions {
            properties: Some(props),
            env: Some(
                [
                    ("MRSM_ROOT_DIR".to_string(), root.display().to_string()),
                    (
                        "MRSM_PLUGINS_DIR".to_string(),
                        home.plugins.path().display().to_string(),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
        &ctx,
    )
    .unwrap();
    let saved = job.daemon().write_properties();
    assert!(saved.success, "{saved}");
    // Start the recorded fixture through the same standalone entry point as the
    // server, so the native launcher is captured by that process at startup.
    let started = home
        .command()
        .env_remove("MRSM_PYTHON")
        .args([
            "start",
            "jobs",
            "team-asks",
            "--no-stagger",
            "--noask",
            "--nopretty",
        ])
        .output()
        .unwrap();
    assert!(started.status.success(), "{}", text(&started));
    let deadline = Instant::now() + Duration::from_secs(60);
    while !job.is_blocking_on_stdin() {
        assert!(
            Instant::now() < deadline,
            "the team job never waited for input: {:?} {:?} {:?}",
            job.status(),
            job.get_logs(),
            std::fs::read_to_string(root.join("daemon_errors.log")),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    job.write_stdin("from outside\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !(job.status() == meerschaum_jobs::Status::Stopped && home.result("team-asks").is_some())
    {
        assert!(
            Instant::now() < deadline,
            "the team job did not finish: {:?} {:?} {:?}",
            job.status(),
            job.get_logs(),
            std::fs::read_to_string(job_dir.join("properties.json")),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let logs = job.get_logs().unwrap_or_default();
    assert!(logs.contains("got [from outside]"), "{logs}");
    assert!(!home.waiting("team-asks"));
    if support.abi() > 0 {
        assert!(logs.contains("read-only"), "{logs}");
        assert!(!job_dir.join("escaped").exists());
    } else {
        eprintln!("No Landlock here: the sandbox is not checked.");
    }
    let _ = job.delete();
    if let Ok(tmp) = meerschaum_core::team_scope::sandbox::team_tmp_dir(&root, "id-tpr-prompts") {
        let _ = std::fs::remove_dir_all(tmp);
    }
}
