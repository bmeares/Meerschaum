//! The full-screen views (`watch run`, `watch jobs`) and the shell's F2 / F3 shortcuts in a real
//! pseudo-terminal, rendered with `vt100`: drawing, keys, the help and confirmation boxes,
//! resizes, quitting back to a restored screen; and the line output off a terminal.
//!
//! Scratch root and home under `~/.cache` (not `/tmp`), an empty plugins directory, a SQLite
//! instance of the test's own, no inherited `MRSM_*`. Every process started here is killed.
#![cfg(unix)]

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{Receiver, TryRecvError, channel};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");
const ROWS: u16 = 30;
const COLS: u16 = 120;
const F2: &str = "\x1bOQ";
const F3: &str = "\x1bOR";

fn env_for(base: &Path) -> Vec<(String, String)> {
    let root = base.join("root");
    let home = base.join("home");
    let plugins = base.join("plugins");
    for p in [&root, &home, &plugins] {
        std::fs::create_dir_all(p).unwrap();
    }
    let p = |path: &PathBuf| path.display().to_string();
    vec![
        ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
        ("HOME".into(), p(&home)),
        ("TERM".into(), "xterm-256color".into()),
        ("LANG".into(), "C.UTF-8".into()),
        ("LC_ALL".into(), "C.UTF-8".into()),
        ("MRSM_ROOT_DIR".into(), p(&root)),
        ("MRSM_PLUGINS_DIR".into(), p(&plugins)),
        ("MRSM_NO_AUTO_INSTALL".into(), "1".into()),
        ("MRSM_NO_UPDATE_CHECK".into(), "1".into()),
        (
            "MRSM_CONFIG".into(),
            r#"{"meerschaum":{"instance":"sql:tuitest"},"shell":{"updates":{"check_remote":false}}}"#
                .into(),
        ),
        (
            "MRSM_SQL_TUITEST".into(),
            format!(
                r#"{{"flavor":"sqlite","database":"{}"}}"#,
                base.join("tuitest.db").display()
            ),
        ),
    ]
}

fn mrsm(env: &[(String, String)], args: &[&str]) -> (bool, String, String) {
    let mut command = Command::new(BIN);
    command.env_clear().args(args);
    for (k, v) in env {
        command.env(k, v);
    }
    let out = command.output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn ok(env: &[(String, String)], args: &[&str]) -> String {
    let (success, out, err) = mrsm(env, args);
    assert!(success, "{args:?}\n{out}\n{err}");
    out
}

/// A scratch directory under the home cache with the chain `raw -> clean` registered.
struct Setup {
    base: PathBuf,
    env: Vec<(String, String)>,
}

impl Setup {
    fn new(name: &str) -> Setup {
        let base = PathBuf::from(std::env::var("HOME").expect("HOME"))
            .join(".cache")
            .join("mrsm-tui-pty")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let env = env_for(&base);
        ok(
            &env,
            &[
                "register",
                "pipes",
                "-c",
                "sql:tuitest",
                "-m",
                "raw",
                "--params",
                r#"{"fetch":{"definition":"SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2, 'b'"},"columns":{"id":"id"}}"#,
                "--noask",
            ],
        );
        ok(
            &env,
            &[
                "register",
                "pipes",
                "-c",
                "sql:tuitest",
                "-m",
                "clean",
                "--params",
                r#"{"fetch":{"definition":"SELECT id, v FROM {{ Pipe('sql:tuitest', 'raw', instance='sql:tuitest') }}"},"columns":{"id":"id"}}"#,
                "--noask",
            ],
        );
        Setup { base, env }
    }

    /// Sync the chain as one engine run; returns its ID.
    fn run(&self) -> String {
        ok(
            &self.env,
            &["sync", "pipes", "-m", "clean", "--parents", "--yes"],
        );
        let json = ok(&self.env, &["show", "runs", "--nopretty"]);
        let runs: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
        runs[0]["run"]["run_id"].as_str().unwrap().to_string()
    }

    /// Register `slow` (a root whose fetch takes a few seconds) and its child `after`.
    fn slow_chain(&self) {
        ok(
            &self.env,
            &[
                "register",
                "pipes",
                "-c",
                "sql:tuitest",
                "-m",
                "slow",
                "--params",
                r#"{"fetch":{"definition":"WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 20000000) SELECT MAX(x) AS id, 'a' AS v FROM c"},"columns":{"id":"id"}}"#,
                "--noask",
            ],
        );
        ok(
            &self.env,
            &[
                "register",
                "pipes",
                "-c",
                "sql:tuitest",
                "-m",
                "after",
                "--params",
                r#"{"fetch":{"definition":"SELECT id, v FROM {{ Pipe('sql:tuitest', 'slow', instance='sql:tuitest') }}"},"columns":{"id":"id"}}"#,
                "--noask",
            ],
        );
    }

    /// Start syncing `slow -> after` in the background and wait until its run is recorded as
    /// running. Returns the process and the run ID.
    fn start_slow_run(&self) -> (Background, String) {
        let mut command = Command::new(BIN);
        command
            .env_clear()
            .args(["sync", "pipes", "-m", "after", "--parents", "--yes"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        for (k, v) in &self.env {
            command.env(k, v);
        }
        let child = Background(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            let (_, json, _) = mrsm(&self.env, &["show", "runs", "--nopretty"]);
            if let Ok(runs) = serde_json::from_str::<serde_json::Value>(json.trim())
                && let Some(run) = runs.as_array().and_then(|r| r.first())
                && run["status"] == "running"
            {
                return (child, run["run"]["run_id"].as_str().unwrap().to_string());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("the slow run was never recorded as running");
    }
}

/// A background `mrsm`, killed when dropped.
struct Background(std::process::Child);

impl Background {
    fn running(&mut self) -> bool {
        matches!(self.0.try_wait(), Ok(None))
    }

    fn wait(&mut self, seconds: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.0.try_wait() {
                return status.success();
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("the background sync did not end");
    }

    /// `kill -9`: the run's row is never marked finished.
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        self.kill();
    }
}

impl Drop for Setup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

/// Answers cursor position requests (`CSI 6 n`) like a terminal.
#[derive(Default)]
struct Replies(Vec<String>);

impl vt100::Callbacks for Replies {
    fn unhandled_csi(
        &mut self,
        screen: &mut vt100::Screen,
        i1: Option<u8>,
        _i2: Option<u8>,
        params: &[&[u16]],
        c: char,
    ) {
        if c == 'n' && i1.is_none() && params == [&[6u16][..]] {
            let (row, col) = screen.cursor_position();
            self.0.push(format!("\x1b[{};{}R", row + 1, col + 1));
        }
    }
}

struct Term {
    parser: vt100::Parser<Replies>,
    raw: Vec<u8>,
    rx: Receiver<Vec<u8>>,
    writer: Box<dyn Write + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    master: Box<dyn portable_pty::MasterPty + Send>,
}

impl Term {
    fn spawn(env: &[(String, String)], cwd: &Path, args: &[&str]) -> Term {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: ROWS,
                cols: COLS,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new(BIN);
        cmd.env_clear();
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.args(args);
        cmd.cwd(cwd);
        let child = pair.slave.spawn_command(cmd).unwrap();
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let writer = pair.master.take_writer().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            let mut buf = [0u8; 65536];
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
                    break;
                }
            }
        });
        Term {
            parser: vt100::Parser::new_with_callbacks(ROWS, COLS, 0, Replies::default()),
            raw: Vec::new(),
            rx,
            writer,
            child,
            master: pair.master,
        }
    }

    fn pump(&mut self, limit: Duration) {
        let start = Instant::now();
        while start.elapsed() < limit {
            match self.rx.try_recv() {
                Ok(bytes) => {
                    self.parser.process(&bytes);
                    for reply in std::mem::take(&mut self.parser.callbacks_mut().0) {
                        let _ = self.writer.write_all(reply.as_bytes());
                        let _ = self.writer.flush();
                    }
                    self.raw.extend_from_slice(&bytes);
                }
                Err(TryRecvError::Disconnected) => return,
                Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }

    fn send(&mut self, text: &str) {
        self.writer.write_all(text.as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

    fn resize(&mut self, rows: u16, cols: u16) {
        self.master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        self.parser.screen_mut().set_size(rows, cols);
    }

    fn screen(&self) -> String {
        self.parser.screen().contents()
    }

    fn alternate(&self) -> bool {
        self.parser.screen().alternate_screen()
    }

    /// Pump until `check(screen)` holds, or panic after `seconds`.
    fn wait_screen(&mut self, what: &str, seconds: u64, check: impl Fn(&str) -> bool) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(seconds) {
            self.pump(Duration::from_millis(100));
            if check(&self.screen()) {
                return;
            }
        }
        panic!(
            "timed out waiting for {what}:\n{}\n--- raw ---\n{:?}",
            self.screen(),
            String::from_utf8_lossy(&self.raw)
        );
    }

    /// Pump until the alternate screen is `on`.
    fn wait_alternate(&mut self, on: bool, seconds: u64) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(seconds) {
            self.pump(Duration::from_millis(100));
            if self.alternate() == on {
                return;
            }
        }
        panic!(
            "the alternate screen is not {}:\n{}",
            if on { "on" } else { "off" },
            self.screen()
        );
    }

    fn wait_exit(&mut self, seconds: u64) -> portable_pty::ExitStatus {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(seconds) {
            self.pump(Duration::from_millis(100));
            if let Ok(Some(status)) = self.child.try_wait() {
                self.pump(Duration::from_millis(300));
                return status;
            }
        }
        panic!("the process did not end:\n{}", self.screen());
    }

    fn no_crash_text(&self) {
        let raw = String::from_utf8_lossy(&self.raw);
        for bad in ["panicked", "Traceback", "RUST_BACKTRACE"] {
            assert!(!raw.contains(bad), "{bad:?} in the output:\n{raw}");
        }
    }

    /// The terminal modes a view must leave as it found them.
    fn assert_restored(&self) {
        let screen = self.parser.screen();
        assert!(!screen.alternate_screen(), "{}", self.screen());
        assert!(!screen.hide_cursor(), "the cursor is shown again");
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn watch_run_draws_keys_resizes_and_quits() {
    let setup = Setup::new("run");
    let run_id = setup.run();
    let mut term = Term::spawn(&setup.env, &setup.base, &["watch", "run"]);
    term.wait_screen("the run monitor", 60, |s| {
        s.contains("Timeline") && s.contains("sql:tuitest:clean") && s.contains("succeeded")
    });
    assert!(term.alternate());
    let screen = term.screen();
    assert!(
        screen.contains(&format!("run {run_id} on sql:tuitest")),
        "{screen}"
    );
    // The events and the run row arrive separately: wait for both.
    term.wait_screen("the whole run", 30, |s| {
        [
            "Nodes",
            "Events",
            "sql:tuitest:raw",
            "run succeeded",
            "2 ok",
        ]
        .iter()
        .all(|text| s.contains(text))
    });
    // `?` opens the key help; any key closes it.
    term.send("?");
    term.wait_screen("the help", 10, |s| {
        s.contains("Keys") && s.contains("node details")
    });
    term.send("x");
    term.wait_screen("the help to close", 10, |s| !s.contains("node details"));
    // Enter opens the selected node's details; Esc closes them without quitting.
    term.send("\r");
    term.wait_screen("the details", 10, |s| {
        s.contains("Esc closes") && s.contains("inserted")
    });
    term.send("\x1b");
    term.wait_screen("the details to close", 10, |s| !s.contains("Esc closes"));
    assert!(term.alternate());
    // A smaller terminal drops the timeline; a larger one brings it back.
    term.resize(14, 70);
    term.wait_screen("the small layout", 10, |s| {
        !s.contains("Timeline") && s.contains("Nodes")
    });
    term.resize(40, 140);
    term.wait_screen("the large layout", 10, |s| s.contains("Timeline"));
    term.send("q");
    let status = term.wait_exit(20);
    assert!(status.success(), "{}", term.screen());
    term.assert_restored();
    term.no_crash_text();
}

#[test]
fn watch_run_debug_output_waits_for_the_main_screen() {
    let setup = Setup::new("debug");
    setup.run();
    let mut term = Term::spawn(&setup.env, &setup.base, &["watch", "run", "--debug"]);
    term.wait_screen("the run monitor", 60, |s| {
        s.contains("Timeline") && s.contains("succeeded")
    });
    // Let the poller run a few queries while the view is shown.
    term.pump(Duration::from_secs(2));
    term.send("q");
    let status = term.wait_exit(20);
    assert!(status.success(), "{}", term.screen());
    term.assert_restored();
    let raw = String::from_utf8_lossy(&term.raw).into_owned();
    let enter = raw.find("\x1b[?1049h").expect("the alternate screen");
    let leave = raw.rfind("\x1b[?1049l").expect("the main screen again");
    let view = &raw[enter..leave];
    assert!(
        !view.contains("| meerschaum_"),
        "debug output over the view:\n{view:?}"
    );
    // What the view held back is printed once the main screen is back.
    assert!(
        raw[leave..].contains("| meerschaum_connector_sql::driver:"),
        "{:?}",
        &raw[leave..]
    );
    term.no_crash_text();
}

#[test]
fn watch_run_ctrl_c_restores_the_terminal() {
    let setup = Setup::new("ctrlc");
    let run_id = setup.run();
    let mut term = Term::spawn(&setup.env, &setup.base, &["watch", "run", &run_id]);
    term.wait_screen("the run monitor", 60, |s| s.contains("Timeline"));
    term.send("\x03");
    term.wait_exit(20);
    term.assert_restored();
    term.no_crash_text();
}

/// Send `signal` (a `kill` name: `TERM`, `INT`, `HUP`) to the process in the terminal.
fn signal(term: &Term, signal: &str) {
    let pid = term.child.process_id().expect("the child's pid");
    let status = Command::new("kill")
        .args([&format!("-{signal}"), &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -{signal} {pid}");
}

impl Term {
    /// The terminal is back in cooked mode (canonical input and echo), as `stty` reads it.
    fn assert_cooked(&self) {
        let tty = self.master.tty_name().expect("the pty's name");
        let out = Command::new("stty")
            .arg("-F")
            .arg(&tty)
            .arg("-a")
            .output()
            .unwrap();
        let modes = String::from_utf8_lossy(&out.stdout);
        let words: Vec<&str> = modes.split_whitespace().collect();
        for mode in ["icanon", "echo", "isig"] {
            assert!(words.contains(&mode), "{mode} is off:\n{modes}");
        }
    }
}

/// `watch run` full-screen, ended by `signals` sent from outside (each `(signal, pause)`):
/// whatever ends it, the terminal is left on the main screen, the cursor shown, in cooked mode.
fn watch_run_ended_by(name: &str, signals: &[(&str, u64)]) -> portable_pty::ExitStatus {
    let setup = Setup::new(name);
    let run_id = setup.run();
    let mut term = Term::spawn(&setup.env, &setup.base, &["watch", "run", &run_id]);
    term.wait_screen("the run monitor", 60, |s| s.contains("Timeline"));
    assert!(term.alternate());
    for (name, pause) in signals {
        signal(&term, name);
        std::thread::sleep(Duration::from_millis(*pause));
    }
    let status = term.wait_exit(20);
    term.assert_restored();
    term.assert_cooked();
    term.no_crash_text();
    status
}

#[test]
fn watch_run_sigterm_restores_the_terminal() {
    let status = watch_run_ended_by("sigterm", &[("TERM", 0)]);
    assert!(!status.success());
}

#[test]
fn watch_run_sighup_restores_the_terminal() {
    let status = watch_run_ended_by("sighup", &[("HUP", 0)]);
    assert!(!status.success());
}

#[test]
fn watch_run_forced_sigint_restores_the_terminal() {
    // Two SIGINTs back to back: the second one forces the exit (`Interrupted.`, status 130)
    // unless the view already quit on the first.
    watch_run_ended_by("sigint2", &[("INT", 0), ("INT", 0)]);
}

#[test]
fn watch_jobs_lists_follows_logs_and_confirms() {
    let setup = Setup::new("jobs");
    let _cleanup = DeleteJobs(setup.env.clone(), &["tuijob", "tuisleep"]);
    // A job that ends at once (its log has the version).
    ok(
        &setup.env,
        &["show", "version", "-d", "--name", "tuijob", "--noask"],
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let out = ok(&setup.env, &["show", "jobs", "--nopretty"]);
        if out.contains("tuijob") {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let mut term = Term::spawn(&setup.env, &setup.base, &["watch", "jobs"]);
    term.wait_screen("the jobs monitor", 60, |s| {
        s.contains("jobs on local") && s.contains("tuijob") && s.contains("Log: tuijob")
    });
    term.wait_screen("the job's log", 30, |s| s.contains("Meerschaum v"));
    // Starting a stopped job asks first; `n` does nothing.
    term.send("s");
    term.wait_screen("the confirmation", 10, |s| {
        s.contains("Start job 'tuijob'? [y/N]")
    });
    term.send("n");
    term.wait_screen("the confirmation to close", 10, |s| !s.contains("[y/N]"));
    // Pausing a stopped job is refused without a question.
    term.send("p");
    term.wait_screen("the refusal", 10, |s| s.contains("is not running"));
    // `y` runs the command.
    term.pump(Duration::from_secs(1));
    term.send("s");
    term.wait_screen("the confirmation", 10, |s| {
        s.contains("Start job 'tuijob'? [y/N]")
    });
    term.send("y");
    term.wait_screen("the job to start", 30, |s| s.contains("start 'tuijob':"));
    // Stopping a running job.
    ok(
        &setup.env,
        &["sh", "sleep", "300", "-d", "--name", "tuisleep", "--noask"],
    );
    term.send("r");
    term.wait_screen("the running job", 30, |s| {
        s.lines()
            .any(|l| l.contains("tuisleep") && l.contains("running"))
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    // The list order is the executor's: try up, then down.
    let mut key = "k";
    while !term.screen().contains("Log: tuisleep") && Instant::now() < deadline {
        term.send(key);
        term.pump(Duration::from_millis(300));
        key = if key == "k" { "j" } else { "k" };
    }
    term.send("x");
    term.wait_screen("the confirmation", 10, |s| {
        s.contains("Stop job 'tuisleep'? [y/N]")
    });
    term.send("y");
    term.wait_screen("the job to stop", 60, |s| {
        s.lines()
            .any(|l| l.contains("tuisleep") && l.contains("stopped"))
    });
    term.send("\x1b");
    let status = term.wait_exit(20);
    assert!(status.success(), "{}", term.screen());
    term.assert_restored();
    term.no_crash_text();
}

/// Deletes (and so stops) the named jobs when dropped, also when a test fails.
struct DeleteJobs(Vec<(String, String)>, &'static [&'static str]);

impl Drop for DeleteJobs {
    fn drop(&mut self) {
        let mut args = vec!["delete", "jobs"];
        args.extend_from_slice(self.1);
        args.push("-y");
        let _ = mrsm(&self.0, &args);
    }
}

#[test]
fn shell_f2_and_f3_open_the_views_and_return_to_the_line() {
    let setup = Setup::new("shell");
    setup.run();
    let mut term = Term::spawn(&setup.env, &setup.base, &[]);
    term.wait_screen("the prompt", 60, |s| s.contains('➤'));
    term.send("show ver");
    term.wait_screen("the typed line", 10, |s| s.contains("show ver"));
    term.pump(Duration::from_millis(300));
    term.send(F3);
    term.wait_screen("the jobs monitor", 30, |s| s.contains("jobs on local"));
    assert!(term.alternate());
    term.send("q");
    term.wait_alternate(false, 20);
    term.wait_screen("the line, kept", 10, |s| {
        s.lines().any(|l| l.contains('➤') && l.contains("show ver"))
    });
    term.send(F2);
    term.wait_screen("the run monitor", 30, |s| s.contains("Timeline"));
    term.send("q");
    term.wait_alternate(false, 20);
    term.wait_screen("the line, kept", 10, |s| {
        s.lines().any(|l| l.contains('➤') && l.contains("show ver"))
    });
    // The kept line still runs.
    term.send("sion\r");
    term.wait_screen("the version", 30, |s| s.contains("Meerschaum v"));
    term.send("exit\r");
    term.wait_exit(30);
    term.assert_restored();
    term.no_crash_text();
}

#[test]
fn line_output_off_a_terminal() {
    let setup = Setup::new("lines");
    let (success, out, _) = mrsm(&setup.env, &["watch", "run"]);
    assert!(!success);
    assert!(out.contains("No runs have been recorded"), "{out}");
    let run_id = setup.run();
    // Text lines until the run ends; the result is the run's.
    let (success, out, err) = mrsm(&setup.env, &["watch", "run"]);
    assert!(success, "{out}\n{err}");
    assert!(out.contains("run started: 2 nodes"), "{out}");
    assert!(out.contains("sql:tuitest:clean  succeeded"), "{out}");
    assert!(out.contains("run succeeded"), "{out}");
    // The event lines are plain (the closing result is the dispatcher's usual tuple).
    assert!(
        out.lines()
            .filter(|l| l.starts_with(|c: char| c.is_ascii_digit()))
            .all(|l| !l.contains('\u{1b}')),
        "no escapes in the event lines: {out:?}"
    );
    // JSON lines with `--nopretty`.
    let (success, out, _) = mrsm(&setup.env, &["watch", "run", &run_id, "--nopretty"]);
    assert!(success);
    let events: Vec<serde_json::Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{e}: {l}")))
        .collect();
    assert_eq!(events[0]["event"], "run_started", "{out}");
    assert_eq!(events.last().unwrap()["event"], "run_finished", "{out}");
    assert!(events.iter().all(|e| e["run_id"] == run_id.as_str()));
    let (success, out, _) = mrsm(&setup.env, &["watch", "run", "01NOPE"]);
    assert!(!success);
    assert!(out.contains("No run '01NOPE' was found."), "{out}");
    // `watch jobs` prints `show jobs`.
    let (_, watched, _) = mrsm(&setup.env, &["watch", "jobs"]);
    let (_, shown, _) = mrsm(&setup.env, &["show", "jobs"]);
    assert_eq!(watched, shown);
    // A bad subaction lists the choices.
    let (success, out, err) = mrsm(&setup.env, &["watch", "nothing"]);
    assert!(!success);
    assert!(
        out.contains("watch jobs") && out.contains("watch runs"),
        "{out}\n{err}"
    );
    // Help.
    let help = ok(&setup.env, &["watch", "--help"]);
    assert!(help.contains("Options:"), "{help}");
    let help = ok(&setup.env, &["watch", "run", "--help"]);
    assert!(help.contains("Gantt"), "{help}");
}

#[test]
fn nopretty_on_a_terminal_prints_lines() {
    let setup = Setup::new("nopretty");
    let run_id = setup.run();
    let mut term = Term::spawn(
        &setup.env,
        &setup.base,
        &["watch", "run", &run_id, "--nopretty"],
    );
    let status = term.wait_exit(60);
    assert!(status.success(), "{}", term.screen());
    assert!(!term.alternate());
    assert!(
        term.screen().contains("\"event\":\"run_finished\""),
        "{}",
        term.screen()
    );
}

#[test]
fn watch_run_follows_a_run_in_another_process() {
    let setup = Setup::new("inprogress");
    setup.slow_chain();
    let (mut sync, run_id) = setup.start_slow_run();
    let mut term = Term::spawn(&setup.env, &setup.base, &["watch", "run"]);
    term.wait_screen("the running run", 60, |s| {
        s.contains(&format!("run {run_id}"))
            && s.contains("sql:tuitest:slow")
            && s.contains("running")
    });
    assert!(sync.running(), "the run is watched while it is in progress");
    // The nodes move on as the other process persists its events.
    term.wait_screen("the run to end", 120, |s| {
        s.contains("sql:tuitest:after") && s.contains("run succeeded") && s.contains("2 ok")
    });
    assert!(sync.wait(60));
    term.send("q");
    let status = term.wait_exit(20);
    assert!(status.success(), "{}", term.screen());
    term.assert_restored();
    term.no_crash_text();
}

#[test]
fn line_output_follows_a_run_until_it_ends() {
    let setup = Setup::new("followlines");
    setup.slow_chain();
    let (mut sync, run_id) = setup.start_slow_run();
    assert!(sync.running());
    let (success, out, err) = mrsm(&setup.env, &["watch", "run"]);
    assert!(success, "{out}\n{err}");
    // Every event, the first ones included (persisted although the node held the database).
    assert!(out.contains("run started: 2 nodes"), "{out}");
    assert!(out.contains("sql:tuitest:slow  started"), "{out}");
    assert!(out.contains("sql:tuitest:after  succeeded"), "{out}");
    assert!(out.contains("run succeeded"), "{out}");
    assert!(sync.wait(60));
    let (success, out, _) = mrsm(&setup.env, &["watch", "run", &run_id, "--nopretty"]);
    assert!(success);
    assert!(
        out.lines()
            .next()
            .is_some_and(|l| l.contains("run_started")),
        "{out}"
    );
}

#[test]
fn a_run_whose_process_died_is_not_followed_forever() {
    let setup = Setup::new("deadrun");
    setup.slow_chain();
    let (mut sync, dead) = setup.start_slow_run();
    std::thread::sleep(Duration::from_secs(1));
    sync.kill();
    // A newer run: `watch run` opens it, not the dead one that still says "running".
    let newer = setup.run();
    let json = ok(&setup.env, &["show", "runs", "--nopretty"]);
    assert!(json.contains(&dead), "{json}");
    let started = Instant::now();
    let (success, out, err) = mrsm(&setup.env, &["watch", "run", "--nopretty"]);
    assert!(success, "{out}\n{err}");
    assert!(out.contains(&newer) && !out.contains(&dead), "{out}");
    assert!(started.elapsed() < Duration::from_secs(30));
    // Following the dead run gives up after `--timeout` seconds without new events.
    let started = Instant::now();
    let (success, out, _) = mrsm(&setup.env, &["watch", "run", &dead, "--timeout", "2"]);
    assert!(!success, "{out}");
    assert!(out.contains("may have died"), "{out}");
    assert!(started.elapsed() < Duration::from_secs(30));
}

/// `MRSM_CONFIG` with `shell:keybindings`.
fn with_bindings(env: &[(String, String)], bindings: &str) -> Vec<(String, String)> {
    env.iter()
        .map(|(k, v)| {
            if k == "MRSM_CONFIG" {
                let mut config: serde_json::Value = serde_json::from_str(v).unwrap();
                config["shell"]["keybindings"] = serde_json::from_str(bindings).unwrap();
                (k.clone(), config.to_string())
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect()
}

#[test]
fn shell_key_command_output_stays_above_the_kept_line() {
    let setup = Setup::new("keyoutput");
    let env = with_bindings(&setup.env, r#"{"f5": "show version"}"#);
    let mut term = Term::spawn(&env, &setup.base, &[]);
    term.wait_screen("the prompt", 60, |s| s.contains('➤'));
    term.send("show ver");
    term.wait_screen("the typed line", 10, |s| s.contains("show ver"));
    term.pump(Duration::from_millis(300));
    // F2 with no runs: the error stays on the screen, the line comes back below it.
    term.send(F2);
    term.wait_screen("the error above the kept line", 30, |s| {
        let lines: Vec<&str> = s.lines().collect();
        let error = lines
            .iter()
            .position(|l| l.contains("No runs have been recorded"));
        let prompt = lines
            .iter()
            .rposition(|l| l.contains('➤') && l.contains("show ver"));
        matches!((error, prompt), (Some(e), Some(p)) if e < p)
    });
    term.pump(Duration::from_millis(500));
    assert!(
        term.screen().contains("No runs have been recorded"),
        "{}",
        term.screen()
    );
    // A short command bound to a key: its output stays too.
    term.send("\x1b[15~");
    term.wait_screen("the version above the kept line", 30, |s| {
        let lines: Vec<&str> = s.lines().collect();
        let version = lines.iter().position(|l| l.contains("Meerschaum v"));
        let prompt = lines
            .iter()
            .rposition(|l| l.contains('➤') && l.contains("show ver"));
        matches!((version, prompt), (Some(v), Some(p)) if v < p)
    });
    term.pump(Duration::from_millis(500));
    let screen = term.screen();
    assert!(screen.contains("Meerschaum v"), "{screen}");
    assert!(screen.contains("No runs have been recorded"), "{screen}");
    // One prompt with the kept line (the old one was erased, not left behind).
    assert_eq!(
        screen
            .lines()
            .filter(|l| l.contains('➤') && l.contains("show ver"))
            .count(),
        1,
        "{screen}"
    );
    term.send("sion\r");
    // A typed line clears the screen first (v4), then prints.
    term.wait_screen("the typed command's output", 30, |s| {
        s.contains("Meerschaum v") && !s.contains("No runs have been recorded")
    });
    term.send("exit\r");
    term.wait_exit(30);
    term.assert_restored();
    term.no_crash_text();
}
