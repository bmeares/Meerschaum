//! Ctrl-C while an action runs (wave 18, decision 1), in a real pseudo-terminal: it cancels only
//! that action. The shell prints `Interrupted.`, goes back to its prompt and keeps working;
//! `mrsm <action>` exits 130 with no panic or traceback; a program the shell started keeps the
//! terminal's Ctrl-C and a second one within two seconds kills it.
//!
//! The slow action is `show data` on a pipe whose table is a SQLite view that takes many seconds
//! to read. Temporary root and home, empty plugins directory, a SQLite instance of the test's
//! own, no inherited `MRSM_*`.
#![cfg(unix)]

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{Receiver, TryRecvError, channel};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");
const SLOW_VIEW: &str = "CREATE VIEW slowv AS WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
SELECT x+1 FROM c WHERE x<300000000) SELECT x AS id FROM c";
const SLOW_SHOW: &str = "show data -c plugin:x -m slow -l None -i sql:w18sh";
const PROMPT: &str = "sql:w18sh";

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
            r#"{"meerschaum":{"instance":"sql:w18sh"},"shell":{"updates":{"check_remote":false}}}"#
                .into(),
        ),
        (
            "MRSM_SQL_W18SH".into(),
            format!(
                r#"{{"flavor":"sqlite","database":"{}"}}"#,
                base.join("w18sh.db").display()
            ),
        ),
    ]
}

fn run(env: &[(String, String)], args: &[&str]) {
    let mut command = Command::new(BIN);
    command.env_clear().args(args);
    for (k, v) in env {
        command.env(k, v);
    }
    let out = command.output().unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn create_view(database: &Path) {
    use meerschaum_connector_sql::{
        SqlConnector,
        flavor::Flavor,
        uri::{ConnectionSpec, ConnectorAttributes},
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(database.to_string_lossy().into_owned()),
        ..Default::default()
    };
    let connector = runtime
        .block_on(SqlConnector::connect(
            "w18sh",
            ConnectionSpec::from_attributes(&attrs).unwrap(),
        ))
        .unwrap();
    let queries = [SLOW_VIEW.to_string()];
    assert!(
        runtime
            .block_on(connector.exec_queries(&queries, true, true))
            .iter()
            .all(Option::is_some)
    );
}

/// A scratch directory under the home cache (not `/tmp`), with the slow pipe registered.
struct Setup {
    base: PathBuf,
    env: Vec<(String, String)>,
}

impl Setup {
    fn new(name: &str) -> Setup {
        let cache = PathBuf::from(std::env::var("HOME").expect("HOME"))
            .join(".cache")
            .join("mrsm-w18-sh-tests");
        let base = cache.join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let env = env_for(&base);
        create_view(&base.join("w18sh.db"));
        run(
            &env,
            &[
                "register",
                "pipes",
                "-c",
                "plugin:x",
                "-m",
                "slow",
                "-l",
                "None",
                "-i",
                "sql:w18sh",
                "--params",
                r#"{"target":"slowv","columns":{"id":"id"}}"#,
                "--noask",
            ],
        );
        run(
            &env,
            &[
                "register",
                "pipes",
                "-c",
                "sql:w18sh",
                "-m",
                "fast",
                "-l",
                "None",
                "-i",
                "sql:w18sh",
                "--params",
                r#"{"fetch":{"definition":"SELECT 1 AS id"},"columns":{"id":"id"}}"#,
                "--noask",
            ],
        );
        run(
            &env,
            &[
                "register",
                "pipes",
                "-c",
                "sql:w18sh",
                "-m",
                "slowsync",
                "-l",
                "None",
                "-i",
                "sql:w18sh",
                "--params",
                r#"{"fetch":{"definition":"SELECT id FROM slowv"},"columns":{"id":"id"}}"#,
                "--noask",
            ],
        );
        Setup { base, env }
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
    _master: Box<dyn portable_pty::MasterPty + Send>,
}

impl Term {
    fn spawn(env: &[(String, String)], cwd: &Path, args: &[&str]) -> Term {
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
            parser: vt100::Parser::new_with_callbacks(30, 120, 0, Replies::default()),
            raw: Vec::new(),
            rx,
            writer,
            child,
            _master: pair.master,
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

    fn screen(&self) -> String {
        self.parser.screen().contents()
    }

    fn raw_text(&self) -> String {
        String::from_utf8_lossy(&self.raw).into_owned()
    }

    /// Pump until `check(screen)` holds, or panic after `seconds`.
    fn wait_screen(&mut self, what: &str, seconds: u64, check: impl Fn(&str) -> bool) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(seconds) {
            self.pump(Duration::from_millis(150));
            if check(&self.screen()) {
                return;
            }
        }
        panic!(
            "timed out waiting for {what}:\n{}\n--- raw ---\n{:?}",
            self.screen(),
            self.raw_text()
        );
    }

    /// Pump until the raw output has `needle` (a spinner frame, which the screen no longer shows).
    fn wait_raw(&mut self, needle: &str, seconds: u64) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(seconds) {
            self.pump(Duration::from_millis(150));
            if self.raw_text().contains(needle) {
                return;
            }
        }
        panic!("no {needle:?} in:\n{}", self.raw_text());
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
        let raw = self.raw_text();
        for bad in ["panicked", "Traceback", "RUST_BACKTRACE", "thread '"] {
            assert!(!raw.contains(bad), "{bad:?} in the output:\n{raw}");
        }
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_shell(setup: &Setup) -> Term {
    let mut term = Term::spawn(&setup.env, &setup.base, &[]);
    term.wait_screen("the shell prompt", 60, |s| s.contains(PROMPT));
    term
}

#[test]
fn ctrl_c_cancels_a_slow_action_and_the_shell_keeps_working() {
    let setup = Setup::new("slow-show");
    let mut term = start_shell(&setup);
    term.send(&format!("{SLOW_SHOW}\r"));
    term.wait_raw("Reading data", 60);
    let started = Instant::now();
    term.send("\x03");
    term.wait_screen("Interrupted.", 20, |s| s.contains("Interrupted."));
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "the action was not cancelled promptly: {:?}",
        started.elapsed()
    );
    // Back at the prompt: the shell still runs commands.
    term.wait_screen("the prompt after Interrupted.", 20, |s| {
        s.lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .is_some_and(|l| l.contains(PROMPT) || l.contains('➤'))
            || s.contains(PROMPT)
    });
    term.send("show version\r");
    term.wait_screen("the version", 30, |s| s.contains("Meerschaum v"));
    term.send("exit\r");
    let status = term.wait_exit(30);
    assert!(status.success(), "{status:?}");
    term.no_crash_text();
}

#[test]
fn ctrl_c_at_an_idle_prompt_clears_the_line_and_only_exit_leaves() {
    let setup = Setup::new("idle");
    let mut term = start_shell(&setup);
    term.send("show vers");
    term.pump(Duration::from_millis(500));
    term.send("\x03");
    term.pump(Duration::from_millis(800));
    assert!(
        term.child.try_wait().unwrap().is_none(),
        "Ctrl-C at the prompt ended the shell:\n{}",
        term.screen()
    );
    term.send("show version --nopretty\r");
    term.wait_screen("the version and the next prompt", 30, |screen| {
        let lines: Vec<_> = screen.lines().collect();
        lines
            .iter()
            .position(|line| line.trim() == env!("CARGO_PKG_VERSION"))
            .is_some_and(|row| lines[row + 1..].iter().any(|line| line.contains('➤')))
    });
    term.send("\x04");
    let status = term.wait_exit(30);
    assert!(status.success(), "{status:?}");
    term.no_crash_text();
}

#[test]
fn ctrl_c_outside_the_shell_exits_130_with_interrupted_and_no_panic() {
    let setup = Setup::new("standalone");
    let mut term = Term::spawn(
        &setup.env,
        &setup.base,
        &[
            "show",
            "data",
            "-c",
            "plugin:x",
            "-m",
            "slow",
            "-l",
            "None",
            "-i",
            "sql:w18sh",
        ],
    );
    term.wait_raw("Reading data", 60);
    term.send("\x03");
    let status = term.wait_exit(30);
    assert_eq!(status.exit_code(), 130, "{status:?}");
    assert!(term.screen().contains("Interrupted."), "{}", term.screen());
    assert!(
        !term.screen().contains("Reading data"),
        "the spinner line stayed:\n{}",
        term.screen()
    );
    term.no_crash_text();
}

#[test]
fn a_program_the_shell_started_gets_ctrl_c_and_the_shell_returns() {
    let setup = Setup::new("program");
    let mut term = start_shell(&setup);
    term.send("sh sleep 30\r");
    term.pump(Duration::from_millis(1500));
    let started = Instant::now();
    term.send("\x03");
    term.wait_screen("Interrupted.", 20, |s| s.contains("Interrupted."));
    assert!(started.elapsed() < Duration::from_secs(10));
    term.send("show version\r");
    term.wait_screen("the version", 30, |s| s.contains("Meerschaum v"));
    term.send("exit\r");
    assert!(term.wait_exit(30).success());
    term.no_crash_text();
}

#[test]
fn a_second_ctrl_c_within_two_seconds_kills_a_program_which_ignores_the_first() {
    let setup = Setup::new("kill");
    let mut term = start_shell(&setup);
    // The script ignores SIGINT (and so does its `sleep`): only the second Ctrl-C (SIGKILL)
    // ends the wait.
    let script = setup.base.join("ignore_int.sh");
    std::fs::write(&script, "#!/bin/bash\ntrap '' INT\nsleep 4\n").unwrap();
    std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    term.send(&format!("sh {}\r", script.display()));
    term.pump(Duration::from_millis(1500));
    term.send("\x03");
    term.pump(Duration::from_millis(600));
    assert!(
        !term.screen().contains("Interrupted."),
        "one Ctrl-C ended a program which ignores it:\n{}",
        term.screen()
    );
    term.send("\x03");
    term.wait_screen("Interrupted.", 3, |s| s.contains("Interrupted."));
    term.send("exit\r");
    assert!(term.wait_exit(30).success());
    // The program's own children (the `sleep`) end by themselves in a few seconds.
    std::thread::sleep(Duration::from_secs(3));
}

#[test]
fn an_action_which_stops_on_the_flag_stops_gracefully_and_the_shell_goes_on() {
    let setup = Setup::new("loop");
    let mut term = start_shell(&setup);
    term.send("sync pipes -c sql:w18sh -m fast -l None -i sql:w18sh --loop --min-seconds 100\r");
    term.wait_screen("the sleep between laps", 60, |s| s.contains("Sleeping for"));
    term.send("\x03");
    // The lap loop sees the stop flag and returns; the shell prompt comes back.
    term.pump(Duration::from_secs(2));
    term.send("show version\r");
    term.wait_screen("the version", 30, |s| s.contains("Meerschaum v"));
    term.send("exit\r");
    assert!(term.wait_exit(30).success());
    term.no_crash_text();
}

#[test]
fn ctrl_c_cancels_a_sync_before_it_writes_anything_and_the_shell_goes_on() {
    let setup = Setup::new("sync");
    let mut term = start_shell(&setup);
    term.send("sync pipes -c sql:w18sh -m slowsync -l None -i sql:w18sh\r");
    // The fetch reads the slow view: give it time to be well into the query.
    term.pump(Duration::from_secs(4));
    assert!(
        !term.screen().contains("Interrupted."),
        "the sync ended on its own:\n{}",
        term.screen()
    );
    let started = Instant::now();
    term.send("\x03");
    term.wait_screen("Interrupted.", 20, |s| s.contains("Interrupted."));
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
    term.send("show version\r");
    term.wait_screen("the version", 30, |s| s.contains("Meerschaum v"));
    term.send("exit\r");
    assert!(term.wait_exit(30).success());
    term.no_crash_text();
}
