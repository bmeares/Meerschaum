//! The prompts of wave 18 in a real pseudo-terminal: secrets echo `*` (decision 2) and the
//! `bootstrap pipes` questions open a filtering dropdown of suggestions (decision 4).
//!
//! Temporary root and home, empty plugins directory, a SQLite instance of the test's own, no
//! inherited `MRSM_*`.
#![cfg(unix)]
// The pseudo-terminal harness is shared in shape with `interrupt_shell.rs`; each file uses part of it.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{Receiver, TryRecvError, channel};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");
const SLOW_VIEW: &str = "CREATE VIEW slowv AS WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
SELECT x+1 FROM c WHERE x<300000000) SELECT x AS id FROM c";

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
            .join("mrsm-w18-sh-prompts");
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

fn dropdown_rows(term: &Term) -> Vec<String> {
    term.screen()
        .lines()
        .filter(|l| l.starts_with("  ") && !l.trim().is_empty() && !l.trim_start().starts_with('-'))
        .map(|l| l.trim().to_string())
        .collect()
}

#[test]
fn edit_config_suggests_keys_before_typing_and_filters_them() {
    let setup = Setup::new("edit-config");
    let mut term = Term::spawn(&setup.env, &setup.base, &["edit", "config"]);
    term.wait_screen("the configuration dropdown", 60, |s| {
        s.contains("Configuration key:") && s.contains("... ")
    });
    term.send("stac");
    term.wait_screen("the filtered YAML key", 10, |s| {
        s.lines().any(|line| line.trim() == "stack") && !s.contains("more")
    });
    term.send("\x03");
    assert!(!term.wait_exit(30).success());
}

#[test]
fn a_password_prompt_echoes_a_star_per_character() {
    let setup = Setup::new("password");
    let mut term = Term::spawn(
        &setup.env,
        &setup.base,
        &["register", "users", "bob", "-i", "sql:w18sh"],
    );
    term.wait_screen("the password question", 60, |s| {
        s.contains("Password for user 'bob'")
    });
    term.send("abcdefghij");
    term.pump(Duration::from_millis(600));
    let screen = term.screen();
    let line = screen
        .lines()
        .find(|l| l.contains("Password for user 'bob'"))
        .unwrap()
        .to_string();
    assert!(line.ends_with(&"*".repeat(10)), "{line:?}");
    assert!(
        !screen.contains("abcdefghij"),
        "the secret was echoed:\n{screen}"
    );
    term.send("\x7f\x7f");
    term.pump(Duration::from_millis(600));
    let line = term
        .screen()
        .lines()
        .find(|l| l.contains("Password for user 'bob'"))
        .unwrap()
        .trim_end()
        .to_string();
    assert!(line.ends_with(&format!(": {}", "*".repeat(8))), "{line:?}");
    // Ctrl-C cancels the prompt and the registration.
    term.send("\x03");
    let status = term.wait_exit(30);
    assert!(!status.success(), "{status:?}");
    assert!(
        !term.raw_text().contains("abcdefgh"),
        "the secret reached the terminal"
    );
}

#[test]
fn bootstrap_pipes_suggests_connectors_metrics_and_accepts_unknown_keys() {
    let setup = Setup::new("bootstrap");
    let mut term = Term::spawn(
        &setup.env,
        &setup.base,
        &["bootstrap", "pipes", "-i", "sql:w18sh"],
    );
    term.wait_screen("the connector question", 60, |s| s.contains("Connector:"));
    // Everything is suggested before anything is typed (the rows past the eighth are counted).
    term.wait_screen("the dropdown", 10, |s| {
        s.contains("sql:w18sh") && s.contains("... 1 more")
    });
    term.send("w18");
    term.pump(Duration::from_millis(600));
    assert_eq!(
        dropdown_rows(&term)
            .iter()
            .filter(|r| r.contains("sql:"))
            .count(),
        1,
        "the dropdown filters as you type:\n{}",
        term.screen()
    );
    assert!(!term.screen().contains("more"), "{}", term.screen());
    // Down selects the row, Enter takes it.
    term.send("\x1b[B");
    term.send("\r");
    term.wait_screen("the metric question", 30, |s| s.contains("Metric:"));
    // The metrics registered on the instance, those of the connector first.
    term.wait_screen("the metric dropdown", 10, |s| {
        s.contains("fast") && s.contains("slow")
    });
    let rows = dropdown_rows(&term);
    let fast = rows.iter().position(|r| r == "fast").expect("fast");
    let slow = rows.iter().position(|r| r == "slow").expect("slow");
    assert!(fast < slow, "{rows:?}");
    term.send("\x03");
    let status = term.wait_exit(30);
    assert!(!status.success(), "{status:?}");
    assert!(term.raw_text().contains("No pipes were bootstrapped."));
}

#[test]
fn an_unknown_connector_is_accepted_with_a_warning() {
    let setup = Setup::new("unknown");
    let mut term = Term::spawn(
        &setup.env,
        &setup.base,
        &["bootstrap", "pipes", "-i", "sql:w18sh"],
    );
    term.wait_screen("the connector question", 60, |s| s.contains("Connector:"));
    term.send("sql:nowhere\r");
    term.wait_screen("the metric question", 30, |s| s.contains("Metric:"));
    assert!(
        term.screen().contains("is not configured on this host"),
        "{}",
        term.screen()
    );
    term.send("\x03");
    let status = term.wait_exit(30);
    assert!(!status.success(), "{status:?}");
}
