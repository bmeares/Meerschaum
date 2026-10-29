//! A Ctrl-C (SIGINT) or SIGTERM which ends the process while the progress spinner is drawn must
//! leave the spinner line cleared (wave 17b). Ctrl-C cancels the action and exits 130 with
//! `Interrupted.` (wave 18); SIGTERM still ends the process "killed by the signal".
//!
//! The action is `show data` on a pipe whose table is a SQLite view that takes many seconds to
//! read, run in a real PTY. Temporary root, empty plugins directory, no inherited `MRSM_*`.
#![cfg(unix)]

use std::io::{Read, Write};
use std::path::Path;
use std::process::Command;
use std::sync::mpsc::{Receiver, TryRecvError, channel};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");
const SLOW_VIEW: &str = "CREATE VIEW slowv AS WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL \
SELECT x+1 FROM c WHERE x<300000000) SELECT x AS id FROM c";

fn env_for(root: &Path, plugins: &Path) -> Vec<(String, String)> {
    vec![
        ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
        ("HOME".into(), root.display().to_string()),
        ("TERM".into(), "xterm-256color".into()),
        ("MRSM_ROOT_DIR".into(), root.display().to_string()),
        ("MRSM_PLUGINS_DIR".into(), plugins.display().to_string()),
        ("MRSM_NO_AUTO_INSTALL".into(), "1".into()),
        ("MRSM_NO_UPDATE_CHECK".into(), "1".into()),
        (
            "MRSM_SQL_W17I".into(),
            format!(
                r#"{{"flavor":"sqlite","database":"{}"}}"#,
                root.join("w17i.db").display()
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

struct Term {
    parser: vt100::Parser,
    raw: Vec<u8>,
    rx: Receiver<Vec<u8>>,
    writer: Box<dyn Write + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    _master: Box<dyn portable_pty::MasterPty + Send>,
}

impl Term {
    fn spawn(env: &[(String, String)], root: &Path, args: &[&str]) -> Term {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 100,
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
        cmd.cwd(root);
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
            parser: vt100::Parser::new(24, 100, 0),
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
                    self.raw.extend_from_slice(&bytes);
                }
                Err(TryRecvError::Disconnected) => return,
                Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }

    /// Pump until the spinner has drawn something.
    fn wait_for_spinner(&mut self) {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(60) {
            self.pump(Duration::from_millis(200));
            if String::from_utf8_lossy(&self.raw).contains("Reading data") {
                return;
            }
        }
        panic!(
            "no spinner was drawn:\n{}",
            String::from_utf8_lossy(&self.raw)
        );
    }

    fn wait_exit(&mut self) -> portable_pty::ExitStatus {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(20) {
            self.pump(Duration::from_millis(100));
            if let Ok(Some(status)) = self.child.try_wait() {
                self.pump(Duration::from_millis(300));
                return status;
            }
        }
        panic!("the process did not end");
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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
            "w17i",
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

fn setup() -> (tempfile::TempDir, tempfile::TempDir, Vec<(String, String)>) {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let env = env_for(root.path(), plugins.path());
    create_view(&root.path().join("w17i.db"));
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
            "sql:w17i",
            "--params",
            r#"{"target":"slowv","columns":{"id":"id"}}"#,
            "--noask",
        ],
    );
    (root, plugins, env)
}

fn blank_screen(term: &Term) -> bool {
    term.parser
        .screen()
        .contents()
        .lines()
        .all(|line| !line.contains("Reading data") && !line.contains("0/1"))
}

fn interrupted_while_drawn(byte: Option<&str>, signal: Option<&str>) {
    let (root, _plugins, env) = setup();
    let mut term = Term::spawn(
        &env,
        root.path(),
        &[
            "show", "data", "-c", "plugin:x", "-m", "slow", "-l", "None", "-i", "sql:w17i",
        ],
    );
    term.wait_for_spinner();
    match (byte, signal) {
        (Some(byte), _) => {
            term.writer.write_all(byte.as_bytes()).unwrap();
            term.writer.flush().unwrap();
        }
        (None, Some(sig)) => {
            let pid = term.child.process_id().expect("pid");
            let status = Command::new("kill")
                .args([format!("-{sig}"), pid.to_string()])
                .status()
                .unwrap();
            assert!(status.success());
        }
        _ => unreachable!(),
    }
    let status = term.wait_exit();
    assert!(
        !status.success(),
        "the interrupted process reported success"
    );
    if byte.is_some() {
        // Ctrl-C cancels the action (wave 18, decision 1): `Interrupted.`, exit status 130, not a
        // process killed by the signal. SIGTERM below still ends the process by the signal.
        assert_eq!(status.exit_code(), 130, "{status:?}");
        assert!(
            term.parser.screen().contents().contains("Interrupted."),
            "{:?}",
            term.parser.screen().contents()
        );
    }
    assert!(
        blank_screen(&term),
        "the spinner line stayed on screen:\n{:?}",
        term.parser.screen().contents()
    );
    let raw = String::from_utf8_lossy(&term.raw).to_string();
    let last_frame = raw.rfind("Reading data").unwrap();
    assert!(
        raw[last_frame..].contains("\r\x1b[2K"),
        "no clear sequence after the last frame: {:?}",
        &raw[last_frame..]
    );
}

#[test]
fn ctrl_c_clears_the_spinner() {
    interrupted_while_drawn(Some("\x03"), None);
}

#[test]
fn sigterm_clears_the_spinner() {
    interrupted_while_drawn(None, Some("TERM"));
}
