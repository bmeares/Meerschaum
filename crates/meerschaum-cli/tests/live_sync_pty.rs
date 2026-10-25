//! The inline live view of graph runs in a real pseudo-terminal: `sync pipes --parents` shows a
//! region of running rows at the bottom of the terminal, prints the finished lines into the
//! scrollback and leaves no region behind; `--nopretty`, piped output and a terminal without
//! ANSI keep the output they had. Ctrl-C, a resize, a Python plugin's output and its `input()`
//! prompt work while the region is shown.
//!
//! Temporary root and home (under `~/.cache`, not `/tmp`), empty plugins directory, a SQLite
//! instance of the test's own, no inherited `MRSM_*`. The plugin tests need a Python with this
//! tree's built extension (`MRSM_TEST_HOOKS_PYTHON`, else `.venv`) and skip without one.
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

/// A query SQLite takes a second or two over (one row).
fn slow_sql(n: u64) -> String {
    format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<{n}) \
         SELECT max(x) AS id FROM c"
    )
}

struct Setup {
    base: PathBuf,
    env: Vec<(String, String)>,
}

/// An interpreter for the Python plugin tests (see the module docs); `None` skips them.
fn python() -> Option<PathBuf> {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    if !repository.join("meerschaum/_core").is_dir() {
        return None;
    }
    std::env::var_os("MRSM_TEST_HOOKS_PYTHON")
        .map(PathBuf::from)
        .into_iter()
        .chain([repository.join(".venv/bin/python")])
        .find(|path| path.is_file())
        .filter(|python| {
            Command::new(python)
                .args(["-c", "import meerschaum._core as c; assert c.AVAILABLE"])
                .env("PYTHONPATH", &repository)
                .output()
                .is_ok_and(|out| out.status.success())
        })
}

/// A plugin that prints `noise` lines (one every 100 ms), then asks for input when `ask` is set.
const TALKER: &str = r#"__version__ = "0.0.1"

def fetch(pipe, **kw):
    import time
    for i in range(pipe.parameters.get("noise", 0)):
        print(f"noisy line {i}", flush=True)
        time.sleep(0.1)
    if pipe.parameters.get("ask"):
        try:
            answer = input("Type something: ")
        except EOFError:
            answer = "eof"
        print("got", repr(answer), flush=True)
    return {"id": [1]}
"#;

impl Setup {
    fn new(name: &str, formatting: &str) -> Setup {
        Setup::graph(name, formatting, 12_000_000)
    }

    /// `src -> mid_a, mid_b -> leaf`, the first three taking `slow` recursions each.
    fn graph(name: &str, formatting: &str, slow: u64) -> Setup {
        let setup = Setup::empty(name, formatting);
        let parent = |m: &str| format!(r#"{{"connector":"sql:lv","metric":"{m}"}}"#);
        setup.register(
            "src",
            &format!(r#"{{"sql":"{}","columns":{{"id":"id"}}}}"#, slow_sql(slow)),
        );
        for mid in ["mid_a", "mid_b"] {
            setup.register(
                mid,
                &format!(
                    r#"{{"parents":[{}],"sql":"{}","columns":{{"id":"id"}}}}"#,
                    parent("src"),
                    slow_sql(slow)
                ),
            );
        }
        setup.register(
            "leaf",
            &format!(
                r#"{{"parents":[{},{}],"sql":"SELECT 1 AS id","columns":{{"id":"id"}}}}"#,
                parent("mid_a"),
                parent("mid_b")
            ),
        );
        setup
    }

    /// A fast chain `src -> chain` (a deterministic order for the pinned output).
    fn chain(name: &str, formatting: &str) -> Setup {
        let setup = Setup::empty(name, formatting);
        setup.register("src", r#"{"sql":"SELECT 1 AS id","columns":{"id":"id"}}"#);
        setup.register(
            "chain",
            r#"{"parents":[{"connector":"sql:lv","metric":"src"}],"sql":"SELECT 1 AS id","columns":{"id":"id"}}"#,
        );
        setup
    }

    /// The `talker` plugin with Python workers from `python`, and a slow SQL root to hang its
    /// pipes under.
    fn plugin(name: &str, python: &Path) -> Setup {
        let mut setup = Setup::empty(name, r#"{"ansi":true,"unicode":true}"#);
        let root = setup.base.join("root");
        std::fs::write(setup.base.join("plugins/talker.py"), TALKER).unwrap();
        // Share the interpreter's packages read-only: no venv is built during the test.
        let environment = python.parent().unwrap().parent().unwrap();
        std::fs::create_dir_all(root.join("venvs")).unwrap();
        for venv in ["mrsm", "talker"] {
            std::os::unix::fs::symlink(environment, root.join("venvs").join(venv)).unwrap();
        }
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        setup.env.extend([
            ("MRSM_PYTHON".into(), python.display().to_string()),
            ("UV_PYTHON".into(), python.display().to_string()),
            ("PYTHONPATH".into(), repository.display().to_string()),
        ]);
        for (k, v) in setup.env.iter_mut() {
            if k == "MRSM_CONFIG" {
                *v = v.replacen(
                    r#""meerschaum":"#,
                    r#""system":{"engine":{"python_workers":1}},"meerschaum":"#,
                    1,
                );
            }
        }
        setup.register(
            "src",
            &format!(
                r#"{{"sql":"{}","columns":{{"id":"id"}}}}"#,
                slow_sql(6_000_000)
            ),
        );
        setup
    }

    /// A pipe of the `talker` plugin under `src`.
    fn talker(&self, metric: &str, params: &str) {
        let out = self
            .command(&[
                "register",
                "pipes",
                "-c",
                "plugin:talker",
                "-m",
                metric,
                "-i",
                "sql:lv",
                "--params",
                &format!(
                    r#"{{"parents":[{{"connector":"sql:lv","metric":"src"}}],"columns":{{"id":"id"}},{params}}}"#
                ),
                "--noask",
                "-y",
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn empty(name: &str, formatting: &str) -> Setup {
        let base = PathBuf::from(std::env::var("HOME").expect("HOME"))
            .join(".cache")
            .join("mrsm-live-sync-tests")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let root = base.join("root");
        let home = base.join("home");
        let plugins = base.join("plugins");
        for p in [&root, &home, &plugins] {
            std::fs::create_dir_all(p).unwrap();
        }
        let p = |path: &Path| path.display().to_string();
        let env = vec![
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
                format!(
                    r#"{{"meerschaum":{{"instance":"sql:lv"}},"shell":{{"updates":{{"check_remote":false}}}},"formatting":{formatting}}}"#
                ),
            ),
            (
                "MRSM_SQL_LV".into(),
                format!(
                    r#"{{"flavor":"sqlite","database":"{}"}}"#,
                    base.join("lv.db").display()
                ),
            ),
        ];
        Setup { base, env }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(BIN);
        command.env_clear().args(args);
        for (k, v) in &self.env {
            command.env(k, v);
        }
        command
    }

    fn register(&self, metric: &str, params: &str) {
        let out = self
            .command(&[
                "register", "pipes", "-c", "sql:lv", "-m", metric, "-i", "sql:lv", "--params",
                params, "--noask", "-y",
            ])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

impl Drop for Setup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
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
    fn spawn(setup: &Setup, args: &[&str]) -> Term {
        Term::spawn_program(setup, BIN, args, COLS)
    }

    fn spawn_program(setup: &Setup, program: &str, args: &[&str], cols: u16) -> Term {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: ROWS,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new(program);
        cmd.env_clear();
        for (k, v) in &setup.env {
            cmd.env(k, v);
        }
        cmd.args(args);
        cmd.cwd(&setup.base);
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
            parser: vt100::Parser::new(ROWS, cols, 2000),
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
                Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }

    /// Resize the terminal (the child gets SIGWINCH).
    fn resize(&mut self, cols: u16) {
        self._master
            .resize(PtySize {
                rows: ROWS,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        self.parser.screen_mut().set_size(ROWS, cols);
    }

    fn wait_raw(&mut self, what: &str, seconds: u64, needle: &str) {
        let start = Instant::now();
        while !self.raw_text().contains(needle) {
            assert!(
                start.elapsed() < Duration::from_secs(seconds),
                "timed out waiting for {what}:\n{}",
                self.screen()
            );
            self.pump(Duration::from_millis(50));
        }
    }

    fn screen(&self) -> String {
        self.parser.screen().contents()
    }

    fn raw_text(&self) -> String {
        String::from_utf8_lossy(&self.raw).into_owned()
    }

    fn send(&mut self, text: &str) {
        self.writer.write_all(text.as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

    fn wait_screen(&mut self, what: &str, seconds: u64, check: impl Fn(&str) -> bool) -> String {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(seconds) {
            self.pump(Duration::from_millis(50));
            let screen = self.screen();
            if check(&screen) {
                return screen;
            }
        }
        panic!(
            "timed out waiting for {what}:\n{}\n--- raw ---\n{:?}",
            self.screen(),
            self.raw_text()
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

    /// The whole session as text: scrollback and screen.
    fn transcript(&mut self) -> String {
        let mut text = String::new();
        let screen = self.parser.screen_mut();
        screen.set_scrollback(usize::MAX);
        let back = screen.scrollback();
        for offset in (1..=back).rev() {
            self.parser.screen_mut().set_scrollback(offset);
            let cols = self.parser.screen().size().1;
            let first = self
                .parser
                .screen()
                .rows(0, cols)
                .next()
                .unwrap_or_default();
            text.push_str(&first);
            text.push('\n');
        }
        self.parser.screen_mut().set_scrollback(0);
        text.push_str(&self.screen());
        text
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

const SYNC: &[&str] = &[
    "sync",
    "pipes",
    "-c",
    "sql:lv",
    "-m",
    "leaf",
    "--parents",
    "-i",
    "sql:lv",
];

fn label(metric: &str) -> String {
    format!("Pipe('sql:lv', '{metric}')")
}

#[test]
fn a_graph_run_shows_running_rows_then_the_final_lines() {
    let setup = Setup::new("inline", r#"{"ansi":true,"unicode":true}"#);
    let mut term = Term::spawn(&setup, SYNC);
    // The region: the header and a running row of the slow root.
    let screen = term.wait_screen("the live region", 120, |s| {
        s.contains("Syncing 0/4 pipes") && s.contains(&format!("  {}  ", label("src")))
    });
    assert!(screen.contains("1 running"), "{screen}");
    // Both middle nodes run at once: two rows.
    let screen = term.wait_screen("two running rows", 120, |s| {
        s.contains("2 running")
            && s.contains(&format!("  {}  ", label("mid_a")))
            && s.contains(&format!("  {}  ", label("mid_b")))
    });
    assert!(
        screen.contains(&format!("✔ {} +1 ins ~0 upd +0 ups", label("src"))),
        "the finished root is printed above the rows:\n{screen}"
    );
    // Typing does not write over the region (echo is off while it is shown).
    term.send("zz");
    let status = term.wait_exit(180);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let transcript = term.transcript();
    for metric in ["src", "mid_a", "mid_b", "leaf"] {
        let line = format!("  ✔ {} +1 ins ~0 upd +0 ups", label(metric));
        assert_eq!(
            transcript.lines().filter(|l| l.trim_end() == line).count(),
            1,
            "{metric} finished once:\n{transcript}"
        );
    }
    // No region is left behind, the summary and the Gantt chart follow.
    assert!(!transcript.contains("Syncing 4/4 pipes"), "{transcript}");
    assert!(!transcript.contains(" running "), "{transcript}");
    assert!(!transcript.contains("zz"), "{transcript}");
    assert!(
        transcript.contains("Successfully synced pipes:"),
        "{transcript}"
    );
    assert!(transcript.contains("It took"), "{transcript}");
    let raw = term.raw_text();
    assert!(raw.contains("\x1b[J"), "the region is redrawn in place");
    assert!(raw.contains("\x1b[?25h"), "the cursor is shown again");
    assert!(!raw.contains("\x1b[?1049h"), "no alternate screen");
    // The node lines of the old view are gone.
    assert!(!transcript.contains("▶"), "{transcript}");
}

/// The run-dependent parts of a plain run's output masked: the run ID, the Gantt bars and the
/// durations. Everything else (line breaks and padding included) is compared as is.
fn mask(text: &str) -> String {
    let mut out = String::new();
    for (i, line) in text.replace("\r\n", "\n").split('\n').enumerate() {
        let masked = if let Some(rest) = line.strip_prefix("run ") {
            let path = rest
                .find("critical path:")
                .map(|i| &rest[i..])
                .unwrap_or("");
            format!("run <RUN>  {path}")
        } else if line.starts_with("Pipe(") && line.contains(['█', '░']) {
            let (label, rest) = line.split_once("  ").unwrap_or((line, ""));
            let words: Vec<&str> = rest
                .split_whitespace()
                .filter(|w| !w.contains(['█', '░']))
                .skip(1)
                .collect();
            format!("{label}  <BARS>  <T>  {}", words.join(" "))
        } else if let Some((before, after)) = line.split_once("It took ") {
            let after = after
                .split_once(" to sync")
                .map(|(_, a)| a)
                .unwrap_or(after);
            format!("{before}It took <T> to sync{after}")
        } else {
            line.to_string()
        };
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&masked);
    }
    out
}

/// What a piped `sync pipes -m chain --parents` prints (ANSI and unicode off): v4's per-pipe
/// lines, the summary and the Gantt chart; no live output.
const PIPED: &str = concat!(
    "\n",
    " + Finished syncing Pipe('sql:lv', 'src'):\n",
    "    Inserted 1, updated 0 rows.\n",
    "    \n",
    "\n",
    "\n",
    " + Finished syncing Pipe('sql:lv', 'chain'):\n",
    "    Inserted 1, updated 0 rows.\n",
    "    \n",
    "\n",
    "\n",
    "                           Successfully synced pipes:                           \n",
    "                                                                                \n",
    "  Pipe('sql:lv', 'src')                                                         \n",
    "  ---------------------                                                         \n",
    "  Inserted 1, updated 0 rows.                                                   \n",
    "                                                                                \n",
    "                                                                                \n",
    "  Pipe('sql:lv', 'chain')                                                       \n",
    "  -----------------------                                                       \n",
    "  Inserted 1, updated 0 rows.                                                   \n",
    "                                                                                \n",
    "                                                                                \n",
    "                                                                                \n",
    "\n",
    "run <RUN>  critical path: Pipe('sql:lv', 'src') → Pipe('sql:lv', 'chain')\n",
    "Pipe('sql:lv', 'src')  <BARS>  <T>  +1 ins ✔\n",
    "Pipe('sql:lv', 'chain')  <BARS>  <T>  +1 ins ✔\n",
    "\n",
    " + It took <T> to sync 2 pipes\n",
    "    (2 succeeded, 0 failed).\n",
    "\n",
);

/// The same with `--nopretty` on a terminal (the second run: nothing new).
const NOPRETTY: &str = concat!(
    "\n",
    r#"{"pipe": {"connector_keys": "sql:lv", "metric_key": "src", "location_key": null, "instance_keys": "sql:lv"}, "success": true, "message": "Inserted 0, updated 0 rows."}"#,
    "\n",
);

#[test]
fn nopretty_and_piped_output_keep_their_output() {
    // Pinned: the plain paths print exactly what they printed before the live view.
    let setup = Setup::chain("pinned", r#"{"ansi":false,"unicode":false}"#);
    let args = [
        "sync",
        "pipes",
        "-c",
        "sql:lv",
        "-m",
        "chain",
        "--parents",
        "-i",
        "sql:lv",
    ];
    let out = setup.command(&args).output().unwrap();
    assert!(out.status.success());
    let piped = mask(&String::from_utf8_lossy(&out.stdout));
    if std::env::var_os("MRSM_TEST_PRINT_GOLDEN").is_some() {
        eprintln!("{piped:?}");
    }
    assert_eq!(piped, PIPED, "{piped}");
    let mut nopretty = args.to_vec();
    nopretty.push("--nopretty");
    let mut term = Term::spawn(&setup, &nopretty);
    assert!(term.wait_exit(180).success(), "{}", term.screen());
    let text = mask(&term.raw_text());
    if std::env::var_os("MRSM_TEST_PRINT_GOLDEN").is_some() {
        eprintln!("{text:?}");
    }
    assert_eq!(text, NOPRETTY, "{text}");

    // With ANSI on: still no region and no node lines when `--nopretty` or piped.
    let setup = Setup::new("plain", r#"{"ansi":true,"unicode":true}"#);
    let mut args = SYNC.to_vec();
    args.push("--nopretty");
    let mut term = Term::spawn(&setup, &args);
    let status = term.wait_exit(180);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let raw = term.raw_text();
    for region in ["\x1b[J", "\x1b[?25l", "Syncing ", "  ✔ Pipe(", "▶"] {
        assert!(
            !raw.contains(region),
            "{region:?} in --nopretty output:\n{raw:?}"
        );
    }

    // Piped: no live output of any kind.
    let out = setup.command(SYNC).output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    // (The Gantt chart at the end marks its rows with `✔`; the node lines are not printed.)
    for region in ["\x1b[J", "\x1b[?25l", "Syncing ", "  ✔ Pipe(", "▶"] {
        assert!(
            !stdout.contains(region),
            "{region:?} in piped output:\n{stdout:?}"
        );
    }
    assert!(stdout.contains("Successfully synced pipes:"), "{stdout}");
}

#[test]
fn ctrl_c_during_a_graph_run_restores_the_terminal() {
    // A shorter root query: SQLite finishes it soon after the interrupt, so the lease is handed
    // back within the drain bound of an interrupted action (3 s) even on a loaded machine.
    let setup = Setup::graph("ctrlc", r#"{"ansi":true,"unicode":true}"#, 4_000_000);
    // A shell around `mrsm` prints the terminal's modes once it ends (`trap :` keeps the shell
    // alive without making the child ignore SIGINT).
    let script = r#"trap : INT; "$0" "$@"; echo "EXIT=$?"; stty -a"#;
    let mut args = vec!["-c", script, BIN];
    args.extend_from_slice(SYNC);
    let mut term = Term::spawn_program(&setup, "/bin/sh", &args, COLS);
    term.wait_screen("the live region", 120, |s| {
        s.contains("Syncing 0/4 pipes") && s.contains(&format!("  {}  ", label("src")))
    });
    term.send("\x03");
    term.wait_screen("the modes after the exit", 60, |s| {
        s.contains("EXIT=") && s.contains("icanon")
    });
    let _ = term.wait_exit(30);
    term.no_crash_text();
    let transcript = term.transcript();
    assert!(transcript.contains("Interrupted"), "{transcript}");
    assert!(!transcript.contains("EXIT=0"), "{transcript}");
    let modes: String = transcript
        .lines()
        .skip_while(|l| !l.contains("EXIT="))
        .collect::<Vec<_>>()
        .join(" ");
    for mode in ["icanon", "echo"] {
        assert!(
            modes.split_whitespace().any(|m| m == mode),
            "{mode} is on again: {modes}"
        );
    }
    let raw = term.raw_text();
    let hidden = raw.rfind("\x1b[?25l");
    assert!(
        hidden.is_some() && raw.rfind("\x1b[?25h") > hidden,
        "the cursor was hidden, then shown again"
    );
    // The run's leases were released: the next run goes through.
    let out = setup.command(SYNC).output().unwrap();
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_resize_redraws_the_region_at_the_new_width() {
    let setup = Setup::new("resize", r#"{"ansi":true,"unicode":true}"#);
    let mut term = Term::spawn_program(&setup, BIN, SYNC, 40);
    // At 40 columns the key help does not fit the header.
    let screen = term.wait_screen("the narrow region", 120, |s| {
        s.contains("Syncing 0/4") && s.contains("Pipe('sql:lv', 'src')")
    });
    assert!(!screen.contains("w: watch"), "{screen}");
    for line in screen.lines() {
        assert!(line.chars().count() < 40, "{line:?}");
    }
    term.resize(120);
    term.wait_screen("the wide region", 60, |s| s.contains("w: watch"));
    let status = term.wait_exit(180);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let transcript = term.transcript();
    assert!(
        transcript.contains(&format!("  ✔ {} +1 ins ~0 upd +0 ups", label("leaf"))),
        "{transcript}"
    );
    assert!(!transcript.contains(" running "), "{transcript}");
}

#[test]
fn a_plugin_prompt_during_an_inline_run_reads_the_answer() {
    let Some(python) = python() else {
        eprintln!("skipped: a Python with the built extension is required");
        return;
    };
    let setup = Setup::plugin("prompt", &python);
    setup.talker("ask", r#""noise":3,"ask":true"#);
    let mut term = Term::spawn(
        &setup,
        &[
            "sync",
            "pipes",
            "-c",
            "plugin:talker",
            "-m",
            "ask",
            "--parents",
            "-i",
            "sql:lv",
        ],
    );
    term.wait_screen("the live region", 120, |s| s.contains("Syncing 0/2 pipes"));
    term.wait_screen("the plugin's prompt", 120, |s| {
        s.contains("Type something: ")
    });
    term.pump(Duration::from_millis(300));
    term.send("hello\r");
    let status = term.wait_exit(120);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let transcript = term.transcript();
    assert!(transcript.contains("got 'hello'"), "{transcript}");
    // The answer was echoed where it was typed.
    assert!(transcript.contains("Type something: hello"), "{transcript}");
    assert!(transcript.contains("noisy line 2"), "{transcript}");
    assert!(
        transcript.contains("  ✔ Pipe('plugin:talker', 'ask') +1 ins"),
        "{transcript}"
    );
}

#[test]
fn plugin_output_while_the_monitor_is_open_is_printed_after_it() {
    let Some(python) = python() else {
        eprintln!("skipped: a Python with the built extension is required");
        return;
    };
    let setup = Setup::plugin("noise", &python);
    setup.talker("noise", r#""noise":40"#);
    let mut term = Term::spawn(
        &setup,
        &[
            "sync",
            "pipes",
            "-c",
            "plugin:talker",
            "-m",
            "noise",
            "--parents",
            "-i",
            "sql:lv",
        ],
    );
    term.wait_screen("the plugin's output above the region", 120, |s| {
        s.contains("noisy line 1") && s.contains("w: watch")
    });
    term.send("w");
    term.wait_raw("the full-screen monitor", 60, "\x1b[?1049h");
    term.pump(Duration::from_millis(1500));
    term.send("d");
    term.wait_raw("the monitor to close", 60, "\x1b[?1049l");
    let status = term.wait_exit(120);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let raw = term.raw_text();
    let opened = raw.find("\x1b[?1049h").unwrap();
    let closed = raw[opened..].find("\x1b[?1049l").unwrap() + opened;
    assert!(
        !raw[opened..closed].contains("noisy line"),
        "no plugin output on the monitor: {:?}",
        &raw[opened..closed]
    );
    let transcript = term.transcript();
    for i in 0..40 {
        let line = format!("noisy line {i}");
        assert_eq!(
            transcript.lines().filter(|l| l.trim_end() == line).count(),
            1,
            "{line} once, in order:\n{transcript}"
        );
    }
    assert!(
        transcript.contains("  ✔ Pipe('plugin:talker', 'noise') +1 ins"),
        "{transcript}"
    );
}

#[test]
fn a_terminal_without_ansi_keeps_one_line_per_node() {
    let setup = Setup::new("noansi", r#"{"ansi":false,"unicode":true}"#);
    let mut term = Term::spawn(&setup, SYNC);
    let status = term.wait_exit(180);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let raw = term.raw_text();
    assert!(!raw.contains("\x1b[J"), "{raw:?}");
    let transcript = term.transcript();
    assert!(
        transcript.contains(&format!("  ▶ {} (", label("src"))),
        "{transcript}"
    );
    assert!(
        transcript.contains(&format!("  ✔ {} +1 ins ~0 upd +0 ups", label("leaf"))),
        "{transcript}"
    );
}

#[test]
fn w_opens_the_run_monitor_and_detaching_returns_to_the_inline_view() {
    let setup = Setup::new("watch", r#"{"ansi":true,"unicode":true}"#);
    let mut term = Term::spawn(&setup, SYNC);
    term.wait_screen("the live region with its key help", 120, |s| {
        s.contains("w: watch") && s.contains(&format!("  {}  ", label("src")))
    });
    term.send("w");
    term.wait_raw("the full-screen monitor", 60, "\x1b[?1049h");
    term.pump(Duration::from_millis(800));
    // Detach: the monitor leaves the alternate screen and the run goes on inline.
    term.send("d");
    term.wait_raw("the monitor to close", 60, "\x1b[?1049l");
    let status = term.wait_exit(180);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let transcript = term.transcript();
    for metric in ["src", "mid_a", "mid_b", "leaf"] {
        let line = format!("  ✔ {} +1 ins ~0 upd +0 ups", label(metric));
        assert_eq!(
            transcript.lines().filter(|l| l.trim_end() == line).count(),
            1,
            "{metric} finished once (the run was not cancelled):\n{transcript}"
        );
    }
    assert!(transcript.contains("It took"), "{transcript}");
    // The region was drawn again after the monitor.
    let raw = term.raw_text();
    let after = &raw[raw.rfind("\x1b[?1049l").unwrap()..];
    assert!(after.contains("Syncing "), "{after:?}");
}

#[test]
fn loop_laps_show_the_running_rows_under_their_usual_output() {
    let setup = Setup::new("loop", r#"{"ansi":true,"unicode":true}"#);
    let mut term = Term::spawn(
        &setup,
        &[
            "sync",
            "pipes",
            "-c",
            "sql:lv",
            "-m",
            "src",
            "-i",
            "sql:lv",
            "--loop",
            "--min-seconds",
            "1",
        ],
    );
    term.wait_screen("a lap's running row", 120, |s| {
        s.contains("Syncing 0/1 pipes") && s.contains(&format!("  {}  ", label("src")))
    });
    // The lap's v4 lines, then the next lap.
    term.wait_screen("the lap summary", 120, |s| s.contains("Sleeping for 1"));
    term.wait_screen("the second lap's row", 120, |s| {
        s.contains("Syncing 0/1 pipes") && s.contains(&format!("  {}  ", label("src")))
    });
    // Ctrl-C stops the loop after the lap (the cooperative stop of `--loop`).
    term.send("\x03");
    let _ = term.wait_exit(120);
    term.no_crash_text();
    let transcript = term.transcript();
    assert!(
        transcript.contains(&format!("Finished syncing {}", label("src"))),
        "{transcript}"
    );
    assert!(
        !transcript.contains("  ✔ Pipe("),
        "no node lines in a loop:\n{transcript}"
    );
    assert!(
        term.raw_text().contains("\x1b[?25h"),
        "the cursor is shown again"
    );
}

#[test]
fn sync_pipelines_shows_the_same_live_region() {
    let setup = Setup::new("pipeline", r#"{"ansi":true,"unicode":true}"#);
    let out = setup
        .command(&[
            "register",
            "pipelines",
            "nightly",
            "--params",
            r#"{"targets":["sql:lv|sql:lv|leaf|None"],"parents":true}"#,
            "-i",
            "sql:lv",
            "-y",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let mut term = Term::spawn(
        &setup,
        &["sync", "pipelines", "nightly", "-i", "sql:lv", "-y"],
    );
    term.wait_screen("the live region", 120, |s| {
        s.contains("Syncing 0/4 pipes") && s.contains(&format!("  {}  ", label("src")))
    });
    let status = term.wait_exit(180);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    let transcript = term.transcript();
    assert!(transcript.contains("Run: nightly"), "{transcript}");
    for metric in ["src", "mid_a", "mid_b", "leaf"] {
        let line = format!("  ✔ {} +1 ins ~0 upd +0 ups", label(metric));
        assert!(transcript.contains(&line), "{line}:\n{transcript}");
    }
    assert!(!transcript.contains(" running "), "{transcript}");
}

#[cfg(target_os = "linux")]
#[test]
fn ctrl_z_gives_the_terminal_back_and_fg_draws_the_region_again() {
    let setup = Setup::new("tstp", r#"{"ansi":true,"unicode":true}"#);
    let mut term = Term::spawn(&setup, SYNC);
    term.wait_screen("the live region", 120, |s| {
        s.contains("Syncing 0/4 pipes") && s.contains(&format!("  {}  ", label("src")))
    });
    let pid = term.child.process_id().unwrap().to_string();
    let state = || {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .unwrap_or_default()
            .rsplit(')')
            .next()
            .and_then(|rest| rest.split_whitespace().next())
            .unwrap_or_default()
            .to_string()
    };
    let signal = |name: &str| {
        assert!(
            Command::new("kill")
                .args([name, &pid])
                .status()
                .unwrap()
                .success()
        );
    };
    // (A session leader's group is orphaned and the kernel drops a terminal's Ctrl-Z there:
    // the signal is sent directly, as the terminal would to a shell's job.)
    let mark = term.raw.len();
    signal("-TSTP");
    let start = Instant::now();
    while state() != "T" {
        assert!(start.elapsed() < Duration::from_secs(10), "not stopped");
        term.pump(Duration::from_millis(50));
    }
    term.pump(Duration::from_millis(200));
    let stopped = String::from_utf8_lossy(&term.raw[mark..]).into_owned();
    assert!(stopped.ends_with("\x1b[J\x1b[?25h"), "{stopped:?}");
    let mark = term.raw.len();
    signal("-CONT");
    let start = Instant::now();
    while !String::from_utf8_lossy(&term.raw[mark..]).contains("Syncing ") {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "no redraw after fg"
        );
        term.pump(Duration::from_millis(50));
    }
    let status = term.wait_exit(180);
    term.no_crash_text();
    assert!(status.success(), "{}", term.screen());
    assert!(term.transcript().contains("It took"));
}
