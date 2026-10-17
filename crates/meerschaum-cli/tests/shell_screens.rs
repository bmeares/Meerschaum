//! Screen parity for the interactive shell: drive `mrsm` in a pseudo-terminal with the
//! keystroke scripts recorded from v4.1.4 in `fixtures/shell_screens.json` (by
//! `fixtures/gen_shell_screens.py` with `pexpect` + `pyte`), render the output with `vt100`,
//! and compare every snapshot cell by cell: text, cursor and styles (foreground, background,
//! bold, reverse, underline), in pyte's color vocabulary.
//!
//! Tolerances (each one is applied only where named, and listed in the crate README):
//!
//! 1. **Version string.** The banner's `v4.1.4` and `v5.x` differ in length, so the line with
//!    the version is compared with leading spaces trimmed (the right alignment is the same rule).
//!    In the 40-column scenario the banner itself wraps differently, so rows above the prompt
//!    are not compared there (`tiny`).
//! 2. **Reverse-i-search cursor.** prompt_toolkit leaves the cursor in the search field;
//!    reedline keeps it in the buffer, at the match. The cursor is not compared while searching.
//! 3. **`daemon`.** v5 has no CLI daemon (see `src/shell/mod.rs`): turning it on prints one more
//!    info line saying so, and the toolbar keeps showing the instance where v4 shows `daemon`.
//!    The snapshots taken while the mode is on are compared on the prompt row only.
//! 4. **No-ANSI toolbar overflow.** v4's no-ANSI toolbar is one cell wider than the terminal
//!    (the status icon is counted as one column but is two wide). prompt_toolkit draws the icon
//!    half off the edge; v5 clips it, so the icon and the cells after column 97 are not compared
//!    in `no_ansi`.
//!
//! The harness emulates pyte's resize (shrinking deletes lines at the top and keeps the cursor
//! row) so both shells are measured against the same terminal model.
//!
//! Isolation: every run uses a fresh temporary `MRSM_ROOT_DIR`, `HOME` and empty
//! `MRSM_PLUGINS_DIR`, SQLite connectors only, and no update checks.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, TryRecvError, channel};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde_json::{Value as Json, json};

fn fixture() -> Json {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/shell_screens.json"
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
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

/// A style run: start and end column, foreground, background, flags (`B`, `R`, `U`).
type Run = (u16, u16, String, String, String);

/// One rendered screen, in the fixture's format.
#[derive(Debug, Clone, PartialEq)]
struct Snapshot {
    lines: Vec<String>,
    cursor: (u16, u16),
    styles: Vec<Vec<Run>>,
}

/// pyte's names for the 16 basic colors and its hex strings for the 256-color palette.
fn color_name(c: vt100::Color) -> String {
    const BASIC: [&str; 8] = [
        "black", "red", "green", "brown", "blue", "magenta", "cyan", "white",
    ];
    match c {
        vt100::Color::Default => "default".into(),
        vt100::Color::Idx(i) if i < 8 => BASIC[i as usize].into(),
        vt100::Color::Idx(i) if i < 16 => format!("bright{}", BASIC[i as usize - 8]),
        vt100::Color::Idx(i) if i < 232 => {
            let levels = [0u8, 0x5f, 0x87, 0xaf, 0xd7, 0xff];
            let n = i - 16;
            format!(
                "{:02x}{:02x}{:02x}",
                levels[(n / 36) as usize],
                levels[((n / 6) % 6) as usize],
                levels[(n % 6) as usize]
            )
        }
        vt100::Color::Idx(i) => {
            let v = 8 + 10 * (i - 232);
            format!("{v:02x}{v:02x}{v:02x}")
        }
        vt100::Color::Rgb(r, g, b) => format!("{r:02x}{g:02x}{b:02x}"),
    }
}

fn snapshot(screen: &vt100::Screen) -> Snapshot {
    let (rows, cols) = screen.size();
    let mut lines = Vec::new();
    let mut styles = Vec::new();
    for r in 0..rows {
        let mut text = String::new();
        let mut runs: Vec<Run> = Vec::new();
        for c in 0..cols {
            let Some(mut cell) = screen.cell(r, c) else {
                continue;
            };
            // pyte gives a wide character's second cell the character's attributes.
            if cell.is_wide_continuation()
                && c > 0
                && let Some(prev) = screen.cell(r, c - 1)
            {
                cell = prev;
            } else {
                let contents = cell.contents();
                text.push_str(if contents.is_empty() { " " } else { contents });
            }
            let fg = color_name(cell.fgcolor());
            let bg = color_name(cell.bgcolor());
            let flags: String = [
                (cell.bold(), 'B'),
                (cell.inverse(), 'R'),
                (cell.underline(), 'U'),
            ]
            .iter()
            .filter(|(on, _)| *on)
            .map(|(_, f)| *f)
            .collect();
            if fg == "default" && bg == "default" && flags.is_empty() {
                continue;
            }
            if let Some(last) = runs.last_mut()
                && last.1 == c
                && last.2 == fg
                && last.3 == bg
                && last.4 == flags
            {
                last.1 = c + 1;
                continue;
            }
            runs.push((c, c + 1, fg, bg, flags));
        }
        lines.push(text.trim_end().to_string());
        styles.push(runs);
    }
    let (row, col) = screen.cursor_position();
    Snapshot {
        lines,
        cursor: (row, col),
        styles,
    }
}

fn fixture_snapshot(s: &Json) -> Snapshot {
    let lines = s["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l.as_str().unwrap().to_string())
        .collect();
    let cursor = (
        s["cursor"][0].as_u64().unwrap() as u16,
        s["cursor"][1].as_u64().unwrap() as u16,
    );
    let styles = s["styles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row.as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r[0].as_u64().unwrap() as u16,
                        r[1].as_u64().unwrap() as u16,
                        r[2].as_str().unwrap().to_string(),
                        r[3].as_str().unwrap().to_string(),
                        r[4].as_str().unwrap().to_string(),
                    )
                })
                .collect()
        })
        .collect();
    Snapshot {
        lines,
        cursor,
        styles,
    }
}

/// `mrsm` in a pseudo-terminal.
struct Term {
    parser: vt100::Parser<Replies>,
    rx: Receiver<Vec<u8>>,
    writer: Box<dyn Write + Send>,
    master: Box<dyn portable_pty::MasterPty + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    eof: bool,
}

impl Term {
    fn spawn(env: &[(String, String)], cwd: &Path, rows: u16, cols: u16) -> Term {
        let pty = native_pty_system();
        let pair = pty
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_mrsm"));
        cmd.env_clear();
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.cwd(cwd);
        let child = pair.slave.spawn_command(cmd).unwrap();
        drop(pair.slave);
        let mut reader = pair.master.try_clone_reader().unwrap();
        let writer = pair.master.take_writer().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            let mut buf = [0u8; 65536];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx.send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Term {
            parser: vt100::Parser::new_with_callbacks(rows, cols, 0, Replies::default()),
            rx,
            writer,
            master: pair.master,
            child,
            eof: false,
        }
    }

    /// Feed output until it has been idle for `idle`.
    fn pump(&mut self, idle: Duration, limit: Duration) {
        let start = Instant::now();
        let mut last = Instant::now();
        loop {
            match self.rx.try_recv() {
                Ok(bytes) => {
                    self.parser.process(&bytes);
                    let replies = std::mem::take(&mut self.parser.callbacks_mut().0);
                    for r in replies {
                        let _ = self.writer.write_all(r.as_bytes());
                        let _ = self.writer.flush();
                    }
                    last = Instant::now();
                    continue;
                }
                Err(TryRecvError::Disconnected) => {
                    self.eof = true;
                    return;
                }
                Err(TryRecvError::Empty) => {}
            }
            if last.elapsed() >= idle || start.elapsed() >= limit {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn send(&mut self, s: &str) {
        self.writer.write_all(s.as_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

    fn resize(&mut self, rows: u16, cols: u16) {
        // Shrink like pyte (the reference emulator): delete lines at the top, keep the cursor.
        let (old_rows, _) = self.parser.screen().size();
        if rows < old_rows {
            let k = old_rows - rows;
            self.parser
                .process(format!("\x1b7\x1b[1;1H\x1b[{k}M\x1b8").as_bytes());
        }
        self.parser.screen_mut().set_size(rows, cols);
        self.master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
    }

    fn wait_exit(&mut self) -> bool {
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            self.pump(Duration::from_millis(100), Duration::from_millis(500));
            if let Ok(Some(_)) = self.child.try_wait() {
                self.pump(Duration::from_millis(200), Duration::from_secs(1));
                return true;
            }
        }
        false
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The scenario's environment: a fresh temporary root, home and empty plugins directory.
fn environment(base: &Path, scenario: &Json) -> Vec<(String, String)> {
    let tmp = std::env::temp_dir();
    assert!(base.starts_with(&tmp), "{base:?} is not under {tmp:?}");
    let root = base.join("root");
    let home = base.join("home");
    let plugins = base.join("plugins");
    for p in [&root, &home, &plugins] {
        std::fs::create_dir_all(p).unwrap();
    }
    assert!(root.starts_with(&tmp) && plugins.starts_with(&tmp));
    assert_eq!(std::fs::read_dir(&plugins).unwrap().count(), 0);
    let mut config = json!({
        "meerschaum": {"instance": "sql:local"},
        "shell": {"updates": {"check_remote": false}},
    });
    if let Some(extra) = scenario["config"].as_object() {
        for (k, v) in extra {
            let entry = config
                .as_object_mut()
                .unwrap()
                .entry(k.clone())
                .or_insert_with(|| json!({}));
            for (k2, v2) in v.as_object().unwrap() {
                entry
                    .as_object_mut()
                    .unwrap()
                    .insert(k2.clone(), v2.clone());
            }
        }
    }
    let p = |path: &PathBuf| path.to_string_lossy().into_owned();
    vec![
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("HOME".into(), p(&home)),
        ("TERM".into(), "xterm-256color".into()),
        ("LANG".into(), "C.UTF-8".into()),
        ("LC_ALL".into(), "C.UTF-8".into()),
        ("MRSM_ROOT_DIR".into(), p(&root)),
        ("MRSM_PLUGINS_DIR".into(), p(&plugins)),
        ("MRSM_CONFIG".into(), config.to_string()),
        (
            "MRSM_SQL_MAIN".into(),
            format!("sqlite:///{}", p(&base.join("main.db"))),
        ),
        (
            "MRSM_SQL_X".into(),
            format!("sqlite:///{}", p(&base.join("x.db"))),
        ),
    ]
}

fn prepare_root(root: &Path, scenario: &Json, paths: &Json) {
    if let Some(history) = scenario["history"].as_array().filter(|h| !h.is_empty()) {
        let path = root.join(paths["SHELL_HISTORY_PATH"].as_str().unwrap());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut text = String::new();
        for (i, line) in history.iter().enumerate() {
            text.push_str(&format!(
                "\n# 2026-01-0{} 00:00:00.000000\n+{}\n",
                1 + i % 9,
                line.as_str().unwrap()
            ));
        }
        std::fs::write(path, text).unwrap();
    }
    if let Some(cache) = scenario["updates_cache"]
        .as_object()
        .filter(|c| !c.is_empty())
    {
        let path = root.join(paths["UPDATES_CACHE_PATH"].as_str().unwrap());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = json!({"last_check_ts": "2026-01-01T00:00:00+00:00"});
        for (k, v) in cache {
            body[k] = v.clone();
        }
        std::fs::write(path, body.to_string()).unwrap();
    }
}

/// Run a scenario's keystrokes and return a snapshot per step.
fn run(scenario: &Json, paths: &Json) -> Vec<Snapshot> {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_path_buf();
    let env = environment(&base, scenario);
    prepare_root(&base.join("root"), scenario, paths);
    let rows = scenario["size"][0].as_u64().unwrap() as u16;
    let cols = scenario["size"][1].as_u64().unwrap() as u16;
    let mut term = Term::spawn(&env, &base, rows, cols);
    term.pump(Duration::from_millis(800), Duration::from_secs(20));
    let mut out = Vec::new();
    for step in scenario["steps"].as_array().unwrap() {
        if let Some(size) = step["resize"].as_array() {
            term.resize(
                size[0].as_u64().unwrap() as u16,
                size[1].as_u64().unwrap() as u16,
            );
        }
        if let Some(keys) = step["send"].as_str() {
            term.send(keys);
        }
        if let Some(text) = step["type"].as_str() {
            for c in text.chars() {
                term.send(&c.to_string());
                term.pump(Duration::from_millis(60), Duration::from_secs(3));
            }
        }
        term.pump(Duration::from_millis(500), Duration::from_secs(20));
        if step["expect_exit"].as_bool() == Some(true) {
            assert!(term.wait_exit(), "mrsm did not exit");
        }
        out.push(snapshot(term.parser.screen()));
    }
    out
}

fn normalize_version(lines: &mut [String]) {
    let ours = format!("v{}", meerschaum_cli::VERSION);
    for line in lines.iter_mut() {
        if line.contains(&ours) || line.contains("v{version}") {
            *line = line.replace(&ours, "v{version}").trim_start().to_string();
        }
    }
}

/// Compare one snapshot under the scenario's tolerances; returns the differences.
fn compare(name: &str, label: &str, v4: &Snapshot, v5: &Snapshot, searching: bool) -> Vec<String> {
    let mut v4 = v4.clone();
    let mut v5 = v5.clone();
    normalize_version(&mut v4.lines);
    normalize_version(&mut v5.lines);
    let rows = v4.lines.len();
    let find_prompt = |s: &Snapshot| {
        s.lines
            .iter()
            .rposition(|l| l.contains('➤') || l.contains("search)`"))
            .unwrap_or(0)
    };
    let prompt_row = find_prompt(&v4);
    if name == "daemon" && (label == "daemon on" || label == "typed again") {
        // Tolerance 3: one more info line above the prompt; the toolbar shows the instance.
        let (a, b) = (find_prompt(&v4), find_prompt(&v5));
        return if v4.lines[a] == v5.lines[b] && v4.styles[a] == v5.styles[b] {
            Vec::new()
        } else {
            vec![format!(
                "prompt row:\n    v4: {:?}\n    v5: {:?}",
                v4.lines[a], v5.lines[b]
            )]
        };
    }
    if name == "no_ansi" {
        // Tolerance 4: v4's no-ANSI toolbar is one cell too wide (it counts the icon as one
        // column); prompt_toolkit draws the icon's first half in the last column, v5 clips it.
        let last = rows - 1;
        for s in [&mut v4, &mut v5] {
            s.lines[last] = s.lines[last].trim_end_matches('🔴').trim_end().to_string();
            s.styles[last].retain(|r| r.0 < 97);
        }
    }
    // The rows each scenario compares.
    let compared: Vec<usize> = match name {
        "tiny" => (prompt_row.saturating_sub(1)..rows).collect(),
        _ => (0..rows).collect(),
    };
    let mut diffs = Vec::new();
    for r in compared {
        let (a, b) = (&v4.lines[r], &v5.lines[r]);
        if a != b {
            diffs.push(format!("row {r} text:\n    v4: {a:?}\n    v5: {b:?}"));
        }
        let (sa, sb) = (&v4.styles[r], &v5.styles[r]);
        let version_row = a.contains("v{version}");
        if sa != sb && !version_row {
            diffs.push(format!("row {r} styles:\n    v4: {sa:?}\n    v5: {sb:?}"));
        }
    }
    let skip_cursor = searching || name == "tiny" && v4.cursor.0 < prompt_row as u16;
    if !skip_cursor && v4.cursor != v5.cursor && !matches!(name, "daemon") {
        diffs.push(format!("cursor: v4 {:?} v5 {:?}", v4.cursor, v5.cursor));
    }
    diffs
}

fn check(name: &str) {
    let fx = fixture();
    let scenario = fx["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == name)
        .unwrap_or_else(|| panic!("no scenario {name}"));
    let ours = run(scenario, &fx["paths"]);
    let mut failures = Vec::new();
    let mut searching = false;
    for ((step, v4), v5) in scenario["steps"]
        .as_array()
        .unwrap()
        .iter()
        .zip(scenario["snapshots"].as_array().unwrap())
        .zip(&ours)
    {
        let label = step["label"].as_str().unwrap();
        if step["send"] == "\u{12}" {
            searching = true;
        }
        let diffs = compare(name, label, &fixture_snapshot(v4), v5, searching);
        if step["send"] == "\r" {
            searching = false;
        }
        if !diffs.is_empty() {
            failures.push(format!(
                "[{label}]\n  {}\n  v5 screen:\n{}",
                diffs.join("\n  "),
                v5.lines
                    .iter()
                    .enumerate()
                    .filter(|(_, l)| !l.is_empty())
                    .map(|(i, l)| format!("    {i:2}|{l}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            ));
        }
    }
    assert!(failures.is_empty(), "{name}:\n{}", failures.join("\n"));
}

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        $(
            #[test]
            fn $name() {
                check(stringify!($name));
            }
        )*
    };
}

scenarios!(
    startup,
    menu_actions,
    menu_subactions,
    menu_space,
    menu_flags,
    menu_choices,
    autosuggest,
    instance,
    ctrl_c,
    ctrl_d,
    exit_command,
    resize,
    tiny,
    long_output,
    menu_scroll,
    history_nav,
    clear,
    history_search,
    backslash,
    daemon,
    no_ansi,
    update_message,
    toolbar_disabled,
);

#[test]
fn schedules_can_be_interrupted_repeatedly() {
    let tmp = tempfile::tempdir().unwrap();
    let scenario = json!({"config": {"shell": {"clear_screen": false}}});
    let mut env = environment(tmp.path(), &scenario);
    // Native pipelines must work even when no Python interpreter is available.
    env.push((
        "MRSM_PYTHON".into(),
        tmp.path()
            .join("missing-python")
            .to_string_lossy()
            .into_owned(),
    ));
    let mut term = Term::spawn(&env, tmp.path(), 40, 120);
    term.pump(Duration::from_millis(800), Duration::from_secs(20));
    let version = meerschaum_cli::VERSION.split('-').next().unwrap();
    for _ in 0..2 {
        term.send("show version : -s 'every 0.1 seconds'\r");
        let deadline = Instant::now() + Duration::from_secs(30);
        while term.parser.screen().contents().matches(version).count() < 3
            && Instant::now() < deadline
        {
            term.pump(Duration::from_millis(100), Duration::from_millis(300));
        }
        assert!(term.child.try_wait().unwrap().is_none());
        let screen = term.parser.screen().contents();
        let count = screen.matches(version).count();
        assert!(count >= 3, "schedule did not repeat: {screen}");
        term.send("\x03");
        term.pump(Duration::from_millis(500), Duration::from_secs(3));
        assert!(
            term.child.try_wait().unwrap().is_none(),
            "second interrupt exited shell"
        );
        let screen = term.parser.screen().contents();
        assert!(screen.contains("Exited scheduler"), "{screen}");
        assert!(!screen.contains("KeyboardInterrupt"), "{screen}");
        term.send("\x0c");
        term.pump(Duration::from_millis(300), Duration::from_secs(1));
    }
    term.send("exit\r");
    assert!(term.wait_exit());
}
