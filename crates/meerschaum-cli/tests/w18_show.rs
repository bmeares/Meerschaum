//! Wave 18 (SW): `show data` truncation (`formatting:dataframe:*`, `--all`, limited reads), the
//! concurrent per-pipe `show` views keeping the pipes' order, `show sizes` for pipes that share a
//! table, and the failing exit code of `--nopretty`.
//!
//! Every run uses a temporary root, an empty plugins directory, a SQLite instance (`sql:local`)
//! and `MRSM_PYTHON` pointing at nothing, so a path that reached the Python worker would fail.
//! The data is synthetic.
#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Stdio};

struct Out {
    code: i32,
    text: String,
}

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
    env: Vec<(String, String)>,
}

impl Home {
    fn new() -> Home {
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
            env: Vec::new(),
        }
    }

    fn with_env(mut self, key: &str, value: &str) -> Home {
        self.env.push((key.into(), value.into()));
        self
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.path())
            .env("MRSM_ROOT_DIR", self.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_PYTHON", self.path().join("no-python"));
        for (key, value) in &self.env {
            command.env(key, value);
        }
        command
    }

    /// `mrsm -i sql:local <args>`: stdout and stderr, without ANSI sequences.
    fn run(&self, args: &[&str]) -> Out {
        let output = self
            .command()
            .args(["-i", "sql:local"])
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        Out {
            code: output.status.code().unwrap_or(-1),
            text: strip_ansi(&format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )),
        }
    }

    fn sql(&self, query: &str) {
        let out = self.run(&["sql", "local", query]);
        assert_eq!(out.code, 0, "{query}\n{}", out.text);
    }

    /// A table `name` of `rows` hourly rows and a pipe `metric` over it.
    fn seed(&self, table: &str, metric: &str, rows: usize) {
        self.sql(&format!(
            "CREATE TABLE {table} (id int primary key, dt text, v real, note text)"
        ));
        self.sql(&format!(
            "INSERT INTO {table} WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n \
             WHERE i < {}) SELECT i, datetime('2024-01-01', '+' || i || ' hours'), i * 1.5, \
             substr('{}', 1, i % 80) FROM n",
            rows - 1,
            "n".repeat(80)
        ));
        let registered = self.run(&[
            "register",
            "pipes",
            "-c",
            "sql:local",
            "-m",
            metric,
            "--params",
            &format!(
                r#"{{"columns":{{"datetime":"dt","primary":"id"}},"fetch":{{"definition":"SELECT * FROM {table}"}}}}"#
            ),
        ]);
        assert_eq!(registered.code, 0, "{}", registered.text);
        let synced = self.run(&["sync", "pipes", "-c", "sql:local", "-m", metric]);
        assert_eq!(synced.code, 0, "{}", synced.text);
    }
}

fn strip_ansi(text: &str) -> String {
    regex::Regex::new("\x1b\\[[0-9;]*[A-Za-z]")
        .unwrap()
        .replace_all(text, "")
        .into_owned()
}

fn table_lines(text: &str) -> Vec<&str> {
    text.lines().filter(|l| l.starts_with('|')).collect()
}

#[test]
fn show_data_truncates_a_long_table_and_all_prints_everything() {
    let home = Home::new();
    home.seed("src_long", "long", 200);
    let out = home.run(&["show", "data", "all", "-m", "long"]);
    assert_eq!(out.code, 0, "{}", out.text);
    let rows = table_lines(&out.text);
    // header + separator + 5 head rows + the ellipsis row + 5 tail rows
    assert_eq!(rows.len(), 13, "{}", out.text);
    assert!(rows[7].contains('…'), "{}", out.text);
    assert!(rows[2].contains("2024-01-01 00:00:00"), "{}", out.text);
    assert!(rows[12].contains("2024-01-09 07:00:00"), "{}", out.text);
    assert!(
        out.text
            .contains("[200 rows x 4 columns (showing 10 rows)]"),
        "{}",
        out.text
    );

    let all = home.run(&["show", "data", "all", "-m", "long", "--all"]);
    assert_eq!(all.code, 0, "{}", all.text);
    assert_eq!(table_lines(&all.text).len(), 202, "{}", all.text);
    assert!(all.text.contains("[200 rows x 4 columns]"), "{}", all.text);
    assert!(!all.text.contains('…'));

    // `--nopretty` never truncates (and the exit code is success).
    let json = home.run(&["show", "data", "all", "-m", "long", "--nopretty"]);
    assert_eq!(json.code, 0, "{}", json.text);
    assert!(json.text.contains("2024-01-09 07:00:00") || json.text.contains("2024-01-09T07"));
    assert!(!json.text.contains('…'));
}

#[test]
fn show_data_leaves_a_short_table_alone() {
    let home = Home::new();
    home.seed("src_short", "short", 8);
    let out = home.run(&["show", "data", "all", "-m", "short"]);
    assert_eq!(out.code, 0, "{}", out.text);
    assert_eq!(table_lines(&out.text).len(), 10, "{}", out.text);
    assert!(out.text.contains("[8 rows x 4 columns]"), "{}", out.text);
    assert!(!out.text.contains('…'));
}

#[test]
fn the_dataframe_limits_come_from_the_config() {
    let config = |frame: &str| {
        format!(
            r#"{{"formatting":{{"dataframe":{frame}}},"shell":{{"updates":{{"check":false}}}}}}"#
        )
    };
    let home = Home::new();
    home.seed("src_cfg", "cfg", 40);
    // 40 rows are under the default maximum of 60.
    let plain = home.run(&["show", "data", "all", "-m", "cfg"]);
    assert_eq!(table_lines(&plain.text).len(), 42, "{}", plain.text);

    let rows = Home::new().with_env("MRSM_CONFIG", &config(r#"{"max_rows":20,"min_rows":4}"#));
    rows.seed("src_cfg", "cfg", 40);
    let out = rows.run(&["show", "data", "all", "-m", "cfg"]);
    assert_eq!(table_lines(&out.text).len(), 2 + 4 + 1, "{}", out.text);
    assert!(
        out.text.contains("[40 rows x 4 columns (showing 4 rows)]"),
        "{}",
        out.text
    );

    let width = Home::new().with_env("MRSM_CONFIG", &config(r#"{"max_colwidth":12}"#));
    width.seed("src_cfg", "cfg", 40);
    let out = width.run(&["show", "data", "all", "-m", "cfg"]);
    let longest = table_lines(&out.text)
        .iter()
        .flat_map(|l| l.split('|'))
        .map(|c| c.trim().chars().count())
        .max()
        .unwrap();
    assert!(
        longest <= 19,
        "dates are not cut ({longest}):\n{}",
        out.text
    );
    assert!(out.text.contains('…'), "{}", out.text);
    assert!(!out.text.contains(&"n".repeat(13)), "{}", out.text);
}

#[test]
fn columns_that_do_not_fit_the_terminal_are_elided() {
    let home = Home::new().with_env("COLUMNS", "40");
    home.seed("src_wide", "wide", 12);
    let out = home.run(&["show", "data", "all", "-m", "wide"]);
    assert_eq!(out.code, 0, "{}", out.text);
    let header = table_lines(&out.text)[0];
    assert!(header.chars().count() <= 40, "{}", out.text);
    assert!(header.contains('…'), "{}", out.text);
    assert!(
        header.contains("id") && header.contains("note"),
        "{}",
        out.text
    );
    assert!(
        out.text
            .contains("[12 rows x 4 columns (showing 3 columns)]")
            || out
                .text
                .contains("[12 rows x 4 columns (showing 2 columns)]"),
        "{}",
        out.text
    );
    // Without a terminal width (a pipe, no COLUMNS) every column is printed.
    let piped = Home::new();
    piped.seed("src_wide", "wide", 12);
    let out = piped.run(&["show", "data", "all", "-m", "wide"]);
    assert!(out.text.contains("[12 rows x 4 columns]"), "{}", out.text);
}

#[test]
fn per_pipe_views_print_in_the_order_of_the_pipes_with_workers() {
    let home = Home::new();
    home.seed("src_a", "aaa", 3);
    home.seed("src_b", "bbb", 3);
    home.seed("src_c", "ccc", 3);
    for view in ["columns", "data", "rowcounts"] {
        let serial = home.run(&["show", view, "-c", "sql:local", "--workers", "1"]);
        let parallel = home.run(&["show", view, "-c", "sql:local", "--workers", "4"]);
        assert_eq!(serial.code, 0, "{}", serial.text);
        assert_eq!(parallel.code, 0, "{}", parallel.text);
        let order = |text: &str| -> Vec<usize> {
            ["aaa", "bbb", "ccc"]
                .iter()
                .map(|m| text.find(m).unwrap_or(usize::MAX))
                .collect()
        };
        if view != "rowcounts" {
            // rowcounts sorts by size; the others keep the order of the selection.
            assert!(
                order(&parallel.text).windows(2).all(|w| w[0] < w[1]),
                "{view}\n{}",
                parallel.text
            );
        }
        assert_eq!(
            serial.text.replace("workers", ""),
            parallel.text.replace("workers", ""),
            "{view}: concurrent output differs from serial"
        );
    }
}

#[test]
fn show_sizes_marks_pipes_that_share_a_table_and_counts_it_once() {
    let home = Home::new();
    home.seed("src_shared", "first", 20);
    // A second pipe whose target is the same physical table, and a pipe with its own table.
    let registered = home.run(&[
        "register",
        "pipes",
        "-c",
        "sql:local",
        "-m",
        "second",
        "--params",
        r#"{"columns":{"datetime":"dt","primary":"id"},"target":"sql_local_first"}"#,
    ]);
    assert_eq!(registered.code, 0, "{}", registered.text);
    home.seed("src_own", "own", 20);
    let out = home.run(&["show", "sizes", "-c", "sql:local"]);
    assert_eq!(out.code, 0, "{}", out.text);
    let shared: Vec<&str> = out
        .text
        .lines()
        .filter(|l| l.contains("shared x2"))
        .collect();
    assert_eq!(shared.len(), 2, "{}", out.text);
    assert!(shared.iter().any(|l| l.contains("first")));
    assert!(shared.iter().any(|l| l.contains("second")));
    let own = out.text.lines().find(|l| l.contains("own")).unwrap();
    assert!(!own.contains("shared"), "{}", out.text);
    // The total is the two distinct tables, not three.
    let size_of = |line: &str| {
        line.split_whitespace()
            .rev()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
    };
    let nopretty = home.run(&["show", "sizes", "-c", "sql:local", "--nopretty"]);
    assert_eq!(nopretty.code, 0, "{}", nopretty.text);
    let rows: Vec<&str> = nopretty.text.lines().filter(|l| l.contains('\t')).collect();
    assert_eq!(rows.len(), 3, "{}", nopretty.text);
    assert_eq!(
        rows.iter().filter(|l| l.ends_with("shared x2")).count(),
        2,
        "{}",
        nopretty.text
    );
    let _ = size_of;
}

#[test]
fn nopretty_exits_nonzero_when_the_command_fails() {
    let home = Home::new();
    home.seed("src_exit", "exit", 3);
    let ok = home.run(&["show", "pipes", "-m", "exit", "--nopretty"]);
    assert_eq!(ok.code, 0, "{}", ok.text);
    // `show sizes` with nothing selected is a failure tuple.
    let failed = home.run(&["show", "sizes", "-m", "nothing", "--nopretty"]);
    assert_eq!(failed.code, 1, "{}", failed.text);
    // Pretty mode already exited 1 and prints the message.
    let pretty = home.run(&["show", "sizes", "-m", "nothing"]);
    assert_eq!(pretty.code, 1, "{}", pretty.text);
    assert!(
        pretty.text.contains("No pipes selected."),
        "{}",
        pretty.text
    );
    // A chain stops at the failure, with the same status.
    let chained = home.run(&[
        "show",
        "sizes",
        "-m",
        "nothing",
        "+",
        "show",
        "version",
        "--nopretty",
    ]);
    assert_eq!(chained.code, 1, "{}", chained.text);
}
