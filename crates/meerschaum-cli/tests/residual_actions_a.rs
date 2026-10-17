//! Wave 15 (task N1): the actions that used to fall through to the Python worker's `core_action`
//! (`login`, `os`, `sql`, `stack`, the connector, token, cache and index actions, `show
//! schedules|targets|tokens`, `start connectors|gui`, the `bootstrap` wizards and `api start`),
//! run natively.
//!
//! Every test runs the `mrsm` binary against a temporary root with an empty plugins directory and
//! `MRSM_PYTHON` pointing at a program that does not exist, so any path that still reached the
//! Python worker would fail with a `WorkerError`. The instance is SQLite (`sql:local`); the one
//! network test starts a loopback `mrsm start api` on a random port (never 8989).
//!
//! The expected messages are the ones v4.1.4 prints for the same inputs (recorded in
//! `crates/meerschaum-py/PARITY.md`, "Wave 15 (task N1)").
#![cfg(unix)]

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// What one run printed.
struct Out {
    code: i32,
    text: String,
}

/// A temporary root, plugins directory and (optionally) the API connectors to define.
struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
    endpoints: Vec<(String, u16)>,
    env: Vec<(String, String)>,
}

impl Home {
    fn new() -> Home {
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
            endpoints: Vec::new(),
            env: Vec::new(),
        }
    }

    fn with_endpoint(mut self, label: &str, port: u16) -> Home {
        self.endpoints.push((label.to_string(), port));
        self
    }

    fn with_env(mut self, key: &str, value: impl Into<String>) -> Home {
        self.env.push((key.to_string(), value.into()));
        self
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_PYTHON", self.root.path().join("no-python"));
        for (key, value) in &self.env {
            command.env(key, value);
        }
        for (label, port) in &self.endpoints {
            command.env(
                format!("MRSM_API_{}", label.to_uppercase()),
                format!("http://127.0.0.1:{port}"),
            );
        }
        command
    }

    /// Run `mrsm <args>` with `stdin` as its standard input.
    fn run_with(&self, mut command: Command, args: &[&str], stdin: &str) -> Out {
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        Out {
            code: output.status.code().unwrap_or(-1),
            text: strip_ansi(&format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )),
        }
    }

    fn run_in(&self, args: &[&str], stdin: &str) -> Out {
        self.run_with(self.command(), args, stdin)
    }

    /// `mrsm -i sql:local <args>` with no input.
    fn run(&self, args: &[&str]) -> Out {
        let mut full = vec!["-i", "sql:local"];
        full.extend_from_slice(args);
        self.run_in(&full, "")
    }

    /// `mrsm -i sql:local <args>` answering the prompts with `stdin`.
    fn ask(&self, args: &[&str], stdin: &str) -> Out {
        let mut full = vec!["-i", "sql:local"];
        full.extend_from_slice(args);
        self.run_in(&full, stdin)
    }

    /// Run SQL against the instance (`sql local "<query>"`).
    fn sql(&self, query: &str) -> Out {
        self.run(&["sql", "local", query])
    }

    /// Every file under `<root>/config` concatenated.
    fn config_text(&self) -> String {
        let mut text = String::new();
        let dir = self.root.path().join("config");
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Ok(content) = std::fs::read_to_string(entry.path()) {
                    text.push_str(&content);
                }
            }
        }
        text
    }
}

/// The text without ANSI escape sequences (the output is colored even when piped, like v4's).
fn strip_ansi(text: &str) -> String {
    regex::Regex::new("\x1b\\[[0-9;]*[A-Za-z]")
        .unwrap()
        .replace_all(text, "")
        .into_owned()
}

fn no_worker(out: &Out) {
    assert!(
        !out.text.contains("WorkerError"),
        "reached the Python worker:\n{}",
        out.text
    );
}

/// A SQLite table `src` with one row and a pipe over it that has a datetime and an id.
fn seed_pipe(home: &Home) {
    assert_eq!(
        home.sql("CREATE TABLE src (id int primary key, dt text, v real)")
            .code,
        0
    );
    assert_eq!(
        home.sql("INSERT INTO src VALUES (1, '2024-01-01 00:00:00', 1.5)")
            .code,
        0
    );
    let registered = home.run(&[
        "register",
        "pipes",
        "-c",
        "sql:local",
        "-m",
        "foo",
        "-l",
        "a",
        "--params",
        r#"{"columns":{"datetime":"dt","primary":"id"},"fetch":{"definition":"SELECT * FROM src"}}"#,
    ]);
    assert_eq!(registered.code, 0, "{}", registered.text);
    let synced = home.run(&["sync", "pipes", "-c", "sql:local", "-m", "foo"]);
    assert_eq!(synced.code, 0, "{}", synced.text);
}

#[test]
fn no_group_a_action_reaches_the_python_worker() {
    let home = Home::new();
    // `--help` goes through the action docs; the rest are the non-interactive forms (every
    // prompt is answered by an empty input, which ends in `EOFError`, never in the worker).
    let help = [
        "login",
        "os",
        "sql",
        "stack",
        "bootstrap connectors",
        "bootstrap pipes",
        "bootstrap jobs",
        "clear cache",
        "copy connectors",
        "delete cache",
        "delete config",
        "delete connectors",
        "drop indices",
        "edit definition",
        "edit tokens",
        "flush indices",
        "flush pipes",
        "register connectors",
        "register tokens",
        "show schedules",
        "show targets",
        "show tokens",
        "start connectors",
        "start gui",
        "start pipeline",
        "api start",
        "show daemons",
        "start daemons",
        "stop daemons",
        "restart daemons",
    ];
    for action in help {
        let mut args: Vec<&str> = action.split(' ').collect();
        args.push("--help");
        let out = home.run(&args);
        no_worker(&out);
        assert_eq!(out.code, 0, "`{action} --help`:\n{}", out.text);
    }
    let forms: &[&[&str]] = &[
        &["login"],
        &["os", "echo", "hi"],
        &["sql", "local", "SELECT 1 AS a"],
        &["bootstrap", "connectors"],
        &["bootstrap", "pipes"],
        &["bootstrap", "jobs"],
        &["clear", "cache", "--yes"],
        &["copy", "connectors", "sql:local", "sql:other", "--yes"],
        &["delete", "cache", "--yes"],
        &["delete", "config", "--yes"],
        &["delete", "connectors", "sql:other", "--yes"],
        &["drop", "indices", "--yes"],
        &["edit", "definition", "--yes"],
        &["edit", "tokens"],
        &["flush", "indices", "--yes"],
        &["flush", "pipes", "--yes"],
        &["register", "connectors", "sql:third", "--noask"],
        &["register", "tokens"],
        &["show", "schedules", "daily"],
        &["show", "targets"],
        &["show", "tokens"],
        &["start", "connectors", "sql:local"],
        &["show", "daemons"],
        &["start", "daemons"],
        &["stop", "daemons"],
        &["restart", "daemons"],
        &["start", "daemons", "--force", "--timeout-seconds", "1"],
        &[
            "show",
            "version",
            "--nopretty",
            ":",
            "--timeout-seconds",
            "5",
        ],
        &["os", "true", ":", "--timeout-seconds", "5"],
    ];
    for form in forms {
        let out = home.run(form);
        no_worker(&out);
    }
}

#[test]
fn os_runs_a_command_prints_its_output_and_reports_the_exit_code() {
    let home = Home::new();
    let out = home.run(&["os", "echo", "hello", "world"]);
    assert_eq!(out.code, 0, "{}", out.text);
    assert!(out.text.starts_with("hello world\n"), "{}", out.text);
    assert!(out.text.contains("hello world"), "{}", out.text);

    let failed = home.run(&["os", "false"]);
    assert_eq!(failed.code, 1, "{}", failed.text);

    // No command: v4 returns the action's docstring as the failure.
    let empty = home.run(&["os"]);
    assert_eq!(empty.code, 1);
    assert!(
        empty
            .text
            .contains("Launch a subprocess and read its output to stdout."),
        "{}",
        empty.text
    );
}

#[test]
fn sql_reads_execs_and_prints_none_for_a_bad_query() {
    let home = Home::new();
    let created = home.sql("CREATE TABLE t (a int, b text)");
    assert_eq!(created.code, 0, "{}", created.text);
    assert!(created.text.contains("Successfully executed query"));

    // An empty table: tabulate draws the separator without alignment colons.
    let empty = home.sql("SELECT * FROM t");
    assert_eq!(empty.code, 0, "{}", empty.text);
    assert!(empty.text.contains("|-----|-----|"), "{}", empty.text);
    assert!(
        empty.text.contains("[0 rows x 2 columns]"),
        "{}",
        empty.text
    );

    assert_eq!(home.sql("INSERT INTO t VALUES (1, 'x')").code, 0);
    let rows = home.sql("SELECT * FROM t");
    assert!(rows.text.contains("|   a | b   |"), "{}", rows.text);
    assert!(rows.text.contains("|   1 | x   |"), "{}", rows.text);
    assert!(rows.text.contains("[1 row x 2 columns]"), "{}", rows.text);

    let json = home.run(&["sql", "local", "SELECT a FROM t", "--nopretty"]);
    assert!(
        json.text.contains(r#"{"columns":["a"],"data":[[1]]}"#),
        "{}",
        json.text
    );

    // v4 logs the error and prints `None`; the action itself succeeds.
    let bad = home.sql("SELEC nothing");
    assert_eq!(bad.code, 0, "{}", bad.text);
    assert!(bad.text.contains("None"), "{}", bad.text);
    assert!(bad.text.contains("syntax error"), "{}", bad.text);

    // `sql sql:local <query>` accepts full keys too, and an unknown label fails loudly.
    let keyed = home.run(&["sql", "sql:local", "SELECT 2 AS two"]);
    assert!(keyed.text.contains("two"), "{}", keyed.text);
    let unknown = home.run(&["sql", "nonexistent", "SELECT 1"]);
    no_worker(&unknown);
}

#[test]
fn show_data_prints_the_backtrack_like_v4() {
    let home = Home::new();
    assert_eq!(home.sql("CREATE TABLE foo_t (dt text, v int)").code, 0);
    let registered = home.run(&[
        "register",
        "pipes",
        "-c",
        "sql:local",
        "-m",
        "foo",
        "--params",
        r#"{"columns":{"datetime":"dt"},"target":"foo_t"}"#,
    ]);
    assert_eq!(registered.code, 0, "{}", registered.text);
    // The default is the int 1440; an explicit `60` goes through `float()`.
    let default = home.run(&["show", "data", "-m", "foo"]);
    assert!(
        default.text.contains("Last 1440 minutes of data for"),
        "{}",
        default.text
    );
    let explicit = home.run(&["show", "data", "60", "-m", "foo"]);
    assert!(
        explicit.text.contains("Last 60.0 minutes of data for"),
        "{}",
        explicit.text
    );
    let one = home.run(&["show", "data", "1", "-m", "foo"]);
    assert!(
        one.text.contains("Last 1.0 minute of data for"),
        "{}",
        one.text
    );
}

#[test]
fn login_of_an_unknown_api_connector_warns_like_v4() {
    let home = Home::new();
    let out = home.run(&["login", "api:nonexist"]);
    assert_eq!(out.code, 1, "{}", out.text);
    let incorrect = out
        .text
        .find("Incorrect attributes for connector 'api:nonexist'.");
    let missing = out
        .text
        .find("Missing 'host' for connector 'api:nonexist'.");
    let unable = out
        .text
        .find("Unable to build connector 'api:nonexist'. Is it registered?");
    assert!(incorrect < missing && missing < unable, "{}", out.text);
    assert!(
        out.text.contains("Logged into 0 connectors."),
        "{}",
        out.text
    );
}

#[test]
fn an_api_connector_without_credentials_warns_to_login_once() {
    // Nothing listens on the endpoint: the warning comes from the client's `token` lookup,
    // before the (failing) request, and is printed once.
    let home = Home::new().with_endpoint("nolive", free_port());
    let out = home.run_in(&["show", "pipes", "-i", "api:nolive"], "");
    no_worker(&out);
    assert_eq!(
        out.text
            .matches("Please login with the command `login api:nolive`.")
            .count(),
        1,
        "{}",
        out.text
    );
    // The warning is on stderr, in v4's warning style.
    let mut command = home.command();
    command.env("MRSM_ROOT_DIR", home.path());
    let raw = command
        .args(["show", "pipes", "-i", "api:nolive"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&raw.stdout);
    let stderr = String::from_utf8_lossy(&raw.stderr);
    assert!(!stdout.contains("Please login"), "{stdout}");
    assert!(
        stderr.contains(
            "\x1b[1;33m \u{1f514} Please login with the command `login api:nolive`.\x1b[0m\n"
        ),
        "{stderr:?}"
    );
}

#[test]
fn stack_prints_the_generated_compose_project() {
    let home = Home::new();
    let out = home.run(&["stack", "--help"]);
    assert_eq!(out.code, 0);
    assert!(
        out.text
            .contains("Control the Meerschaum stack with Docker Compose.")
    );
    // The project files are generated under the root (the compose file itself, then `docker
    // compose`, which is only available on some hosts).
    let config = home.run(&["stack", "config"]);
    no_worker(&config);
    let stack = home.path().join("stack");
    assert!(
        stack.exists() || config.text.to_lowercase().contains("docker"),
        "{}",
        config.text
    );
}

#[test]
fn show_schedules_prints_the_next_timestamps() {
    let home = Home::new();
    let out = home.run(&[
        "show",
        "schedules",
        "daily starting 00:00",
        "3",
        "--nopretty",
    ]);
    assert_eq!(out.code, 0, "{}", out.text);
    let lines: Vec<&str> = out
        .text
        .lines()
        .filter(|l| l.ends_with("00:00:00+00:00"))
        .collect();
    assert_eq!(lines.len(), 3, "{}", out.text);

    let pretty = home.run(&["show", "schedules", "every 2 hours", "2"]);
    assert!(
        pretty
            .text
            .contains("Next 2 timestamps for schedule 'every 2 hours':"),
        "{}",
        pretty.text
    );

    let none = home.run(&["show", "schedules"]);
    assert_eq!(none.code, 1);
    assert!(none.text.contains("Provide a schedule to be parsed."));

    let bad = home.run(&["show", "schedules", "foo bar"]);
    assert_eq!(bad.code, 1);
    assert!(
        bad.text.contains("Invalid cron schedule 'foo bar'."),
        "{}",
        bad.text
    );
}

#[test]
fn connectors_register_copy_show_and_delete() {
    let home = Home::new();

    let none = home.run(&["register", "connectors"]);
    no_worker(&none);
    assert_eq!(none.code, 1, "{}", none.text);

    let registered = home.run(&[
        "register",
        "connectors",
        "sql:scratch",
        "--params",
        r#"{"flavor":"sqlite","database":"/tmp/w15n1_scratch.db"}"#,
    ]);
    assert_eq!(registered.code, 0, "{}", registered.text);
    assert!(
        registered
            .text
            .contains("Successfully registered connector 'sql:scratch'."),
        "{}",
        registered.text
    );
    assert!(home.config_text().contains("w15n1_scratch.db"));

    // Registering an existing connector shows both parameter sets and changes nothing.
    let again = home.run(&["register", "connectors", "sql:scratch", "--noask"]);
    assert_eq!(again.code, 1, "{}", again.text);
    assert!(again.text.contains("Existing Parameters"), "{}", again.text);
    assert!(
        again
            .text
            .contains("Connector 'sql:scratch' already exists."),
        "{}",
        again.text
    );
    assert!(
        again.text.contains("Nothing was changed."),
        "{}",
        again.text
    );
    // Without parameters, a connector that is already empty has nothing to do.
    let empty = home.run(&["register", "connectors", "sql:blank", "--noask"]);
    assert_eq!(empty.code, 0, "{}", empty.text);
    let blank = home.run(&["register", "connectors", "sql:blank", "--noask"]);
    assert_eq!(blank.code, 0, "{}", blank.text);
    assert!(
        blank.text.contains("Connector exists, nothing to do."),
        "{}",
        blank.text
    );

    // A `uri` parameter.
    let uri = home.run(&[
        "register",
        "connectors",
        "sql:viauri",
        "--params",
        "uri:sqlite:////tmp/w15n1_uri.db",
    ]);
    assert_eq!(uri.code, 0, "{}", uri.text);

    // `copy connectors`: asks unless `--yes`, and EOF ends the prompt.
    let declined = home.ask(&["copy", "connectors", "sql:scratch", "sql:dup"], "n\n");
    assert_eq!(declined.code, 1, "{}", declined.text);
    assert!(
        declined
            .text
            .contains("Register connector 'sql:dup' with the above attributes?")
    );
    let copied = home.ask(&["copy", "connectors", "sql:scratch", "sql:dup"], "y\n");
    assert_eq!(copied.code, 0, "{}", copied.text);
    assert!(
        copied
            .text
            .contains("Successfully registered connector 'sql:dup'."),
        "{}",
        copied.text
    );
    let eof = home.run(&["copy", "connectors", "sql:scratch", "sql:dup2"]);
    assert!(eof.text.contains("EOFError"), "{}", eof.text);

    let shown = home.run(&["show", "connectors", "sql"]);
    assert!(shown.text.contains("'scratch'"), "{}", shown.text);
    assert!(shown.text.contains("'dup'"), "{}", shown.text);

    // `delete connectors` asks first.
    let kept = home.ask(&["delete", "connectors", "sql:dup"], "n\n");
    assert!(
        kept.text
            .contains("Are you sure you want to delete connector 'sql:dup'")
    );
    assert!(home.config_text().contains("dup"));
    let deleted = home.run(&["delete", "connectors", "sql:dup", "--yes"]);
    assert_eq!(deleted.code, 0, "{}", deleted.text);
    assert!(
        !home
            .run(&["show", "connectors", "sql"])
            .text
            .contains("'dup'")
    );

    // Starting a connector polls it until it connects.
    let started = home.run(&["start", "connectors", "sql:scratch"]);
    no_worker(&started);
    let local = home.run(&["start", "connectors", "sql:local"]);
    assert_eq!(local.code, 0, "{}", local.text);
    assert!(
        local.text.contains("Connected to 'sql:local'."),
        "{}",
        local.text
    );
    assert!(
        local
            .text
            .contains("Successfully started connector 'sql:local'."),
        "{}",
        local.text
    );
    let invalid = home.run(&["start", "connectors", "nonsense"]);
    assert_eq!(invalid.code, 1);
    assert!(
        invalid
            .text
            .contains("No valid connector keys were provided.")
    );
}

#[test]
fn delete_and_clear_cache_ask_then_clear_the_cache_directory() {
    let home = Home::new();
    let cache = home.path().join(".cache");
    std::fs::create_dir_all(cache.join("nested")).unwrap();
    std::fs::write(cache.join("nested").join("blob"), vec![0u8; 2048]).unwrap();

    let kept = home.ask(&["delete", "cache"], "n\n");
    assert!(
        kept.text
            .contains("Are you sure you want to remove the cache directory ("),
        "{}",
        kept.text
    );
    assert!(kept.text.contains("Nothing was deleted."), "{}", kept.text);
    assert!(cache.join("nested").join("blob").exists());

    let eof = home.run(&["delete", "cache"]);
    assert!(eof.text.contains("EOFError"), "{}", eof.text);
    assert!(cache.join("nested").join("blob").exists());

    let cleared = home.ask(&["delete", "cache"], "y\n");
    assert_eq!(cleared.code, 0, "{}", cleared.text);
    assert!(
        cleared.text.contains("Successfully cleared "),
        "{}",
        cleared.text
    );
    assert!(
        !cleared.text.contains("Successfully cleared 0 Bytes."),
        "{}",
        cleared.text
    );
    assert!(!cache.join("nested").exists());

    let again = home.run(&["clear", "cache", "--yes"]);
    assert_eq!(again.code, 0, "{}", again.text);
    assert!(
        again.text.contains("Successfully cleared 0 Bytes."),
        "{}",
        again.text
    );
}

#[test]
fn cache_of_a_connector_cleans_its_metadata() {
    let home = Home::new();
    let out = home.run(&["delete", "cache", "sql:local", "--yes"]);
    assert_eq!(out.code, 0, "{}", out.text);
    let missing = home.run(&["delete", "cache", "x", "--yes"]);
    assert_eq!(missing.code, 1);
    assert!(
        missing.text.contains("Connector 'x' does not exist."),
        "{}",
        missing.text
    );
}

#[test]
fn show_targets_groups_pipes_by_their_target_table() {
    let home = Home::new();
    seed_pipe(&home);
    let second = home.run(&[
        "register",
        "pipes",
        "-c",
        "sql:local",
        "-m",
        "bar",
        "--params",
        r#"{"columns":{"datetime":"dt"},"target":"custom_t"}"#,
    ]);
    assert_eq!(second.code, 0, "{}", second.text);

    let out = home.run(&["show", "targets"]);
    assert_eq!(out.code, 0, "{}", out.text);
    assert!(out.text.contains("custom_t"), "{}", out.text);
    assert!(out.text.contains("sql_local_foo_a"), "{}", out.text);
    assert!(out.text.contains("'bar'"), "{}", out.text);

    let one = home.run(&["show", "targets", "custom_t", "--nopretty"]);
    assert_eq!(one.code, 0, "{}", one.text);
    assert!(one.text.starts_with("custom_t\n"), "{}", one.text);
    assert!(!one.text.contains("sql_local_foo_a"), "{}", one.text);

    let none = home.run(&["show", "targets", "nothing_here"]);
    assert_eq!(none.code, 1);
    assert!(none.text.contains("No targets to show."), "{}", none.text);
}

#[test]
fn drop_and_flush_indices_and_flush_pipes() {
    let home = Home::new();
    seed_pipe(&home);

    // `drop indices` lists the indices and asks.
    let declined = home.ask(&["drop", "indices", "-m", "foo"], "n\n");
    assert_eq!(declined.code, 1, "{}", declined.text);
    assert!(
        declined
            .text
            .contains("Are you sure you want to drop these indices?"),
        "{}",
        declined.text
    );
    assert!(
        declined.text.contains("IX_sql_local_foo_a_"),
        "{}",
        declined.text
    );
    assert!(
        declined.text.contains("No pipes were dropped."),
        "{}",
        declined.text
    );

    let dropped = home.run(&["drop", "indices", "-m", "foo", "--yes"]);
    assert_eq!(dropped.code, 0, "{}", dropped.text);
    assert!(
        dropped
            .text
            .contains("Finished dropping indices for 1 pipe"),
        "{}",
        dropped.text
    );
    let indices = home.sql(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'IX_sql_local_foo_a%'",
    );
    assert!(
        indices.text.contains("[0 rows x 1 column]"),
        "{}",
        indices.text
    );

    // `flush indices` = drop, then re-create.
    let flushed = home.run(&["flush", "indices", "-m", "foo", "--yes"]);
    assert_eq!(flushed.code, 0, "{}", flushed.text);
    let indices = home.sql(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'IX_sql_local_foo_a%'",
    );
    assert!(
        indices.text.contains("IX_sql_local_foo_a_"),
        "{}",
        indices.text
    );

    // `flush pipes` drops the pipe's table, then syncs it again.
    assert_eq!(home.sql("DELETE FROM src WHERE id = 1").code, 0);
    assert_eq!(
        home.sql("INSERT INTO src VALUES (2, '2024-01-02 00:00:00', 2.5)")
            .code,
        0
    );
    let pipes = home.run(&["flush", "pipes", "-m", "foo", "--yes"]);
    assert_eq!(pipes.code, 0, "{}", pipes.text);
    let data = home.sql("SELECT id FROM sql_local_foo_a");
    assert!(data.text.contains("[1 row x 1 column]"), "{}", data.text);
    assert!(
        data.text.contains("|    2 |") || data.text.contains("2"),
        "{}",
        data.text
    );

    // Nothing selected.
    let none = home.run(&["drop", "indices", "-m", "does_not_exist", "--yes"]);
    assert_eq!(none.code, 1);
    assert!(none.text.contains("No pipes to drop."), "{}", none.text);
}

#[test]
fn edit_definition_edits_the_fetch_definition_in_the_editor() {
    let home = Home::new();
    seed_pipe(&home);
    // The "editor" replaces the definition file's content.
    let editor = home.path().join("editor.sh");
    std::fs::write(
        &editor,
        "#!/bin/sh\nprintf 'SELECT * FROM src WHERE id > 1' > \"$1\"\n",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&editor, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut command = home.command();
    command.env("EDITOR", &editor);
    let out = home.run_with(
        command,
        &[
            "-i",
            "sql:local",
            "edit",
            "definition",
            "-m",
            "foo",
            "--noask",
        ],
        "",
    );
    assert_eq!(out.code, 0, "{}", out.text);
    let shown = home.run(&["show", "pipes", "-m", "foo", "--nopretty"]);
    assert!(
        shown.text.contains("SELECT * FROM src WHERE id > 1"),
        "{}",
        shown.text
    );

    let none = home.run(&["edit", "definition", "-m", "nothing_here"]);
    assert_eq!(none.code, 1);
    assert!(none.text.contains("No pipes to edit."), "{}", none.text);
}

#[test]
fn tokens_register_show_and_edit() {
    let home = Home::new();
    let none = home.run(&["register", "tokens"]);
    assert_eq!(none.code, 1, "{}", none.text);
    assert!(
        none.text
            .contains("No users are registered to 'sql:local'."),
        "{}",
        none.text
    );

    // v4 asks for the password twice and the email.
    let user = home.ask(&["register", "users", "bob"], "password1\npassword1\n\n");
    assert_eq!(user.code, 0, "{}", user.text);

    let registered = home.ask(
        &["register", "tokens", "--name", "mytok", "--ttl-days", "10"],
        "1\n",
    );
    assert_eq!(registered.code, 0, "{}", registered.text);
    assert!(
        registered.text.contains("Registered token 'mytok'."),
        "{}",
        registered.text
    );
    assert!(
        registered.text.contains("Client Secret:"),
        "{}",
        registered.text
    );
    assert!(
        registered.text.contains("API Key: mrsm-key:"),
        "{}",
        registered.text
    );
    assert!(
        registered
            .text
            .contains("Successfully registered token 'mytok'."),
        "{}",
        registered.text
    );

    let expiring = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "other",
            "--end",
            "2030-01-01",
        ],
        "1\n",
    );
    assert_eq!(expiring.code, 0, "{}", expiring.text);
    assert!(
        expiring
            .text
            .contains("Expiration: 2030-01-01T00:00:00+00:00")
    );

    let table = home.run(&["show", "tokens"]);
    assert_eq!(table.code, 0, "{}", table.text);
    assert!(
        table
            .text
            .contains("Registered Tokens on instance 'sql:local'"),
        "{}",
        table.text
    );
    assert!(table.text.contains("mytok"), "{}", table.text);
    assert!(table.text.contains("bob"), "{}", table.text);
    let raw = home.run(&["show", "tokens", "--nopretty"]);
    let first = raw
        .text
        .lines()
        .find(|l| l.contains("\"label\": \"mytok\""));
    assert!(first.is_some(), "{}", raw.text);

    // `edit tokens` prompts for every field (label, expiration, scopes, invalidate?).
    // Without a terminal the typed text is appended to each prefilled value (like
    // `prompt_toolkit`), so the empty answers keep the expiration and the scopes.
    let edited = home.ask(&["edit", "tokens", "mytok"], "renamed\n\n\nn\n");
    assert_eq!(edited.code, 0, "{}", edited.text);
    assert!(
        edited.text.contains("Successfully edited 1 token."),
        "{}",
        edited.text
    );
    let after = home.run(&["show", "tokens", "--nopretty"]);
    assert!(
        after.text.contains("\"label\": \"mytokrenamed\""),
        "{}",
        after.text
    );
    assert!(
        !after.text.contains("\"label\": \"mytok\""),
        "{}",
        after.text
    );

    let missing = home.run(&["edit", "tokens"]);
    assert_eq!(missing.code, 1);
    assert!(
        missing
            .text
            .contains("Provide token labels or IDs for the tokens to edit")
    );
}

/// The output with its line wrapping undone.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The `scopes` of the token with `label` in `show tokens --nopretty`.
fn token_scopes(home: &Home, label: &str) -> String {
    let raw = home.run(&["show", "tokens", "--nopretty"]);
    let line = raw
        .text
        .lines()
        .find(|l| l.contains(&format!("\"label\": \"{label}\"")))
        .unwrap_or_else(|| panic!("no token {label}: {}", raw.text));
    let start = line.find("\"scopes\": [").expect("scopes") + "\"scopes\": ".len();
    let end = start + line[start..].find(']').expect("end of scopes") + 1;
    line[start..end].to_string()
}

#[test]
fn a_star_scope_is_stored_literally_for_tokens_and_users() {
    let home = Home::new();
    // The owner holds `*`: a token carries at most its owner's scopes.
    let user = home.ask(
        &["register", "users", "bob", "--scopes", "*"],
        "password1\npassword1\n\n",
    );
    assert_eq!(user.code, 0, "{}", user.text);

    // `register tokens --scopes '*'` keeps the wildcard; custom (plugin) scope names are
    // accepted as given.
    let star = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "everything",
            "--scopes",
            "*",
        ],
        "1\n",
    );
    assert_eq!(star.code, 0, "{}", star.text);
    let custom = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "custom",
            "--scopes",
            "myplugin:read",
            "myplugin:write",
        ],
        "1\n",
    );
    assert_eq!(custom.code, 0, "{}", custom.text);
    assert_eq!(token_scopes(&home, "everything"), r#"["*"]"#);
    assert_eq!(
        token_scopes(&home, "custom"),
        r#"["myplugin:read", "myplugin:write"]"#
    );

    // `edit tokens` prefills the stored scopes: keeping them keeps `*` (v4 expanded it to the
    // built-in list), and typing `*` over a custom list stores `*`. The prompt names the
    // semantics.
    let kept = home.ask(&["edit", "tokens", "everything"], "\n\n\nn\n");
    assert_eq!(kept.code, 0, "{}", kept.text);
    assert!(
        one_line(&kept.text).contains("`*` grants all scopes, including custom and future scopes"),
        "{}",
        kept.text
    );
    assert_eq!(token_scopes(&home, "everything"), r#"["*"]"#);
    let kept = home.ask(&["edit", "tokens", "custom"], "\n\n\nn\n");
    assert_eq!(kept.code, 0, "{}", kept.text);
    assert_eq!(
        token_scopes(&home, "custom"),
        r#"["myplugin:read", "myplugin:write"]"#
    );

    // `register users --scopes '*'` and `edit users` keep the wildcard in the user's attributes.
    let scoped = home.ask(
        &["register", "users", "carol", "--scopes", "*"],
        "password1\npassword1\n\n",
    );
    assert_eq!(scoped.code, 0, "{}", scoped.text);
    let edited = home.ask(&["edit", "users", "bob"], "n\nn\nn\n*\n");
    assert_eq!(edited.code, 0, "{}", edited.text);
    assert!(
        one_line(&edited.text)
            .contains("`*` grants all scopes, including custom and future scopes"),
        "{}",
        edited.text
    );
    for user in ["bob", "carol"] {
        let row = home.run(&[
            "sql",
            "local",
            &format!("SELECT attributes FROM mrsm_users WHERE username = '{user}'"),
            "--nopretty",
        ]);
        assert!(
            row.text.replace('\\', "").contains(r#""scopes": ["*"]"#),
            "{user}: {}",
            row.text
        );
    }
}

#[test]
fn a_token_carries_at_most_its_owners_scopes() {
    let home = Home::new();
    // `ron` holds three scopes (one custom); `ada` is an administrator: `edit users` makes her one.
    let ron = home.ask(
        &[
            "register",
            "users",
            "ron",
            "--scopes",
            "pipes:read",
            "jobs:read",
            "myplugin:read",
        ],
        "password1\npassword1\n\n",
    );
    assert_eq!(ron.code, 0, "{}", ron.text);
    let ada = home.ask(&["register", "users", "ada"], "password1\npassword1\n\n");
    assert_eq!(ada.code, 0, "{}", ada.text);
    let admin = home.ask(&["edit", "users", "ada"], "n\ny\ny\nn\n\n");
    assert_eq!(admin.code, 0, "{}", admin.text);

    // The owner is the user chosen at the prompt: `1` is `ada`, `2` is `ron` (sorted).
    // A scope the owner holds, or none at all (the owner's own), is fine.
    let ok = home.ask(
        &[
            "register",
            "tokens",
            "--name",
            "own",
            "--scopes",
            "pipes:read",
            "myplugin:read",
        ],
        "2\n",
    );
    assert_eq!(ok.code, 0, "{}", ok.text);
    let default = home.ask(&["register", "tokens", "--name", "default"], "2\n");
    assert_eq!(default.code, 0, "{}", default.text);
    assert_eq!(
        token_scopes(&home, "default"),
        r#"["pipes:read", "jobs:read", "myplugin:read"]"#
    );

    // Anything beyond them is refused, naming every disallowed scope, and nothing is registered.
    for (scopes, named) in [
        (vec!["pipes:write"], vec!["pipes:write"]),
        (vec!["*"], vec!["*"]),
        (
            vec!["jobs:read", "other:read", "pipes:drop"],
            vec!["other:read", "pipes:drop"],
        ),
    ] {
        let mut args = vec!["register", "tokens", "--name", "denied", "--scopes"];
        args.extend(scopes.iter().copied());
        let refused = home.ask(&args, "2\n");
        assert_eq!(refused.code, 1, "{}", refused.text);
        let text = one_line(&refused.text);
        assert!(
            text.contains("Cannot grant scopes that user 'ron' does not hold"),
            "{text}"
        );
        for scope in named {
            assert!(text.contains(&format!("'{scope}'")), "{text}");
        }
    }
    assert!(
        !home
            .run(&["show", "tokens", "--nopretty"])
            .text
            .contains("\"denied\""),
    );

    // `edit tokens` applies the cap to the token's owner: keeping scopes works, adding one fails.
    let kept = home.ask(&["edit", "tokens", "own"], "\n\n\nn\n");
    assert_eq!(kept.code, 0, "{}", kept.text);
    let widened = home.ask(&["edit", "tokens", "own"], "\n\n pipes:write\nn\n");
    assert_eq!(widened.code, 1, "{}", widened.text);
    assert!(
        one_line(&widened.text).contains("'pipes:write'"),
        "{}",
        widened.text
    );
    assert_eq!(
        token_scopes(&home, "own"),
        r#"["pipes:read", "myplugin:read"]"#
    );

    // An administrator, and the holder of `*` (see the test above), may grant anything.
    for (name, scopes) in [
        ("grand-star", vec!["*"]),
        ("grand-custom", vec!["brand:new", "pipes:write"]),
    ] {
        let mut args = vec!["register", "tokens", "--name", name, "--scopes"];
        args.extend(scopes.iter().copied());
        let granted = home.ask(&args, "1\n");
        assert_eq!(granted.code, 0, "{scopes:?}: {}", granted.text);
    }
    assert_eq!(token_scopes(&home, "grand-star"), r#"["*"]"#);
}

#[test]
fn bootstrap_connectors_wizard_registers_the_connector() {
    let home = Home::new();
    let database = home.path().join("wizard.db");
    // Type 2 (`sql`), the label, flavor 11 (`sqlite`), the database, then confirm.
    let input = format!("2\nwizard\n11\n{}\ny\n", database.display());
    let out = home.ask(&["bootstrap", "connectors"], &input);
    assert_eq!(out.code, 0, "{}", out.text);
    assert!(
        out.text.contains("Please choose a connector type."),
        "{}",
        out.text
    );
    assert!(out.text.contains("New label for 'sql' connector:"));
    assert!(
        out.text
            .contains("Are you ok with these new attributes for connector 'sql:wizard'?")
    );
    let config = home.config_text();
    assert!(config.contains("wizard"), "{config}");
    assert!(config.contains("wizard.db"), "{config}");
    let shown = home.run(&["show", "connectors", "sql:wizard"]);
    assert!(shown.text.contains("wizard.db"), "{}", shown.text);

    // The end of input is an `EOFError`, as in v4.
    let eof = home.run(&["bootstrap", "connectors"]);
    assert_eq!(eof.code, 1, "{}", eof.text);
    assert!(eof.text.contains("EOFError"), "{}", eof.text);

    // An existing label asks about overwriting.
    let again = home.ask(&["bootstrap", "connectors"], "2\nwizard\nn\n");
    assert!(
        again
            .text
            .contains("Connector 'sql:wizard' already exists."),
        "{}",
        again.text
    );
    assert!(
        again
            .text
            .contains("Do you want to overwrite connector 'sql:wizard'?"),
        "{}",
        again.text
    );
}

#[test]
fn bootstrap_pipes_registers_the_pipe() {
    let home = Home::new();
    let out = home.ask(
        &["bootstrap", "pipes"],
        "sql:local\nmetric1\nloc1\ndt\nid\nn\n\n\n\nn\nn\n",
    );
    assert_eq!(out.code, 0, "{}", out.text);
    assert!(
        out.text.contains("Where are the data coming from?"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("What kind of data is this?"),
        "{}",
        out.text
    );
    assert!(
        out.text
            .contains("Finished bootstrapping Pipe('sql:local', 'metric1', 'loc1'"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("(1 succeeded, 0 failed)."),
        "{}",
        out.text
    );
    let shown = home.run(&["show", "pipes", "-m", "metric1", "--nopretty"]);
    assert!(
        shown.text.contains("\"datetime\": \"dt\""),
        "{}",
        shown.text
    );
    assert!(shown.text.contains("\"id\": \"id\""), "{}", shown.text);

    let eof = home.run(&["bootstrap", "pipes"]);
    assert_eq!(eof.code, 1, "{}", eof.text);
    assert!(eof.text.contains("EOFError"), "{}", eof.text);

    let job = home.run(&["bootstrap", "jobs"]);
    assert_eq!(job.code, 1, "{}", job.text);
    assert!(
        job.text
            .contains("What is the name of the job you'd like to create?"),
        "{}",
        job.text
    );
    assert!(job.text.contains("EOFError"), "{}", job.text);
}

#[test]
fn edit_config_rollback_restores_the_previous_file() {
    let home = Home::new();
    let editor = home.path().join("editor.sh");
    std::fs::write(
        &editor,
        "#!/bin/sh\nsed -i 's/\"max_daemons\": 120/\"max_daemons\": 77/' \"$1\"\n",
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&editor, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut command = home.command();
    command.env("EDITOR", &editor);
    let edited = home.run_with(command, &["edit", "config", "system"], "\n");
    assert_eq!(edited.code, 0, "{}", edited.text);
    let path = home.path().join("config").join("system.json");
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .contains("\"max_daemons\": 77")
    );

    let none = home.run_in(&["edit", "config", "pipes", "--rollback"], "");
    assert_eq!(none.code, 1, "{}", none.text);

    let rolled = home.run_in(&["edit", "config", "system", "--rollback"], "y\n");
    assert_eq!(rolled.code, 0, "{}", rolled.text);
    assert!(
        rolled.text.contains("Restored 'system' from backup"),
        "{}",
        rolled.text
    );
    assert!(
        rolled.text.contains("Rolled back 1 config key(s)."),
        "{}",
        rolled.text
    );
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .contains("\"max_daemons\": 120")
    );

    // `--noask` restores without asking, and a key without backups says so.
    let quiet = home.run_in(&["edit", "config", "system", "--rollback", "--noask"], "");
    assert_eq!(quiet.code, 0, "{}", quiet.text);
    let unknown = home.run_in(&["edit", "config", "shell", "--rollback", "--noask"], "");
    assert_eq!(unknown.code, 1, "{}", unknown.text);
    assert!(
        unknown
            .text
            .contains("No configuration backups found for 'shell'."),
        "{}",
        unknown.text
    );
}

#[test]
fn start_gui_without_the_gui_extra_says_what_to_install() {
    let home = Home::new();
    let out = home.run(&["start", "gui"]);
    assert_eq!(out.code, 1, "{}", out.text);
    assert!(out.text.contains("`gui` extra"), "{}", out.text);
    no_worker(&out);
}

// --- the loopback API -------------------------------------------------------------------------

fn free_port() -> u16 {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    port
}

/// A server started with `<args>` over a temporary root, stopped when dropped.
struct Server {
    home: Home,
    child: Child,
    port: u16,
}

impl Server {
    fn start(prefix: &[&str]) -> Server {
        let home = Home::new();
        let port = free_port();
        let mut command = home.command();
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        command
            .args(["--noask"])
            .args(prefix)
            .args(["-i", "sql:local", "--host", "127.0.0.1", "-p"])
            .arg(port.to_string())
            .args(["--no-dash", "-w", "2"]);
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "the server exited before it listened"
            );
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "the server did not listen");
            std::thread::sleep(Duration::from_millis(50));
        }
        Server { home, child, port }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &format!("-{}", self.child.id())])
            .stderr(Stdio::null())
            .status();
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn config_file(root: &Path) -> PathBuf {
    root.join("config").join("meerschaum.yaml")
}

#[test]
fn login_prompts_for_credentials_and_saves_them() {
    let server = Server::start(&["start", "api"]);
    let registered = server
        .home
        .ask(&["register", "users", "alice"], "password1\npassword1\n\n");
    assert_eq!(registered.code, 0, "{}", registered.text);

    let client = Home::new().with_endpoint("srv", server.port);
    // Wrong password: asked whether to try again; declining fails.
    let wrong = client.run_in(&["login", "api:srv"], "alice\nnotthepassword\nn\n");
    no_worker(&wrong);
    assert_eq!(wrong.code, 1, "{}", wrong.text);
    assert!(
        wrong.text.contains("Logged into 0 connectors."),
        "{}",
        wrong.text
    );
    assert!(
        wrong
            .text
            .contains("Would you like to try different login credentials for connector 'api:srv'?"),
        "{}",
        wrong.text
    );

    // Right password: logs in and offers to save the credentials.
    let right = client.run_in(&["login", "api:srv"], "alice\npassword1\ny\n");
    assert_eq!(right.code, 0, "{}", right.text);
    assert!(
        right.text.contains("Username for connector 'api:srv':"),
        "{}",
        right.text
    );
    assert!(
        right.text.contains("Logged into 1 connector."),
        "{}",
        right.text
    );
    assert!(
        right
            .text
            .contains("Would you like to save the new credentials for connector 'api:srv'?"),
        "{}",
        right.text
    );
    let saved = std::fs::read_to_string(config_file(client.path())).unwrap_or_default();
    assert!(saved.contains("alice"), "{saved}\n{}", client.config_text());

    // Not an API connector.
    let other = client.run_in(&["login", "sql:local"], "");
    assert_eq!(other.code, 1);
    assert!(
        other
            .text
            .contains("Connector 'sql:local' is not an API connector. Skipping...")
    );
}

#[test]
fn api_start_is_an_alias_of_start_api() {
    let server = Server::start(&["api", "start", "--no-auth"]);
    let mut stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    stream
        .write_all(b"GET /healthcheck HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    std::io::Read::read_to_string(&mut stream, &mut response).unwrap();
    assert!(
        response.starts_with("HTTP/1.0 200") || response.starts_with("HTTP/1.1 200"),
        "{response}"
    );
}

#[test]
fn start_gui_serves_on_loopback_and_stops_the_server_when_the_window_closes() {
    // A stand-in interpreter: `import webview` succeeds; the window script records its URL,
    // checks that the server answers on that port, and exits.
    let home = Home::new();
    let fake = home.path().join("fake-python");
    let record = home.path().join("window.txt");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/bash\nif [ \"$2\" = \"import webview\" ]; then exit 0; fi\n\
             url=\"$3\"; port=\"${{url##*:}}\"; port=\"${{port%%/*}}\"\n\
             if (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null; then up=listening; else up=closed; fi\n\
             echo \"$url $up\" > {}\nexit 0\n",
            record.display()
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut command = home.command();
    command.env("MRSM_PYTHON", &fake);
    let out = home.run_with(command, &["-i", "sql:local", "start", "gui"], "");
    assert_eq!(out.code, 0, "{}", out.text);
    let seen = std::fs::read_to_string(&record).expect("the window ran");
    let (url, state) = seen.trim().split_once(' ').unwrap();
    assert!(
        url.starts_with("http://127.0.0.1:") && url.ends_with("/dash/"),
        "{seen}"
    );
    assert_eq!(state, "listening", "{seen}");
    // The server is gone once the window is closed.
    let port: u16 = url
        .trim_start_matches("http://127.0.0.1:")
        .trim_end_matches("/dash/")
        .parse()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_ok() {
        assert!(
            Instant::now() < deadline,
            "the GUI's server is still running"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn the_cli_daemons_are_not_used_in_v5() {
    let home = Home::new();
    let shown = home.run(&["show", "daemons"]);
    assert_eq!(shown.code, 0, "{}", shown.text);
    assert!(
        shown.text.contains("No CLI daemons are running."),
        "{}",
        shown.text
    );
    for action in ["start", "restart"] {
        let out = home.run(&[action, "daemons"]);
        assert_eq!(out.code, 1, "{}", out.text);
        assert!(
            out.text
                .contains("The CLI daemons are not used in Meerschaum v5"),
            "{}",
            out.text
        );
    }
    // With no workers, v4 only recreates the CLI resources directory.
    let stopped = home.run(&["stop", "daemons"]);
    assert_eq!(stopped.code, 0, "{}", stopped.text);
    assert!(
        home.path().join(".internal").join("cli").is_dir(),
        "no CLI resources directory"
    );
}

#[test]
fn a_pipeline_with_a_timeout_runs_in_a_child_and_is_killed_when_late() {
    let home = Home::new();
    let fine = home.run(&[
        "show",
        "version",
        "--nopretty",
        ":",
        "--timeout-seconds",
        "30",
    ]);
    assert_eq!(fine.code, 0, "{}", fine.text);
    assert!(fine.text.contains("5.0.0"), "{}", fine.text);
    assert!(!fine.text.contains("MRSM_RESULT"), "{}", fine.text);

    let failed = home.run(&["os", "false", ":", "--timeout-seconds", "30"]);
    assert_eq!(failed.code, 1, "{}", failed.text);

    // The step's children are killed with it (v4: `os.setsid` + `os.killpg`).
    let pidfile = home.path().join("grandchild.pid");
    let script = home.path().join("step.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nsleep 30 &\necho $! > {}\nwait\n",
            pidfile.display()
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let script = script.display().to_string();
    let started = Instant::now();
    let late = home.run(&["os", &script, ":", "--timeout-seconds", "2"]);
    assert_eq!(late.code, 1, "{}", late.text);
    assert!(
        late.text
            .contains("Failed to execute pipeline within 2.0 seconds."),
        "{}",
        late.text
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the step was not killed"
    );
    let grandchild: u32 = std::fs::read_to_string(&pidfile)
        .expect("the step started its child")
        .trim()
        .parse()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Path::new(&format!("/proc/{grandchild}")).exists() {
        let state = std::fs::read_to_string(format!("/proc/{grandchild}/stat")).unwrap_or_default();
        // A zombie awaiting its reaper is already dead.
        if state.contains(") Z ") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a grandchild of the step survived the timeout"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn start_connectors_retries_a_connection_that_comes_up_late() {
    let port = free_port();
    let listener = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1800));
        let socket = TcpListener::bind(("127.0.0.1", port)).unwrap();
        // Answer every request with an empty JSON body until the test ends.
        let deadline = Instant::now() + Duration::from_secs(20);
        socket.set_nonblocking(true).unwrap();
        while Instant::now() < deadline {
            match socket.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0u8; 2048];
                    let _ = std::io::Read::read(&mut stream, &mut buffer);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                    );
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
            if DONE.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }
    });
    let home = Home::new().with_endpoint("late", port);
    let out = home.run_in(
        &[
            "start",
            "connectors",
            "api:late",
            "--min-seconds",
            "1",
            "--noask",
        ],
        "",
    );
    DONE.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = listener.join();
    assert_eq!(out.code, 0, "{}", out.text);
    assert!(
        out.text.contains("Connection to 'api:late' failed."),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed connection attempt (1 / 50)"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Connected to 'api:late'."),
        "{}",
        out.text
    );
    assert!(
        out.text
            .contains("Successfully started connector 'api:late'."),
        "{}",
        out.text
    );
}

static DONE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[test]
fn start_connectors_gives_up_after_the_retries_on_a_port_that_never_opens() {
    let port = free_port();
    let home = Home::new().with_env(
        "MRSM_SQL_DEAD",
        format!("postgresql://user:pass@127.0.0.1:{port}/db"),
    );
    let out = home.run_in(
        &[
            "start",
            "connectors",
            "sql:dead",
            "--min-seconds",
            "0",
            "--noask",
        ],
        "",
    );
    no_worker(&out);
    assert_eq!(out.code, 1, "{}", out.text);
    // The keys are valid: the failure is the connection, retried every time.
    assert!(!out.text.contains("Invalid connector keys"), "{}", out.text);
    assert!(
        out.text.contains("Failed connection attempt (1 / 50)"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed connection attempt (50 / 50)"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed to start 1 connectors."),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed to start connector 'sql:dead'."),
        "{}",
        out.text
    );
}

#[test]
fn sql_passes_the_password_in_the_environment_not_on_the_command_line() {
    let bin = tempfile::tempdir().unwrap();
    for (program, variable) in [("pgcli", "PGPASSWORD"), ("mycli", "MYSQL_PWD")] {
        let path = bin.path().join(program);
        std::fs::write(
            &path,
            format!("#!/bin/sh\necho \"ARGV: $@\"\necho \"{variable}=${variable}\"\n"),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let path = format!(
        "{}:{}",
        bin.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let home = Home::new()
        .with_env("PATH", path)
        .with_env("MRSM_SQL_PG", "postgresql://alice:s%40cret@127.0.0.1:1/db")
        .with_env("MRSM_SQL_MY", "mysql://bob:p%2Bw@127.0.0.1:1/db");
    // The databases are down: the client is started anyway and reports its own errors.
    let pg = home.run(&["sql", "pg"]);
    assert!(
        pg.text.contains("ARGV: postgresql://alice@127.0.0.1:1/db"),
        "{}",
        pg.text
    );
    assert!(pg.text.contains("PGPASSWORD=s@cret"), "{}", pg.text);
    assert!(
        !pg.text.contains("ARGV: postgresql://alice:"),
        "{}",
        pg.text
    );
    let my = home.run(&["sql", "my"]);
    assert!(
        my.text.contains("ARGV: mysql://bob@127.0.0.1:1/db"),
        "{}",
        my.text
    );
    assert!(my.text.contains("MYSQL_PWD=p+w"), "{}", my.text);
}

#[test]
fn api_prints_the_remote_result_once_like_v4() {
    // v4's job daemon runs `entry()` and stores the tuple: the job's log has the action's own
    // output only, and the client prints the result tuple once.
    let server = Server::start(&["start", "api", "--no-auth"]);
    let client = Home::new().with_endpoint("srv", server.port);
    let failed = client.run_in(&["api", "srv", "sync", "pipes", "-m", "nonexistent"], "");
    assert_eq!(failed.code, 1, "{}", failed.text);
    assert_eq!(
        failed.text.matches("(0 succeeded, 0 failed)").count(),
        1,
        "{}",
        failed.text
    );
    let ok = client.run_in(&["api", "srv", "os", "echo", "hi"], "");
    assert_eq!(ok.code, 0, "{}", ok.text);
    assert_eq!(ok.text.matches("hi").count(), 2, "{}", ok.text);
    assert_eq!(ok.text.matches('\u{1f389}').count(), 1, "{}", ok.text);
}

#[test]
fn a_jobs_log_ends_with_its_result_like_v4() {
    // `Daemon._run_exit` finishes every job log with `mrsm.pprint(result)`: a failed job's
    // message is in `show logs`.
    let home = Home::new();
    let script = home.path().join("fail.sh");
    std::fs::write(&script, "#!/bin/sh\necho oops >&2\nexit 3\n").unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let script = script.display().to_string();
    let started = home.run_in(&["os", &script, "-d", "--name", "failjob", "--noask"], "");
    assert_eq!(started.code, 0, "{}", started.text);
    let deadline = Instant::now() + Duration::from_secs(20);
    let logs = loop {
        let logs = home.run_in(&["show", "logs", "failjob", "--nopretty"], "");
        if logs.text.contains("oops") || Instant::now() > deadline {
            break logs;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(logs.text.contains("oops"), "{}", logs.text);
    // The result block (icon, message) is in the log.
    assert!(logs.text.contains(" \u{1f4a2} oops"), "{}", logs.text);
}

#[test]
fn a_remote_job_without_an_action_fails_instead_of_opening_a_shell() {
    // `api srv` with nothing to run sends sysargs that end up as `mrsm -y`.
    let server = Server::start(&["start", "api", "--no-auth"]);
    let client = Home::new().with_endpoint("srv", server.port);
    let started = Instant::now();
    let out = client.run_in(&["api", "srv"], "");
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the job is waiting on a shell"
    );
    assert_eq!(out.code, 1, "{}", out.text);
    assert!(out.text.contains("no action to run"), "{}", out.text);
    // Nothing is left running on the server's root.
    let jobs = server.home.run(&["show", "jobs", "--nopretty"]);
    assert!(!jobs.text.contains(".api-temp"), "{}", jobs.text);
}

#[test]
fn a_timed_pipeline_step_that_reads_the_terminal_is_not_stopped() {
    use portable_pty::{CommandBuilder, PtySize, native_pty_system};
    // A step that prompts, run from a terminal: in a background process group of the same session
    // it would get SIGTTIN and sit stopped. v4's `os.setsid` gives it a session of its own.
    let home = Home::new();
    let pidfile = home.path().join("reader.pid");
    let script = home.path().join("reader.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho $$ > {}\nread answer\necho got\n",
            pidfile.display()
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 100,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_mrsm"));
    cmd.env_clear();
    cmd.env("PATH", std::env::var("PATH").unwrap_or_default());
    cmd.env("HOME", home.path());
    cmd.env("MRSM_ROOT_DIR", home.path());
    cmd.env("MRSM_PLUGINS_DIR", home.plugins.path());
    cmd.env("MRSM_PYTHON", home.path().join("no-python"));
    cmd.args([
        "os",
        &script.display().to_string(),
        ":",
        "--timeout-seconds",
        "4",
    ]);
    let mut child = pair.slave.spawn_command(cmd).unwrap();
    drop(pair.slave);
    let mut reader = pair.master.try_clone_reader().unwrap();
    let _writer = pair.master.take_writer().unwrap();
    let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let sink = output.clone();
    std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        while let Ok(n) = std::io::Read::read(&mut reader, &mut buffer) {
            if n == 0 {
                break;
            }
            sink.lock().unwrap().extend_from_slice(&buffer[..n]);
        }
    });
    let started = Instant::now();
    let deadline = started + Duration::from_secs(20);
    while !pidfile.exists() {
        assert!(Instant::now() < deadline, "the step never started");
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(1000));
    let pid: u32 = std::fs::read_to_string(&pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let state = stat
        .rsplit(") ")
        .next()
        .and_then(|s| s.chars().next())
        .unwrap_or('?');
    assert_ne!(state, 'T', "the step was stopped by the terminal: {stat}");
    // It is killed at the deadline, not left waiting.
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "the pipeline did not end");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "ended after {:?}",
        started.elapsed()
    );
    std::thread::sleep(Duration::from_millis(200));
    let text = strip_ansi(&String::from_utf8_lossy(&output.lock().unwrap()));
    assert!(
        text.contains("Failed to execute pipeline within 4.0 seconds."),
        "{text}"
    );
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists()
            || std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .unwrap_or_default()
                .contains(") Z "),
        "the step survived"
    );
}
