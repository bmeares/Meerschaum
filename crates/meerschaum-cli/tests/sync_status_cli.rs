//! The per-pipe sync status (rc15 contract C2) from the command line: `sync pipes` records each
//! pipe's outcome (from a job, with the job's name), `copy pipes` records the copy on the
//! destination, and `show pipes` ends with a line for each pipe whose latest sync failed.
//!
//! Every run uses a temporary root, an empty plugins directory, SQLite instances (`sql:local` and
//! `sql:dest`) and `MRSM_PYTHON` pointing at nothing. The data is synthetic.
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
}

impl Home {
    fn new() -> Home {
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        }
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    fn command(&self, env: &[(&str, &str)]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.path())
            .env("MRSM_ROOT_DIR", self.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_PYTHON", self.path().join("no-python"))
            .env(
                "MRSM_SQL_DEST",
                format!("sqlite:///{}", self.path().join("dest.db").display()),
            );
        for (key, value) in env {
            command.env(key, value);
        }
        command
    }

    /// `mrsm -i sql:local <args>` with extra environment variables: stdout and stderr, without
    /// ANSI sequences.
    fn run_with(&self, env: &[(&str, &str)], args: &[&str]) -> Out {
        let output = self
            .command(env)
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

    fn run(&self, args: &[&str]) -> Out {
        self.run_with(&[], args)
    }

    /// `mrsm sql <label> <query>`: its output.
    fn sql(&self, label: &str, query: &str) -> String {
        let out = self.run(&["sql", label, query]);
        assert_eq!(out.code, 0, "{query}\n{}", out.text);
        out.text
    }

    /// A pipe `metric` on `sql:local` over `definition`.
    fn register(&self, metric: &str, definition: &str) {
        let registered = self.run(&[
            "register",
            "pipes",
            "-c",
            "sql:local",
            "-m",
            metric,
            "--params",
            &format!(
                r#"{{"columns":{{"datetime":"dt"}},"fetch":{{"definition":"{definition}"}}}}"#
            ),
        ]);
        assert_eq!(registered.code, 0, "{}", registered.text);
    }

    /// The stored sync statuses on `label`, one `pipe_ref source job rows_inserted` line each.
    fn statuses(&self, label: &str) -> String {
        self.sql(
            label,
            "SELECT pipe_ref || ' ' || source || ' ' || COALESCE(job, '-') || ' ' || \
             rows_inserted AS s FROM mrsm_engine_sync_status",
        )
    }
}

fn strip_ansi(text: &str) -> String {
    regex::Regex::new("\x1b\\[[0-9;]*[A-Za-z]")
        .unwrap()
        .replace_all(text, "")
        .into_owned()
}

fn seed(home: &Home) {
    home.sql("local", "CREATE TABLE src_ok (dt text, v real)");
    home.sql(
        "local",
        "INSERT INTO src_ok VALUES ('2024-01-01 00:00:00', 1.5), ('2024-01-01 01:00:00', 2.5)",
    );
}

#[test]
fn show_pipes_names_the_pipes_whose_latest_sync_failed() {
    let home = Home::new();
    seed(&home);
    home.register("ok", "SELECT * FROM src_ok");
    home.register("broken", "SELECT * FROM src_missing");
    let synced = home.run(&["sync", "pipes", "-c", "sql:local", "-m", "ok"]);
    assert_eq!(synced.code, 0, "{}", synced.text);
    for _ in 0..2 {
        home.run(&["sync", "pipes", "-c", "sql:local", "-m", "broken"]);
    }

    let shown = home.run(&["show", "pipes", "-c", "sql:local"]);
    assert_eq!(shown.code, 0, "{}", shown.text);
    let notes: Vec<&str> = shown
        .text
        .lines()
        .filter(|l| l.contains(" failed its last"))
        .collect();
    assert_eq!(notes.len(), 1, "{}", shown.text);
    assert!(
        notes[0].contains("'broken'") && notes[0].contains("failed its last 2 syncs (latest "),
        "{}",
        notes[0]
    );

    // One pipe on its own says it too; a healthy pipe and `--nopretty` print no note.
    let one = home.run(&["show", "pipes", "-c", "sql:local", "-m", "broken"]);
    assert!(one.text.contains("failed its last 2 syncs"), "{}", one.text);
    for args in [
        &["show", "pipes", "-c", "sql:local", "-m", "ok"][..],
        &["show", "pipes", "-c", "sql:local", "--nopretty"][..],
    ] {
        let out = home.run(args);
        assert_eq!(out.code, 0, "{}", out.text);
        assert!(!out.text.contains(" failed its last"), "{}", out.text);
    }
}

#[test]
fn the_command_line_jobs_and_copies_record_their_syncs() {
    let home = Home::new();
    seed(&home);
    home.register("ok", "SELECT * FROM src_ok");
    home.register("in_a_job", "SELECT * FROM src_ok");
    let synced = home.run(&["sync", "pipes", "-c", "sql:local", "-m", "ok"]);
    assert_eq!(synced.code, 0, "{}", synced.text);
    let in_job = home.run_with(
        &[("MRSM_DAEMON_ID", "rc15-status-job")],
        &["sync", "pipes", "-c", "sql:local", "-m", "in_a_job"],
    );
    assert_eq!(in_job.code, 0, "{}", in_job.text);
    let local = home.statuses("local");
    assert!(
        local.contains("sql:local|sql:local|ok|None cli - 2"),
        "{local}"
    );
    assert!(
        local.contains("sql:local|sql:local|in_a_job|None job rc15-status-job 2"),
        "{local}"
    );

    // `copy pipes` syncs the copied rows into the new pipe: that sync is recorded too.
    let copied = home.run(&[
        "copy",
        "pipes",
        "sql:dest",
        "-c",
        "sql:local",
        "-m",
        "ok",
        "--sync-data",
        "all",
        "-y",
    ]);
    assert_eq!(copied.code, 0, "{}", copied.text);
    let dest = home.statuses("dest");
    assert!(
        dest.contains("sql:dest|sql:local|ok|None cli - 2"),
        "{dest}"
    );
}
