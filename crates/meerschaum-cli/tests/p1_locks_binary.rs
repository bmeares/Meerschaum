//! Locks through the `mrsm` binary (rc15 wave 2, AC5): every write a lock names is refused on
//! the command line, whatever the flags (`-y`, `--force`, `--allow-protected`), with the message
//! that names the pipe, the lock and the `mrsm unlock pipes` command; the writes a lock does not
//! name still run, and unlocking restores them. A Pipeline run skips a sync-locked member as
//! **locked** without failing.
//!
//! The instance is a SQLite file in a temporary root; the data is synthetic.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use meerschaum_connector::DataQuery;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::Pipe;

struct Home {
    _dir: tempfile::TempDir,
    root: PathBuf,
    db: PathBuf,
}

impl Home {
    fn new() -> Home {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        Home {
            db: dir.path().join("p1.db"),
            root,
            _dir: dir,
        }
    }

    /// `mrsm <args>`: the exit code and stdout followed by stderr.
    fn mrsm(&self, args: &[&str]) -> (i32, String) {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &self.root)
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", self.root.join("plugins"))
            .env("MRSM_SQL_P1", format!("sqlite:///{}", self.db.display()))
            .env("MRSM_PYTHON", self.root.join("no-python"))
            .env("NO_COLOR", "1")
            .args(args)
            .arg("-i")
            .arg("sql:p1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        // Nothing to answer: a prompt reads end-of-file.
        child.stdin.take().unwrap().write_all(b"").unwrap();
        let output = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        (output.status.code().unwrap_or(-1), text)
    }

    fn ok(&self, args: &[&str]) -> String {
        let (code, text) = self.mrsm(args);
        assert_eq!(code, 0, "{args:?}:\n{text}");
        text
    }

    /// `args` is refused by the lock `lock` (named with the unlock command), whatever its exit
    /// code.
    fn refused(&self, args: &[&str], lock: &str) -> String {
        let (_, text) = self.mrsm(args);
        let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            flat.contains(&format!("it is locked ({lock}")),
            "{args:?} was not refused by `{lock}`:\n{text}"
        );
        assert!(flat.contains("mrsm unlock pipes"), "{args:?}:\n{text}");
        assert!(flat.contains("Pipe('sql:p1', 'copy'"), "{args:?}:\n{text}");
        text
    }

    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    async fn conn(&self) -> SqlConnector {
        SqlConnector::from_uri("p1", &format!("sqlite:///{}", self.db.display()))
            .await
            .unwrap()
    }

    fn exec(&self, sql: &str) {
        self.block_on(async {
            assert!(self.conn().await.exec(sql).await.is_some(), "failed: {sql}");
        });
    }

    fn rowcount(&self, metric: &str) -> i64 {
        self.block_on(async {
            let pipe = Pipe::builder("sql:p1", metric)
                .instance("sql:p1")
                .build()
                .unwrap();
            self.conn()
                .await
                .get_pipe_rowcount(&pipe, &DataQuery::default())
                .await
                .unwrap()
                .unwrap_or(0)
        })
    }

    fn registered(&self, metric: &str) -> bool {
        self.block_on(async {
            let pipe = Pipe::builder("sql:p1", metric)
                .instance("sql:p1")
                .build()
                .unwrap();
            self.conn()
                .await
                .get_pipe_id(&pipe)
                .await
                .unwrap()
                .is_some()
        })
    }
}

const COPY: &[&str] = &["-c", "sql:p1", "-m", "copy"];

fn with<'a>(head: &[&'a str], tail: &[&'a str]) -> Vec<&'a str> {
    let mut out = head.to_vec();
    out.extend_from_slice(COPY);
    out.extend_from_slice(tail);
    out
}

#[test]
fn every_locked_write_is_refused_on_the_command_line() {
    let home = Home::new();
    home.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)");
    home.exec(
        "INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0), ('2026-01-01 01:00:00', 1, 2.0)",
    );
    home.ok(&with(
        &["register", "pipes"],
        &[
            "--params",
            r#"{"fetch": {"definition": "SELECT ts, id, v FROM src"}, "columns": {"datetime": "ts", "id": "id"}, "dtypes": {"ts": "datetime", "id": "int", "v": "float"}}"#,
        ],
    ));
    home.ok(&with(&["sync", "pipes"], &[]));
    assert_eq!(home.rowcount("copy"), 2);
    home.exec("INSERT INTO src VALUES ('2026-01-01 02:00:00', 1, 3.0)");

    // `all`: every write, whatever the flags.
    home.ok(&with(&["lock", "pipes"], &[]));
    home.refused(&with(&["sync", "pipes"], &[]), "all");
    home.refused(&with(&["sync", "pipes"], &["--replace", "-y"]), "all");
    home.refused(&with(&["clear", "pipes"], &["-y", "--force"]), "all");
    home.refused(
        &with(&["drop", "pipes"], &["-y", "--force", "--allow-protected"]),
        "all",
    );
    home.refused(&with(&["delete", "pipes"], &["-y", "-f"]), "all");
    home.refused(&with(&["verify", "pipes"], &["-y"]), "all");
    home.refused(&with(&["deduplicate", "pipes"], &["-y"]), "all");
    home.refused(
        &with(
            &["edit", "pipes"],
            &["--params", r#"{"tags": ["edited"]}"#, "-y"],
        ),
        "all",
    );
    assert_eq!(home.rowcount("copy"), 2);
    assert!(home.registered("copy"));

    // One kind at a time: only its writes are refused.
    home.ok(&with(&["unlock", "pipes"], &[]));
    home.ok(&with(&["lock", "pipes"], &["--locks", "sync"]));
    home.refused(&with(&["sync", "pipes"], &[]), "sync");
    home.refused(&with(&["verify", "pipes"], &["-y"]), "sync");
    home.refused(&with(&["deduplicate", "pipes"], &["-y"]), "sync");
    home.ok(&with(
        &["edit", "pipes"],
        &["--params", r#"{"tags": ["edited"]}"#, "-y"],
    ));
    assert_eq!(home.rowcount("copy"), 2);

    home.ok(&with(&["unlock", "pipes"], &["--locks", "sync"]));
    home.ok(&with(&["lock", "pipes"], &["--locks", "parameters"]));
    home.refused(
        &with(
            &["edit", "pipes"],
            &["--params", r#"{"tags": ["again"]}"#, "-y"],
        ),
        "parameters",
    );
    // Changing the locks themselves is the one edit a `parameters` lock allows.
    home.ok(&with(&["lock", "pipes"], &["--locks", "clear"]));
    // A sync writes under `parameters` (it does not record what it learns).
    home.ok(&with(&["sync", "pipes"], &[]));
    assert_eq!(home.rowcount("copy"), 3);
    // A replacing sync deletes rows: `clear` refuses it, and `clear pipes`.
    home.refused(&with(&["sync", "pipes"], &["--replace", "-y"]), "clear");
    home.refused(&with(&["clear", "pipes"], &["-y"]), "clear");

    home.ok(&with(&["unlock", "pipes"], &[]));
    home.ok(&with(&["lock", "pipes"], &["--locks", "drop"]));
    home.refused(
        &with(&["drop", "pipes"], &["-y", "--allow-protected"]),
        "drop",
    );
    // Deleting a pipe drops its table first.
    home.refused(&with(&["delete", "pipes"], &["-y", "-f"]), "drop");
    // `verify schema` changes the table: `drop` refuses it.
    home.ok(&with(
        &["edit", "pipes"],
        &["--params", r#"{"dtypes": {"extra": "int"}}"#, "-y"],
    ));
    home.refused(&with(&["verify", "schema"], &["-y"]), "drop");
    assert_eq!(home.rowcount("copy"), 3);

    home.ok(&with(&["unlock", "pipes"], &["--locks", "drop"]));
    home.ok(&with(&["lock", "pipes"], &["--locks", "delete"]));
    home.refused(&with(&["delete", "pipes"], &["-y", "-f"]), "delete");
    home.ok(&with(&["clear", "pipes"], &["-y"]));
    assert_eq!(home.rowcount("copy"), 0);

    // Unlocked, every write runs again.
    home.ok(&with(&["unlock", "pipes"], &[]));
    home.ok(&with(&["sync", "pipes"], &[]));
    assert_eq!(home.rowcount("copy"), 3);
    home.ok(&with(&["delete", "pipes"], &["-y", "-f"]));
    assert!(!home.registered("copy"));
}

#[test]
fn a_pipeline_run_skips_a_locked_member_without_failing() {
    let home = Home::new();
    home.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)");
    home.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)");
    home.ok(&with(
        &["register", "pipes"],
        &[
            "--params",
            r#"{"fetch": {"definition": "SELECT ts, id, v FROM src"}, "columns": {"datetime": "ts", "id": "id"}, "dtypes": {"ts": "datetime", "id": "int", "v": "float"}}"#,
        ],
    ));
    home.ok(&[
        "register",
        "pipes",
        "-c",
        "sql:p1",
        "-m",
        "doubled",
        "--params",
        r#"{"fetch": {"definition": "SELECT ts, id, v * 2 AS v FROM {{ Pipe('sql:p1', 'copy', instance='sql:p1') }}"}, "columns": {"datetime": "ts", "id": "id"}, "dtypes": {"ts": "datetime", "id": "int", "v": "float"}}"#,
    ]);
    home.ok(&[
        "sync",
        "pipes",
        "-c",
        "sql:p1",
        "-m",
        "doubled",
        "--parents",
    ]);
    assert_eq!(home.rowcount("copy"), 1);
    assert_eq!(home.rowcount("doubled"), 1);

    home.exec("INSERT INTO src VALUES ('2026-01-01 01:00:00', 1, 2.0)");
    home.ok(&with(&["lock", "pipes"], &["--locks", "sync"]));
    let out = home.ok(&[
        "sync",
        "pipes",
        "-c",
        "sql:p1",
        "-m",
        "doubled",
        "--parents",
    ]);
    let flat = out.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(flat.contains("it is locked (sync)"), "{out}");
    assert!(flat.contains("1 not synced: locked."), "{out}");
    assert_eq!(home.rowcount("copy"), 1, "the locked parent was synced");
    assert_eq!(home.rowcount("doubled"), 1);
}
