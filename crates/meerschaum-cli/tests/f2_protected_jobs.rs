//! rc15 Q20: a job started with `--allow-protected` keeps the flag (its sysargs carry it, and
//! the job may write to a protected connector), while a job started without it is refused.
//!
//! The jobs run the debug `mrsm-cli` as `local` jobs under a scratch root; the protected
//! connector is a SQLite file defined through `MRSM_CONFIG`, which the jobs inherit.
#![cfg(unix)]

use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

struct Env {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
    config: String,
}

impl Env {
    fn new() -> Self {
        let base = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap()
            .join(".cache/mrsm-f2-protected-job-tests");
        std::fs::create_dir_all(&base).unwrap();
        let tmp = tempfile::tempdir_in(base).unwrap();
        let root = tmp.path().join("root");
        let plugins = tmp.path().join("plugins");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&plugins).unwrap();
        let db = tmp.path().join("prot.db").to_string_lossy().into_owned();
        let config = json!({
            "meerschaum": {
                "instance": "sql:local",
                "connectors": {"sql": {
                    "prot": {"flavor": "sqlite", "database": db, "protected": true},
                    "plain": {"flavor": "sqlite", "database": db},
                }},
            },
            "formatting": {"ansi": false, "unicode": false},
            "shell": {"updates": {"check": false}},
            "jobs": {"startup": {"enabled": false}},
        })
        .to_string();
        Self {
            _tmp: tmp,
            root,
            plugins,
            config,
        }
    }

    fn db(&self) -> PathBuf {
        self._tmp.path().join("prot.db")
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_") || ["TMUX", "TMUX_PANE", "RUST_LOG"].contains(&key.as_str())
            {
                command.env_remove(key);
            }
        }
        command
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env("MRSM_NOASK", "true")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_CONFIG", &self.config)
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.status.success(), "{args:?}\n{text}");
        text
    }

    fn logs(&self, name: &str) -> String {
        let output = self.run(&["show", "logs", name, "--nopretty"]);
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    /// The job's sysargs as Rust records them (`target.args[0]`).
    fn job_sysargs(&self, name: &str) -> Vec<String> {
        let path = self.root.join("jobs").join(name).join("properties.json");
        let props: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        props["target"]["args"][0]
            .as_array()
            .unwrap_or_else(|| panic!("no target in {props}"))
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }

    fn wait_for(&self, what: &str, seconds: u64, mut f: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline {
            if f() {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("timed out waiting for {what}");
    }
}

fn rows(db: &Path) -> i64 {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let conn = meerschaum_connector_sql::SqlConnector::from_uri(
            "f2_jobs",
            &format!("sqlite:///{}", db.display()),
        )
        .await
        .unwrap();
        conn.value("SELECT COUNT(*) FROM canary")
            .await
            .and_then(|v| v.as_i64())
            .unwrap_or(-1)
    })
}

#[test]
fn jobs_keep_allow_protected() {
    let env = Env::new();
    env.ok(&["sql", "plain", "CREATE TABLE canary (x INTEGER)"]);
    assert_eq!(rows(&env.db()), 0);

    // Without the flag, the job's write is refused.
    let insert = "INSERT INTO canary VALUES (1)";
    env.ok(&["sql", "prot", insert, "-d", "--name", "f2-refused"]);
    env.wait_for("the refused job's refusal", 60, || {
        env.logs("f2-refused")
            .contains("Refusing to execute SQL on protected instance 'sql:prot'")
    });
    assert_eq!(rows(&env.db()), 0);

    // With it, the job keeps it and writes.
    env.ok(&[
        "sql",
        "prot",
        insert,
        "--allow-protected",
        "-d",
        "--name",
        "f2-allowed",
    ]);
    let sysargs = env.job_sysargs("f2-allowed");
    assert!(
        sysargs.iter().any(|a| a == "--allow-protected"),
        "{sysargs:?}"
    );
    env.wait_for("the allowed job's insert", 60, || rows(&env.db()) == 1);

    let _ = env.run(&["stop", "jobs", "f2-refused", "f2-allowed", "-y"]);
    let _ = env.run(&["delete", "jobs", "f2-refused", "f2-allowed", "-f"]);
}
