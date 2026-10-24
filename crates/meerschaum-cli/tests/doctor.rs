//! `mrsm doctor` end to end: the built binary against a temporary root, a throwaway SQLite
//! instance outside the root and (for most tests) a fake Python interpreter that prints a canned
//! answer, so the install, extras and venv checks are deterministic on any machine.
//!
//! The run must be read-only: `never_writes_into_the_root` snapshots the root before and after.
//! Nothing here touches a real root, the network or Docker (a stack file is never written, so the
//! `stack` group skips).
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Instant;

use serde_json::Value;

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct Env {
    _dir: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
    db: PathBuf,
    python: PathBuf,
}

/// The answer a healthy interpreter gives the doctor's probe.
fn probe_json(python: &Path, meerschaum_version: &str, python_version: &str) -> String {
    serde_json::json!({
        "version": python_version,
        "executable": python,
        "prefix": "/fake",
        "base_prefix": "/fake",
        "modules": {
            "pandas": {"found": true, "version": "3.0.1"},
            "pyarrow": {"found": true, "version": "20.0.0"},
        },
        "meerschaum": {
            "found": true,
            "dir": "/fake/site-packages/meerschaum",
            "version": meerschaum_version,
            "has_worker": true,
            "protocol": 1,
            "copies": [{"dir": "/fake/site-packages/meerschaum", "version": meerschaum_version}],
            "user_site": "/fake/user-site",
        },
    })
    .to_string()
}

impl Env {
    fn new() -> Env {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let plugins = dir.path().join("plugins");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&plugins).unwrap();
        let env = Env {
            db: dir.path().join("instance.db"),
            python: dir.path().join("fake-python"),
            _dir: dir,
            root,
            plugins,
        };
        env.fake_python(VERSION, "3.13.1");
        env
    }

    /// Replace the interpreter by a script that prints `probe_json` whatever it is asked.
    fn fake_python(&self, meerschaum_version: &str, python_version: &str) {
        let json = probe_json(&self.python, meerschaum_version, python_version);
        let tmp = self.python.with_extension("tmp");
        std::fs::write(&tmp, format!("#!/bin/sh\nprintf '%s\\n' '{json}'\n")).unwrap();
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::rename(&tmp, &self.python).unwrap();
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        cmd.args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &self.root)
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env(
                "MRSM_SQL_W17CDRTEST",
                format!("sqlite:///{}", self.db.display()),
            )
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum": {"instance": "sql:w17cdrtest"}}"#,
            )
            .env("MRSM_PYTHON", &self.python)
            .env("MRSM_NONINTERACTIVE", "1")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    /// Initialise the instance: the database file and the Meerschaum tables.
    fn init_instance(&self) {
        let out = self.run(&[
            "register",
            "pipes",
            "-c",
            "sql:w17cdrtest",
            "-m",
            "doctor",
            "-l",
            "a",
            "-i",
            "sql:w17cdrtest",
            "--noask",
        ]);
        assert!(out.status.success(), "{}", text(&out));
        assert!(self.db.exists());
    }

    /// `doctor --nopretty` as JSON.
    fn doctor_json(&self, extra: &[&str]) -> Value {
        let mut args = vec!["doctor", "--nopretty"];
        args.extend_from_slice(extra);
        let out = self.run(&args);
        serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|e| panic!("not JSON ({e}):\n{}", text(&out)))
    }

    /// Every file under the root with its size and modification time.
    fn snapshot(&self) -> Vec<String> {
        let mut files = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let path = entry.path();
                let meta = std::fs::symlink_metadata(&path).unwrap();
                if meta.is_dir() {
                    files.push(format!("dir {}", path.display()));
                    stack.push(path);
                } else {
                    files.push(format!(
                        "{} {} {:?}",
                        path.display(),
                        meta.len(),
                        meta.modified().unwrap()
                    ));
                }
            }
        }
        files.sort();
        files
    }
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn check<'a>(doc: &'a Value, group: &str, name: &str) -> &'a Value {
    doc["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["group"] == group && c["name"] == name)
        .unwrap_or_else(|| panic!("no check {group}: {name} in {doc:#}"))
}

fn statuses(doc: &Value, status: &str) -> Vec<String> {
    doc["checks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["status"] == status)
        .map(|c| format!("{}: {} ({})", c["group"], c["name"], c["detail"]))
        .collect()
}

#[test]
fn a_healthy_sqlite_instance_has_no_failure() {
    let env = Env::new();
    env.init_instance();
    let doc = env.doctor_json(&[]);
    assert!(
        statuses(&doc, "FAIL").is_empty(),
        "unexpected failures: {:?}",
        statuses(&doc, "FAIL")
    );
    assert_eq!(doc["summary"]["ok"], true);
    assert_eq!(doc["version"], VERSION);
    assert_eq!(check(&doc, "instance", "connection")["status"], "PASS");
    assert_eq!(check(&doc, "instance", "tables")["status"], "PASS");
    assert_eq!(
        check(&doc, "install", "meerschaum package")["status"],
        "PASS"
    );
    assert_eq!(check(&doc, "install", "worker python")["status"], "PASS");
    assert_eq!(check(&doc, "paths", "root")["status"], "PASS");
    assert_eq!(check(&doc, "extras", "pandas")["status"], "PASS");
    assert_eq!(check(&doc, "stack", "services")["status"], "SKIP");
    // The default instance's connector variable is reported by name, never by value.
    let env_detail = check(&doc, "config", "environment")["detail"]
        .as_str()
        .unwrap();
    assert!(env_detail.contains("MRSM_SQL_W17CDRTEST"), "{env_detail}");
    assert!(!env_detail.contains("instance.db"), "{env_detail}");
}

#[test]
fn the_json_document_has_the_documented_shape() {
    let env = Env::new();
    env.init_instance();
    let doc = env.doctor_json(&[]);
    let summary = doc["summary"].as_object().unwrap();
    for key in ["ok", "pass", "warn", "fail", "skip", "text"] {
        assert!(summary.contains_key(key), "summary.{key}");
    }
    let checks = doc["checks"].as_array().unwrap();
    assert!(checks.len() >= 20, "{} checks", checks.len());
    for c in checks {
        for key in ["group", "name", "status", "detail", "fix"] {
            assert!(c.get(key).is_some(), "{key} in {c}");
        }
        assert!(["PASS", "WARN", "FAIL", "SKIP"].contains(&c["status"].as_str().unwrap()));
        let needs_fix = matches!(c["status"].as_str().unwrap(), "WARN" | "FAIL");
        assert_eq!(c["fix"].is_string(), needs_fix, "fix of {c}");
        assert!(c.get("duration_ms").is_none(), "no timings without --debug");
    }
    let groups: Vec<&str> = checks
        .iter()
        .map(|c| c["group"].as_str().unwrap())
        .collect();
    for group in [
        "install",
        "paths",
        "config",
        "instance",
        "drivers",
        "extras",
        "plugins",
        "jobs",
        "stack",
        "extensions",
    ] {
        assert!(groups.contains(&group), "group {group}");
    }
    // Groups come in print order.
    let first = |g: &str| groups.iter().position(|x| *x == g).unwrap();
    assert!(first("install") < first("paths") && first("paths") < first("config"));
    assert!(first("config") < first("instance") && first("instance") < first("stack"));
    // `--debug` adds the time of each check.
    let timed = env.doctor_json(&["--debug"]);
    assert!(timed["checks"][0].get("duration_ms").is_some());
}

#[test]
fn the_pretty_report_is_a_table_and_the_exit_code_follows_failures() {
    let env = Env::new();
    env.init_instance();
    let out = env.run(&["doctor"]);
    let shown = text(&out);
    assert!(out.status.success(), "{shown}");
    for word in ["Group", "install", "PASS", "instance", "No problems found"] {
        assert!(shown.contains(word), "{word} missing from:\n{shown}");
    }
    let bad = env.run(&["doctor", "-i", "sql:nope"]);
    let shown = text(&bad);
    assert!(!bad.status.success(), "{shown}");
    assert!(shown.contains("FAIL") && shown.contains("Fix:"), "{shown}");
    assert!(shown.contains("check failed"), "{shown}");
}

#[test]
fn a_missing_instance_fails_with_the_fix() {
    let env = Env::new();
    let doc = env.doctor_json(&["-i", "sql:w17cdrnope"]);
    let c = check(&doc, "instance", "connection");
    assert_eq!(c["status"], "FAIL");
    assert!(c["detail"].as_str().unwrap().contains("not defined"));
    let fix = c["fix"].as_str().unwrap();
    assert!(
        fix.contains("MRSM_SQL_W17CDRNOPE") && fix.contains("bootstrap connector"),
        "{fix}"
    );
    assert_eq!(doc["summary"]["ok"], false);
    assert_eq!(check(&doc, "extensions", "postgresql")["status"], "SKIP");
}

#[test]
fn a_sqlite_database_that_does_not_exist_is_reported_not_created() {
    let env = Env::new();
    // Prime the root so the snapshot below sees only the doctor's effect.
    env.run(&["show", "version", "--nopretty"]);
    assert!(!env.db.exists());
    let doc = env.doctor_json(&[]);
    let c = check(&doc, "instance", "connection");
    assert_eq!(c["status"], "WARN");
    assert!(c["fix"].is_string());
    assert!(!env.db.exists(), "the doctor created the database");
}

#[tokio::test]
async fn wal_databases_keep_their_mode_and_contents_during_doctor() {
    use meerschaum_connector_sql::driver::{SqlDriver, SqlValue};
    use meerschaum_connector_sql::uri::ConnectionSpec;

    for scheme in ["sqlite", "geopackage"] {
        let env = Env::new();
        env.init_instance();
        let uri = format!("{scheme}:///{}", env.db.display());
        let mut spec = ConnectionSpec::from_uri(&uri, None, None).unwrap();
        spec.pool_size = 1;
        spec.max_overflow = 0;
        let writer = SqlDriver::connect(&spec).await.unwrap();
        writer
            .exec("CREATE TABLE sentinel (value TEXT); INSERT INTO sentinel VALUES ('keep')")
            .await
            .unwrap();
        assert_eq!(
            writer.value("PRAGMA journal_mode=WAL").await.unwrap(),
            Some(SqlValue::Text("wal".into()))
        );
        writer
            .exec("UPDATE sentinel SET value = 'live WAL data'")
            .await
            .unwrap();
        let before = std::fs::read(&env.db).unwrap();
        let config_before = env.snapshot();

        // Keep the WAL writer connected: ordinary DELETE-mode probing would either mutate
        // the database's persistent mode or fail trying to take its exclusive mode-switch lock.
        let out = env
            .command(&["doctor", "--nopretty"])
            .env("MRSM_SQL_W17CDRTEST", &uri)
            .output()
            .unwrap();
        let doc: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(
            check(&doc, "instance", "connection")["status"],
            "PASS",
            "{doc:#}"
        );
        assert_eq!(
            std::fs::read(&env.db).unwrap(),
            before,
            "{scheme}: database changed"
        );
        assert_eq!(
            env.snapshot(),
            config_before,
            "{scheme}: root files changed"
        );
        assert_eq!(
            writer.value("PRAGMA journal_mode").await.unwrap(),
            Some(SqlValue::Text("wal".into()))
        );
        assert_eq!(
            writer.value("SELECT value FROM sentinel").await.unwrap(),
            Some(SqlValue::Text("live WAL data".into()))
        );
        writer.close().await;
    }
}

#[test]
fn a_missing_configured_venv_directory_remains_absent() {
    let env = Env::new();
    env.init_instance();
    let venvs = env
        ._dir
        .path()
        .join("outside-root")
        .join("configured-venvs");
    assert!(!venvs.exists());
    let before = env.snapshot();
    let out = env
        .command(&["doctor", "--nopretty"])
        .env("MRSM_VENVS_DIR", &venvs)
        .output()
        .unwrap();
    let doc: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        check(&doc, "instance", "connection")["status"],
        "PASS",
        "{doc:#}"
    );
    assert!(
        !venvs.parent().unwrap().exists(),
        "doctor created the configured venv parent"
    );
    assert_eq!(env.snapshot(), before);
}

#[test]
fn an_unreachable_server_fails_fast() {
    let env = Env::new();
    // A port nothing listens on.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let mut cmd = env.command(&["doctor", "--nopretty", "-i", "sql:w17cdrdown"]);
    cmd.env(
        "MRSM_SQL_W17CDRDOWN",
        format!("postgresql://user:secret@127.0.0.1:{port}/db"),
    );
    let start = Instant::now();
    let out = cmd.output().unwrap();
    assert!(start.elapsed().as_secs() < 4, "took {:?}", start.elapsed());
    let doc: Value = serde_json::from_slice(&out.stdout).unwrap();
    let c = check(&doc, "instance", "connection");
    assert_eq!(c["status"], "FAIL");
    assert!(c["fix"].is_string());
    assert!(!text(&out).contains("secret"), "a credential leaked");
}

#[test]
fn a_shadowing_meerschaum_is_a_failure_when_plugins_need_it() {
    let env = Env::new();
    env.init_instance();
    env.fake_python("4.1.4", "3.13.1");
    // No Python plugin: nothing needs that interpreter, so a stale copy is a warning.
    let doc = env.doctor_json(&[]);
    let c = check(&doc, "install", "meerschaum package");
    assert_eq!(c["status"], "WARN", "{c}");
    assert!(
        c["detail"]
            .as_str()
            .unwrap()
            .contains("only plugins need it")
    );
    assert_eq!(doc["summary"]["ok"], true);
    // With a Python plugin installed it is a failure naming the path.
    std::fs::write(
        env.plugins.join("needs_python.py"),
        "def fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    let doc = env.doctor_json(&[]);
    let c = check(&doc, "install", "meerschaum package");
    assert_eq!(c["status"], "FAIL");
    let detail = c["detail"].as_str().unwrap();
    assert!(
        detail.contains("4.1.4") && detail.contains("/fake/site-packages/meerschaum"),
        "{detail}"
    );
    assert!(c["fix"].as_str().unwrap().contains("MRSM_PYTHON"));
    assert_eq!(doc["summary"]["ok"], false);
}

#[test]
fn an_old_python_fails_and_missing_python_only_warns() {
    let env = Env::new();
    env.init_instance();
    env.fake_python(VERSION, "3.9.2");
    let doc = env.doctor_json(&[]);
    assert_eq!(check(&doc, "install", "worker python")["status"], "FAIL");
    std::fs::remove_file(&env.python).unwrap();
    let doc = env.doctor_json(&[]);
    assert_eq!(check(&doc, "install", "worker python")["status"], "WARN");
    assert_eq!(
        check(&doc, "install", "meerschaum package")["status"],
        "SKIP"
    );
    assert!(
        statuses(&doc, "FAIL").is_empty(),
        "{:?}",
        statuses(&doc, "FAIL")
    );
}

#[test]
fn a_venv_built_by_another_python_is_a_warning() {
    let env = Env::new();
    env.init_instance();
    let venv = env.root.join("venvs").join("old_plugin");
    std::fs::create_dir_all(venv.join("lib").join("python3.8").join("site-packages")).unwrap();
    let doc = env.doctor_json(&[]);
    let c = check(&doc, "plugins", "venvs");
    assert_eq!(c["status"], "WARN", "{c}");
    assert!(c["detail"].as_str().unwrap().contains("old_plugin"));
    assert!(c["fix"].as_str().unwrap().contains("verify venvs"));
}

#[test]
fn jobs_report_failures_and_stale_pid_files() {
    let env = Env::new();
    env.init_instance();
    let jobs = env.root.join("jobs");
    let failed = jobs.join("failed_job");
    let stale = jobs.join("stale_job");
    for dir in [&failed, &stale] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::write(
        failed.join("properties.json"),
        r#"{"result": [false, "Sync failed\nsecond line"]}"#,
    )
    .unwrap();
    std::fs::write(stale.join("properties.json"), "{}").unwrap();
    // PID 2^22 is above Linux's default pid_max: no such process.
    std::fs::write(stale.join("process.pid"), "4194301").unwrap();
    let before = env.snapshot();
    let doc = env.doctor_json(&[]);
    assert_eq!(check(&doc, "jobs", "failed jobs")["status"], "WARN");
    assert!(
        check(&doc, "jobs", "failed jobs")["detail"]
            .as_str()
            .unwrap()
            .contains("failed_job (Sync failed)")
    );
    assert_eq!(check(&doc, "jobs", "pid files")["status"], "WARN");
    assert!(
        stale.join("process.pid").exists(),
        "the doctor must not remove the stale PID file"
    );
    assert_eq!(env.snapshot(), before);
}

#[test]
fn config_that_does_not_parse_fails() {
    let env = Env::new();
    env.init_instance();
    std::fs::write(env.root.join("config").join("pipes.json"), "{ not json").unwrap();
    let doc = env.doctor_json(&[]);
    let c = check(&doc, "config", "files");
    assert_eq!(c["status"], "FAIL");
    assert!(c["detail"].as_str().unwrap().contains("pipes.json"));
}

/// The first command of a root normally writes the default config files, the plugin manifest and
/// the entry-point cache, and a changed plugin makes the next command rebuild the manifest in a
/// worker (venvs, `.pyc` files, symlinks). `doctor` does none of it: no warm-up run here, a good
/// and a broken plugin, a stale manifest, every output mode, directories included.
#[test]
fn never_writes_into_the_root() {
    let env = Env::new();
    // The instance lives outside the root, so the root is untouched until the doctor runs.
    std::fs::write(
        env.plugins.join("w17cdrgood.py"),
        "def fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    std::fs::write(
        env.plugins.join("w17cdrbad.py"),
        "import w17cdr_missing_module\n",
    )
    .unwrap();
    let before = env.snapshot();
    assert!(before.iter().all(|f| f.starts_with("dir ")), "{before:?}");
    let out = env.run(&["doctor", "--nopretty"]);
    let doc: Value =
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("{e}: {}", text(&out)));
    assert_eq!(check(&doc, "plugins", "manifest")["status"], "WARN");
    assert!(
        check(&doc, "plugins", "manifest")["detail"]
            .as_str()
            .unwrap()
            .contains("missing")
    );
    assert_eq!(check(&doc, "plugins", "imports")["status"], "SKIP");
    assert_eq!(
        env.snapshot(),
        before,
        "a first run on an empty root wrote files"
    );
    env.run(&["doctor"]);
    env.run(&["doctor", "--nopretty", "--debug"]);
    env.run(&["doctor", "-i", "sql:nope"]);
    assert_eq!(env.snapshot(), before);
}

#[test]
fn malformed_doctor_arguments_never_load_plugins_or_write_caches() {
    let env = Env::new();
    std::fs::write(
        env.plugins.join("w17cdrparse_sentinel.py"),
        "raise AssertionError('malformed doctor imported a plugin')\n",
    )
    .unwrap();
    // Stronger than an import-only sentinel: even entry-point discovery must not launch
    // Python before the parser rejects a doctor command. Ordinary commands exercise it below.
    std::fs::write(
        &env.python,
        "#!/bin/sh\nprintf invoked > \"$MRSM_ROOT_DIR/python-started\"\nexit 1\n",
    )
    .unwrap();
    let before = env.snapshot();
    for args in [
        vec!["doctor", "--port", "not-a-number"],
        vec!["doctor", "--chunk-days", "9223372036854775807"],
        vec!["+", "doctor", "--chunk-days", "9223372036854775807"],
    ] {
        let out = env.run(&args);
        assert!(!out.status.success(), "{args:?}: {}", text(&out));
        assert!(
            !env.root.join("python-started").exists(),
            "{args:?} started Python: {}",
            text(&out)
        );
        assert_eq!(env.snapshot(), before, "{args:?}: {}", text(&out));
    }
    env.run(&["show", "version"]);
    assert!(
        env.root.join("python-started").is_file(),
        "the normal-command control did not exercise plugin discovery"
    );
}

#[test]
fn a_stale_manifest_is_reported_and_never_rebuilt() {
    let env = Env::new();
    env.init_instance();
    let plugin = env.plugins.join("w17cdrstale.py");
    std::fs::write(&plugin, "def fetch(pipe, **kw):\n    return None\n").unwrap();
    // Build the manifest with the real interpreter and this repository (an ordinary command).
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().unwrap_or(repo);
    let mut build = env.command(&["show", "plugins", "--nopretty"]);
    build.env("PYTHONPATH", &repo).env_remove("MRSM_PYTHON");
    let built = build.output().unwrap();
    let manifest = env.root.join("cache").join("plugins_manifest.json");
    assert!(manifest.exists(), "{}", text(&built));
    // Fresh: PASS. Then a plugin changes.
    let doc = env.doctor_json(&[]);
    assert_eq!(
        check(&doc, "plugins", "manifest")["status"],
        "PASS",
        "{doc:#}"
    );
    std::fs::write(
        &plugin,
        "import w17cdr_missing_module\n\n\ndef fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    let before = env.snapshot();
    let cached = std::fs::read(&manifest).unwrap();
    for args in [vec!["doctor", "--nopretty"], vec!["doctor"]] {
        let out = env.run(&args);
        assert!(!text(&out).is_empty());
    }
    let doc = env.doctor_json(&[]);
    let c = check(&doc, "plugins", "manifest");
    assert_eq!(c["status"], "WARN", "{c}");
    assert!(c["detail"].as_str().unwrap().contains("stale"), "{c}");
    assert!(c["detail"].as_str().unwrap().contains("w17cdrstale"), "{c}");
    assert!(c["fix"].as_str().unwrap().contains("mrsm reload"));
    assert_eq!(env.snapshot(), before, "the doctor changed the root");
    assert_eq!(
        std::fs::read(&manifest).unwrap(),
        cached,
        "the manifest was rebuilt"
    );
}

#[test]
fn a_duckdb_instance_without_its_library_is_not_probed_or_installed() {
    let env = Env::new();
    // Only meaningful where DuckDB's library is not on the system path.
    let drivers = env.run(&["show", "drivers", "--nopretty"]);
    let listing = String::from_utf8_lossy(&drivers.stdout).to_string();
    let missing = listing
        .lines()
        .any(|l| l.starts_with("duckdb\t") && l.contains("\tmissing\t"));
    if !missing {
        eprintln!("skipped: DuckDB's library is present here:\n{listing}");
        return;
    }
    let db = env.root.parent().unwrap().join("instance.duckdb");
    std::fs::write(&db, b"not a database, only a file that exists").unwrap();
    let before = env.snapshot();
    // Auto-install is allowed here (no MRSM_NO_AUTO_INSTALL); a download would fail fast through
    // this dead proxy, but the doctor must not even try (nor create `<root>/drivers`).
    let mut cmd = env.command(&["doctor", "--nopretty", "-i", "sql:w17cdrduck"]);
    cmd.env_remove("MRSM_NO_AUTO_INSTALL")
        .env("MRSM_SQL_W17CDRDUCK", format!("duckdb:///{}", db.display()))
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("HTTP_PROXY", "http://127.0.0.1:9");
    let out = cmd.output().unwrap();
    let shown = text(&out);
    assert!(!shown.contains("Installing"), "{shown}");
    let doc: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("{e}: {shown}"));
    let c = check(&doc, "instance", "connection");
    assert_eq!(c["status"], "WARN", "{c}");
    assert!(c["detail"].as_str().unwrap().contains("not probed"), "{c}");
    assert!(c["fix"].as_str().unwrap().contains("install driver duckdb"));
    assert!(
        !env.root.join("drivers").exists(),
        "the doctor created <root>/drivers"
    );
    assert_eq!(env.snapshot(), before);
    // With installs disabled the instance cannot work: a failure.
    let mut cmd = env.command(&["doctor", "--nopretty", "-i", "sql:w17cdrduck"]);
    cmd.env("MRSM_SQL_W17CDRDUCK", format!("duckdb:///{}", db.display()));
    let doc: Value = serde_json::from_slice(&cmd.output().unwrap().stdout).unwrap();
    assert_eq!(check(&doc, "instance", "connection")["status"], "FAIL");
}

/// This release as the Python package writes it (`5.0.0-rc.1` is `5.0.0rc1`).
fn python_version() -> String {
    VERSION.replace("-rc.", "rc").replace('-', "")
}

/// A fake `meerschaum` package: `__init__.py` records that it was imported, `config/_version.py`
/// holds `version` (none: no such file), `worker/__init__.py` holds the protocol (none: no
/// `worker` module, like v4).
fn fake_package(site: &Path, marker: &Path, version: Option<&str>, protocol: Option<u32>) {
    let package = site.join("meerschaum");
    std::fs::create_dir_all(package.join("config")).unwrap();
    std::fs::write(
        package.join("__init__.py"),
        format!(
            "open({:?}, 'w').write('imported')\n",
            marker.display().to_string()
        ),
    )
    .unwrap();
    if let Some(version) = version {
        std::fs::write(
            package.join("config").join("_version.py"),
            format!("__version__ = \"{version}\"\n"),
        )
        .unwrap();
    }
    if let Some(protocol) = protocol {
        std::fs::create_dir_all(package.join("worker")).unwrap();
        std::fs::write(
            package.join("worker").join("__init__.py"),
            format!("PROTOCOL_VERSION: int = {protocol}\n"),
        )
        .unwrap();
    }
}

/// Run `show version` with a plugin (so a worker starts) against the packages on `site_dirs`.
fn run_with_packages(env: &Env, site_dirs: &[&Path]) -> Output {
    std::fs::write(
        env.plugins.join("w17cdrneedsworker.py"),
        "def fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    let mut cmd = env.command(&["show", "version"]);
    cmd.env("PYTHONPATH", std::env::join_paths(site_dirs).unwrap())
        .env_remove("MRSM_PYTHON");
    cmd.output().unwrap()
}

fn count(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

/// A stale `meerschaum` of another major version (v4 in the user site, say) is never imported by
/// a worker: importing v4 runs its start-up, which installs packages (`semver`) whatever
/// `MRSM_NO_AUTO_INSTALL` says. The worker host reads the package's version without importing it,
/// and says why once (the dispatcher asks for a worker several times).
#[test]
fn a_package_of_another_major_version_is_never_imported_and_refused_once() {
    let env = Env::new();
    let site = env.root.parent().unwrap().join("stale-site");
    let marker = env.root.parent().unwrap().join("stale-was-imported");
    fake_package(&site, &marker, Some("4.1.4"), None);
    let out = run_with_packages(&env, &[&site]);
    let shown = text(&out);
    assert!(!marker.exists(), "the stale package was imported: {shown}");
    assert_eq!(
        count(&shown, "cannot be used by this program"),
        1,
        "{shown}"
    );
    assert!(
        shown.contains("4.1.4") && shown.contains("another major version"),
        "{shown}"
    );
    // The only copy: the advice is to install the matching release, not to uninstall.
    assert!(
        shown.contains(&format!("pip install 'meerschaum=={}'", python_version())),
        "{shown}"
    );
    assert!(!shown.contains("uninstall"), "{shown}");
    assert!(
        !env.root.join("venvs").join("mrsm").exists(),
        "an install happened: {shown}"
    );
    assert!(!shown.contains("Installing packages"), "{shown}");
}

#[test]
fn another_copy_behind_the_refused_one_is_named_in_the_advice() {
    let env = Env::new();
    let (stale, right) = (
        env.root.parent().unwrap().join("stale-site"),
        env.root.parent().unwrap().join("right-site"),
    );
    let marker = env.root.parent().unwrap().join("was-imported");
    fake_package(&stale, &marker, Some("4.1.4"), None);
    fake_package(&right, &marker, Some(&python_version()), Some(1));
    let shown = text(&run_with_packages(&env, &[&stale, &right]));
    assert!(!marker.exists(), "{shown}");
    assert_eq!(
        count(&shown, "cannot be used by this program"),
        1,
        "{shown}"
    );
    assert!(shown.contains("uninstall this one"), "{shown}");
    assert!(
        shown.contains(&stale.join("meerschaum").display().to_string()),
        "{shown}"
    );
}

#[test]
fn a_package_that_looks_like_v4_and_another_protocol_are_refused() {
    let env = Env::new();
    let site = env.root.parent().unwrap().join("odd-site");
    let marker = env.root.parent().unwrap().join("was-imported");
    // No readable version and no `worker` module: v4's shape.
    fake_package(&site, &marker, None, None);
    let shown = text(&run_with_packages(&env, &[&site]));
    assert!(!marker.exists(), "{shown}");
    assert_eq!(
        count(&shown, "cannot be used by this program"),
        1,
        "{shown}"
    );
    assert!(shown.contains("looks like Meerschaum 4"), "{shown}");
    // The same release with another worker protocol.
    let env = Env::new();
    let site = env.root.parent().unwrap().join("protocol-site");
    let marker = env.root.parent().unwrap().join("was-imported");
    fake_package(&site, &marker, Some(&python_version()), Some(2));
    let shown = text(&run_with_packages(&env, &[&site]));
    assert!(!marker.exists(), "{shown}");
    assert_eq!(
        count(&shown, "cannot be used by this program"),
        1,
        "{shown}"
    );
    assert!(
        shown.contains("worker protocol 2") && shown.contains("speaks 1"),
        "{shown}"
    );
}

/// Another release of the same major version, same protocol: the worker starts (the package is
/// imported) and one warning names both versions and how to align them.
#[test]
fn another_release_of_the_same_major_starts_with_one_warning() {
    let env = Env::new();
    let site = env.root.parent().unwrap().join("near-site");
    let marker = env.root.parent().unwrap().join("was-imported");
    let major = VERSION.split('.').next().unwrap();
    let other = format!("{major}.9.9");
    fake_package(&site, &marker, Some(&other), Some(1));
    let shown = text(&run_with_packages(&env, &[&site]));
    assert!(marker.exists(), "the worker should have started: {shown}");
    assert!(!shown.contains("cannot be used by this program"), "{shown}");
    assert_eq!(
        count(&shown, "Warning: the Python package `meerschaum` is "),
        1,
        "{shown}"
    );
    assert!(
        shown.contains(&other) && shown.contains(&python_version()),
        "{shown}"
    );
    assert!(shown.contains("align them"), "{shown}");
}

/// The package's `PROTOCOL_VERSION` the guard reads is the one the host speaks.
#[test]
fn the_python_worker_protocol_matches_the_hosts() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = std::fs::read_to_string(repo.join("meerschaum/worker/__init__.py")).unwrap();
    let line = source
        .lines()
        .find(|l| l.starts_with("PROTOCOL_VERSION"))
        .expect("PROTOCOL_VERSION in meerschaum/worker/__init__.py");
    let number: u32 = line.rsplit('=').next().unwrap().trim().parse().unwrap();
    assert_eq!(
        number,
        meerschaum_core::python_guard::WORKER_PROTOCOL_VERSION
    );
}

/// `doctor + <command>`: the read-only config and the cached manifest end with doctor's step, so
/// the next step validates the plugins (and writes the manifest) as any command does.
#[test]
fn a_command_chained_after_doctor_runs_normally() {
    let env = Env::new();
    std::fs::write(
        env.plugins.join("w17cdrchained.py"),
        "def fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().unwrap_or(repo);
    let mut cmd = env.command(&["doctor", "--nopretty", "+", "show", "version", "--nopretty"]);
    cmd.env("PYTHONPATH", &repo).env_remove("MRSM_PYTHON");
    let out = cmd.output().unwrap();
    let shown = text(&out);
    // The doctor step saw no manifest and wrote nothing ...
    let first: Value = serde_json::Deserializer::from_slice(&out.stdout)
        .into_iter::<Value>()
        .next()
        .unwrap_or_else(|| panic!("no JSON: {shown}"))
        .unwrap_or_else(|e| panic!("{e}: {shown}"));
    assert_eq!(check(&first, "plugins", "manifest")["status"], "WARN");
    // ... and the step after it validated the plugins and wrote the manifest and the config.
    assert!(
        env.root
            .join("cache")
            .join("plugins_manifest.json")
            .exists(),
        "the chained step ran in doctor's read-only mode: {shown}"
    );
    assert!(env.root.join("config").exists(), "{shown}");
}

#[test]
fn a_broken_plugin_is_a_warning() {
    let env = Env::new();
    env.init_instance();
    std::fs::write(
        env.plugins.join("w17cdrbroken.py"),
        "import w17cdr_module_that_does_not_exist\n\n\ndef fetch(pipe, **kw):\n    return None\n",
    )
    .unwrap();
    // The real interpreter and this repository, like the other plugin tests.
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().unwrap_or(repo);
    // The doctor reads the manifest an ordinary command built (it never imports plugins itself).
    let mut build = env.command(&["show", "plugins", "--nopretty"]);
    build.env("PYTHONPATH", &repo).env_remove("MRSM_PYTHON");
    build.output().unwrap();
    let mut cmd = env.command(&["doctor", "--nopretty"]);
    cmd.env("PYTHONPATH", &repo).env_remove("MRSM_PYTHON");
    let out = cmd.output().unwrap();
    let doc: Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("not JSON ({e}):\n{}", text(&out)));
    let imports = check(&doc, "plugins", "imports");
    assert_eq!(imports["status"], "WARN", "{doc:#}");
    assert!(imports["detail"].as_str().unwrap().contains("w17cdrbroken"));
    assert!(imports["fix"].is_string());
    assert_eq!(check(&doc, "plugins", "installed")["status"], "PASS");
}

/// Empty path overrides have the same working-directory semantics in every build.
#[test]
fn empty_path_variables_are_accepted_in_every_build() {
    for var in ["MRSM_ROOT_DIR", "MRSM_CONFIG_DIR", "MRSM_VENVS_DIR"] {
        let env = Env::new();
        let cwd = env.root.parent().unwrap().join("cwd");
        std::fs::create_dir_all(&cwd).unwrap();
        let output = env
            .command(&["show", "version", "--nopretty"])
            .env(var, "")
            .current_dir(&cwd)
            .output()
            .unwrap();
        assert!(output.status.success(), "{var}: {}", text(&output));
    }
}

#[test]
fn a_local_binary_uses_the_default_root_without_an_opt_in() {
    let env = Env::new();
    let home = env.root.parent().unwrap().join("home");
    let xdg = home.join(".config");
    std::fs::create_dir_all(&xdg).unwrap();
    let output = env
        .command(&["show", "config", "--nopretty"])
        .env_remove("MRSM_ROOT_DIR")
        .env_remove("MRSM_CONFIG_DIR")
        .env("HOME", &home)
        .env("XDG_CONFIG_HOME", &xdg)
        .current_dir(&home)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", text(&output));
    assert!(
        xdg.join("meerschaum/config").is_dir(),
        "the CLI should use its normal default root"
    );
}
