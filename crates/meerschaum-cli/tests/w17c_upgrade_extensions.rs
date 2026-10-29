//! Wave 17c: `mrsm upgrade extensions` through the binary over a temporary root.
//!
//! A non-PostgreSQL instance has nothing to update (always runs). With
//! `MRSM_TEST_TIMESCALE_URI` set (the HA image on 5459 ships older TimescaleDB and toolkit
//! versions), the test creates three scratch databases with OLD extension versions: the
//! instance's own, one named as an action word, and one that is not named (it must stay
//! untouched). A declined prompt changes nothing; `-y` updates the two targets; a second run
//! finds nothing to do. The databases are dropped afterwards.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use meerschaum_connector_sql::extensions::{self, run_first_statement, spec_for_database};
use meerschaum_connector_sql::uri::ConnectionSpec;

struct Env {
    _tmp: tempfile::TempDir,
    root: PathBuf,
}

impl Env {
    fn new() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        Env { _tmp: tmp, root }
    }

    /// Run `mrsm` with `stdin` written to it (`None`: `MRSM_NOASK`).
    fn run(&self, extra_env: &[(&str, &str)], args: &[&str], stdin: Option<&str>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_") || matches!(key.as_str(), "RUST_LOG" | "NO_COLOR" | "TERM")
            {
                command.env_remove(key);
            }
        }
        command
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", self.root.join("plugins"))
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env(
                "MRSM_CONFIG",
                r#"{"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false}}"#,
            )
            .envs(extra_env.iter().copied())
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::piped());
        if stdin.is_none() {
            command.env("MRSM_NOASK", "true");
        }
        let mut child = command.spawn().unwrap();
        {
            let mut pipe = child.stdin.take().unwrap();
            if let Some(input) = stdin {
                let _ = pipe.write_all(input.as_bytes());
            }
        }
        child.wait_with_output().unwrap()
    }
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn a_non_postgres_instance_has_nothing_to_update() {
    let env = Env::new();
    let db = env.root.join("w17c.db");
    let uri = format!("sqlite:///{}", db.display());
    let out = env.run(
        &[("MRSM_SQL_W17CEXT", uri.as_str())],
        &["upgrade", "extensions", "-i", "sql:w17cext"],
        None,
    );
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("Nothing to update"), "{all}");
    assert!(all.contains("not a PostgreSQL instance"), "{all}");
}

#[test]
fn help_documents_upgrade_extensions() {
    let env = Env::new();
    let out = env.run(&[], &["upgrade", "extensions", "--help"], None);
    let all = text(&out);
    assert!(all.contains("postgis_extensions_upgrade()"), "{all}");
}

/// `uri` with its database replaced.
fn uri_for(uri: &str, database: &str) -> String {
    let (base, query) = match uri.split_once('?') {
        Some((b, q)) => (b, format!("?{q}")),
        None => (uri, String::new()),
    };
    let (prefix, _) = base.rsplit_once('/').expect("a URI with a database");
    format!("{prefix}/{database}{query}")
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The version `ext` is installed at in `spec`'s database (`None` when it is not).
fn installed(rt: &tokio::runtime::Runtime, spec: &ConnectionSpec, ext: &str) -> Option<String> {
    rt.block_on(extensions::status(spec, None, Duration::from_secs(20)))
        .unwrap()
        .unwrap()
        .extension(ext)
        .map(|e| e.installed.clone())
}

fn default_version(rt: &tokio::runtime::Runtime, spec: &ConnectionSpec, ext: &str) -> String {
    rt.block_on(extensions::status(spec, None, Duration::from_secs(20)))
        .unwrap()
        .unwrap()
        .extension(ext)
        .and_then(|e| e.available.clone())
        .unwrap()
}

/// Install the newest older version of `ext` the server can still create.
fn create_old(rt: &tokio::runtime::Runtime, spec: &ConnectionSpec, ext: &str) -> Option<String> {
    // Read the candidates in a throwaway database session (it may load libraries).
    let conn = rt
        .block_on(meerschaum_connector_sql::SqlConnector::connect(
            "w17c_probe",
            spec.clone(),
        ))
        .unwrap();
    let sql = format!(
        "SELECT string_agg(version, ',' ORDER BY string_to_array(version, '.')::int[] DESC) \
         FROM (SELECT v.version FROM pg_available_extension_versions v \
         JOIN pg_available_extensions a ON a.name = v.name \
         WHERE v.name = '{ext}' AND v.version <> a.default_version \
         AND v.version ~ '^[0-9]+(\\.[0-9]+)*$' \
         AND EXISTS (SELECT 1 FROM pg_extension_update_paths(v.name) p \
         WHERE p.source = v.version AND p.target = a.default_version AND p.path IS NOT NULL)) t"
    );
    let versions = match rt.block_on(conn.value(&sql)) {
        Some(meerschaum_connector_sql::driver::SqlValue::Text(s)) => s,
        _ => return None,
    };
    for version in versions.split(',').take(4) {
        let create = format!("CREATE EXTENSION \"{ext}\" VERSION '{version}' CASCADE");
        if rt
            .block_on(run_first_statement(spec, None, &create))
            .is_ok()
        {
            return Some(version.to_string());
        }
    }
    None
}

#[test]
fn updates_the_instance_and_named_databases_only() {
    let Some(uri) = std::env::var("MRSM_TEST_TIMESCALE_URI")
        .ok()
        .filter(|u| !u.is_empty())
    else {
        eprintln!("MRSM_TEST_TIMESCALE_URI is not set; skipping.");
        return;
    };
    let rt = rt();
    let base = ConnectionSpec::from_uri(&uri, Some("w17c_admin"), None).unwrap();
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let names: Vec<String> = ["own", "named", "other"]
        .iter()
        .map(|n| format!("w17ck2_{tag}_{n}"))
        .collect();
    for name in &names {
        rt.block_on(run_first_statement(
            &base,
            None,
            &format!("CREATE DATABASE \"{name}\" TEMPLATE template0"),
        ))
        .unwrap();
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        body(&rt, &uri, &base, &names);
    }));
    for name in &names {
        rt.block_on(run_first_statement(
            &base,
            None,
            &format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"),
        ))
        .unwrap();
    }
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

fn body(rt: &tokio::runtime::Runtime, uri: &str, base: &ConnectionSpec, names: &[String]) {
    let specs: Vec<ConnectionSpec> = names
        .iter()
        .map(|n| spec_for_database(base, Some(n)))
        .collect();
    let mut old = Vec::new();
    for spec in &specs {
        old.push(
            create_old(rt, spec, "timescaledb")
                .expect("the server ships an older TimescaleDB with an update path"),
        );
    }
    let toolkit_old = create_old(rt, &specs[0], "timescaledb_toolkit");
    let ts_default = default_version(rt, &specs[0], "timescaledb");

    let env = Env::new();
    let instance_uri = uri_for(uri, &names[0]);
    let vars = [("MRSM_SQL_W17CEXT", instance_uri.as_str())];
    let args = [
        "upgrade",
        "extensions",
        names[1].as_str(),
        "-i",
        "sql:w17cext",
        "--nopretty",
    ];

    // Declined: the plan names both targets, nothing changes. (`--nopretty` prints the plan
    // as tab-separated rows and no final tuple.)
    let out = env.run(&vars, &args, Some("n\n"));
    let all = text(&out);
    assert!(
        all.contains(&format!(
            "{}\ttimescaledb\t{}\t{ts_default}\tALTER EXTENSION timescaledb UPDATE;",
            names[0], old[0]
        )),
        "{all}"
    );
    assert!(
        all.contains(&format!("{}\ttimescaledb\t", names[1])),
        "{all}"
    );
    assert!(
        !all.contains(&names[2]),
        "an unnamed database is never planned: {all}"
    );
    assert_eq!(
        installed(rt, &specs[0], "timescaledb"),
        Some(old[0].clone())
    );
    let pretty_args = &args[..args.len() - 1];
    let out = env.run(&vars, pretty_args, Some("n\n"));
    let all = text(&out);
    assert!(!out.status.success(), "{all}");
    assert!(all.contains("Nothing was updated"), "{all}");
    assert_eq!(
        installed(rt, &specs[1], "timescaledb"),
        Some(old[1].clone())
    );

    // `-y`: the instance's database and the named one are updated.
    let mut yes_args = pretty_args.to_vec();
    yes_args.push("-y");
    let out = env.run(&vars, &yes_args, None);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    let updated = 2 + usize::from(toolkit_old.is_some());
    assert!(
        all.contains(&format!(
            "Updated {updated} extensions, failed 0, skipped 0"
        )),
        "{all}"
    );
    assert!(all.contains("TimescaleDB was updated"), "{all}");
    for spec in &specs[..2] {
        assert_eq!(installed(rt, spec, "timescaledb"), Some(ts_default.clone()));
    }
    if toolkit_old.is_some() {
        assert_eq!(
            installed(rt, &specs[0], "timescaledb_toolkit"),
            Some(default_version(rt, &specs[0], "timescaledb_toolkit"))
        );
    }
    // The unnamed database keeps its version.
    assert_eq!(
        installed(rt, &specs[2], "timescaledb"),
        Some(old[2].clone())
    );

    // Nothing left to do: no prompt, success.
    let out = env.run(&vars, pretty_args, Some(""));
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("Every extension is up to date"), "{all}");
}

/// `sql:main` on a local server with a stack compose file in the root, but the stack project has
/// no `db` container (or Docker does not answer): it is not the stack's database, so no
/// server-level change (telemetry, `shared_preload_libraries`) is planned or offered. The run
/// declines every question, so nothing changes on the shared test server either way.
#[test]
fn sql_main_without_a_stack_container_gets_no_server_changes() {
    let Some(uri) = std::env::var("MRSM_TEST_TIMESCALE_URI")
        .ok()
        .filter(|u| !u.is_empty())
    else {
        eprintln!("MRSM_TEST_TIMESCALE_URI is not set; skipping.");
        return;
    };
    let rt = rt();
    let base = ConnectionSpec::from_uri(&uri, Some("w17c_admin"), None).unwrap();
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let name = format!("w17ck2_{tag}_main");
    rt.block_on(run_first_statement(
        &base,
        None,
        &format!("CREATE DATABASE \"{name}\" TEMPLATE template0"),
    ))
    .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let spec = spec_for_database(&base, Some(&name));
        let old = create_old(&rt, &spec, "timescaledb").expect("an older TimescaleDB");
        rt.block_on(run_first_statement(
            &spec,
            None,
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
        ))
        .unwrap();
        let env = Env::new();
        let compose = env.root.join("stack").join("docker-compose.yaml");
        std::fs::create_dir_all(compose.parent().unwrap()).unwrap();
        std::fs::write(&compose, "services: {}\n").unwrap();
        let config = format!(
            r#"{{"meerschaum": {{"connectors": {{"sql": {{"main": {{"uri": "{}"}}}}}}}},
               "stack": {{"project_name": "w17ck2noctr{tag}"}},
               "shell": {{"updates": {{"check": false}}}},
               "formatting": {{"ansi": false, "unicode": false}}}}"#,
            uri_for(&uri, &name)
        );
        let out = env.run(
            &[("MRSM_CONFIG", config.as_str())],
            &["upgrade", "extensions", "-i", "sql:main", "--nopretty"],
            Some("n\nn\n"),
        );
        let all = text(&out);
        assert!(
            all.contains(&format!("{name}\ttimescaledb\t{old}\t")),
            "the instance is the scratch database: {all}"
        );
        assert!(!all.contains("(server)"), "{all}");
        assert!(!all.contains("telemetry"), "{all}");
        assert!(!all.contains("shared_preload_libraries"), "{all}");
        assert_eq!(installed(&rt, &spec, "timescaledb"), Some(old));
    }));
    rt.block_on(run_first_statement(
        &base,
        None,
        &format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"),
    ))
    .unwrap();
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
