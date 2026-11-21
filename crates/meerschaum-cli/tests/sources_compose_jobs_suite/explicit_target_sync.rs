//! `mrsm sync pipes` over a `{{ Pipe() }}` chain whose referenced pipe sets an explicit
//! `target` (wave 8 task O): the reference must render the registered table, not the default
//! name (it failed with `no such table` before). Temporary root, empty plugins directory.

use std::path::PathBuf;
use std::process::Command;

use meerschaum_connector::DataQuery;
use meerschaum_connector_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};
use meerschaum_core::{Pipe, Value};

fn pipe(metric: &str, sql: &str, target: &str) -> Pipe {
    let mut pipe = Pipe::builder("sql:local", metric)
        .instance("sql:local")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(
            &serde_json::json!({
                "columns": {"datetime": "ts", "id": "site"},
                "dtypes": {"ts": "datetime"},
                "sql": sql,
                "target": target,
                "tags": ["explicit"],
            })
            .to_string(),
        )
        .unwrap()
        .to_map(),
    );
    pipe
}

fn mrsm(root: &PathBuf, plugins: &PathBuf, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MRSM_") || matches!(key.as_str(), "RUST_LOG" | "NO_COLOR" | "TERM") {
            command.env_remove(key);
        }
    }
    command
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_NOASK", "true")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env(
            "MRSM_CONFIG",
            r#"{"meerschaum":{"instance":"sql:local"},"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false}}"#,
        )
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn sync_pipes_renders_the_explicit_target_of_a_referenced_pipe() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let plugins = tmp.path().join("plugins");
    std::fs::create_dir_all(root.join("sqlite")).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            root.join("sqlite/mrsm_local.db")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    };
    let connector = runtime
        .block_on(SqlConnector::connect(
            "local",
            ConnectionSpec::from_attributes(&attrs).unwrap(),
        ))
        .unwrap();
    let queries = [
        "CREATE TABLE readings (ts TEXT, site TEXT, temp REAL)".to_string(),
        "INSERT INTO readings VALUES ('2024-01-01 00:00:00', 'a', 1.0), \
         ('2024-01-02 00:00:00', 'b', 2.0)"
            .to_string(),
    ];
    assert!(
        runtime
            .block_on(connector.exec_queries(&queries, true, true))
            .iter()
            .all(Option::is_some)
    );
    let raw = pipe("raw", "SELECT * FROM readings", "custom_raw_table");
    let clean = pipe(
        "clean",
        "SELECT ts, site, temp * 2 AS temp FROM \
         {{ Pipe('sql:local', 'raw', instance='sql:local') }}",
        "custom_clean_table",
    );
    for p in [&raw, &clean] {
        assert!(runtime.block_on(connector.register_pipe(p)).success);
    }

    let out = mrsm(
        &root,
        &plugins,
        &[
            "sync",
            "pipes",
            "-i",
            "sql:local",
            "-t",
            "explicit",
            "-w",
            "1",
        ],
    );
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "{text}");
    assert!(!text.contains("no such table"), "{text}");

    for (pipe, rows) in [(&raw, 2), (&clean, 2)] {
        let n = runtime
            .block_on(connector.get_pipe_rowcount(pipe, &DataQuery::default()))
            .unwrap();
        assert_eq!(n, Some(rows), "{pipe}: {text}");
    }
}
