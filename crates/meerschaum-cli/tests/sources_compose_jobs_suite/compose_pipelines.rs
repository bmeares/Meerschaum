//! Compose declarations use the same saved registry and execution path as the CLI.
use meerschaum_cli::native::Session;
use meerschaum_core::config::ConfigOptions;
use meerschaum_core::{Config, Map, Pipe, Value};
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn run(path: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(args)
        .current_dir(path)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", path.join("home"))
        .env("MRSM_ROOT_DIR", path.join("host"))
        .env("MRSM_PLUGINS_DIR", path.join("plugins"))
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}
fn success(output: Output) -> String {
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{text}");
    text
}

#[test]
fn compose_pipeline_survives_reapply_and_runs_through_the_native_registry() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path();
    for dir in ["home", "host", "plugins"] {
        std::fs::create_dir(path.join(dir)).unwrap();
    }
    let database = path.join("instance.db");
    let body = format!(
        r#"
project_name: example
root_dir: ./root
plugins_dir: ./plugins
config:
  meerschaum:
    instance: sql:demo
    connectors:
      sql:
        demo: {{flavor: sqlite, database: '{}'}}
pipelines:
  daily:
    description: Example
    defaults: {{workers: 1}}
    pipes:
      - {{connector: 'sql:demo', metric: raw}}
      - {{connector: 'sql:demo', metric: totals}}
pipes:
  - connector: sql:demo
    metric: raw
    parameters:
      columns: {{id: id}}
      sql: SELECT 1 AS id
  - connector: sql:demo
    metric: totals
    parameters:
      columns: {{id: id}}
      sql: SELECT id, id + 1 AS value FROM {{{{ Pipe('sql:demo', 'raw') }}}}
"#,
        database.display().to_string().replace('\\', "/")
    );
    let file = path.join("mrsm-compose.yaml");
    std::fs::write(&file, &body).unwrap();
    let explained = success(run(path, &["compose", "explain"]));
    assert!(explained.contains("Pipelines in 'example'") && explained.contains("daily"));
    assert!(!database.exists() && !path.join("root").exists());
    success(run(path, &["compose", "up", "--dry"]));

    let opts = ConfigOptions::with_env(vec![
        ("HOME".into(), path.join("home").display().to_string()),
        (
            "MRSM_ROOT_DIR".into(),
            path.join("host").display().to_string(),
        ),
        (
            "MRSM_SQL_DEMO".into(),
            serde_json::json!({"flavor":"sqlite","database":database}).to_string(),
        ),
    ]);
    let config = Config::new(opts).unwrap();
    let session = Session::new();
    let instance = session.instance("sql:demo", &config).unwrap();
    let mut registration = Pipe::builder("mrsm", "pipeline")
        .location(Some("daily"))
        .instance("sql:demo")
        .build()
        .unwrap();
    let read = || -> Map {
        session
            .block_on(instance.conn().get_pipe_attributes(&registration))
            .unwrap()["parameters"]
            .to_map()
    };
    let first = read();
    let definition = first["pipeline"].to_map();
    assert_eq!(definition["revision"], Value::Int(1));
    let identity = definition["id"].clone();
    success(run(path, &["compose", "up", "--dry"]));
    assert_eq!(read()["pipeline"].get("revision"), Some(&Value::Int(1)));
    success(run(
        path,
        &["compose", "sync", "pipelines", "daily", "-i", "sql:demo"],
    ));
    let totals = Pipe::builder("sql:demo", "totals")
        .instance("sql:demo")
        .build()
        .unwrap();
    assert_eq!(
        session
            .block_on(
                instance
                    .conn()
                    .get_pipe_rowcount(&totals, &Default::default())
            )
            .unwrap(),
        Some(1)
    );

    let mut params = read();
    let mut def = params["pipeline"].to_map();
    def.insert(
        "layout".into(),
        Value::from_json_str(r#"{"raw":{"x":12}}"#).unwrap(),
    );
    params.insert("pipeline".into(), Value::Map(def));
    params.insert(
        "access".into(),
        Value::from_json_str(r#"{"visibility":"private","owner":"alice"}"#).unwrap(),
    );
    registration.set_parameters(params);
    assert!(
        session
            .block_on(instance.conn().edit_pipe(&registration, false))
            .success
    );
    std::fs::write(
        &file,
        body.replace("description: Example", "description: Updated"),
    )
    .unwrap();
    success(run(path, &["compose", "up", "--dry"]));
    let changed = session
        .block_on(instance.conn().get_pipe_attributes(&registration))
        .unwrap()["parameters"]
        .to_map();
    assert_eq!(changed["pipeline"].get("id"), Some(&identity));
    assert_eq!(changed["pipeline"].get("revision"), Some(&Value::Int(2)));
    assert_eq!(
        changed["pipeline"]
            .get("layout")
            .unwrap()
            .get("raw")
            .unwrap()
            .get("x"),
        Some(&Value::Int(12))
    );
    assert_eq!(changed["access"].get("owner"), Some(&Value::from("alice")));
    success(run(
        path,
        &[
            "compose",
            "edit",
            "pipelines",
            "daily",
            "-i",
            "sql:demo",
            "--params",
            r#"{"revision":2,"description":"CLI edit"}"#,
            "--noask",
        ],
    ));
    let after = session
        .block_on(instance.conn().get_pipe_attributes(&registration))
        .unwrap()["parameters"]
        .to_map();
    assert_eq!(
        after["compose"].get("project_name"),
        Some(&Value::from("example"))
    );
    assert_eq!(after["access"].get("owner"), Some(&Value::from("alice")));
    session.shutdown();
}
