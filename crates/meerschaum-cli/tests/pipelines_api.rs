//! Registered Pipeline lifecycle over an API registry, with exact SQL member references.
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::Pipe;
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command, Output},
    sync::Arc,
};

fn run(root: &Path, plugins: &Path, port: u16, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MRSM_") || ["TMUX", "TMUX_PANE", "RUST_LOG"].contains(&key.as_str()) {
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
            json!({"meerschaum": {"instance":"api:registry", "connectors": {
            "api": {"registry": {"host":"127.0.0.1", "port":port}},
            "sql": {"member": {"flavor":"sqlite", "database":root.join("member.db")}}
        }}, "formatting":{"ansi":false,"unicode":false}, "shell":{"updates":{"check":false}}})
            .to_string(),
        )
        .args(args)
        .output()
        .unwrap()
}
fn ok(root: &Path, plugins: &Path, port: u16, args: &[&str]) -> String {
    let output = run(root, plugins, port, args);
    assert!(
        output.status.success(),
        "{args:?}\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pipeline_lifecycle_uses_api_metadata_and_sql_members() {
    let base = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .unwrap()
        .join(".cache/mrsm-pipeline-api-tests");
    std::fs::create_dir_all(&base).unwrap();
    let temp = tempfile::tempdir_in(base).unwrap();
    let root = temp.path();
    let plugins = root.join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let registry = Arc::new(
        SqlConnector::from_uri(
            "registry",
            &format!("sqlite:///{}", root.join("registry.db").display()),
        )
        .await
        .unwrap(),
    );
    let member = Arc::new(
        SqlConnector::from_uri(
            "member",
            &format!("sqlite:///{}", root.join("member.db").display()),
        )
        .await
        .unwrap(),
    );
    let mut cfg = ServerConfig::for_root("sql:registry", root);
    cfg.no_auth = true;
    cfg.no_dash = true;
    cfg.no_webterm = true;
    let state = AppState::builder(cfg, registry.clone())
        .secret(b"01234567890123456789012345678901".to_vec())
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        axum::serve(listener, meerschaum_server::app(state))
            .await
            .unwrap();
    });
    let sql = "SELECT 1 AS id -- keep {{ Pipe('sql:member', 'source') }}";
    ok(
        root,
        &plugins,
        port,
        &[
            "register",
            "pipes",
            "-i",
            "sql:member",
            "-c",
            "sql:member",
            "-m",
            "source",
            "--params",
            &json!({"columns":{"id":"id"}, "sql":sql}).to_string(),
            "-y",
        ],
    );
    let reference = "sql:member|sql:member|source|None";
    ok(
        root,
        &plugins,
        port,
        &[
            "register",
            "pipelines",
            "daily",
            "-i",
            "api:registry",
            "--params",
            &json!({"targets":[reference]}).to_string(),
        ],
    );
    let definitions: Value = serde_json::from_str(&ok(
        root,
        &plugins,
        port,
        &["show", "pipelines", "-i", "api:registry", "--nopretty"],
    ))
    .unwrap();
    assert_eq!(definitions[0]["targets"], json!([reference]));
    assert_eq!(definitions[0]["registry"], "api:registry");
    ok(
        root,
        &plugins,
        port,
        &[
            "edit",
            "pipelines",
            "daily",
            "-i",
            "api:registry",
            "--params",
            &json!({"revision":definitions[0]["revision"],"description":"Edited remotely"})
                .to_string(),
        ],
    );
    let pipe = Pipe::builder("sql:member", "source")
        .instance("sql:member")
        .build()
        .unwrap();
    ok(
        root,
        &plugins,
        port,
        &[
            "analyze",
            "pipelines",
            "daily",
            "-i",
            "api:registry",
            "--nopretty",
        ],
    );
    let attrs = member.get_pipe_attributes(&pipe).await.unwrap();
    assert!(attrs["parameters"].get("tags").is_none());
    ok(
        root,
        &plugins,
        port,
        &[
            "analyze",
            "pipelines",
            "daily",
            "-i",
            "api:registry",
            "--yes",
        ],
    );
    let attrs = member.get_pipe_attributes(&pipe).await.unwrap();
    assert_eq!(
        attrs["parameters"].get("tags").unwrap().to_json_string(),
        "[\"pipeline-daily\"]"
    );
    assert_eq!(attrs["parameters"].get("sql").unwrap().as_str(), Some(sql));
    ok(
        root,
        &plugins,
        port,
        &[
            "delete",
            "pipelines",
            "daily",
            "-i",
            "api:registry",
            "--yes",
        ],
    );
    let definitions: Value = serde_json::from_str(&ok(
        root,
        &plugins,
        port,
        &["show", "pipelines", "-i", "api:registry", "--nopretty"],
    ))
    .unwrap();
    assert_eq!(definitions, json!([]));
    let attrs = member.get_pipe_attributes(&pipe).await.unwrap();
    assert!(
        !attrs["parameters"]
            .get("tags")
            .unwrap()
            .to_json_string()
            .contains("pipeline-daily")
    );
    assert_eq!(attrs["parameters"].get("sql").unwrap().as_str(), Some(sql));
    assert!(member.get_pipe_id(&pipe).await.unwrap().is_some());
    task.abort();
}
