//! End-to-end plugin actions and native callback writes against the released v4 fixture.
use meerschaum_connector_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};
use meerschaum_core::{Pipe, Value};
use std::path::{Path, PathBuf};
use std::process::Command;

fn command(root: &Path, plugins: &Path, python: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MRSM_") || matches!(key.as_str(), "RUST_LOG" | "NO_COLOR" | "TERM") {
            command.env_remove(key);
        }
    }
    command.env("MRSM_ROOT_DIR", root).env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_PYTHON", python)
        .env("PYTHONPATH", Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .env("MRSM_NOASK", "true")
        .env("MRSM_CONFIG", r#"{"meerschaum":{"instance":"sql:local"},"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false},"system":{"engine":{"python_workers":1}}}"#);
    command
}

#[test]
fn plugin_actions_fetch_and_callback_match_v4() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let python = repo.join(".venv-ref/bin/python");
    if !python.is_file() {
        eprintln!("Reference interpreter unavailable; skipping Python parity.");
        return;
    }
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/wiring_transcript.json")).unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("root");
    let plugins = temporary.path().join("plugins");
    std::fs::create_dir_all(root.join("sqlite")).unwrap();
    std::fs::create_dir_all(root.join("venvs")).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    #[cfg(unix)]
    for name in ["mrsm", "wiring", "wiring_sync", "wk_custom"] {
        std::os::unix::fs::symlink(
            repo.join(".venv-ref").canonicalize().unwrap(),
            root.join("venvs").join(name),
        )
        .unwrap();
    }
    for (name, source) in fixture["plugins"].as_object().unwrap() {
        std::fs::create_dir_all(plugins.join(name).parent().unwrap()).unwrap();
        std::fs::write(plugins.join(name), source.as_str().unwrap()).unwrap();
    }
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
    for (source, target) in [
        ("plugin:wiring", "wiring_fetch"),
        ("plugin:wiring_sync", "wiring_callback"),
    ] {
        let mut pipe = Pipe::builder(source, target)
            .instance("sql:local")
            .build()
            .unwrap();
        pipe.set_parameters(
            Value::from_json_str(
                &serde_json::json!({"columns":{"id":"id"}, "target":target}).to_string(),
            )
            .unwrap()
            .to_map(),
        );
        assert!(runtime.block_on(connector.register_pipe(&pipe)).success);
    }
    let mut child = Pipe::builder("sql:local", "wiring_child")
        .instance("sql:local")
        .build()
        .unwrap();
    child.set_parameters(Value::from_json_str(r#"{"columns":{"id":"id"},"target":"wiring_child","sql":"SELECT id, val * 2 AS val FROM wiring_fetch","parents":[{"connector":"plugin:wiring","metric":"wiring_fetch"}]}"#).unwrap().to_map());
    assert!(runtime.block_on(connector.register_pipe(&child)).success);
    for step in fixture["steps"].as_array().unwrap() {
        let args: Vec<_> = step
            .get("rust_args")
            .unwrap_or(&step["args"])
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let output = command(&root, &plugins, &python)
            .args(&args)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code().map(i64::from),
            step["rc"].as_i64(),
            "{args:?}\n{stdout}\n{stderr}"
        );
        if step["compare"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "stdout")
        {
            assert_eq!(
                stdout,
                step["stdout"].as_str().unwrap(),
                "{args:?}\nstderr={stderr}"
            );
        }
    }
    for (table, expected) in fixture["tables"].as_object().unwrap() {
        let frame = runtime
            .block_on(connector.read(&format!("SELECT id, val FROM {table} ORDER BY id")))
            .unwrap();
        let actual: Vec<serde_json::Value> = (0..frame.height())
            .map(|i| {
                serde_json::json!([
                    frame.columns()[0].data.values()[i]
                        .py_str()
                        .parse::<i64>()
                        .unwrap(),
                    frame.columns()[1].data.values()[i]
                        .py_str()
                        .parse::<i64>()
                        .unwrap()
                ])
            })
            .collect();
        assert_eq!(serde_json::Value::Array(actual), *expected, "{table}");
    }
    let instance: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("wkmem_wiring.json")).unwrap()).unwrap();
    assert_eq!(
        instance["pipes"]["plugin:wiring_sync|wiring_instance|None"]["rows"],
        fixture["instance_rows"]
    );
    // A pure plugin action must not open the configured instance merely to print output.
    let echo = command(&root, &plugins, &python)
        .env("MRSM_CONFIG", r#"{"meerschaum":{"instance":"sql:unconfigured"},"formatting":{"ansi":false,"unicode":false}}"#)
        .args(["wiring", "echo", "hello", "--wiring-count", "3"]).output().unwrap();
    assert!(
        echo.status.success(),
        "{}",
        String::from_utf8_lossy(&echo.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&echo.stdout),
        "wiring output: hello\n\n + count=3\n\n"
    );
    let help = command(&root, &plugins, &python)
        .arg("--help")
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&help.stdout).contains("--wiring-count"));
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("cache/plugins_manifest.json")).unwrap())
            .unwrap();
    assert!(
        manifest["plugins"]["wiring"]["actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["name"] == "wiring_echo" && a["has_completer"] == true)
    );
}
