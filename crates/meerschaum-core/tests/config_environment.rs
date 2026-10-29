use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::{Map, Value};

fn map(json: &str) -> Map {
    Value::from_json_str(json).unwrap().to_map()
}

fn registry(root: &std::path::Path, extra: &[(&str, &str)]) -> Config {
    let mut env = vec![("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned())];
    env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = false;
    Config::new(opts).unwrap()
}

fn env(vars: &[(&str, &str)]) -> Vec<(String, String)> {
    vars.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn reusable_layers_preserve_symlink_targets_and_patch_order_without_writes() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), &[]);
    config.set_config(map(r#"{"custom":{"host":"file-host"},"meerschaum":{"connectors":{"sql":{"example":{"host":"MRSM{custom:host}","database":"file.db"}}}}}"#));
    config.apply_environment_patches(&env(&[
        (
            "MRSM_CONFIG",
            r#"{"custom":{"host":"config-host","only_config":true}}"#,
        ),
        ("MRSM_PATCH", r#"{"custom":{"host":"patch-host"}}"#),
    ]));
    assert_eq!(
        config.apply_environment_uris(&env(&[
            ("MRSM_SQL_EXAMPLE", r#"{"port":5559}"#),
            ("MRSM_API_OTHER", "https://example.invalid"),
        ])),
        2
    );
    assert_eq!(
        config.get(&["custom", "host"]),
        Some(Value::from("patch-host"))
    );
    assert_eq!(
        config.get(&["custom", "only_config"]),
        Some(Value::Bool(true))
    );
    assert_eq!(
        config.get(&["meerschaum", "connectors", "sql", "example", "host"]),
        Some(Value::from("patch-host"))
    );
    assert_eq!(
        config.get(&["meerschaum", "connectors", "sql", "example", "database"]),
        Some(Value::from("file.db"))
    );
    assert!(config.take_warnings().is_empty());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn malformed_layers_warn_skip_patch_and_keep_literal_uri() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), &[]);
    config.set_config(map(r#"{"custom":{"keep":true}}"#));
    config.apply_environment_patches(&env(&[
        ("MRSM_CONFIG", "{"),
        ("MRSM_PATCH", r#"{"custom":{"valid":true}}"#),
    ]));
    config.apply_environment_uris(&env(&[("MRSM_SQL_BROKEN", "{invalid}")]));
    assert_eq!(config.get(&["custom", "keep"]), Some(Value::Bool(true)));
    assert_eq!(config.get(&["custom", "valid"]), Some(Value::Bool(true)));
    assert_eq!(
        config.get(&["meerschaum", "connectors", "sql", "broken", "uri"]),
        Some(Value::from("{invalid}"))
    );
    let warnings = config.take_warnings();
    assert_eq!(warnings.len(), 2, "{warnings:?}");
    assert!(warnings[0].contains("MRSM_CONFIG"));
    assert!(warnings[1].contains("sql:broken"));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn initialization_still_applies_uris_before_config_and_patch() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(
        root.path(),
        &[
            ("MRSM_SQL_EXAMPLE", r#"{"host":"uri-host","port":5559}"#),
            (
                "MRSM_CONFIG",
                r#"{"meerschaum":{"connectors":{"sql":{"example":{"host":"config-host"}}}}}"#,
            ),
            (
                "MRSM_PATCH",
                r#"{"meerschaum":{"connectors":{"sql":{"example":{"host":"patch-host"}}}}}"#,
            ),
        ],
    );
    assert_eq!(
        config.get(&["meerschaum", "connectors", "sql", "example", "host"]),
        Some(Value::from("patch-host"))
    );
    assert_eq!(
        config.get(&["meerschaum", "connectors", "sql", "example", "port"]),
        Some(Value::Int(5559))
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn variables_of_unknown_connector_types_are_ignored() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), &[]);
    let applied = config.apply_environment_uris(&env(&[
        ("MRSM_SQL_REAL", "sqlite:///x.db"),
        ("MRSM_DUCKDB_LIB", "/opt/libduckdb.so"),
        ("MRSM_CI_MIN_FREE_GB", "10"),
        ("MRSM_TEST_PG_URI", "postgresql://x"),
        ("MRSM_MQTTDEMO_BROKER", r#"{"host":"localhost"}"#),
    ]));
    // Only the built-in type: no plugin registers `mqttdemo` here.
    assert_eq!(applied, 1);
    let notices = config.take_notices();
    assert!(
        notices
            .iter()
            .all(|n| matches!(n, meerschaum_core::config::Notice::Debug(_))),
        "{notices:?}"
    );
    assert!(
        config
            .get(&["meerschaum", "connectors", "duckdb"])
            .is_none()
    );
    assert!(config.get(&["meerschaum", "connectors", "ci"]).is_none());
    assert!(config.get(&["meerschaum", "connectors", "test"]).is_none());
    assert!(
        config
            .get(&["meerschaum", "connectors", "mqttdemo"])
            .is_none()
    );
    assert!(
        notices.iter().any(|n| matches!(n,
            meerschaum_core::config::Notice::Debug(t) if t.contains("MRSM_DUCKDB_LIB"))),
        "{notices:?}"
    );
}

#[test]
fn variables_of_plugin_connector_types_are_read() {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir_all(plugins.join("mqttdemo")).unwrap();
    std::fs::write(
        plugins.join("mqttdemo/__init__.py"),
        "from meerschaum.connectors import make_connector, Connector\n\n@make_connector\nclass MQTTDemoConnector(Connector):\n    pass\n",
    )
    .unwrap();
    let config = registry(
        root.path(),
        &[("MRSM_PLUGINS_DIR", plugins.to_str().unwrap())],
    );
    // A type the cached manifest lists is known too, with no plugin source present.
    std::fs::create_dir_all(root.path().join("cache")).unwrap();
    std::fs::write(
        root.path().join("cache/plugins_manifest.json"),
        r#"{"connector_types":{"viamanifest":{"plugin":"p"}}}"#,
    )
    .unwrap();
    let applied = config.apply_environment_uris(&env(&[
        ("MRSM_MQTTDEMO_BROKER", r#"{"host":"localhost"}"#),
        ("MRSM_VIAMANIFEST_X", r#"{"host":"h"}"#),
        ("MRSM_NOTATYPE_X", r#"{"host":"h"}"#),
    ]));
    assert_eq!(applied, 2);
    assert_eq!(
        config.get(&["meerschaum", "connectors", "mqttdemo", "broker", "host"]),
        Some(Value::from("localhost"))
    );
    assert_eq!(
        config.get(&["meerschaum", "connectors", "viamanifest", "x", "host"]),
        Some(Value::from("h"))
    );
    assert!(
        config
            .get(&["meerschaum", "connectors", "notatype"])
            .is_none()
    );
}
