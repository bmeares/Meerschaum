//! Logical API aliases must win before native defaults, caches, and driver selection.

use std::collections::BTreeSet;

use meerschaum_cli::native::{Instance, Session};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::team_scope::api_execution::{self, ALIAS_ATTRIBUTE};
use meerschaum_core::{Map, Value};

#[test]
fn aliases_preserve_keys_and_prevent_cached_or_explicit_native_sql_resolution() {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let config = Config::new(ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.path().display().to_string(),
        ),
    ]))
    .unwrap();
    let session = Session::new();
    session.set_python_worker(false);
    // A previously cached native connector cannot outlive the authority of the new snapshot.
    assert!(matches!(
        session.instance("sql:local", &config).unwrap(),
        Instance::Sql(_)
    ));
    let aliases = BTreeSet::from([
        "sql:local".into(),
        "valkey:team".into(),
        "api:remote".into(),
    ]);
    config.set_config(
        api_execution::configuration(
            &config.snapshot(),
            &aliases,
            "sql:local",
            "http://127.0.0.1:1",
            &root.path().join("credential.json"),
        )
        .unwrap(),
    );
    for keys in &aliases {
        let instance = session.instance(keys, &config).unwrap();
        assert!(instance.sql().is_none());
        assert_eq!(instance.keys(), *keys);
        assert_eq!(instance.type_name(), keys.split_once(':').unwrap().0);
    }
    assert!(session.sql_connector("local", &config).is_err());
    assert!(session.instance("sql:main", &config).is_err());
    assert!(session.instance("valkey:main", &config).is_err());
}

#[test]
fn malformed_descriptors_never_fall_back_to_native_driver_attributes() {
    let root = tempfile::tempdir().unwrap();
    let config = Config::new(ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.path().join("plugins").display().to_string(),
        ),
    ]))
    .unwrap();
    let mut projected = api_execution::configuration(
        &Map::new(),
        &BTreeSet::from(["sql:team".into()]),
        "sql:team",
        "http://127.0.0.1:1",
        &root.path().join("credential.json"),
    )
    .unwrap();
    let attrs = projected
        .get_mut("meerschaum")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .get_mut("connectors")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .get_mut("sql")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .get_mut("team")
        .unwrap()
        .as_map_mut()
        .unwrap();
    assert!(attrs.contains_key(ALIAS_ATTRIBUTE));
    attrs.insert("flavor".into(), "sqlite".into());
    let forbidden = root.path().join("must-not-create.sqlite");
    attrs.insert(
        "database".into(),
        Value::from(forbidden.display().to_string()),
    );
    config.set_config(projected);
    let session = Session::new();
    session.set_python_worker(false);
    assert!(session.instance("sql:team", &config).is_err());
    assert!(session.sql_connector("team", &config).is_err());
    assert!(!forbidden.exists());
}
