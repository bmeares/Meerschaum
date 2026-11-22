//! Tests of the config registry: lookups and their fallbacks, file reading and writing,
//! symlink substitution and the environment's connectors and patches.

use super::*;

#[test]
fn connector_env_vars() {
    let env: Vec<(String, String)> = [
        ("MRSM_SQL_FOO", "sqlite://"),
        ("MRSM_ROOT_DIR", "/x"),
        ("MRSM_SQL_A.B", "x"),
        ("MRSM_API_MY-LABEL", "http://x"),
        ("MRSM_API_HOST_PROXY_TOKEN", "synthetic-internal-token"),
        ("MRSM_API_HOSTED", "http://x"),
        ("HOME", "/home"),
    ]
    .iter()
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .collect();
    assert_eq!(
        get_connector_env_vars(&env),
        vec!["MRSM_SQL_FOO", "MRSM_API_MY-LABEL", "MRSM_API_HOSTED"]
    );
    assert_eq!(
        parse_connector_env_var("MRSM_SQL_MY_DB"),
        Some(("sql".to_string(), "my_db".to_string()))
    );
}

#[test]
fn native_execution_markers_cannot_create_raw_api_connectors() {
    let root = tempfile::tempdir().unwrap();
    let mut env = vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_API_DATA".into(),
            "https://fixture.invalid:8443".into(),
        ),
    ];
    env.extend(
        NATIVE_EXECUTION_ENV_VARS
            .iter()
            .map(|name| ((*name).into(), "1".into())),
    );
    assert_eq!(
        get_connector_env_vars(&env),
        vec!["MRSM_API_DATA".to_string()]
    );
    let config = Config::new(ConfigOptions::with_env(
        env.into_iter()
            .filter(|(name, _)| {
                matches!(
                    name.as_str(),
                    "MRSM_ROOT_DIR" | "MRSM_API_DATA" | "MRSM_API_EXECUTION"
                )
            })
            .collect(),
    ))
    .unwrap();
    assert!(
        config
            .get(&["meerschaum", "connectors", "api", "execution"])
            .is_none()
    );
    assert_eq!(
        config
            .get(&["meerschaum", "connectors", "api", "data", "uri"])
            .and_then(|value| value.as_str().map(str::to_owned)),
        Some("https://fixture.invalid:8443".into())
    );
}

#[test]
fn defaults_have_markers_resolved() {
    let d = default_config("/root", 8, false);
    assert_eq!(
        Value::Map(d.clone()).get_path(&["system", "cli", "max_daemons"]),
        Some(&Value::Int(24))
    );
    assert_eq!(
        Value::Map(d).get_path(&["api", "uvicorn", "workers"]),
        Some(&Value::Int(4))
    );
}

#[test]
fn missing_legacy_defaults_and_connector_refresh_keep_execution_aliases_atomic() {
    use crate::team_scope::api_execution::{Alias, VERSION};
    let root = tempfile::tempdir().unwrap();
    let config_dir = root.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let mut options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        ("MRSM_CONFIG_DIR".into(), config_dir.display().to_string()),
    ]);
    options.allow_writes = false;
    options.home = root.path().into();
    let config = Config::new(options).unwrap();
    let mut alias = Alias {
        version: VERSION,
        logical_keys: "sql:main".into(),
        callback_url: "http://127.0.0.1:12345".into(),
        credential_file: root.path().join("credential.json"),
    };
    let snapshot = |attributes: Map| {
        Map::from_iter([
            (
                "meerschaum".into(),
                Value::Map(Map::from_iter([(
                    "connectors".into(),
                    Value::Map(Map::from_iter([(
                        "sql".into(),
                        Value::Map(Map::from_iter([("main".into(), Value::Map(attributes))])),
                    )])),
                )])),
            ),
            ("pipes".into(), Value::Map(Map::new())),
        ])
    };
    config.set_config(snapshot(alias.attributes()));
    // This is a real missing legacy key: its parity fallback merges defaults into every
    // loaded section, which formerly restored shipped sql:main native attributes.
    assert!(
        config
            .get(&["pipes", "parameters", "verify", "chunk_minutes"])
            .is_some()
    );
    let exact = config
        .get(&["meerschaum", "connectors", "sql", "main"])
        .unwrap()
        .to_map();
    assert_eq!(exact, alias.attributes());
    assert_eq!(
        Alias::from_attributes(&exact, "sql:main").unwrap(),
        Some(alias.clone())
    );
    assert!(
        config
            .get(&["meerschaum", "connectors", "sql", "main", "database"])
            .is_none()
    );
    alias.callback_url = "http://127.0.0.1:12346".into();
    let refreshed = Value::Map(Map::from_iter([(
        "connectors".into(),
        Value::Map(Map::from_iter([(
            "sql".into(),
            Value::Map(Map::from_iter([(
                "main".into(),
                Value::Map(alias.attributes()),
            )])),
        )])),
    )]));
    std::fs::write(
        config_dir.join("meerschaum.json"),
        refreshed.to_json_string(),
    )
    .unwrap();
    assert!(config.refresh_connectors());
    assert_eq!(
        config
            .get(&["meerschaum", "connectors", "sql", "main"])
            .unwrap()
            .to_map(),
        alias.attributes()
    );
    let mut malformed = alias.attributes();
    malformed.insert("password".into(), "synthetic-explicit-invalid".into());
    config.set_config(snapshot(malformed.clone()));
    assert!(
        config
            .get(&["pipes", "parameters", "verify", "chunk_minutes"])
            .is_some()
    );
    let mixed = config
        .get(&["meerschaum", "connectors", "sql", "main"])
        .unwrap()
        .to_map();
    assert_eq!(
        mixed, malformed,
        "Default fallback silently repaired a malformed explicit alias"
    );
    assert!(Alias::from_attributes(&mixed, "sql:main").is_err());
}

#[test]
fn initial_environment_patch_never_merges_native_defaults_into_execution_alias() {
    use crate::team_scope::api_execution::{self, Alias};
    let root = tempfile::tempdir().unwrap();
    let snapshot = api_execution::configuration(
        &Map::new(),
        &std::collections::BTreeSet::from(["sql:main".to_string()]),
        "sql:main",
        "http://127.0.0.1:12345",
        &root.path().join("credential.json"),
    )
    .unwrap();
    let file = root.path().join("api-config.json");
    std::fs::write(&file, Value::Map(snapshot.clone()).to_json_string()).unwrap();
    for variable in ["MRSM_CONFIG", "MRSM_CONFIG_FILE"] {
        let patch = if variable.ends_with("_FILE") {
            file.display().to_string()
        } else {
            Value::Map(snapshot.clone()).to_json_string()
        };
        let mut options = ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            (variable.into(), patch),
        ]);
        options.allow_writes = false;
        options.home = root.path().into();
        let config = Config::new(options).unwrap();
        let attributes = config
            .get(&["meerschaum", "connectors", "sql", "main"])
            .unwrap()
            .to_map();
        assert_eq!(
            attributes.len(),
            1,
            "{variable} restored native default keys: {:?}",
            attributes.keys().collect::<Vec<_>>()
        );
        assert!(
            Alias::from_attributes(&attributes, "sql:main")
                .unwrap()
                .is_some()
        );
        assert!(
            config
                .get(&["meerschaum", "connectors", "sql", "main", "password"])
                .is_none()
        );
        assert_eq!(
            config.get(&["meerschaum", "connectors", "sql", "default"]),
            Some(Value::Null)
        );
    }
    // A caller's explicitly mixed descriptor remains invalid through the same overlay.
    let mut malformed = snapshot;
    malformed
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
        .get_mut("main")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("password".into(), "synthetic-explicit-invalid".into());
    let mut options = ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        ("MRSM_CONFIG".into(), Value::Map(malformed).to_json_string()),
    ]);
    options.allow_writes = false;
    options.home = root.path().into();
    let config = Config::new(options).unwrap();
    let attributes = config
        .get(&["meerschaum", "connectors", "sql", "main"])
        .unwrap()
        .to_map();
    assert_eq!(
        attributes.len(),
        2,
        "An incoming malformed alias was silently repaired"
    );
    assert!(Alias::from_attributes(&attributes, "sql:main").is_err());
}

fn scratch(env: &[(&str, &str)]) -> (tempfile::TempDir, Config) {
    let root = tempfile::tempdir().unwrap();
    let mut vars = vec![(
        "MRSM_ROOT_DIR".to_string(),
        root.path().to_string_lossy().into_owned(),
    )];
    vars.extend(env.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let config = Config::new(ConfigOptions::with_env(vars)).unwrap();
    (root, config)
}

#[test]
fn a_failing_yaml_dump_leaves_the_file_alone() {
    let (root, config) = scratch(&[]);
    let directory = root.path().join("config");
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("meerschaum.yaml");
    std::fs::write(&path, "instance: sql:keep\n").unwrap();
    let new = ConfigValue::map_from_value(
        Value::from_json_str(r#"{"meerschaum": {"instance": "sql:new"}}"#)
            .unwrap()
            .as_map()
            .unwrap(),
    );

    let failing = |_: &ConfigValue| Err("cannot represent an object".to_string());
    let wrote = config.with_state(|s| s.write_config_with(Some(&new), Some(&directory), &failing));
    assert!(!wrote);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "instance: sql:keep\n"
    );
    let warnings = config.take_warnings();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("FAILED TO WRITE") && w.contains("cannot represent")),
        "{warnings:?}"
    );

    assert!(config.with_state(|s| s.write_config_lossless(Some(&new), Some(&directory))));
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .contains("instance: sql:new")
    );
}

#[test]
fn a_date_in_a_config_file_survives_get_and_write() {
    let (root, config) = scratch(&[]);
    let directory = root.path().join("config");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
            directory.join("typed.yaml"),
            "when: 2024-01-01\nstamp: 2024-01-01T12:30:00Z\nraw: !!binary eHl6\nbig: 123456789012345678901234567890\nkeys:\n  1: number\n  b: text\nlink: MRSM{typed:when}\ntext: 'at MRSM{typed:stamp}'\n",
        )
        .unwrap();
    let (valid, value) = config.get_with_lossless(&["typed"], GetOptions::default());
    assert!(valid);
    let map = value.as_map().unwrap();
    assert!(matches!(map["when"], ConfigValue::Date(_)));
    assert!(matches!(map["stamp"], ConfigValue::DateTime(_)));
    assert_eq!(map["raw"], ConfigValue::Bytes(b"xyz".to_vec()));
    assert!(matches!(map["big"], ConfigValue::BigInt(_)));
    // The symlink to a date is that date; embedded in text it is `str(datetime)`.
    assert_eq!(map["link"], map["when"]);
    assert_eq!(
        map["text"],
        ConfigValue::Str("at 2024-01-01 12:30:00+00:00".into())
    );
    // Substitution went through a JSON text: the typed keys are strings now, as in v4.
    assert!(map["keys"].as_map().is_some());

    let out = tempfile::tempdir().unwrap();
    let mut one = ConfigMap::new();
    one.insert("typed".into(), value);
    assert!(config.with_state(|s| s.write_config_lossless(Some(&one), Some(out.path()))));
    let text = std::fs::read_to_string(out.path().join("typed.yaml")).unwrap();
    assert!(text.contains("when: 2024-01-01"), "{text}");
    assert!(text.contains("raw: !!binary |"), "{text}");
}

#[test]
fn unreadable_json_values_are_reported_like_python() {
    let (root, config) = scratch(&[]);
    let directory = root.path().join("config");
    std::fs::create_dir_all(&directory).unwrap();
    let mut one = ConfigMap::new();
    one.insert(
        "plain".into(),
        ConfigValue::from_node(&crate::yaml::node::Node::Date(crate::yaml::node::Date {
            year: 2024,
            month: 1,
            day: 1,
        })),
    );
    let mut wrapper = ConfigMap::new();
    let mut inner = ConfigMap::new();
    inner.insert("filetype".into(), ConfigValue::str("json"));
    inner.insert("when".into(), one["plain"].clone());
    wrapper.insert("plain".into(), ConfigValue::Map(inner));
    assert!(!config.with_state(|s| s.write_config_lossless(Some(&wrapper), Some(&directory))));
    assert!(!directory.join("plain.json").exists());
    let notices = config.take_notices();
    assert_eq!(
            notices,
            vec![Notice::Print(
                "FAILED TO WRITE!\nObject of type date is not JSON serializable\n{'sort_keys': False, 'indent': 2}"
                    .to_string()
            )]
        );
}
