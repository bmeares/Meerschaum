//! 5.1 secret handling in the config registry: `MRSM{file:...}` / `MRSM{env:...}` references,
//! the `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` variables, provenance and the writer that never
//! persists what the environment or a reference holds, and the permission warning.
//!
//! Every value is synthetic; each test works in its own temporary root.

use std::path::{Path, PathBuf};
use std::sync::Once;

use meerschaum_core::Value;
use meerschaum_core::config::secrets;
use meerschaum_core::config::{Config, ConfigError, ConfigOptions};

/// The environment variables the `env:` references of these tests read (set once, before any
/// test reads them).
const ENV_SECRET: &str = "env-s3cret-for-tests";
const ENV_VAR: &str = "MRSM_TEST_SECRET_CONFIG_VALUE";
static SET_ENV: Once = Once::new();

fn set_env() {
    SET_ENV.call_once(|| {
        // SAFETY: set once, before any test of this binary reads it, and never changed again;
        // the variable is private to these tests.
        unsafe { std::env::set_var(ENV_VAR, ENV_SECRET) };
    });
}

fn config_with(root: &Path, extra: &[(&str, String)]) -> Config {
    set_env();
    let mut env = vec![
        (
            "MRSM_ROOT_DIR".to_string(),
            root.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".to_string(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
    ];
    env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.clone())));
    let mut opts = ConfigOptions::with_env(env);
    opts.home = root.to_path_buf();
    Config::new(opts).unwrap()
}

fn write(root: &Path, name: &str, text: &str) -> PathBuf {
    let dir = root.join("config");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path
}

fn get(config: &Config, keys: &[&str]) -> Option<Value> {
    config.get(keys)
}

fn s(text: &str) -> Value {
    Value::Str(text.to_string())
}

#[test]
fn file_and_env_references_resolve_wherever_symlinks_work() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    std::fs::write(root.join("relative-secret"), "rel-s3cret\n").unwrap();
    let absolute = root.join("abs:with:colons");
    std::fs::write(&absolute, "abs-s3cret\r\n").unwrap();
    std::fs::write(root.join("two-newlines"), "keep-one\n\n").unwrap();
    let quoted = root.join("quoted");
    std::fs::write(&quoted, "q\"uo\\te").unwrap();
    write(
        root,
        "meerschaum.yaml",
        &format!(
            "connectors:\n  sql:\n    filed:\n      flavor: postgresql\n      host: localhost\n      \
             password: MRSM{{file:relative-secret}}\n      username: MRSM{{file:{}}}\n      \
             database: MRSM{{file:two-newlines}}\n    envy:\n      flavor: postgresql\n      \
             uri: postgresql://u:MRSM{{env:{ENV_VAR}}}@h:5432/db\n      password: \
             MRSM{{file:{}}}\n",
            absolute.display(),
            quoted.display()
        ),
    );
    let config = config_with(
        root,
        &[
            (
                "MRSM_CONFIG",
                format!(
                    r#"{{"meerschaum": {{"connectors": {{"api": {{"patched": {{"host": "h", "password": "MRSM{{env:{ENV_VAR}}}"}}}}}}}}}}"#
                ),
            ),
            (
                "MRSM_PATCH",
                r#"{"plugins": {"secretive": {"token": "MRSM{file:relative-secret}"}}}"#.into(),
            ),
            (
                "MRSM_SQL_FROMENV",
                format!(
                    r#"{{"flavor": "postgresql", "host": "h", "password": "MRSM{{env:{ENV_VAR}}}"}}"#
                ),
            ),
            (
                "MRSM_SQL_URIENV",
                format!("postgresql://u:MRSM{{env:{ENV_VAR}}}@h:5432/db"),
            ),
        ],
    );
    let filed = ["meerschaum", "connectors", "sql", "filed"];
    assert_eq!(
        get(&config, &[&filed[..], &["password"]].concat()),
        Some(s("rel-s3cret"))
    );
    assert_eq!(
        get(&config, &[&filed[..], &["username"]].concat()),
        Some(s("abs-s3cret"))
    );
    assert_eq!(
        get(&config, &[&filed[..], &["database"]].concat()),
        Some(s("keep-one\n")),
        "exactly one trailing newline is removed"
    );
    let envy = ["meerschaum", "connectors", "sql", "envy"];
    assert_eq!(
        get(&config, &[&envy[..], &["uri"]].concat()),
        Some(s(&format!("postgresql://u:{ENV_SECRET}@h:5432/db"))),
        "substituted into a longer string literally"
    );
    assert_eq!(
        get(&config, &[&envy[..], &["password"]].concat()),
        Some(s("q\"uo\\te")),
        "quotes and backslashes survive"
    );
    assert_eq!(
        get(
            &config,
            &["meerschaum", "connectors", "api", "patched", "password"]
        ),
        Some(s(ENV_SECRET)),
        "inside MRSM_CONFIG"
    );
    assert_eq!(
        get(&config, &["plugins", "secretive", "token"]),
        Some(s("rel-s3cret")),
        "inside MRSM_PATCH"
    );
    assert_eq!(
        get(
            &config,
            &["meerschaum", "connectors", "sql", "fromenv", "password"]
        ),
        Some(s(ENV_SECRET)),
        "inside the JSON of MRSM_<TYPE>_<LABEL>"
    );
    assert_eq!(
        get(
            &config,
            &["meerschaum", "connectors", "sql", "urienv", "uri"]
        ),
        Some(s(&format!("postgresql://u:{ENV_SECRET}@h:5432/db"))),
        "inside the URI of MRSM_<TYPE>_<LABEL>"
    );
    // Every produced value is known to the masking code.
    for value in ["rel-s3cret", "abs-s3cret", ENV_SECRET] {
        assert!(secrets::is_known_value(value), "{value}");
    }
}

#[test]
fn a_rotated_secret_file_is_read_at_the_next_access() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let secret = root.join("rotating");
    std::fs::write(&secret, "first-value\n").unwrap();
    write(
        root,
        "meerschaum.yaml",
        &format!(
            "connectors:\n  sql:\n    rot:\n      flavor: postgresql\n      host: h\n      \
             password: MRSM{{file:{}}}\n",
            secret.display()
        ),
    );
    let config = config_with(root, &[]);
    let keys = ["meerschaum", "connectors", "sql", "rot", "password"];
    assert_eq!(get(&config, &keys), Some(s("first-value")));
    let reads = secrets::file_reads();
    for _ in 0..50 {
        assert_eq!(get(&config, &keys), Some(s("first-value")));
    }
    assert!(
        secrets::file_reads() - reads <= 1,
        "an unchanged file is not read again"
    );
    let next = root.join("rotating.next");
    std::fs::write(&next, "second-value\n").unwrap();
    std::fs::rename(&next, &secret).unwrap();
    assert_eq!(get(&config, &keys), Some(s("second-value")));
    let section = get(&config, &["meerschaum", "connectors", "sql", "rot"]).unwrap();
    assert_eq!(section.get_path(&["password"]), Some(&s("second-value")));
}

#[test]
fn an_unresolved_reference_breaks_only_what_needs_it() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write(
        root,
        "meerschaum.yaml",
        "instance: sql:main\nconnectors:\n  sql:\n    broken:\n      flavor: postgresql\n      \
         host: h\n      password: MRSM{file:no-such-secret}\n    fine:\n      flavor: sqlite\n      \
         database: /tmp/x.db\n",
    );
    let config = config_with(root, &[]);
    // Other keys, other connectors and whole sections read normally.
    assert_eq!(
        get(&config, &["meerschaum", "instance"]),
        Some(s("sql:main"))
    );
    assert_eq!(
        get(
            &config,
            &["meerschaum", "connectors", "sql", "fine", "flavor"]
        ),
        Some(s("sqlite"))
    );
    assert!(get(&config, &["system"]).is_some());
    // The reference stays as its text (it names the source, never a secret).
    let broken = get(&config, &["meerschaum", "connectors", "sql", "broken"])
        .unwrap()
        .to_map();
    assert_eq!(broken["password"], s("MRSM{file:no-such-secret}"));
    // Building the connector fails with the reference, the key and the reason.
    let message = secrets::check_connector_attributes("sql", "broken", &broken).unwrap_err();
    assert!(message.contains("MRSM{file:no-such-secret}"), "{message}");
    assert!(
        message.contains("meerschaum:connectors:sql:broken:password"),
        "{message}"
    );
    assert!(message.contains("the file does not exist"), "{message}");
    // Reading the setting itself warns once, with the same text.
    let _ = config.take_warnings();
    let _ = get(
        &config,
        &["meerschaum", "connectors", "sql", "broken", "password"],
    );
    let warnings = config.take_warnings();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("MRSM{file:no-such-secret}") && w.contains("broken:password")),
        "{warnings:?}"
    );
    let _ = get(
        &config,
        &["meerschaum", "connectors", "sql", "broken", "password"],
    );
    assert!(config.take_warnings().is_empty(), "once per process");
    // An unset variable likewise.
    let attrs = [(
        "password".to_string(),
        s("MRSM{env:MRSM_TEST_SECRET_CONFIG_UNSET_VARIABLE}"),
    )]
    .into_iter()
    .collect();
    let message = secrets::check_connector_attributes("sql", "x", &attrs).unwrap_err();
    assert!(message.contains("MRSM{env:MRSM_TEST_SECRET_CONFIG_UNSET_VARIABLE}"));
}

#[test]
fn a_user_key_named_file_or_env_is_reported_and_references_win() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    std::fs::write(root.join("pw"), "from-the-file\n").unwrap();
    write(root, "file.yaml", "pw: not-this\n");
    write(
        root,
        "meerschaum.yaml",
        "connectors:\n  sql:\n    x:\n      flavor: postgresql\n      host: h\n      password: MRSM{file:pw}\n",
    );
    let config = config_with(root, &[]);
    assert_eq!(get(&config, &["file", "pw"]), Some(s("not-this")));
    let warnings = config.take_warnings();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("'file'") && w.contains("reserved for secret references")),
        "{warnings:?}"
    );
    assert_eq!(
        get(
            &config,
            &["meerschaum", "connectors", "sql", "x", "password"]
        ),
        Some(s("from-the-file"))
    );
}

#[test]
fn no_shipped_top_level_key_is_reserved() {
    let defaults = meerschaum_core::config::default_config("/r", 2, false);
    for key in secrets::RESERVED_KEYS {
        assert!(!defaults.contains_key(key), "{key}");
    }
}

#[test]
fn config_file_variables_are_read_like_their_variables() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let config_file = root.join("mrsm-config.json");
    std::fs::write(
        &config_file,
        r#"{"meerschaum": {"connectors": {"sql": {"fromfile": {"flavor": "sqlite", "database": "/tmp/f.db"}}}}}"#,
    )
    .unwrap();
    let patch_file = root.join("mrsm-patch.json");
    std::fs::write(&patch_file, r#"{"plugins": {"p": {"setting": 7}}}"#).unwrap();
    let config = config_with(
        root,
        &[
            (
                "MRSM_CONFIG_FILE",
                config_file.to_string_lossy().into_owned(),
            ),
            ("MRSM_PATCH_FILE", patch_file.to_string_lossy().into_owned()),
            ("MRSM_SQL_MAIN_FILE", "sqlite:////tmp/main_file.db".into()),
        ],
    );
    assert_eq!(
        get(
            &config,
            &["meerschaum", "connectors", "sql", "fromfile", "flavor"]
        ),
        Some(s("sqlite"))
    );
    assert_eq!(
        get(&config, &["plugins", "p", "setting"]),
        Some(Value::Int(7))
    );
    // `MRSM_SQL_MAIN_FILE` still defines `sql:main_file`; the `*_FILE` config variables are not
    // connectors.
    assert_eq!(
        get(
            &config,
            &["meerschaum", "connectors", "sql", "main_file", "uri"]
        ),
        Some(s("sqlite:////tmp/main_file.db"))
    );
    let vars = meerschaum_core::config::get_connector_env_vars(&[
        ("MRSM_CONFIG_FILE".into(), "x".into()),
        ("MRSM_PATCH_FILE".into(), "x".into()),
        ("MRSM_SQL_MAIN_FILE".into(), "x".into()),
    ]);
    assert_eq!(vars, vec!["MRSM_SQL_MAIN_FILE".to_string()]);
}

#[test]
fn both_forms_of_one_variable_are_an_error_naming_both() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let file = root.join("c.json");
    std::fs::write(&file, "{}").unwrap();
    for (var, file_var) in [
        ("MRSM_CONFIG", "MRSM_CONFIG_FILE"),
        ("MRSM_PATCH", "MRSM_PATCH_FILE"),
    ] {
        let mut env = vec![
            (
                "MRSM_ROOT_DIR".to_string(),
                root.to_string_lossy().into_owned(),
            ),
            (var.to_string(), "{}".to_string()),
            (file_var.to_string(), file.to_string_lossy().into_owned()),
        ];
        let error = Config::new(ConfigOptions::with_env(env.clone())).unwrap_err();
        let ConfigError::Environment(message) = error else {
            panic!("{error:?}")
        };
        assert!(
            message.contains(var) && message.contains(file_var),
            "{message}"
        );
        // An empty `*_FILE` is unset (how a child process drops an inherited one).
        env[2].1 = String::new();
        assert!(Config::new(ConfigOptions::with_env(env)).is_ok());
    }
    let missing = vec![
        (
            "MRSM_ROOT_DIR".to_string(),
            root.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_CONFIG_FILE".to_string(),
            root.join("missing.json").to_string_lossy().into_owned(),
        ),
    ];
    let error = Config::new(ConfigOptions::with_env(missing)).unwrap_err();
    assert!(error.to_string().contains("MRSM_CONFIG_FILE"), "{error}");
}