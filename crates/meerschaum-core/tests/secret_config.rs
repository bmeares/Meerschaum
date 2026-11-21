//! 5.1 secret handling in the config registry: `MRSM{file:...}` / `MRSM{env:...}` references,
//! the `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` variables, provenance and the writer that never
//! persists what the environment or a reference holds, and the permission warning.
//!
//! Every value is synthetic; each test works in its own temporary root.
//!
//! A test binary of its own: `MRSM{env:...}` reads the process environment, which these tests
//! set, and a variable set in a shared suite would reach every other test of that suite.

use std::path::{Path, PathBuf};
use std::sync::Once;

use meerschaum_core::Value;
use meerschaum_core::config::secrets;
use meerschaum_core::config::{Config, ConfigError, ConfigOptions, Origin};

/// The environment variables the `env:` references of these tests read (set once, before any
/// test reads them).
const ENV_SECRET: &str = "env-s3cret-for-tests";
const ENV_VAR: &str = "MRSM_TEST_SECRET_CONFIG_VALUE";
static SET_ENV: Once = Once::new();

#[allow(unsafe_code)] // sets the process environment once, below
fn set_env() {
    SET_ENV.call_once(|| {
        // SAFETY: these tests are the only ones in this binary (kept out of the `integration`
        // suite for this reason): the variable is set once, before any of them reads it, never
        // changed again, and private to them.
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
    let reads = secrets::file_reads(&secret);
    for _ in 0..50 {
        assert_eq!(get(&config, &keys), Some(s("first-value")));
    }
    assert!(
        secrets::file_reads(&secret) == reads,
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

#[test]
fn origins_name_every_layer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = write(
        root,
        "meerschaum.yaml",
        &format!(
            "instance: sql:main\nconnectors:\n  sql:\n    written:\n      flavor: postgresql\n      \
             password: MRSM{{env:{ENV_VAR}}}\n"
        ),
    );
    let config = config_with(
        root,
        &[
            ("MRSM_SQL_ENVCONN", "postgresql://u:p@h/db".into()),
            (
                "MRSM_CONFIG",
                r#"{"meerschaum": {"connectors": {"sql": {"written": {"host": "from-config"}}}}}"#
                    .into(),
            ),
            (
                "MRSM_PATCH",
                r#"{"meerschaum": {"connectors": {"sql": {"written": {"port": 1}}}}}"#.into(),
            ),
        ],
    );
    let origins = config.origins("meerschaum");
    let at = |p: &[&str]| {
        origins
            .origin(&p.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .cloned()
    };
    assert_eq!(at(&["instance"]), Some(Origin::File(path.clone())));
    assert_eq!(
        at(&["connectors", "sql", "written", "host"]),
        Some(Origin::Environment("MRSM_CONFIG".into()))
    );
    assert_eq!(
        at(&["connectors", "sql", "written", "port"]),
        Some(Origin::Environment("MRSM_PATCH".into()))
    );
    assert_eq!(
        at(&["connectors", "sql", "envconn", "uri"]),
        Some(Origin::Environment("MRSM_SQL_ENVCONN".into()))
    );
    assert_eq!(
        at(&["connectors", "sql", "written", "password"]),
        Some(Origin::Secret {
            reference: format!("MRSM{{env:{ENV_VAR}}}"),
            under: Box::new(Origin::File(path)),
        })
    );
    assert_eq!(
        at(&["connectors", "sql", "local", "flavor"]),
        Some(Origin::Default)
    );
}

/// The environment of the writer tests: secrets in every environment layer.
const PATCH_ONLY: &str = "patch-only-s3cret-value";
const CONFIG_ONLY: &str = "config-only-s3cret-value";
const URI_ONLY: &str = "uri-only-s3cret-value";
const FILE_SECRET: &str = "file-only-s3cret-value";

fn writer_root() -> (tempfile::TempDir, Config, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    std::fs::write(root.join("pg-password"), format!("{FILE_SECRET}\n")).unwrap();
    let path = write(
        &root,
        "meerschaum.yaml",
        &format!(
            "instance: sql:main\nconnectors:\n  sql:\n    written:\n      flavor: postgresql\n      \
             host: db.example\n      password: MRSM{{file:pg-password}}\n      username: \
             MRSM{{env:{ENV_VAR}}}\n    linked:\n      flavor: sqlite\n      database: \
             MRSM{{meerschaum:connectors:sql:local:database}}\n"
        ),
    );
    let config = config_with(
        &root,
        &[
            (
                "MRSM_SQL_ENVCONN",
                format!("postgresql://u:{URI_ONLY}@h/db"),
            ),
            (
                "MRSM_CONFIG",
                format!(
                    r#"{{"meerschaum": {{"connectors": {{"api": {{"cfg": {{"host": "h", "password": "{CONFIG_ONLY}"}}}}}}}}}}"#
                ),
            ),
            (
                "MRSM_PATCH",
                format!(
                    r#"{{"meerschaum": {{"connectors": {{"sql": {{"written": {{"options": {{"token": "{PATCH_ONLY}"}}}}}}}}}}}}"#
                ),
            ),
        ],
    );
    (tmp, config, path)
}

fn assert_clean(path: &Path) {
    let text = std::fs::read_to_string(path).unwrap();
    for secret in [PATCH_ONLY, CONFIG_ONLY, URI_ONLY, FILE_SECRET, ENV_SECRET] {
        assert!(
            !text.contains(secret),
            "{secret} reached {}:\n{text}",
            path.display()
        );
    }
    assert!(text.contains("MRSM{file:pg-password}"), "{text}");
    assert!(text.contains(&format!("MRSM{{env:{ENV_VAR}}}")), "{text}");
    assert!(
        text.contains("MRSM{meerschaum:connectors:sql:local:database}"),
        "{text}"
    );
}

/// `write_config({'meerschaum': get_config('meerschaum') + change})`: the pattern of
/// `register connectors`, `delete connectors`, `login` (`save_credentials`), `bootstrap
/// connectors` and the console's writers.
#[test]
fn writing_a_loaded_section_keeps_references_and_drops_the_environment() {
    let (_tmp, config, path) = writer_root();
    let mut section = config.get(&["meerschaum"]).unwrap().to_map();
    if let Some(Value::Map(connectors)) = section.get_mut("connectors")
        && let Some(Value::Map(sql)) = connectors.get_mut("sql")
    {
        sql.insert(
            "added".into(),
            Value::from_json_str(r#"{"flavor": "sqlite", "database": "/tmp/added.db"}"#).unwrap(),
        );
        if let Some(Value::Map(written)) = sql.get_mut("written") {
            written.insert("port".into(), Value::Int(6543));
        }
    }
    let mut whole = meerschaum_core::Map::new();
    whole.insert("meerschaum".into(), Value::Map(section));
    assert!(config.write_config(Some(&whole), None));
    assert_clean(&path);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        text.contains("added:") && text.contains("port: 6543"),
        "{text}"
    );
    assert!(
        !text.contains("envconn") && !text.contains("cfg:"),
        "{text}"
    );
}

/// The whole in-memory config (`write_config()`), the default-fill write of `get_config` and the
/// permanent patch directory all write the whole config.
#[test]
fn writing_the_whole_config_and_default_fills_keep_the_file_as_written() {
    let (_tmp, config, path) = writer_root();
    let _ = config.get(&["meerschaum", "connectors", "sql", "written", "password"]);
    assert!(config.write_config(None, None));
    assert_clean(&path);
    // A key with no file falls back to the defaults and writes the loaded config.
    let _ = config.get(&["plugins", "nothing-here"]);
    let _ = config.get(&["jobs", "logs"]);
    assert_clean(&path);
}

#[test]
fn add_connector_and_team_moves_keep_references() {
    let (_tmp, config, path) = writer_root();
    let attrs = Value::from_json_str(r#"{"flavor": "sqlite", "database": "/tmp/team.db"}"#)
        .unwrap()
        .to_map();
    config.add_connector("sql", "team_x", &attrs).unwrap();
    assert_clean(&path);
    config.move_team_reservations("a", "b").unwrap();
    assert_clean(&path);
}

#[test]
fn a_permanent_patch_directory_keeps_references() {
    let (tmp, _config, path) = writer_root();
    let root = tmp.path();
    let permanent = root.join("permanent_patch_config");
    std::fs::create_dir_all(&permanent).unwrap();
    std::fs::write(permanent.join("meerschaum.yaml"), "instance: sql:other\n").unwrap();
    let config = config_with(
        root,
        &[(
            "MRSM_CONFIG",
            format!(
                r#"{{"meerschaum": {{"connectors": {{"api": {{"cfg": {{"password": "{CONFIG_ONLY}"}}}}}}}}}}"#
            ),
        )],
    );
    drop(config);
    assert_clean(&path);
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .contains("instance: sql:other")
    );
}

/// A stale copy of a rotated secret written back keeps the reference.
#[test]
fn a_stale_resolved_secret_is_never_written() {
    let (tmp, config, path) = writer_root();
    let section = config.get(&["meerschaum"]).unwrap().to_map();
    std::fs::write(
        tmp.path().join("pg-password.next"),
        "rotated-s3cret-value\n",
    )
    .unwrap();
    std::fs::rename(
        tmp.path().join("pg-password.next"),
        tmp.path().join("pg-password"),
    )
    .unwrap();
    let mut whole = meerschaum_core::Map::new();
    whole.insert("meerschaum".into(), Value::Map(section));
    assert!(config.write_config(Some(&whole), None));
    assert_clean(&path);
    assert!(!std::fs::read_to_string(&path).unwrap().contains("rotated"));
}

#[cfg(unix)]
#[test]
fn a_readable_file_with_an_inline_secret_is_reported() {
    use meerschaum_core::config::ConfigValue;
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("meerschaum.yaml");
    std::fs::write(&path, "x").unwrap();
    let inline = ConfigValue::from_value(
        &Value::from_json_str(r#"{"connectors": {"sql": {"a": {"password": "hunter2"}}}}"#)
            .unwrap(),
    );
    let referenced = ConfigValue::from_value(
        &Value::from_json_str(
            r#"{"connectors": {"sql": {"a": {"password": "MRSM{file:/run/secrets/pg}"}}}}"#,
        )
        .unwrap(),
    );
    let in_uri = ConfigValue::from_value(
        &Value::from_json_str(
            r#"{"connectors": {"sql": {"a": {"uri": "postgresql://u:p4ss@h/db"}}}}"#,
        )
        .unwrap(),
    );
    for mode in [0o644, 0o640, 0o604] {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        let message = secrets::exposure_warning(&path, "meerschaum", &inline).unwrap();
        assert!(
            message.contains(&format!("chmod 600 '{}'", path.display())),
            "{message}"
        );
        assert!(message.contains("connectors:sql:a:password"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
        assert!(secrets::exposure_warning(&path, "meerschaum", &in_uri).is_some());
        assert!(secrets::exposure_warning(&path, "meerschaum", &referenced).is_none());
    }
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(secrets::exposure_warning(&path, "meerschaum", &inline).is_none());
    // The cosmetic icons named `key` are not secrets.
    let icons =
        ConfigValue::from_value(&Value::from_json_str(r#"{"emoji": {"key": "K"}}"#).unwrap());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(secrets::exposure_warning(&path, "formatting", &icons).is_none());

    // 5.1 regressions: names that contain `key`, `policy`, `auth`, `session`, ... but are no
    // credential's (two reported on a real config: `allowed_instance_keys`,
    // `VALKEY_RDB_POLICY_DISABLED`).
    for (top, json) in [
        (
            "api",
            r#"{"permissions": {"instances": {"allowed_instance_keys": ["*"]}}}"#,
        ),
        (
            "stack",
            r#"{"docker-compose.yaml": {"services": {"valkey": {"environment":
                {"VALKEY_RDB_POLICY_DISABLED": "no", "VALKEY_RDB_POLICY": "900#1"}}}}}"#,
        ),
        (
            "api",
            r#"{"session_timeout": "30", "auth_type": "basic", "authority": "x"}"#,
        ),
        (
            "meerschaum",
            r#"{"sort_keys": "yes", "primary_key": "id", "key_file": "/k"}"#,
        ),
        (
            "api",
            r#"{"password_policy": "strict", "token_url": "https://example.com/t"}"#,
        ),
        (
            "stack",
            r#"{"grafana": {"GF_SECURITY_ADMIN_PASSWORD__FILE": "/run/secrets/x", "signal": "s"}}"#,
        ),
        (
            "meerschaum",
            r#"{"max_keys": 10, "compass": "n", "passthrough": "yes"}"#,
        ),
    ] {
        let value = ConfigValue::from_value(&Value::from_json_str(json).unwrap());
        assert_eq!(
            secrets::exposure_warning(&path, top, &value),
            None,
            "{top}: {json}"
        );
    }
    for (json, at) in [
        (r#"{"db_pass": "x"}"#, "db_pass"),
        (r#"{"client_secret": "x"}"#, "client_secret"),
        (r#"{"access_token": "x"}"#, "access_token"),
        (r#"{"api_key": "x"}"#, "api_key"),
        (r#"{"apiKey": "x"}"#, "apiKey"),
        (r#"{"privateKey": "x"}"#, "privateKey"),
        (r#"{"credentials": "x"}"#, "credentials"),
        (
            r#"{"GF_SECURITY_ADMIN_PASSWORD": "x"}"#,
            "GF_SECURITY_ADMIN_PASSWORD",
        ),
        (r#"{"password": 1234}"#, "password"),
    ] {
        let value = ConfigValue::from_value(&Value::from_json_str(json).unwrap());
        let message = secrets::exposure_warning(&path, "api", &value)
            .unwrap_or_else(|| panic!("not reported: {json}"));
        assert!(message.contains(&format!("api:{at}")), "{message}");
    }
}

#[test]
fn new_config_files_are_owner_only() {
    let (_tmp, config, _path) = writer_root();
    let _ = config.get(&["plugins"]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let file = config.paths().config_dir.join("plugins.json");
        if file.exists() {
            let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}

#[test]
fn writing_missing_stack_default_preserves_all_mrsm_references_including_lists() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    write(
        &root,
        "meerschaum.yaml",
        "instance: sql:main\nconnectors:\n  sql:\n    main:\n      flavor: timescaledb-ha\n      \
         host: 127.0.0.1\n      port: 5466\n      username: sra\n      password: super-secret-db-pw\n      \
         database: sra\n",
    );
    let config = config_with(&root, &[]);
    let _ = config.get(&["stack"]);
    let stack_path = config.paths().config_dir.join("stack.yaml");
    let text = std::fs::read_to_string(&stack_path).unwrap();
    for expected in [
        "MRSM{!meerschaum}",
        "MRSM{!system}",
        "MRSM{!api}",
        "MRSM{meerschaum:connectors:sql:main:port}:5432",
        "MRSM{meerschaum:connectors:api:main:port}:MRSM{meerschaum:connectors:api:main:port}",
        "http://localhost:MRSM{meerschaum:connectors:api:main:port}/healthcheck",
        "MRSM{meerschaum:connectors:valkey:main:port}:6379",
        "MRSM{meerschaum:connectors:sql:main:database}",
        "MRSM{meerschaum:connectors:sql:main:username}",
        "MRSM{meerschaum:connectors:sql:main:password}",
        "MRSM{stack:docker-compose.yaml:services:db:hostname}:5432",
    ] {
        assert!(text.contains(expected), "missing {expected} in:\n{text}");
    }
    assert!(
        !text.contains("super-secret-db-pw"),
        "password leaked into stack.yaml:\n{text}"
    );
}

#[test]
fn writing_loaded_stack_preserves_mrsm_config_and_list_references() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    write(
        &root,
        "meerschaum.yaml",
        "instance: sql:main\nconnectors:\n  sql:\n    main:\n      flavor: timescaledb-ha\n      \
         host: 127.0.0.1\n      port: 5466\n      username: sra\n      password: super-secret-db-pw\n      \
         database: sra\n",
    );
    let config = config_with(&root, &[]);
    // Load `meerschaum` first so `meerschaum:api_instance` is substituted in memory (matching
    // normal CLI / Python startup), then load `stack` and write it back after a small change.
    let _ = config.get(&["meerschaum"]);
    let mut stack = config.get(&["stack"]).unwrap().to_map();
    stack.insert("project_name".into(), Value::Str("custom-proj".into()));
    let mut to_write = meerschaum_core::config::ConfigMap::new();
    to_write.insert(
        "stack".into(),
        meerschaum_core::config::ConfigValue::from_value(&Value::Map(stack)),
    );
    assert!(config.with_state(|s| s.write_config_lossless(Some(&to_write), None)));

    let stack_path = config.paths().config_dir.join("stack.yaml");
    let text = std::fs::read_to_string(&stack_path).unwrap();
    assert!(text.contains("project_name: custom-proj"), "{text}");
    for expected in [
        "MRSM{!meerschaum}",
        "MRSM{!system}",
        "MRSM{!api}",
        "MRSM{meerschaum:connectors:sql:main:port}:5432",
        "MRSM{meerschaum:connectors:sql:main:password}",
        "MRSM{stack:docker-compose.yaml:services:db:hostname}:5432",
    ] {
        assert!(text.contains(expected), "missing {expected} in:\n{text}");
    }
    assert!(
        !text.contains("super-secret-db-pw"),
        "secret expanded into stack.yaml:\n{text}"
    );
}

/// 5.1.1: in a Docker stack's API container `MRSM_CONFIG` holds the stack host's configuration;
/// a connector the console reserved for a team in the container's own `meerschaum.yaml` is never
/// replaced by a stale copy from it, while `MRSM_PATCH` (explicit) still applies.
#[test]
fn mrsm_config_never_shadows_a_team_reserved_connector() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    write(
        &root,
        "meerschaum.yaml",
        "connectors:\n  sql:\n    bwg:\n      flavor: timescaledb-ha\n      host: db\n      \
         port: 5432\n      username: mrsm_bwg\n      password: team-pw\n      database: bwg\n      \
         teams:\n      - bwg\n    plain:\n      flavor: sqlite\n      database: /tmp/a.db\n",
    );
    let host = r#"{"meerschaum": {"connectors": {"sql": {
        "bwg": {"flavor": "timescaledb", "host": "old", "database": "meerschaum", "password": "old-pw"},
        "plain": {"database": "/tmp/b.db"},
        "anchored": {"flavor": "postgresql", "host": "db", "database": "anchored"}
    }}}}"#;
    let config = config_with(&root, &[("MRSM_CONFIG", host.to_string())]);
    let bwg = |key: &str| {
        config
            .get(&["meerschaum", "connectors", "sql", "bwg", key])
            .unwrap()
    };
    assert_eq!(bwg("database"), s("bwg"));
    assert_eq!(bwg("host"), s("db"));
    assert_eq!(bwg("password"), s("team-pw"));
    // An unreserved connector still takes the stack's value; a new one is still added.
    let plain = config
        .get(&["meerschaum", "connectors", "sql", "plain", "database"])
        .unwrap();
    assert_eq!(plain, s("/tmp/b.db"));
    assert!(
        config
            .get(&["meerschaum", "connectors", "sql", "anchored"])
            .is_some()
    );

    // An explicit MRSM_PATCH still changes a reserved connector.
    let patched = config_with(
        &root,
        &[
            ("MRSM_CONFIG", host.to_string()),
            (
                "MRSM_PATCH",
                r#"{"meerschaum": {"connectors": {"sql": {"bwg": {"host": "db2"}}}}}"#.to_string(),
            ),
        ],
    );
    let host_value = patched
        .get(&["meerschaum", "connectors", "sql", "bwg", "host"])
        .unwrap();
    assert_eq!(host_value, s("db2"));
}
