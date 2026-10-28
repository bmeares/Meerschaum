//! `sync_files(['stack'])` (`meerschaum/config/_sync.py`): the Compose, Grafana and extensions
//! files derived from the `stack` config key.

use meerschaum_core::config::paths::posix;
use meerschaum_core::config::sync::CREATE_EXTENSIONS_TEXT;
use meerschaum_core::config::sync::GRAFANA_DASHBOARDS;
use meerschaum_core::config::{Config, ConfigOptions};

fn registry(root: &std::path::Path, allow_writes: bool) -> Config {
    let env = vec![("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned())];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = allow_writes;
    Config::new(opts).unwrap()
}

#[test]
fn built_in_grafana_dashboards_upgrade_in_the_existing_folder_and_keep_user_files() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), true);
    config
        .with_state(|state| state.sync_files(&["stack"]))
        .unwrap();
    let provider = config.paths().path("GRAFANA_DASHBOARD_PATH");
    let directory = provider.parent().unwrap();
    for (name, source) in GRAFANA_DASHBOARDS {
        assert_eq!(
            std::fs::read_to_string(directory.join(name)).unwrap(),
            *source
        );
        let dashboard: serde_json::Value = serde_json::from_str(source).unwrap();
        assert!(dashboard["uid"].as_str().unwrap().starts_with("mrsm-"));
        assert!(!dashboard["panels"].as_array().unwrap().is_empty());
    }
    let (managed_name, source) = GRAFANA_DASHBOARDS[0];
    let mut old: serde_json::Value = serde_json::from_str(source).unwrap();
    old["version"] = serde_json::json!(0);
    std::fs::write(directory.join(managed_name), old.to_string()).unwrap();
    let custom = directory.join("my-dashboard.json");
    std::fs::write(&custom, "{\"uid\":\"my-dashboard\"}").unwrap();
    let collision = directory.join(GRAFANA_DASHBOARDS[1].0);
    std::fs::write(&collision, "{\"uid\":\"custom-pipes\"}").unwrap();
    config
        .with_state(|state| state.sync_files(&["stack"]))
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(directory.join(managed_name)).unwrap(),
        source
    );
    assert_eq!(
        std::fs::read_to_string(custom).unwrap(),
        "{\"uid\":\"my-dashboard\"}"
    );
    assert_eq!(
        std::fs::read_to_string(collision).unwrap(),
        "{\"uid\":\"custom-pipes\"}"
    );
    assert!(
        std::fs::read_to_string(provider)
            .unwrap()
            .contains("Meerschaum Dashboards")
    );
}

#[test]
fn read_only_paths_keep_a_missing_configured_venv_directory_without_creating_it() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let venvs = tmp.path().join("outside-root").join("venvs");
    let env = vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_VENVS_DIR".into(),
            venvs.to_string_lossy().into_owned(),
        ),
    ];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = false;
    let config = Config::new(opts.clone()).unwrap();
    assert_eq!(config.paths().path("VIRTENV_RESOURCES_PATH"), venvs);
    assert!(!venvs.parent().unwrap().exists());
    // Ordinary writable registries retain v4's create-on-resolution behavior.
    opts.allow_writes = true;
    let writable = Config::new(opts).unwrap();
    assert_eq!(writable.paths().path("VIRTENV_RESOURCES_PATH"), venvs);
    assert!(venvs.is_dir());
}

#[test]
fn stack_files_are_derived_from_the_config_and_keep_their_header() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), true);
    let paths = config.paths();
    let compose = paths.path("STACK_COMPOSE_PATH");
    std::fs::create_dir_all(compose.parent().unwrap()).unwrap();
    std::fs::write(&compose, "# keep this header\n\nstale: true\n").unwrap();

    config
        .with_state(|s| s.sync_files(&["stack"]))
        .expect("sync");

    let text = std::fs::read_to_string(&compose).unwrap();
    assert!(text.starts_with("# keep this header\n\n"), "{text}");
    assert!(!text.contains("stale: true"), "{text}");
    // The `MRSM{}` symlinks are substituted and `<DOLLAR>` becomes `$`.
    assert!(text.contains("POSTGRES_USER: $POSTGRES_USER"), "{text}");
    assert!(!text.contains("MRSM{stack"), "{text}");
    assert!(text.contains("services:"), "{text}");
    for name in ["GRAFANA_DATASOURCE_PATH", "GRAFANA_DASHBOARD_PATH"] {
        assert!(paths.path(name).is_file(), "{name}");
    }
    let initdb = std::fs::read_to_string(paths.path("DB_CREATE_EXTENSIONS_PATH")).unwrap();
    assert_eq!(initdb, CREATE_EXTENSIONS_TEXT);
    // The setup FILE is mounted into the image's init directory (the image's own scripts run
    // first), and the healthcheck asks the TCP socket, which the setup server does not open.
    let initdb_path = posix(&paths.path("DB_CREATE_EXTENSIONS_PATH"));
    assert!(
        text.contains(&format!(
            "- {initdb_path}:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro\n"
        )),
        "{text}"
    );
    assert!(!text.contains(":/docker-entrypoint-initdb.d:"), "{text}");
    assert!(
        text.contains("- pg_isready -h 127.0.0.1 -p 5432 -d $POSTGRES_DB -U $POSTGRES_USER\n"),
        "{text}"
    );
    assert!(text.contains("start_period: 300s\n"), "{text}");
    let datasource = std::fs::read_to_string(paths.path("GRAFANA_DATASOURCE_PATH")).unwrap();
    assert!(
        datasource.contains("postgresVersion: 1800\n"),
        "{datasource}"
    );
}

/// The setup script is safe on any PostgreSQL image (PARITY.md W17K1-001): every extension is
/// created only when the server offers it, `pg_stat_statements` is preloaded through psql's
/// `\gexec` (`ALTER SYSTEM` cannot run in a `DO` block), and each library is its own list element.
#[test]
fn the_extensions_script_creates_only_what_the_image_offers() {
    let text = CREATE_EXTENSIONS_TEXT;
    for ext in [
        "timescaledb",
        "postgis",
        "timescaledb_toolkit",
        "pg_stat_statements",
    ] {
        assert!(text.contains(&format!("'{ext}'")), "{ext}");
    }
    let statements: Vec<&str> = text
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect();
    let code = statements.join("\n");
    assert!(!code.contains("CREATE EXTENSION IF NOT EXISTS timescaledb"));
    assert!(code.contains("FROM pg_available_extensions WHERE name = ext"));
    assert!(code.contains("EXECUTE format('CREATE EXTENSION IF NOT EXISTS %I CASCADE', ext)"));
    assert!(code.contains("string_agg(quote_literal(lib), ', ' ORDER BY ord)"));
    assert!(code.trim_end().ends_with("\\gexec"), "{code}");
    assert!(text.ends_with('\n'));
}

#[test]
fn the_extensions_script_is_never_overwritten() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), true);
    let initdb = config.paths().path("DB_CREATE_EXTENSIONS_PATH");
    std::fs::create_dir_all(initdb.parent().unwrap()).unwrap();
    std::fs::write(&initdb, "-- mine\n").unwrap();
    config
        .with_state(|s| s.sync_files(&["stack"]))
        .expect("sync");
    assert_eq!(std::fs::read_to_string(&initdb).unwrap(), "-- mine\n");
}

#[test]
fn other_keys_and_disabled_writes_touch_nothing() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), true);
    config
        .with_state(|s| s.sync_files(&["meerschaum", "system"]))
        .unwrap();
    let off = tempfile::tempdir().unwrap();
    let config = registry(off.path(), false);
    config.with_state(|s| s.sync_files(&["stack"])).unwrap();
    assert_eq!(std::fs::read_dir(off.path()).unwrap().count(), 0);
}

/// The `stack.yaml` v4.1.4 wrote (recorded in `fixtures/yaml_parity.json` by `gen_yaml.py`).
fn v4_stack_yaml() -> String {
    fn find(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::Object(map) => map.iter().find_map(|(k, v)| match v {
                serde_json::Value::String(s) if k == "stack.yaml" => Some(s.clone()),
                other => find(other),
            }),
            serde_json::Value::Array(items) => items.iter().find_map(find),
            _ => None,
        }
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("yaml_parity.json");
    let fixture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    find(&fixture).expect("a recorded v4 stack.yaml")
}

/// An existing stack keeps the `docker-compose.yaml` section of its `stack.yaml` as it is: the
/// v5 defaults (the setup FILE mount, the TCP healthcheck and its `start_period`) fill in only a
/// key that is missing, so a v4 stack's directory mount, healthcheck and image survive. The
/// parity overlays map the v5 values back to v4's, so this test (without any overlay) is the
/// one that catches a regression dropping a user's settings.
#[test]
fn a_v4_stack_yaml_keeps_its_compose_section() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), true);
    let paths = config.paths();
    let root_text = posix(root.path());
    let stack_yaml = v4_stack_yaml().replace("{ROOT}", &root_text);
    assert!(stack_yaml.contains(":/docker-entrypoint-initdb.d:z,ro\n"));
    std::fs::create_dir_all(&paths.config_dir).unwrap();
    std::fs::write(paths.config_dir.join("stack.yaml"), &stack_yaml).unwrap();

    config
        .with_state(|s| s.sync_files(&["stack"]))
        .expect("sync");

    let text = std::fs::read_to_string(paths.path("STACK_COMPOSE_PATH")).unwrap();
    assert!(
        text.contains(&format!(
            "- {root_text}/db/initdb:/docker-entrypoint-initdb.d:z,ro\n"
        )),
        "{text}"
    );
    assert!(!text.contains("100_meerschaum.sql"), "{text}");
    assert!(
        text.contains("- pg_isready -d $POSTGRES_DB -U $POSTGRES_USER\n"),
        "{text}"
    );
    assert!(!text.contains("start_period"), "{text}");
    assert!(
        text.contains("image: timescale/timescaledb-ha:pg18\n"),
        "{text}"
    );
    // The user's file is not rewritten either.
    assert_eq!(
        std::fs::read_to_string(paths.config_dir.join("stack.yaml")).unwrap(),
        stack_yaml
    );
}
