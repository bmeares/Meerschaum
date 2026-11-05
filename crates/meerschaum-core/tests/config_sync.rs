//! `sync_files(['stack'])` (`meerschaum/config/_sync.py`): the Compose, Grafana and extensions
//! files derived from the `stack` config key.

use meerschaum_core::config::paths::posix;
use meerschaum_core::config::sync::CREATE_EXTENSIONS_TEXT;
use meerschaum_core::config::sync::{
    GRAFANA_DASHBOARDS, GRAFANA_DEFAULT_INSTANCE, RETIRED_GRAFANA_DASHBOARDS,
    managed_dashboard_hash,
};
use meerschaum_core::config::{Config, ConfigOptions};

/// A built-in dashboard as written for the default instance `sql:main`.
fn written(source: &str) -> String {
    source.replace(GRAFANA_DEFAULT_INSTANCE, "sql:main")
}

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
    let mut uids = Vec::new();
    for (name, source) in GRAFANA_DASHBOARDS {
        assert_eq!(
            std::fs::read_to_string(directory.join(name)).unwrap(),
            written(source)
        );
        let dashboard: serde_json::Value = serde_json::from_str(source).unwrap();
        uids.push(dashboard["uid"].as_str().unwrap().to_string());
        assert!(!dashboard["panels"].as_array().unwrap().is_empty());
    }
    assert_eq!(uids, ["mrsm-syncs", "mrsm-engine"]);
    let (managed_name, source) = GRAFANA_DASHBOARDS[0];
    let mut old: serde_json::Value = serde_json::from_str(source).unwrap();
    old["version"] = serde_json::json!(0);
    std::fs::write(directory.join(managed_name), old.to_string()).unwrap();
    let custom = directory.join("my-dashboard.json");
    std::fs::write(&custom, "{\"uid\":\"my-dashboard\"}").unwrap();
    let collision = directory.join(GRAFANA_DASHBOARDS[1].0);
    std::fs::write(&collision, "{\"uid\":\"custom-engine\"}").unwrap();
    config
        .with_state(|state| state.sync_files(&["stack"]))
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(directory.join(managed_name)).unwrap(),
        written(source)
    );
    assert_eq!(
        std::fs::read_to_string(custom).unwrap(),
        "{\"uid\":\"my-dashboard\"}"
    );
    assert_eq!(
        std::fs::read_to_string(collision).unwrap(),
        "{\"uid\":\"custom-engine\"}"
    );
    assert!(
        std::fs::read_to_string(provider)
            .unwrap()
            .contains("Meerschaum Dashboards")
    );
}

#[test]
fn unreadable_and_symlink_dashboard_collisions_are_preserved() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), true);
    let provider = config.paths().path("GRAFANA_DASHBOARD_PATH");
    let directory = provider.parent().unwrap();
    std::fs::create_dir_all(directory).unwrap();
    let undecodable = directory.join(GRAFANA_DASHBOARDS[0].0);
    std::fs::write(&undecodable, b"\xffcustom").unwrap();
    #[cfg(unix)]
    let linked = {
        let target = root.path().join("custom-dashboard.json");
        std::fs::write(&target, GRAFANA_DASHBOARDS[1].1).unwrap();
        let linked = directory.join(GRAFANA_DASHBOARDS[1].0);
        std::os::unix::fs::symlink(&target, &linked).unwrap();
        linked
    };
    config
        .with_state(|state| state.write_grafana_dashboards())
        .unwrap();
    assert_eq!(std::fs::read(undecodable).unwrap(), b"\xffcustom");
    #[cfg(unix)]
    assert!(linked.is_symlink());
    #[cfg(not(unix))]
    assert_eq!(
        std::fs::read_to_string(directory.join(GRAFANA_DASHBOARDS[1].0)).unwrap(),
        written(GRAFANA_DASHBOARDS[1].1)
    );
}

/// The retired built-ins as earlier versions wrote them: `rc16-rc34` (5.0.0rc16 through
/// 5.0.0rc34, verbatim) and `pipe-labels` (the template that filled in the default instance).
fn retired_fixture(version: &str, name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("grafana_retired")
        .join(version)
        .join(name);
    std::fs::read_to_string(path).unwrap()
}

#[test]
fn every_retired_version_meerschaum_wrote_is_recognized() {
    for (name, uid, hashes) in RETIRED_GRAFANA_DASHBOARDS {
        let released = retired_fixture("rc16-rc34", name);
        let template = retired_fixture("pipe-labels", name);
        assert!(template.contains(GRAFANA_DEFAULT_INSTANCE), "{name}");
        for text in [
            released.clone(),
            template.clone(),
            template.replace(GRAFANA_DEFAULT_INSTANCE, "sql:main"),
            template.replace(GRAFANA_DEFAULT_INSTANCE, "sql:other_instance"),
        ] {
            let dashboard: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(dashboard["uid"], *uid);
            assert!(
                hashes.contains(&managed_dashboard_hash(&text).as_str()),
                "{name}"
            );
        }
        // Any edit makes the file the user's.
        let edited = released.replacen("\"refresh\": \"30s\"", "\"refresh\": \"1m\"", 1);
        assert_ne!(edited, released);
        assert!(!hashes.contains(&managed_dashboard_hash(&edited).as_str()));
        let edited = template
            .replace(GRAFANA_DEFAULT_INSTANCE, "sql:main")
            .replacen("\"refresh\": \"30s\"", "\"refresh\": \"1m\"", 1);
        assert!(!hashes.contains(&managed_dashboard_hash(&edited).as_str()));
    }
}

#[test]
fn retired_built_in_dashboards_are_removed_only_when_meerschaum_wrote_them() {
    for version in ["rc16-rc34", "pipe-labels"] {
        let root = tempfile::tempdir().unwrap();
        let config = registry(root.path(), true);
        let directory = config.paths().path("GRAFANA_DASHBOARD_PATH");
        let directory = directory.parent().unwrap().to_path_buf();
        std::fs::create_dir_all(&directory).unwrap();
        for (name, _, _) in RETIRED_GRAFANA_DASHBOARDS {
            // A dashboard written for another default instance is still Meerschaum's.
            let text = retired_fixture(version, name).replace(GRAFANA_DEFAULT_INSTANCE, "sql:old");
            std::fs::write(directory.join(name), text).unwrap();
        }
        std::fs::write(directory.join("my-dashboard.json"), "{\"uid\":\"mine\"}").unwrap();
        config
            .with_state(|state| state.sync_files(&["stack"]))
            .unwrap();
        for (name, _, _) in RETIRED_GRAFANA_DASHBOARDS {
            assert!(!directory.join(name).exists(), "{version}: {name}");
        }
        for (name, source) in GRAFANA_DASHBOARDS {
            assert_eq!(
                std::fs::read_to_string(directory.join(name)).unwrap(),
                written(source)
            );
        }
        assert!(directory.join("my-dashboard.json").exists());
    }
}

#[test]
fn customized_linked_unreadable_and_colliding_retired_dashboards_are_kept() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path(), true);
    let directory = config.paths().path("GRAFANA_DASHBOARD_PATH");
    let directory = directory.parent().unwrap().to_path_buf();
    std::fs::create_dir_all(&directory).unwrap();
    let [performance, pipes, pipelines] = [0, 1, 2].map(|i| RETIRED_GRAFANA_DASHBOARDS[i].0);
    // A customized copy keeps the built-in UID.
    let mut customized: serde_json::Value =
        serde_json::from_str(&retired_fixture("rc16-rc34", performance)).unwrap();
    customized["title"] = serde_json::json!("My Performance");
    let customized = serde_json::to_string_pretty(&customized).unwrap();
    std::fs::write(directory.join(performance), &customized).unwrap();
    // Another dashboard under a retired name.
    std::fs::write(directory.join(pipes), "{\"uid\":\"custom-pipes\"}").unwrap();
    // An unreadable file.
    std::fs::write(directory.join(pipelines), b"\xffcustom").unwrap();
    config
        .with_state(|state| state.sync_files(&["stack"]))
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(directory.join(performance)).unwrap(),
        customized
    );
    assert_eq!(
        std::fs::read_to_string(directory.join(pipes)).unwrap(),
        "{\"uid\":\"custom-pipes\"}"
    );
    assert_eq!(
        std::fs::read(directory.join(pipelines)).unwrap(),
        b"\xffcustom"
    );
    // A symlink is kept even when it points at an unmodified built-in.
    #[cfg(unix)]
    {
        let target = root.path().join("linked-performance.json");
        std::fs::write(&target, retired_fixture("rc16-rc34", performance)).unwrap();
        std::fs::remove_file(directory.join(performance)).unwrap();
        std::os::unix::fs::symlink(&target, directory.join(performance)).unwrap();
        config
            .with_state(|state| state.sync_files(&["stack"]))
            .unwrap();
        assert!(directory.join(performance).is_symlink());
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            retired_fixture("rc16-rc34", performance)
        );
    }
}

#[test]
fn built_in_dashboards_omit_the_configured_default_instance_from_pipe_labels() {
    let root = tempfile::tempdir().unwrap();
    let env = vec![
        (
            "MRSM_ROOT_DIR".into(),
            root.path().to_string_lossy().into_owned(),
        ),
        (
            "MRSM_CONFIG".into(),
            r#"{"meerschaum": {"instance": "sql:other"}}"#.into(),
        ),
    ];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = true;
    let config = Config::new(opts).unwrap();
    config
        .with_state(|state| state.sync_files(&["stack"]))
        .unwrap();
    let directory = config.paths().path("GRAFANA_DASHBOARD_PATH");
    let directory = directory.parent().unwrap();
    for (name, source) in GRAFANA_DASHBOARDS {
        let text = std::fs::read_to_string(directory.join(name)).unwrap();
        assert!(!text.contains(GRAFANA_DEFAULT_INSTANCE), "{name}");
        assert_eq!(text, source.replace(GRAFANA_DEFAULT_INSTANCE, "sql:other"));
        let dashboard: serde_json::Value = serde_json::from_str(&text).unwrap();
        let default = dashboard["templating"]["list"]
            .as_array()
            .unwrap()
            .iter()
            .find(|variable| variable["name"] == "default_instance")
            .unwrap();
        assert_eq!(default["query"], "sql:other");
    }
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
