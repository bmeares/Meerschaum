//! `sync_files(['stack'])` (`meerschaum/config/_sync.py`): the Compose, Grafana and extensions
//! files derived from the `stack` config key.

use meerschaum_core::config::{Config, ConfigOptions};

fn registry(root: &std::path::Path, allow_writes: bool) -> Config {
    let env = vec![("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned())];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = allow_writes;
    Config::new(opts).unwrap()
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
    assert!(initdb.starts_with("CREATE EXTENSION IF NOT EXISTS timescaledb;"));
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
