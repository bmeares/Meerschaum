//! `write_config` never deletes, truncates or half-writes a config file: those files hold
//! credentials (wave 15, task O).

use meerschaum_core::config::{Config, ConfigMap, ConfigOptions, ConfigValue};

fn registry(root: &std::path::Path) -> Config {
    let env = vec![("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned())];
    let mut opts = ConfigOptions::with_env(env);
    opts.allow_writes = true;
    Config::new(opts).unwrap()
}

fn config_of(key: &str, entries: &[(&str, &str)]) -> ConfigMap {
    let mut inner = ConfigMap::new();
    for (k, v) in entries {
        inner.insert(k.to_string(), ConfigValue::str(*v));
    }
    let mut map = ConfigMap::new();
    map.insert(key.to_string(), ConfigValue::Map(inner));
    map
}

fn leftovers(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with('.'))
        .collect()
}

#[cfg(unix)]
#[test]
fn a_read_only_file_is_reported_and_left_intact() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path());
    let dir = config.paths().path("CONFIG_DIR_PATH");
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("custom.json");
    std::fs::write(&target, "{\"password\": \"hunter2\"}\n").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o444)).unwrap();
    if std::fs::OpenOptions::new()
        .write(true)
        .open(&target)
        .is_ok()
    {
        return; // running as root: file modes do not apply
    }
    let (ok, error) = config.with_state(|s| {
        let ok = s.write_config_lossless(Some(&config_of("custom", &[("password", "new")])), None);
        (ok, s.take_write_error())
    });
    assert!(!ok);
    assert_eq!(error.unwrap().0, std::io::ErrorKind::PermissionDenied);
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "{\"password\": \"hunter2\"}\n"
    );
    assert_eq!(
        std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
        0o444
    );
    assert!(leftovers(&dir).is_empty());
}

#[test]
fn a_failed_dump_leaves_the_file_and_a_good_write_replaces_it() {
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path());
    let dir = config.paths().path("CONFIG_DIR_PATH");
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("custom.yaml");
    std::fs::write(&target, "secret: hunter2\n").unwrap();

    let ok = config.with_state(|s| {
        s.write_config_with(Some(&config_of("custom", &[("a", "b")])), None, &|_| {
            Err("cannot represent".to_string())
        })
    });
    assert!(!ok);
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "secret: hunter2\n"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
    }
    let ok = config
        .with_state(|s| s.write_config_lossless(Some(&config_of("custom", &[("a", "b")])), None));
    assert!(ok);
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "a: b\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }
    assert!(leftovers(&dir).is_empty());
}

#[cfg(unix)]
#[test]
fn a_new_config_file_is_created_private() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path());
    let dir = config.paths().path("CONFIG_DIR_PATH");
    std::fs::create_dir_all(&dir).unwrap();
    let ok = config
        .with_state(|s| s.write_config_lossless(Some(&config_of("custom", &[("a", "b")])), None));
    assert!(ok);
    let mode = std::fs::metadata(dir.join("custom.json"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn backups_record_the_debug_lines_v4_prints() {
    use meerschaum_core::config::Notice;
    let root = tempfile::tempdir().unwrap();
    let config = registry(root.path());
    let dir = config.paths().path("CONFIG_DIR_PATH");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("custom.yaml"), "a: 1\n").unwrap();
    let debug_lines = |s: &mut meerschaum_core::config::ConfigState| -> Vec<String> {
        s.take_notices()
            .into_iter()
            .filter_map(|n| match n {
                Notice::Debug(t) => Some(t),
                _ => None,
            })
            .collect()
    };
    config.with_state(|s| {
        s.take_notices();
        assert!(s.backup_config_key("absent").is_none());
        assert_eq!(
            debug_lines(s),
            vec!["No config file to back up for 'absent'.".to_string()]
        );
        let backup = s.backup_config_key("custom").unwrap();
        let lines = debug_lines(s);
        assert_eq!(
            lines,
            vec![format!(
                "Backed up 'custom' to '{}'.",
                backup.to_string_lossy()
            )]
        );
        s.backup_config_key("custom").unwrap();
        assert_eq!(
            debug_lines(s),
            vec!["'custom' unchanged since last backup; skipping.".to_string()]
        );
        // Debug notices are never warnings for Rust callers.
        s.backup_config_key("custom");
        assert!(s.take_warnings().is_empty());
    });
}
