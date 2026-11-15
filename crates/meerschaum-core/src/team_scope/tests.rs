use super::*;
use crate::config::ConfigOptions;

struct Root {
    root: tempfile::TempDir,
    _plugins: tempfile::TempDir,
    config: Config,
}

/// A root whose config holds two teams' connectors (one password in a file), the server's own
/// `sql:main`, Grafana's administrator and a stack compose file with a password.
fn root_with(extra_env: &[(&str, &str)]) -> Root {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let config_dir = root.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(root.path().join("wet_secret"), "wet-pw-from-file\n").unwrap();
    std::fs::write(root.path().join("dry_secret"), "dry-pw-from-file\n").unwrap();
    std::fs::write(
        config_dir.join("meerschaum.yaml"),
        r#"
instance: sql:main
connectors:
  sql:
    main:
      flavor: postgresql
      host: db
      username: server
      password: server-main-pw
      database: meerschaum
    wetlands:
      flavor: postgresql
      host: db
      username: mrsm_wetlands
      password: MRSM{file:wet_secret}
      database: wetlands
      teams: [wetlands]
    drylands:
      flavor: postgresql
      host: db
      username: mrsm_drylands
      password: MRSM{file:dry_secret}
      database: drylands
      teams: [drylands]
    shared:
      flavor: sqlite
      database: /tmp/shared.db
  api:
    partner:
      host: partner.example
      username: bot
      password: partner-pw
"#,
    )
    .unwrap();
    std::fs::write(
        config_dir.join("api.yaml"),
        r#"
grafana:
  enabled: true
  admin_password: grafana-admin-pw
permissions:
  default_role: editor
secret_key_note: MRSM{file:wet_secret}
"#,
    )
    .unwrap();
    std::fs::write(
        config_dir.join("stack.yaml"),
        "docker-compose.yaml:\n  services:\n    db:\n      environment:\n        POSTGRES_PASSWORD: stack-pw\n",
    )
    .unwrap();
    std::fs::write(
        config_dir.join("plugins.yaml"),
        "noaa:\n  api_key: noaa-key\nprivate_plugin:\n  api_key: other-key\n",
    )
    .unwrap();
    std::fs::write(
        config_dir.join("custom.yaml"),
        "token_for_something: custom-secret\n",
    )
    .unwrap();
    let mut env = vec![
        (
            "MRSM_ROOT_DIR".to_string(),
            root.path().display().to_string(),
        ),
        (
            "MRSM_PLUGINS_DIR".to_string(),
            plugins.path().display().to_string(),
        ),
    ];
    env.extend(
        extra_env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
    );
    let config = Config::new(ConfigOptions::with_env(env)).unwrap();
    Root {
        root,
        _plugins: plugins,
        config,
    }
}

fn team(name: &str, isolated: bool, connectors: &[&str], plugins: &[&str]) -> Team {
    let mut team = Team::new(name);
    team.id = Some(format!("id-{name}"));
    team.attributes
        .insert("isolated".into(), Value::Bool(isolated));
    team.set_names(
        "connectors",
        &connectors.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    );
    team.set_names(
        "plugins",
        &plugins.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    );
    team
}

/// The value at `path` in `map` (`Null` when missing).
fn at(map: &Map, path: &[&str]) -> Value {
    let mut current = Value::Map(map.clone());
    for key in path {
        current = current
            .as_map()
            .and_then(|m| m.get(*key))
            .cloned()
            .unwrap_or(Value::Null);
    }
    current
}

fn connectors_of(config: &Config) -> Map {
    config.get(&["meerschaum", "connectors"]).unwrap().to_map()
}

#[test]
fn a_scope_holds_reserved_and_granted_connectors() {
    let r = root_with(&[]);
    let wet = team("wetlands", true, &["sql:shared"], &["noaa"]);
    let scope = TeamScope::of(&wet, &connectors_of(&r.config));
    assert_eq!(
        scope.connectors.iter().cloned().collect::<Vec<_>>(),
        vec!["sql:shared".to_string(), "sql:wetlands".to_string()]
    );
    assert_eq!(scope.instance.as_deref(), Some("sql:wetlands"));
    assert!(scope.allows("sql:wetlands"));
    assert!(!scope.allows("sql:drylands"));
    assert!(!scope.allows("sql"));
    assert!(scope.may_name("plugin:noaa", false));
    assert!(!scope.may_name("plugin:other", false));
    assert!(scope.may_name("manual:gauge", false));
    assert!(!scope.may_name("sql:main", true));
    assert_eq!(scope.key(), "id-wetlands");
    let deleted = TeamScope::deleted("wetlands", Some("id-wetlands"));
    assert!(!deleted.allows("sql:wetlands"));
    assert!(!deleted.may_name("manual:gauge", false));
}

#[test]
fn the_team_config_holds_only_the_teams_connectors() {
    let r = root_with(&[("MRSM_SQL_ENVDB", "postgresql://envuser:env-pw@db:5432/env")]);
    let wet = team("wetlands", true, &["sql:shared"], &["noaa"]);
    let scope = TeamScope::of(&wet, &connectors_of(&r.config));
    let config = team_config(&r.config, &scope);
    let text = Value::Map(config.clone()).to_json_string();
    // The team's own password is resolved from its file; nobody else's credentials are present,
    // nor the reference to another team's file.
    assert!(text.contains("wet-pw-from-file"), "{text}");
    for secret in [
        "dry-pw-from-file",
        "dry_secret",
        "server-main-pw",
        "partner-pw",
        "env-pw",
        "grafana-admin-pw",
        "stack-pw",
        "other-key",
        "custom-secret",
        "wet_secret",
    ] {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
    let sql = at(&config, &["meerschaum", "connectors", "sql"]).to_map();
    assert!(sql["main"].is_null());
    assert!(sql["drylands"].is_null());
    assert!(sql["envdb"].is_null());
    assert!(sql["local"].is_null(), "defaults are withheld too");
    assert_eq!(
        at(&sql, &["shared", "database"]).as_str(),
        Some("/tmp/shared.db")
    );
    assert_eq!(
        at(&sql, &["wetlands", "username"]).as_str(),
        Some("mrsm_wetlands")
    );
    assert!(at(&config, &["meerschaum", "connectors", "api", "partner"]).is_null());
    assert!(at(&config, &["meerschaum", "connectors", "valkey", "main"]).is_null());
    assert_eq!(
        at(&config, &["meerschaum", "instance"]).as_str(),
        Some("sql:wetlands")
    );
    // Withheld sections and keys.
    assert!(!config.contains_key("stack"));
    assert!(!config.contains_key("custom"));
    let api = at(&config, &["api"]).to_map();
    assert!(!api.contains_key("grafana"));
    assert!(!api.contains_key("secret_key_note"));
    assert_eq!(
        at(&api, &["permissions", "default_role"]).as_str(),
        Some("editor")
    );
    // Only the team's plugins' settings.
    let plugins = at(&config, &["plugins"]).to_map();
    assert_eq!(
        at(&plugins, &["noaa", "api_key"]).as_str(),
        Some("noaa-key")
    );
    assert!(!plugins.contains_key("private_plugin"));
    // Ordinary settings pass through.
    assert!(config.contains_key("system"));
    assert!(config.contains_key("formatting"));
}

#[test]
fn a_team_process_sees_only_its_connectors() {
    let r = root_with(&[("MRSM_SQL_ENVDB", "postgresql://u:env-pw@db/env")]);
    let wet = team("wetlands", true, &[], &[]);
    let scope = TeamScope::of(&wet, &connectors_of(&r.config));
    let config = team_config(&r.config, &scope);
    let files = write_scope_files(&scope_dir(r.root.path(), &scope.key()), &config).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&files.config_file), 0o600);
        assert_eq!(mode(&files.config_dir), 0o700);
        assert_eq!(mode(&files.dir), 0o700);
    }
    let launching: Vec<(String, String)> = vec![
        ("MRSM_ROOT_DIR".into(), r.root.path().display().to_string()),
        (
            "MRSM_SQL_ENVDB".into(),
            "postgresql://u:env-pw@db/env".into(),
        ),
        ("MRSM_CONFIG".into(), "{\"x\": 1}".into()),
        ("MRSM_WORKER_EXPECT_VERSION".into(), "5.1.0".into()),
        ("PATH".into(), "/usr/bin".into()),
    ];
    let changes = scoped_env(&launching, &files, &scope, r.root.path(), &[]);
    let env = changes.apply(&launching);
    let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
    assert!(!names.contains(&"MRSM_SQL_ENVDB"));
    assert!(!names.contains(&"MRSM_CONFIG"));
    assert!(names.contains(&"MRSM_WORKER_EXPECT_VERSION"));
    assert!(names.contains(&"PATH"));
    assert!(names.contains(&TEAM_SCOPE_VAR));
    // The process built from that environment: the team's connector and nothing else.
    let team_process = Config::new(ConfigOptions::with_env(env)).unwrap();
    let wet_attrs = team_process
        .get(&["meerschaum", "connectors", "sql", "wetlands"])
        .unwrap();
    assert_eq!(
        at(&wet_attrs.to_map(), &["password"]).as_str(),
        Some("wet-pw-from-file")
    );
    for label in ["main", "drylands", "envdb", "local"] {
        assert!(
            team_process
                .get(&["meerschaum", "connectors", "sql", label])
                .is_none_or(|v| v.is_null()),
            "{label}"
        );
    }
    assert!(
        team_process
            .get(&["api", "grafana"])
            .is_none_or(|v| at(&v.to_map(), &["admin_password"]).is_null()),
    );
    assert_eq!(
        team_process
            .get(&["meerschaum", "instance"])
            .unwrap()
            .as_str(),
        Some("sql:wetlands")
    );
}

#[test]
fn the_fingerprint_follows_grants_and_passwords() {
    let r = root_with(&[]);
    let mut wet = team("wetlands", true, &[], &[]);
    let connectors = connectors_of(&r.config);
    let scope = TeamScope::of(&wet, &connectors);
    let first = fingerprint(&scope, &team_config(&r.config, &scope));
    // Whatever was looked up in between (defaults merged into memory), the same configuration.
    let _ = r.config.get(&["meerschaum"]);
    let _ = r.config.get(&["system"]);
    assert_eq!(first, fingerprint(&scope, &team_config(&r.config, &scope)));
    wet.set_names("connectors", &["sql:shared".to_string()]);
    let granted = TeamScope::of(&wet, &connectors);
    let second = fingerprint(&granted, &team_config(&r.config, &granted));
    assert_ne!(first, second);
    // A rotated password (the file a reference reads) changes it too.
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(r.root.path().join("wet_secret"), "rotated-password\n").unwrap();
    let rotated = fingerprint(&granted, &team_config(&r.config, &granted));
    assert_ne!(second, rotated);
}

#[test]
fn scope_records_survive_renames_and_deletions() {
    let root = tempfile::tempdir().unwrap();
    let scope = TeamScope {
        team: "Wetlands".into(),
        team_id: Some("abc-123".into()),
        connectors: ["sql:wetlands".to_string()].into_iter().collect(),
        ..TeamScope::default()
    };
    write_scope_record(root.path(), &scope).unwrap();
    assert_eq!(
        scope_key_of(root.path(), "wetlands").as_deref(),
        Some("abc-123")
    );
    assert_eq!(read_scope_record(root.path(), "abc-123").unwrap(), scope);
    let renamed = TeamScope {
        team: "Marshes".into(),
        ..scope.clone()
    };
    write_scope_record(root.path(), &renamed).unwrap();
    assert_eq!(scope_key_of(root.path(), "wetlands"), None);
    assert_eq!(
        scope_key_of(root.path(), "MARSHES").as_deref(),
        Some("abc-123")
    );
    write_scope_record(root.path(), &TeamScope::deleted("Marshes", Some("abc-123"))).unwrap();
    assert_eq!(scope_key_of(root.path(), "marshes"), None);
    let record = read_scope_record(root.path(), "abc-123").unwrap();
    assert!(record.deleted && record.connectors.is_empty());
}

#[test]
fn the_acting_team_is_never_a_union() {
    let wet = team("wetlands", true, &[], &[]);
    let dry = team("drylands", true, &[], &[]);
    let open = team("open", false, &[], &[]);
    let records = vec![wet.clone(), dry.clone(), open.clone()];
    let member = |names: &[&str]| -> Vec<(String, TeamRole)> {
        let mut out = vec![("Everyone".to_string(), TeamRole::Member)];
        out.extend(names.iter().map(|n| (n.to_string(), TeamRole::Member)));
        out
    };
    // Administrators and the open side: no team.
    assert_eq!(
        acting_team(true, &member(&["wetlands"]), &records, None, None),
        None
    );
    assert_eq!(acting_team(false, &member(&[]), &records, None, None), None);
    assert_eq!(
        acting_team(false, &member(&["wetlands", "open"]), &records, None, None),
        None
    );
    // One isolated team.
    assert_eq!(
        acting_team(false, &member(&["wetlands"]), &records, None, None).map(|t| t.name),
        Some("wetlands".into())
    );
    // Several: the object's team when they are in it, else their primary team.
    let both = member(&["wetlands", "drylands"]);
    assert_eq!(
        acting_team(false, &both, &records, Some("wetlands"), Some("drylands")).map(|t| t.name),
        Some("drylands".into())
    );
    assert_eq!(
        acting_team(false, &both, &records, Some("drylands"), Some("open")).map(|t| t.name),
        Some("drylands".into())
    );
    assert_eq!(
        acting_team(false, &both, &records, None, None).map(|t| t.name),
        Some("wetlands".into())
    );
}

#[test]
fn a_team_process_skips_the_roots_patch_directories() {
    let r = root_with(&[]);
    let patch = r.root.path().join("patch_config");
    std::fs::create_dir_all(&patch).unwrap();
    std::fs::write(
        patch.join("meerschaum.yaml"),
        "connectors:\n  sql:\n    leaked:\n      flavor: sqlite\n      database: /tmp/leak.db\n",
    )
    .unwrap();
    let env = vec![
        (
            "MRSM_ROOT_DIR".to_string(),
            r.root.path().display().to_string(),
        ),
        (TEAM_SCOPE_VAR.to_string(), "wetlands".to_string()),
    ];
    let config = Config::new(ConfigOptions::with_env(env)).unwrap();
    assert!(
        config
            .get(&["meerschaum", "connectors", "sql", "leaked"])
            .is_none_or(|v| v.is_null())
    );
    assert!(
        patch.exists(),
        "a team process never consumes the directory"
    );
}
