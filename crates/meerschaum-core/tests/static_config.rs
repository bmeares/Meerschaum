//! Parity: `static.toml` equals the Python `STATIC_CONFIG` (dumped by `fixtures/gen_static.py`).

mod common;

use meerschaum_core::static_config::{STATIC_TOML, StaticConfig, parse_static_toml};

#[test]
fn static_toml_matches_python() {
    let fixture = common::load_fixture("static_config.json");
    let cpu = fixture.get("__cpu_count__").and_then(|v| v.as_i64());
    let expected = fixture.get("static_config").expect("static_config key");
    let actual = parse_static_toml(STATIC_TOML, cpu).expect("valid static.toml");
    common::assert_same(&actual, expected, "static.toml");
}

#[test]
fn typed_static_config_parses() {
    let cfg = StaticConfig::from_toml(STATIC_TOML, Some(4)).expect("typed");
    assert_eq!(
        cfg.users.password_hash.pbkdf2_sha256_default_rounds,
        1_000_000
    );
    assert_eq!(cfg.environment.root, "MRSM_ROOT_DIR");
    assert_eq!(cfg.config.symlinks_key, "_symlinks");
    assert_eq!(cfg.tokens.scopes.len(), 23);
    assert_eq!(
        cfg.sql.create_engine_flavors["mssql"].to_sql.get("method"),
        Some(&meerschaum_core::Value::Null)
    );
    assert_eq!(
        cfg.get(&["sql", "default_create_engine_args", "pool_size"])
            .and_then(|v| v.as_i64()),
        Some(4)
    );
    assert_eq!(
        cfg.system.success.ignore,
        vec![
            Some("Success".to_string()),
            Some("successSucceeded".to_string()),
            Some(String::new()),
            None
        ]
    );
    // `os.cpu_count()` returning `None` falls back to the marker's default.
    let none = StaticConfig::from_toml(STATIC_TOML, None).expect("typed");
    assert_eq!(
        none.get(&["sql", "default_create_engine_args", "max_overflow"])
            .and_then(|v| v.as_i64()),
        Some(10)
    );
}
