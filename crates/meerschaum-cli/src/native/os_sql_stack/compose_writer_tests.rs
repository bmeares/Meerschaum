//! Tests of the files `stack` writes: the compose file's credentials, secret files and the read-only role.

use super::*;
use meerschaum_core::config::{Config, ConfigOptions};

fn compose_after(
    root: &std::path::Path,
    env: &[(&str, &str)],
    stack_first: bool,
) -> (String, String) {
    let mut vars: Vec<(String, String)> =
        vec![("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned())];
    vars.extend(env.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let mut opts = ConfigOptions::with_env(vars);
    opts.allow_writes = true;
    opts.home = root.to_path_buf();
    let mut out = Vec::new();
    let mut rt = Runtime::new(
        crate::types::DateContext::system(),
        80,
        &mut out,
        |_: &str| None,
    );
    rt.err = Box::new(std::io::sink());
    rt.config = Some(Config::new(opts).unwrap());
    let path = rt.config().paths().path("STACK_COMPOSE_PATH");
    let read = |path: &std::path::Path| std::fs::read_to_string(path).unwrap_or_default();
    let core = |rt: &mut Runtime<'_>| {
        rt.config()
            .with_state(|s| s.sync_files(&["stack"]))
            .unwrap();
    };
    if stack_first {
        write_stack(&mut rt).unwrap();
        let by_stack = read(&path);
        core(&mut rt);
        (by_stack, read(&path))
    } else {
        core(&mut rt);
        let by_core = read(&path);
        write_stack(&mut rt).unwrap();
        (read(&path), by_core)
    }
}

/// `mrsm stack` writes the hardened compose file, and the core `sync_files` (Python's first
/// read of the `stack` config) never downgrades it: one result on disk.
#[test]
fn the_core_sync_never_downgrades_the_stacks_compose_file() {
    for env in [
        vec![],
        vec![("MRSM_CONFIG", r#"{"api": {"grafana": {"enabled": true}}}"#)],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (by_stack, after_core) = compose_after(dir.path(), &env, true);
        assert!(by_stack.contains("grafana_readonly"), "{by_stack}");
        if !env.is_empty() {
            assert!(by_stack.contains(GRAFANA_PROXY_NETWORK), "{by_stack}");
            assert!(by_stack.contains("GF_AUTH_PROXY_ENABLED"), "{by_stack}");
        }
        assert_eq!(
            after_core, by_stack,
            "the core sync rewrote the compose file"
        );

        // The core sync first: it leaves the compose file to `mrsm stack`.
        let dir = tempfile::tempdir().unwrap();
        let (by_stack, by_core) = compose_after(dir.path(), &env, false);
        assert!(
            by_core.is_empty() || by_core == by_stack,
            "the core sync wrote an unhardened compose file:\n{by_core}"
        );
    }
}

/// A stack without a `grafana` service and without the `api` service's configuration is
/// still written by the core sync (nothing to harden). With the `api` service's
/// `MRSM_CONFIG`, the core sync leaves it to `mrsm stack`, which moves it to a file.
#[test]
fn the_core_sync_writes_a_compose_file_without_grafana() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("config")).unwrap();
    // Nor a `valkey` service (`mrsm stack` gives it its password in a file).
    let patch = r#"{"stack": {"docker-compose.yaml": {"services":
            {"grafana": null, "api": null, "valkey": null}}}}"#;
    let (by_stack, by_core) = compose_after(dir.path(), &[("MRSM_CONFIG", patch)], false);
    assert!(!by_core.is_empty());
    assert!(!by_core.contains("grafana_readonly"), "{by_core}");
    assert!(by_stack.contains("services:"), "{by_stack}");

    let dir = tempfile::tempdir().unwrap();
    let patch = r#"{"stack": {"docker-compose.yaml": {"services": {"grafana": null}}}}"#;
    let (by_stack, by_core) = compose_after(dir.path(), &[("MRSM_CONFIG", patch)], false);
    assert!(by_core.is_empty() || by_core == by_stack, "{by_core}");
    assert!(by_stack.contains("MRSM_CONFIG_FILE"), "{by_stack}");
}

/// A root whose files `write_stack` (and then the core sync) wrote, with `env` and an
/// optional `stack.yaml`: `(compose text, secrets directory)`.
fn stack_files(
    root: &std::path::Path,
    env: &[(&str, &str)],
    stack_yaml: Option<&str>,
) -> (String, std::path::PathBuf) {
    if let Some(text) = stack_yaml {
        std::fs::create_dir_all(root.join("config")).unwrap();
        std::fs::write(root.join("config").join("stack.yaml"), text).unwrap();
    }
    let (by_stack, after_core) = compose_after(root, env, true);
    assert_eq!(
        by_stack, after_core,
        "the core sync rewrote the compose file"
    );
    (by_stack, root.join("stack").join("secrets"))
}

const SECRET_CONFIG: &str = r#"{"meerschaum": {"connectors": {
        "sql": {"main": {"username": "mrsm", "password": "pg-s3cret-xyz"}},
        "api": {"main": {"password": "api-s3cret-xyz"}}}},
        "api": {"grafana": {"enabled": true, "admin_password": "gf-s3cret-xyz"}}}"#;

fn assert_no_secret(text: &str) {
    for secret in ["pg-s3cret-xyz", "api-s3cret-xyz", "gf-s3cret-xyz"] {
        assert!(!text.contains(secret), "{secret} in:\n{text}");
    }
}

/// The default stack's compose file holds paths, not credentials; the `api` service
/// reads them from files only its owner's directory reaches.
#[test]
fn the_stack_passes_the_api_its_configuration_by_file() {
    let dir = tempfile::tempdir().unwrap();
    let (compose, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], None);
    assert_no_secret(&compose);
    assert!(
        compose.contains("MRSM_CONFIG_FILE: /run/secrets/meerschaum/mrsm_config.json"),
        "{compose}"
    );
    assert!(compose.contains("MRSM_PATCH_FILE:"), "{compose}");
    assert!(
        compose.contains("GF_SECURITY_ADMIN_PASSWORD__FILE:"),
        "{compose}"
    );
    assert!(
        compose.contains(super::super::stack_secrets::REVISION_LABEL),
        "{compose}"
    );
    let config = std::fs::read_to_string(secrets.join("api").join("mrsm_config.json")).unwrap();
    let config: serde_json::Value = serde_json::from_str(&config).unwrap();
    assert_eq!(
        config["meerschaum"]["connectors"]["sql"]["main"]["password"],
        "pg-s3cret-xyz"
    );
    assert!(config["system"].is_object());
    let patch = std::fs::read_to_string(secrets.join("api").join("mrsm_patch.json")).unwrap();
    let patch: serde_json::Value = serde_json::from_str(&patch).unwrap();
    assert_eq!(
        patch["meerschaum"]["connectors"]["sql"]["main"]["host"],
        "db"
    );
    assert_eq!(patch["api"]["grafana"]["admin_password"], "gf-s3cret-xyz");
    let owner =
        std::fs::read_to_string(secrets.join("grafana_readonly").join("owner_password")).unwrap();
    assert_eq!(owner, "pg-s3cret-xyz");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&secrets), 0o700);
        assert_eq!(mode(&secrets.join("api")), 0o755);
        assert_eq!(mode(&secrets.join("api").join("mrsm_config.json")), 0o644);
    }
}

/// Upgrades: a `stack.yaml` holding the old default entries (as `MRSM{...}` references or
/// materialized) is moved to files; a customized entry's own keys reach the container.
#[test]
fn an_upgraded_stack_yaml_moves_to_files_and_keeps_customized_keys() {
    let references = r#"
docker-compose.yaml:
  services:
    api:
      environment:
        MRSM_CONFIG: '{"meerschaum": MRSM{!meerschaum}, "system": MRSM{!system}, "api": MRSM{!api}}'
        MRSM_PATCH: '{"meerschaum": {"connectors": {"sql": {"main": {"host": "db", "port": 5432}}}}}'
"#;
    let materialized = r#"
docker-compose.yaml:
  services:
    api:
      environment:
        MRSM_CONFIG: '{"meerschaum": {"connectors": {"sql": {"main": {"password": "pg-s3cret-xyz"}}}}, "custom": {"kept": "yes"}}'
"#;
    for (stack_yaml, custom) in [(references, false), (materialized, true)] {
        let dir = tempfile::tempdir().unwrap();
        let (compose, secrets) = stack_files(
            dir.path(),
            &[("MRSM_CONFIG", SECRET_CONFIG)],
            Some(stack_yaml),
        );
        assert_no_secret(&compose);
        let config = std::fs::read_to_string(secrets.join("api").join("mrsm_config.json")).unwrap();
        assert!(config.contains("pg-s3cret-xyz"), "{config}");
        assert_eq!(config.contains("\"kept\": \"yes\""), custom, "{config}");
        // The user's config file is never rewritten.
        assert_eq!(
            std::fs::read_to_string(dir.path().join("config").join("stack.yaml")).unwrap(),
            stack_yaml
        );
    }
}

/// The stack's Valkey requires `valkey:main`'s password (its images ignore
/// `VALKEY_PASSWORD`): a configuration file in its own secrets subdirectory, mounted
/// read-only, and no password in the compose file.
#[test]
fn the_stacks_valkey_requires_the_configured_password() {
    let dir = tempfile::tempdir().unwrap();
    let config = r#"{"meerschaum": {"connectors": {"valkey": {"main":
            {"password": "vk-s3cret-\"xyz"}}}}}"#;
    let (compose, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", config)], None);
    assert!(!compose.contains("vk-s3cret"), "{compose}");
    assert!(!compose.contains("VALKEY_PASSWORD"), "{compose}");
    assert!(
        compose.contains("- /run/secrets/meerschaum/valkey.conf"),
        "{compose}"
    );
    let conf = std::fs::read_to_string(secrets.join("valkey").join("valkey.conf")).unwrap();
    assert!(conf.contains(r#"requirepass "vk-s3cret-\"xyz""#), "{conf}");
    // The default password (an upgraded root that never set one).
    let dir = tempfile::tempdir().unwrap();
    let (_, secrets) = stack_files(dir.path(), &[], None);
    let conf = std::fs::read_to_string(secrets.join("valkey").join("valkey.conf")).unwrap();
    assert!(conf.contains(r#"requirepass "mrsm""#), "{conf}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&secrets.join("valkey")), 0o755);
        assert_eq!(mode(&secrets.join("valkey").join("valkey.conf")), 0o644);
    }
}

/// `valkey:main` without a password: one is generated once (owner-only), the stack's
/// Valkey requires it and the `api` service is given it; the next write keeps it.
#[test]
fn a_stack_without_a_valkey_password_gets_a_generated_one() {
    let dir = tempfile::tempdir().unwrap();
    let config = r#"{"meerschaum": {"connectors": {"valkey": {"main": {"password": null}}}}}"#;
    let (compose, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", config)], None);
    let generated = std::fs::read_to_string(secrets.join(".valkey_password")).unwrap();
    assert_eq!(generated.len(), 64);
    assert!(!compose.contains(&generated), "{compose}");
    let conf = std::fs::read_to_string(secrets.join("valkey").join("valkey.conf")).unwrap();
    assert!(
        conf.contains(&format!("requirepass \"{generated}\"")),
        "{conf}"
    );
    let patch = std::fs::read_to_string(secrets.join("api").join("mrsm_patch.json")).unwrap();
    let patch: serde_json::Value = serde_json::from_str(&patch).unwrap();
    assert_eq!(
        patch["meerschaum"]["connectors"]["valkey"]["main"]["password"],
        generated.as_str()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(secrets.join(".valkey_password"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let (_, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", config)], None);
    assert_eq!(
        std::fs::read_to_string(secrets.join(".valkey_password")).unwrap(),
        generated
    );
}

/// An image older than 5.1.0 keeps the environment form (its API server would start without
/// its configuration otherwise), and the files of a previous write are removed.
#[test]
fn an_old_image_keeps_the_environment_form() {
    let dir = tempfile::tempdir().unwrap();
    let (_, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], None);
    assert!(secrets.join("api").join("mrsm_config.json").exists());
    let old = r#"
docker-compose.yaml:
  services:
    api:
      image: bmeares/meerschaum:5.0.2-full
"#;
    let compose_path = dir.path().join("stack").join("docker-compose.yaml");
    #[cfg(unix)]
    let mode = |p: &std::path::Path| {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(p).unwrap().permissions().mode() & 0o777
    };
    // The hardened file holds paths: no reason to hide it.
    #[cfg(unix)]
    assert_ne!(mode(&compose_path), 0o600);
    let (compose, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], Some(old));
    assert!(compose.contains("MRSM_CONFIG: "), "{compose}");
    // The environment form holds the credentials: the file is owner-only.
    #[cfg(unix)]
    assert_eq!(mode(&compose_path), 0o600);
    assert!(!compose.contains("MRSM_CONFIG_FILE"), "{compose}");
    assert!(!secrets.join("api").join("mrsm_config.json").exists());
    // `stack:api_config_files: true` is the way for an image the rule cannot vouch for.
    let custom = r#"
api_config_files: true
docker-compose.yaml:
  services:
    api:
      image: example/meerschaum-custom:1
"#;
    let (compose, _) = stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], Some(custom));
    assert!(compose.contains("MRSM_CONFIG_FILE"), "{compose}");
    assert_no_secret(&compose);
}
