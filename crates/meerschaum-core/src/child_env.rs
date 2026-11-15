//! What a process Meerschaum starts that is not Meerschaum itself never inherits (5.1).
//!
//! Meerschaum's configuration patches and connector variables hold credentials: `MRSM_CONFIG`,
//! `MRSM_PATCH`, their file forms `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE`, and every
//! `MRSM_<TYPE>_<LABEL>` connector variable ([`get_connector_env_vars`] whose value is a URI or
//! JSON object, see [`is_secret_variable`]). Children that are
//! Meerschaum (anything started through [`crate::self_exe`], the Python worker, the API host,
//! jobs, the web terminal's `mrsm` shell) need them and inherit them. Everything else (package
//! installers, shell commands and `!command`, editors, SQL clients, `cargo`, `systemctl`, `ps`,
//! ...) gets an environment without them, through [`scrub_command`] (or [`scrub`] for an explicit
//! environment). The running process's own environment is never changed: `get_config` reads it
//! live.
//!
//! The Python package has the same rule in `meerschaum.config.environment.get_child_env()`.
//!
//! [`get_connector_env_vars`]: crate::config::get_connector_env_vars

use crate::static_config::static_config;

/// Whether a variable named `name` holding `value` is one a non-Meerschaum child must not
/// inherit: one of the two patches or their file forms, or a connector variable, i.e. a name
/// [`get_connector_env_vars`] matches whose value defines a connector (a URI with `://` or a
/// JSON object, the two forms a connector variable is read in). Another `MRSM_X_Y` (a path, a
/// flag of the user's own) is kept.
///
/// [`get_connector_env_vars`]: crate::config::get_connector_env_vars
pub fn is_secret_variable(name: &str, value: &str) -> bool {
    let names = &static_config().environment;
    if [
        &names.config,
        &names.patch,
        &names.config_file,
        &names.patch_file,
    ]
    .iter()
    .any(|n| n.as_str() == name)
    {
        return true;
    }
    let value = value.trim();
    (value.contains("://") || value.starts_with('{'))
        && !crate::config::get_connector_env_vars(&[(name.to_string(), String::new())]).is_empty()
}

/// The names in `env` a non-Meerschaum child must not inherit, in order.
pub fn secret_variables(env: &[(String, String)]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (name, value) in env {
        if is_secret_variable(name, value) && !out.contains(name) {
            out.push(name.clone());
        }
    }
    out
}

/// `env` without the variables a non-Meerschaum child must not inherit.
pub fn scrub(env: &[(String, String)]) -> Vec<(String, String)> {
    env.iter()
        .filter(|(name, value)| !is_secret_variable(name, value))
        .cloned()
        .collect()
}

/// Remove from `command`'s environment every variable of this process (and every variable set
/// on it explicitly before this call) that a non-Meerschaum child must not inherit.
pub fn scrub_command(command: &mut std::process::Command) {
    for name in secret_variables(&crate::process_env::vars()) {
        command.env_remove(name);
    }
    let explicit: Vec<String> = command
        .get_envs()
        .filter_map(|(k, v)| {
            let (k, v) = (k.to_string_lossy(), v?.to_string_lossy());
            is_secret_variable(&k, &v).then(|| k.into_owned())
        })
        .collect();
    for name in explicit {
        command.env_remove(name);
    }
}

/// `command` after [`scrub_command`]: `scrubbed(Command::new("pip")).args(...)`.
pub fn scrubbed(mut command: std::process::Command) -> std::process::Command {
    scrub_command(&mut command);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_patches_and_connector_variables_are_secret_and_the_rest_is_not() {
        for (name, value) in [
            ("MRSM_CONFIG", "{}"),
            ("MRSM_PATCH", "x:y"),
            ("MRSM_CONFIG_FILE", "/run/secrets/c"),
            ("MRSM_PATCH_FILE", "/p"),
            ("MRSM_SQL_MAIN", "postgresql://u:p@h/db"),
            ("MRSM_API_PROD", "https://u:p@api.example"),
            ("MRSM_VALKEY_CACHE", r#"{"host": "h", "password": "p"}"#),
            ("MRSM_MYTYPE_LABEL", "mytype://x"),
        ] {
            assert!(is_secret_variable(name, value), "{name}");
        }
        for (name, value) in [
            ("MRSM_ROOT_DIR", "/root"),
            ("MRSM_PLUGINS_DIR", "/plugins"),
            ("MRSM_PYTHON", "python3"),
            ("MRSM_NOASK", "true"),
            ("MRSM_DAEMON_ID", "job"),
            ("MRSM__COMPOSE_PROJECT", "p"),
            ("MRSM_TEST_EDITOR_RECEIPT", "/tmp/receipt.json"),
            ("MRSM_DUCKDB_LIB", "/usr/lib/libduckdb.so"),
            ("PATH", "/bin"),
            ("PGPASSWORD", "postgresql://not-mrsm"),
        ] {
            assert!(!is_secret_variable(name, value), "{name}");
        }
        let env: Vec<(String, String)> = [
            ("PATH", "/bin"),
            ("MRSM_SQL_X", "sqlite:////x.db"),
            ("MRSM_CONFIG", "{}"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert_eq!(secret_variables(&env), ["MRSM_SQL_X", "MRSM_CONFIG"]);
        assert_eq!(scrub(&env), vec![("PATH".to_string(), "/bin".to_string())]);
    }

    #[test]
    fn a_command_loses_the_secret_variables_and_keeps_the_rest() {
        let mut command = std::process::Command::new("env");
        command
            .env("MRSM_SQL_EXPLICIT", "postgresql://u:p@h/d")
            .env("MRSM_TEST_KEEP", "/a/path")
            .env("KEEP_ME", "1");
        scrub_command(&mut command);
        let envs: Vec<(String, Option<String>)> = command
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert!(
            envs.contains(&("MRSM_SQL_EXPLICIT".into(), None)),
            "{envs:?}"
        );
        assert!(
            envs.contains(&("KEEP_ME".into(), Some("1".into()))),
            "{envs:?}"
        );
    }
}
