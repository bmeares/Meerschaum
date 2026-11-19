//! The stack's provisioned Grafana data source reads through a read-only role (5.1).
//!
//! The stack provisions "Meerschaum Main" ([`MAIN_DATASOURCE_NAME`]) in Grafana's main
//! organization from `stack:grafana:datasource`. Whoever writes that file (`mrsm stack`, or
//! `sync_files` when Python first reads the `stack` config) switches a "Meerschaum Main" that
//! connects as `sql:main`'s user, or names no user, to [`MAIN_READONLY_ROLE`] with the generated
//! password kept in `<root>/api/`[`READONLY_PASSWORD_FILENAME`] ([`switch_main_datasource`]). The
//! role itself is set up by the stack (`meerschaum-cli`, `stack_readonly`) before Grafana starts.

use std::path::{Path, PathBuf};

use crate::value::{Map, Value};

/// The read-only login role the stack's provisioned main data source connects as. A team's
/// roles are `mrsm_<slug>` and `mrsm_<slug>_ro`, and a slug never holds `__` nor starts with
/// `_`, so no team can ever be given this name.
pub const MAIN_READONLY_ROLE: &str = "mrsm__main_ro";

/// The name of the data source the stack provisions in Grafana's main organization.
pub const MAIN_DATASOURCE_NAME: &str = "Meerschaum Main";

/// The file (in `API_CONFIG_RESOURCES_PATH`, beside `.api_secret_key`) that keeps the generated
/// password of [`MAIN_READONLY_ROLE`].
pub const READONLY_PASSWORD_FILENAME: &str = ".grafana_readonly_password";

/// Where the role's generated password is kept (`api_config_dir`: `<root>/api`).
pub fn readonly_password_path(api_config_dir: &Path) -> PathBuf {
    api_config_dir.join(READONLY_PASSWORD_FILENAME)
}

/// The secret in `path` (`what` names it in errors), or a new one (32 random bytes as hex)
/// written there, readable by its owner only. A file holding nothing, or `admin`, is refused.
pub fn read_or_generate_secret(path: &Path, what: &str) -> Result<String, String> {
    let failed = |e: &dyn std::fmt::Display| {
        format!("Cannot read or create {what} at {}: {e}", path.display())
    };
    if path.exists() {
        let text = std::fs::read_to_string(path).map_err(|e| failed(&e))?;
        let text = text.trim().to_string();
        if text.is_empty() || text == "admin" {
            return Err(failed(&"the file holds no usable password"));
        }
        return Ok(text);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| failed(&e))?;
    }
    let bytes = crate::users::urandom(32).map_err(|e| failed(&e))?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    std::fs::write(path, &hex).map_err(|e| failed(&e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(hex)
}

/// What [`switch_main_datasource`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MainSource {
    /// "Meerschaum Main" now connects as the read-only role; where it connects.
    Switched {
        /// The database server's host, as Grafana reaches it.
        host: String,
        /// Its port.
        port: u16,
        /// The database the data source reads.
        database: String,
    },
    /// "Meerschaum Main" connects as `user`, who is not `sql:main`'s user: left as written.
    Customized {
        /// The data source's user.
        user: String,
    },
    /// No data source named "Meerschaum Main".
    Absent,
}

fn text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(v) => v.py_str().trim().to_string(),
    }
}

/// `host:port` (a URL scheme or path is ignored; 5432 by default).
fn host_port(url: &str) -> (String, u16) {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let rest = rest.split('/').next().unwrap_or(rest);
    match rest.rsplit_once(':') {
        Some((host, port)) if port.parse::<u16>().is_ok() => {
            (host.to_string(), port.parse().unwrap_or(5432))
        }
        _ => (rest.to_string(), 5432),
    }
}

/// Switch "Meerschaum Main" in the provisioning config `datasource` (`stack:grafana:datasource`,
/// substituted) to [`MAIN_READONLY_ROLE`] with `password`, when it connects as `owner`
/// (`sql:main`'s user) or names no user: the default, and a config file holding a copy of the
/// old default (references or literal values alike). One whose user is anyone else was
/// customized and is left as written. `fallback_database`: `sql:main`'s database, for a data
/// source that names none.
pub fn switch_main_datasource(
    datasource: &mut Value,
    owner: Option<&str>,
    password: &str,
    fallback_database: Option<&str>,
) -> MainSource {
    let Some(Value::List(sources)) = datasource
        .as_map_mut()
        .and_then(|m| m.get_mut("datasources"))
    else {
        return MainSource::Absent;
    };
    let Some(Value::Map(source)) = sources
        .iter_mut()
        .find(|s| text(s.get("name")) == MAIN_DATASOURCE_NAME)
    else {
        return MainSource::Absent;
    };
    let user = text(source.get("user"));
    let owner = owner.map(str::trim).filter(|o| !o.is_empty());
    if !user.is_empty() && Some(user.as_str()) != owner {
        return MainSource::Customized { user };
    }
    source.insert("user".into(), Value::Str(MAIN_READONLY_ROLE.into()));
    // Grafana's deprecated top-level `password` is dropped: only `secureJsonData` is kept.
    source.shift_remove("password");
    let secure = source
        .entry("secureJsonData".to_string())
        .or_insert_with(|| Value::Map(Map::new()));
    if !matches!(secure, Value::Map(_)) {
        *secure = Value::Map(Map::new());
    }
    if let Value::Map(secure) = secure {
        secure.insert("password".into(), Value::Str(password.to_string()));
    }
    let (host, port) = host_port(&text(source.get("url")));
    let database = [
        source.get("jsonData").and_then(|j| j.get("database")),
        source.get("database"),
    ]
    .into_iter()
    .map(text)
    .find(|d| !d.is_empty())
    .or_else(|| fallback_database.map(str::to_string))
    .unwrap_or_default();
    MainSource::Switched {
        host,
        port,
        database,
    }
}

impl super::ConfigState {
    /// [`switch_main_datasource`] with this root's `sql:main` and generated password (an empty
    /// password when it cannot be read or created: the data source then cannot connect, never
    /// as the owner). The error, if any, is the second value.
    pub fn main_readonly_datasource(
        &mut self,
        datasource: &mut Value,
    ) -> (MainSource, Option<String>) {
        let main = self
            .get_config(
                &[
                    "meerschaum".to_string(),
                    "connectors".to_string(),
                    "sql".to_string(),
                    "main".to_string(),
                ],
                super::GetOptions {
                    patch: true,
                    substitute: true,
                    write_missing: false,
                    warn: false,
                },
            )
            .1
            .to_map();
        let setting = |key: &str| {
            main.get(key)
                .filter(|v| !v.is_null())
                .map(Value::py_str)
                .filter(|s| !s.is_empty())
        };
        let path = readonly_password_path(&self.paths().path("API_CONFIG_RESOURCES_PATH"));
        let (password, error) = match read_or_generate_secret(
            &path,
            "the password of Grafana's read-only main data source",
        ) {
            Ok(password) => (password, None),
            Err(e) => (String::new(), Some(e)),
        };
        let found = switch_main_datasource(
            datasource,
            setting("username").as_deref(),
            &password,
            setting("database").as_deref(),
        );
        (found, error)
    }
}
