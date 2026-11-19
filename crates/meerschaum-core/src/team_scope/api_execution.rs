//! Credential-free configuration for API-only execution.
//!
//! This is a preparation primitive, not an admission decision. Callers must authenticate the
//! original source, install the mandatory sandbox before discovering extensions, and refuse
//! unsupported aliases. Keeping the original connector keys preserves saved pipe and engine
//! identities; an alias must never fall through to a database driver.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Map, Value};

pub(crate) mod transport_capture;

/// A connector section with this key is a transport alias, never database attributes.
pub const ALIAS_ATTRIBUTE: &str = "mrsm_api_alias";
/// Set by the supervisor; the CLI enforces the prepared spec without probing an interpreter.
pub const PROFILE_VAR: &str = "MRSM_API_EXECUTION";
/// Current alias format. Unknown formats must fail closed.
pub const VERSION: u32 = 1;
/// Maximum decoded credential-free metadata document, shared with the result protocol.
pub const MAX_METADATA_BYTES: usize = 2 * 1024 * 1024;
/// Maximum decoded result of the exact Main-created connector registration operation.
pub const MAX_REGISTRATION_BYTES: usize = 1024 * 1024;

/// A logical connector carried over the main API with a rotating launch credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Alias {
    /// Alias format.
    pub version: u32,
    /// Original `type:label`; also the main API's logical instance selector.
    pub logical_keys: String,
    /// Main-owned callback origin, validated by the control update and HTTP client.
    pub callback_url: String,
    /// Main-owned, supervisor-rotated credential file (never bearer bytes).
    pub credential_file: PathBuf,
}

/// Strict connector keys: no URI, query, path, default section or hidden label.
pub fn split_keys(keys: &str) -> Result<(&str, &str), String> {
    let (typ, label) = keys
        .split_once(':')
        .ok_or("An API alias needs type:label keys.")?;
    let plain = |s: &str| {
        !s.is_empty()
            && !s.starts_with('.')
            && s.bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
    };
    if !matches!(typ, "sql" | "api" | "valkey") || !plain(label) || label == "default" {
        return Err("This connector cannot be represented by an API instance alias.".into());
    }
    Ok((typ, label))
}

impl Alias {
    /// Check identity and file shape. The HTTP transport additionally validates the origin.
    pub fn validate(&self, expected_keys: &str) -> Result<(), String> {
        split_keys(&self.logical_keys)?;
        let authority = self
            .callback_url
            .strip_prefix("https://")
            .or_else(|| self.callback_url.strip_prefix("http://"))
            .map(|rest| rest.strip_suffix('/').unwrap_or(rest));
        if self.version != VERSION
            || self.logical_keys != expected_keys
            || !self.credential_file.is_absolute()
            || self
                .credential_file
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            || self.callback_url.is_empty()
            || self.callback_url.len() > 2048
            || self
                .callback_url
                .bytes()
                .any(|c| c.is_ascii_control() || c.is_ascii_whitespace())
            || authority
                .is_none_or(|host| host.is_empty() || host.contains(['/', '@', '?', '#', '\\']))
        {
            return Err("Invalid API execution alias.".into());
        }
        Ok(())
    }

    /// A connector section contains only this descriptor; no original attributes survive.
    pub fn attributes(&self) -> Map {
        Map::from_iter([(
            ALIAS_ATTRIBUTE.into(),
            Value::from_json_str(&serde_json::to_string(self).expect("alias serialization"))
                .expect("alias value"),
        )])
    }

    /// Recognize a descriptor before resolving a SQL/Valkey connector. Malformed descriptors
    /// are errors, not absent aliases that may fall through to a native connector.
    pub fn from_attributes(attributes: &Map, expected_keys: &str) -> Result<Option<Self>, String> {
        let Some(value) = attributes.get(ALIAS_ATTRIBUTE) else {
            return Ok(None);
        };
        if attributes.len() != 1 {
            let keys = attributes
                .keys()
                .take(16)
                .map(|key| key.chars().take(64).collect::<String>())
                .collect::<Vec<_>>();
            return Err(format!(
                "An API execution alias cannot contain native connector attributes (attribute keys: {keys:?})."
            ));
        }
        let alias: Self = serde_json::from_str(&value.to_json_string())
            .map_err(|_| "Invalid API execution alias.".to_string())?;
        alias.validate(expected_keys)?;
        Ok(Some(alias))
    }
}

/// Construct a fresh configuration. Only explicitly admitted logical instances survive, with
/// every known default and label masked so registry default merging cannot restore credentials.
/// Arbitrary plugin/settings sections are deliberately absent: they may contain reusable secrets.
pub fn configuration(
    base: &Map,
    logical_instances: &BTreeSet<String>,
    default_instance: &str,
    callback_url: &str,
    credential_file: &Path,
) -> Result<Map, String> {
    if !logical_instances.contains(default_instance) {
        return Err("The API execution default instance is not admitted.".into());
    }
    let mut connectors = Map::new();
    let defaults = crate::config::default_config("", 1, cfg!(windows));
    for known in [&defaults, base] {
        if let Some(types) = known
            .get("meerschaum")
            .and_then(|v| v.get("connectors"))
            .and_then(Value::as_map)
        {
            for (typ, labels) in types {
                let out = connectors
                    .entry(typ.clone())
                    .or_insert_with(|| Value::Map(Map::new()))
                    .as_map_mut()
                    .expect("initialized type");
                out.insert("default".into(), Value::Null);
                if let Some(labels) = labels.as_map() {
                    out.extend(labels.keys().map(|label| (label.clone(), Value::Null)));
                }
            }
        }
    }
    for typ in ["sql", "api", "valkey"] {
        connectors
            .entry(typ.into())
            .or_insert_with(|| Value::Map(Map::from_iter([("default".into(), Value::Null)])));
    }
    for keys in logical_instances {
        let (typ, label) = split_keys(keys)?;
        let alias = Alias {
            version: VERSION,
            logical_keys: keys.clone(),
            callback_url: callback_url.into(),
            credential_file: credential_file.into(),
        };
        alias.validate(keys)?;
        let labels = connectors
            .get_mut(typ)
            .and_then(Value::as_map_mut)
            .expect("type initialized");
        labels.insert(label.into(), Value::Map(alias.attributes()));
    }
    // Ordinary API jobs/actions use the callback transport; it does not change logical pipe ids.
    let callback = Value::Map(Map::from_iter([
        ("uri".into(), callback_url.into()),
        (
            "credential_file".into(),
            credential_file.to_string_lossy().to_string().into(),
        ),
    ]));
    let mut callback_label = "main_callback".to_string();
    while logical_instances.contains(&format!("api:{callback_label}")) {
        callback_label.push('_');
    }
    connectors
        .get_mut("api")
        .and_then(Value::as_map_mut)
        .expect("api initialized")
        .insert(callback_label.clone(), callback);
    Ok(Map::from_iter([
        (
            "meerschaum".into(),
            Value::Map(Map::from_iter([
                ("connectors".into(), Value::Map(connectors)),
                ("instance".into(), default_instance.into()),
                ("api_instance".into(), default_instance.into()),
                ("executor".into(), format!("api:{callback_label}").into()),
            ])),
        ),
        (
            "system".into(),
            Value::Map(Map::from_iter([(
                "engine".into(),
                Value::Map(Map::from_iter([
                    ("python_team_sandbox".into(), "require_sockets".into()),
                    (
                        "python_team_env_passthrough".into(),
                        Value::List(Vec::new()),
                    ),
                ])),
            )])),
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_preserves_logical_identity_without_reusable_credentials() {
        let base = Value::from_json_str(r#"{"meerschaum":{"instance":"sql:main","connectors":{"sql":{"default":{"password":"default-secret"},"main":{"database":"/secret.sqlite","password":"db-secret"},"other":{"uri":"postgres://u:p@db"}},"api":{"main_callback":{"password":"old-secret"}},"custom":{"private":{"key":"custom-secret"}}}},"plugins":{"mine":{"secret":"plugin-secret"}},"stack":{"password":"stack-secret"}}"#).unwrap().to_map();
        let projected = configuration(
            &base,
            &BTreeSet::from(["sql:main".into()]),
            "sql:main",
            "https://main.example",
            Path::new("/launch/credential.json"),
        )
        .unwrap();
        let text = Value::Map(projected.clone()).to_json_string();
        for secret in [
            "default-secret",
            "db-secret",
            "secret.sqlite",
            "postgres://",
            "old-secret",
            "custom-secret",
            "plugin-secret",
            "stack-secret",
        ] {
            assert!(!text.contains(secret), "{secret}");
        }
        let mrsm = projected["meerschaum"].as_map().unwrap();
        assert_eq!(mrsm["instance"].as_str(), Some("sql:main"));
        let connectors = mrsm["connectors"].as_map().unwrap();
        let sql = connectors["sql"].as_map().unwrap();
        let attrs = sql["main"].to_map();
        let alias = Alias::from_attributes(&attrs, "sql:main").unwrap().unwrap();
        assert_eq!(alias.logical_keys, "sql:main");
        assert!(sql["other"].is_null());
    }

    #[test]
    fn malformed_aliases_never_become_native_connector_attributes() {
        let alias = Alias {
            version: VERSION,
            logical_keys: "sql:main".into(),
            callback_url: "https://main.example".into(),
            credential_file: "/launch/credential.json".into(),
        };
        assert!(Alias::from_attributes(&alias.attributes(), "sql:other").is_err());
        let mut attrs = alias.attributes();
        attrs.insert("password".into(), "forbidden".into());
        assert!(Alias::from_attributes(&attrs, "sql:main").is_err());
        for keys in [
            "sql:default",
            "sql:../secret",
            "sql:main?password=x",
            "plugin:mine",
            "valkey:",
        ] {
            assert!(split_keys(keys).is_err(), "{keys}");
        }
    }

    #[test]
    fn even_an_empty_base_cannot_resurrect_shipped_database_credentials() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path();
        let projected = configuration(
            &Map::new(),
            &BTreeSet::from(["sql:granted".into()]),
            "sql:granted",
            "https://main.example",
            &path.join("credential.json"),
        )
        .unwrap();
        let config_file = path.join("config.json");
        super::super::write_private(&config_file, &Value::Map(projected).to_json_string()).unwrap();
        let config_dir = path.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let config = crate::Config::new(crate::config::ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), path.to_string_lossy().into_owned()),
            (
                "MRSM_CONFIG_FILE".into(),
                config_file.to_string_lossy().into_owned(),
            ),
            (
                "MRSM_CONFIG_DIR".into(),
                config_dir.to_string_lossy().into_owned(),
            ),
            ("MRSM_TEAM_SCOPE".into(), "API execution".into()),
        ]))
        .unwrap();
        for label in ["default", "main", "local"] {
            assert!(
                config
                    .get(&["meerschaum", "connectors", "sql", label])
                    .is_none_or(|v| v.is_null()),
                "{label}"
            );
        }
        let attrs = config
            .get(&["meerschaum", "connectors", "sql", "granted"])
            .unwrap()
            .to_map();
        assert!(
            Alias::from_attributes(&attrs, "sql:granted")
                .unwrap()
                .is_some()
        );
    }
}
