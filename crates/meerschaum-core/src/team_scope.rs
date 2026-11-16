//! Team-scoped processes (5.1): the configuration a process gets when it runs Python code for an
//! isolated team, and which team a caller acts for.
//!
//! An isolated team's plugin code runs in processes started for that team (its own Python worker
//! pool on the API server, and its jobs). Those processes never receive the server's
//! configuration: they get [`team_config`], a configuration holding only the connectors of the
//! team's [`TeamScope`], through an owner-only file named by `MRSM_CONFIG_FILE` and an empty
//! config directory of their own (`MRSM_CONFIG_DIR`), and an environment built from an allowlist
//! ([`team_env`]): none of the server's `MRSM_CONFIG`, `MRSM_PATCH`, `MRSM_<TYPE>_<LABEL>` or
//! other variables (cloud credentials, tokens) unless an administrator passes them on by name.
//!
//! **What a team's configuration holds.**
//!
//! - `meerschaum:connectors`: the team's connectors (reserved for it with `teams: [<team>]`,
//!   granted in its settings, and its default instance), with their attributes resolved by the
//!   server (`MRSM{...}` references read, `default` merged). Every other connector the server or
//!   the defaults define is `null` (not defined). The `default` sections stay, without
//!   credentials.
//! - `meerschaum:instance` (and `api_instance`): the team's default instance.
//! - `system`, `formatting`, `shell`, `pipes`, `jobs`, `dash`: as written, without values under a
//!   credential's name ([`crate::config::secrets::is_credential_name`]), secret references
//!   (`MRSM{file:...}`, `MRSM{env:...}`) or URIs with a password.
//! - `api`: the same, without `grafana` (Grafana's administrator).
//! - `plugins`: the settings of the team's plugins only, as written.
//! - Withheld: `stack` (the stack's compose file holds the database and Grafana passwords) and any
//!   other top-level key (a file of your own under the config directory).
//!
//! **Who acts for a team** ([`acting_team`]): an administrator never does; a user every one of
//! whose teams (other than Everyone) is isolated acts for one of them; anyone else keeps the open
//! view. A user in several isolated teams acts for the team of the object at hand (a pipe's team,
//! a job's team) when they are in it, else for their primary team. Never for several at once.
//!
//! This keeps credentials of connectors a team was not given out of that team's processes. Those
//! processes run as the server's operating-system user; on Linux, [`sandbox`] restricts them to
//! the team's own paths so they cannot read the rest from disk either.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::access::{Team, TeamRole, reserved_for, same_team};
use crate::config::secrets::{contains_reference, has_inline_uri_password, is_credential_name};
use crate::config::{Config, GetOptions};
use crate::value::{Map, Value, apply_patch_maps};

/// The directory under the root that holds every team's scope (`team_scopes/<key>/`).
pub const SCOPES_DIR: &str = "team_scopes";
/// The record of a team's scope in its directory (connector keys, never credentials).
pub const SCOPE_FILE: &str = "scope.json";
/// The team's configuration in its directory (owner-only; `MRSM_CONFIG_FILE`).
pub const CONFIG_FILE: &str = "config.json";
/// The team's (empty) config directory (`MRSM_CONFIG_DIR`).
pub const CONFIG_DIR: &str = "config";
/// Team names (lowercase) to their scope keys.
pub const INDEX_FILE: &str = "index.json";
/// Set in a team-scoped process: the team's name. Informational (the configuration is what
/// scopes the process); a process with it set skips the root's `patch_config/` directories.
pub const TEAM_SCOPE_VAR: &str = "MRSM_TEAM_SCOPE";
/// The config directory variable.
pub const CONFIG_DIR_VAR: &str = "MRSM_CONFIG_DIR";
/// A job's own copy of its team's config directory (`jobs/<name>/.mrsm_team_config/`).
pub const JOB_CONFIG_DIR: &str = ".mrsm_team_config";

/// The top-level keys a team's configuration keeps (besides `meerschaum`, `api` and `plugins`).
pub const KEPT_SECTIONS: &[&str] = &["system", "formatting", "shell", "pipes", "jobs", "dash"];
/// The keys of `api` a team's configuration never holds.
pub const WITHHELD_API_KEYS: &[&str] = &["grafana"];

/// What an isolated team's processes may use: its connectors and plugins.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TeamScope {
    /// The team's name.
    pub team: String,
    /// The team's id (`None` for a team that is not registered).
    #[serde(default)]
    pub team_id: Option<String>,
    /// The connector keys (`type:label`) the team may use.
    #[serde(default)]
    pub connectors: BTreeSet<String>,
    /// The plugins the team was granted or installed.
    #[serde(default)]
    pub plugins: BTreeSet<String>,
    /// The team's default instance.
    #[serde(default)]
    pub instance: Option<String>,
    /// The team was deleted: its processes get no connectors at all.
    #[serde(default)]
    pub deleted: bool,
    /// The team is no longer isolated: its jobs get the server's configuration again.
    #[serde(default)]
    pub open: bool,
    /// Its members may install, edit and upload plugins (`attributes['plugins_write']`), so the
    /// team's processes run code the team chose (5.1: what decides whether the web terminal may
    /// use tmux). `None` in a record written before the field: taken as `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugins_write: Option<bool>,
}

impl TeamScope {
    /// The scope of `team`, given the server's `meerschaum:connectors` section (for the
    /// connectors reserved for it).
    pub fn of(team: &Team, connectors: &Map) -> TeamScope {
        let mut keys: BTreeSet<String> = reserved_for(connectors, &team.name).into_iter().collect();
        keys.extend(team.granted_connectors());
        let instance = team
            .default_instance()
            .or_else(|| reserved_for(connectors, &team.name).into_iter().next());
        TeamScope {
            team: team.name.clone(),
            team_id: team.id.clone(),
            connectors: keys,
            plugins: team.granted_plugins().into_iter().collect(),
            instance,
            deleted: false,
            open: false,
            plugins_write: Some(team.may_write_plugins()),
        }
    }

    /// The record of a team that is no longer isolated.
    pub fn opened(team: &Team) -> TeamScope {
        TeamScope {
            team: team.name.clone(),
            team_id: team.id.clone(),
            open: true,
            ..TeamScope::default()
        }
    }

    /// The scope of a deleted team: nothing.
    pub fn deleted(name: &str, id: Option<&str>) -> TeamScope {
        TeamScope {
            team: name.to_string(),
            team_id: id.map(str::to_string),
            deleted: true,
            ..TeamScope::default()
        }
    }

    /// The key of the team's directory and pools: its id when registered, else its name
    /// (lowercase), with anything but letters, digits, `-` and `_` replaced.
    pub fn key(&self) -> String {
        let raw = self
            .team_id
            .clone()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| format!("name-{}", self.team.to_lowercase()));
        raw.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    /// Whether the team may use the connector `keys` (a bare type is its `main` label).
    pub fn allows(&self, keys: &str) -> bool {
        if self.deleted {
            return false;
        }
        let keys = keys.trim();
        let full = if keys.contains(':') {
            keys.to_string()
        } else {
            format!("{keys}:main")
        };
        self.connectors.contains(&full)
    }

    /// Whether a pipe of the team may name the connector keys `keys`: one of its connectors, a
    /// granted data plugin (`plugin:<name>`), or a label that names no connector of the server's
    /// (`configured` false: data pushed through the API). Mirrors the API's rule
    /// ([`crate::access::can_name_connector`]).
    pub fn may_name(&self, keys: &str, configured: bool) -> bool {
        if self.deleted {
            return false;
        }
        if self.allows(keys) {
            return true;
        }
        if let Some(plugin) = keys.trim().strip_prefix("plugin:") {
            return self.plugins.contains(plugin);
        }
        !configured
    }

    /// The scope as a JSON-like value.
    pub fn to_value(&self) -> Value {
        Value::from_json_str(&serde_json::to_string(self).unwrap_or_default()).unwrap_or_default()
    }

    /// A scope from [`TeamScope::to_value`]'s shape.
    pub fn from_value(value: &Value) -> Option<TeamScope> {
        serde_json::from_str(&value.to_json_string()).ok()
    }
}

/// The team a caller acts for when Python code runs on their behalf (see the module
/// documentation): `None` for the open view (administrators, users with an open team, users in
/// no team but Everyone).
///
/// - `user_teams`: the caller's teams with their roles;
/// - `records`: the instance's team records (at least the caller's own);
/// - `primary`: the caller's primary team;
/// - `object_team`: the team of the object acted on (a pipe's, a job's), if any.
pub fn acting_team(
    is_admin: bool,
    user_teams: &[(String, TeamRole)],
    records: &[Team],
    primary: Option<&str>,
    object_team: Option<&str>,
) -> Option<Team> {
    if is_admin {
        return None;
    }
    let own: Vec<&Team> = records
        .iter()
        .filter(|team| !team.is_everyone())
        .filter(|team| {
            user_teams
                .iter()
                .any(|(name, _)| same_team(Some(name), Some(&team.name)))
        })
        .collect();
    if own.is_empty() || !own.iter().all(|team| team.is_isolated()) {
        return None;
    }
    let find = |name: Option<&str>| {
        name.and_then(|name| {
            own.iter()
                .find(|team| same_team(Some(&team.name), Some(name)))
                .map(|team| (*team).clone())
        })
    };
    find(object_team)
        .or_else(|| find(primary))
        .or_else(|| own.first().map(|team| (*team).clone()))
}

fn raw_options() -> GetOptions {
    GetOptions {
        patch: false,
        substitute: false,
        write_missing: false,
        warn: false,
    }
}

/// Remove `_symlinks` keys (the originals of substituted values) at every level.
fn strip_symlinks(value: &Value) -> Value {
    match value {
        Value::Map(map) => Value::Map(
            map.iter()
                .filter(|(k, _)| k.as_str() != "_symlinks")
                .map(|(k, v)| (k.clone(), strip_symlinks(v)))
                .collect(),
        ),
        Value::List(items) => Value::List(items.iter().map(strip_symlinks).collect()),
        other => other.clone(),
    }
}

/// Whether a string value must not reach a team's process: a secret reference or a URI with a
/// password.
fn withheld_text(text: &str) -> bool {
    contains_reference(text) || has_inline_uri_password(text)
}

/// `value` without what may hold a secret: entries under a credential's name, strings with a
/// secret reference or a URI with a password (also in lists).
pub fn scrub(value: &Value) -> Value {
    match value {
        Value::Map(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                if k == "_symlinks" {
                    continue;
                }
                if is_credential_name(k) && !v.is_null() && !matches!(v, Value::Map(_)) {
                    continue;
                }
                if let Value::Str(s) = v
                    && withheld_text(s)
                {
                    continue;
                }
                out.insert(k.clone(), scrub(v));
            }
            Value::Map(out)
        }
        Value::List(items) => Value::List(
            items
                .iter()
                .filter(|v| !matches!(v, Value::Str(s) if withheld_text(s)))
                .map(scrub)
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Every connector label the server's resolved `connectors` section and the defaults define, by
/// type.
fn known_labels(connectors: &Map, defaults: &Map) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for section in [connectors, defaults] {
        for (typ, labels) in section {
            if let Some(labels) = labels.as_map() {
                out.entry(typ.clone())
                    .or_default()
                    .extend(labels.keys().cloned());
            }
        }
    }
    out
}

/// A connector's attributes as the team's process builds it: the label's resolved section, with
/// `default` merged unless it is a SQLite / GeoPackage connector (which never inherit it).
fn connector_attributes(typ: &str, label: &str, connectors: &Map) -> Value {
    let section = |key: &str| {
        connectors
            .get(typ)
            .and_then(Value::as_map)
            .and_then(|labels| labels.get(key))
            .and_then(Value::as_map)
            .cloned()
            .unwrap_or_default()
    };
    let specific = section(label);
    let flavor = specific
        .get("flavor")
        .map(Value::py_str)
        .unwrap_or_default();
    let attrs = if typ == "sql" && matches!(flavor.as_str(), "sqlite" | "geopackage") {
        specific
    } else {
        apply_patch_maps(&section("default"), &specific)
    };
    strip_symlinks(&Value::Map(attrs))
}

/// The configuration of a team's processes (see the module documentation), from the server's
/// `config` (read now: call [`Config::refresh_connectors`] first to see connectors another
/// process added). Credentials of the team's own connectors are resolved into it; nothing of
/// another connector is.
pub fn team_config(config: &Config, scope: &TeamScope) -> Map {
    // Load every key read below first (without writing files), so that what a raw read sees
    // does not depend on what was read before: the result (and its fingerprint) is stable.
    for key in ["meerschaum", "api", "plugins"]
        .into_iter()
        .chain(KEPT_SECTIONS.iter().copied())
    {
        let _ = config.get_with(&[key], raw_options());
    }
    let resolved = GetOptions {
        substitute: true,
        ..raw_options()
    };
    let all_defaults = config.defaults();
    let default_of = |key: &str| all_defaults.get(key).cloned();
    let defaults = default_of("meerschaum")
        .and_then(|m| m.as_map().and_then(|m| m.get("connectors")).cloned())
        .map(|v| v.to_map())
        .unwrap_or_default();
    let (_, connectors) = config.get_with(&["meerschaum", "connectors"], resolved);
    // Over the defaults: whether an earlier lookup merged them into memory does not matter.
    let connectors = apply_patch_maps(&defaults, &connectors.to_map());
    let mut team_connectors = Map::new();
    for (typ, labels) in known_labels(&connectors, &defaults) {
        let mut out = Map::new();
        for label in labels {
            let keys = format!("{typ}:{label}");
            let value = if label == "default" {
                let raw = connectors
                    .get(&typ)
                    .and_then(Value::as_map)
                    .and_then(|m| m.get("default"))
                    .cloned()
                    .unwrap_or_else(|| Value::Map(Map::new()));
                scrub(&raw)
            } else if scope.allows(&keys) {
                connector_attributes(&typ, &label, &connectors)
            } else {
                Value::Null
            };
            out.insert(label, value);
        }
        team_connectors.insert(typ, Value::Map(out));
    }

    let raw = |key: &str| -> Option<Value> {
        let (valid, value) = config.get_with(&[key], raw_options());
        let value = (valid && !value.is_null()).then_some(value);
        match (default_of(key), value) {
            (Some(Value::Map(base)), Some(Value::Map(value))) => {
                Some(Value::Map(apply_patch_maps(&base, &value)))
            }
            (base, value) => value.or(base),
        }
    };
    let mut out = Map::new();

    let mut meerschaum = match raw("meerschaum").map(|v| scrub(&v)) {
        Some(Value::Map(m)) => m,
        _ => Map::new(),
    };
    meerschaum.insert("connectors".into(), Value::Map(team_connectors));
    let instance = scope
        .instance
        .clone()
        .filter(|keys| scope.allows(keys))
        .or_else(|| {
            scope
                .connectors
                .iter()
                .find(|keys| matches!(keys.split(':').next(), Some("sql" | "api" | "valkey")))
                .cloned()
        });
    if let Some(instance) = instance {
        meerschaum.insert("instance".into(), Value::Str(instance.clone()));
        meerschaum.insert("api_instance".into(), Value::Str(instance));
    }
    out.insert("meerschaum".into(), Value::Map(meerschaum));

    for key in KEPT_SECTIONS {
        if let Some(value) = raw(key) {
            out.insert((*key).to_string(), scrub(&value));
        }
    }
    if let Some(Value::Map(mut api)) = raw("api") {
        for key in WITHHELD_API_KEYS {
            api.shift_remove(*key);
        }
        out.insert("api".into(), scrub(&Value::Map(api)));
    }
    if let Some(Value::Map(plugins)) = raw("plugins") {
        let kept: Map = plugins
            .into_iter()
            .filter(|(name, _)| scope.plugins.contains(name))
            .map(|(name, value)| (name, strip_symlinks(&value)))
            .collect();
        out.insert("plugins".into(), Value::Map(kept));
    }
    resolve_own_symlinks(&mut out);
    out
}

/// Resolve the `MRSM{a:b:c}` symlinks of `config`'s strings against `config` itself (the team's
/// configuration, never the server's): a symlink to something the team does not get stays as
/// written. The team's processes then rarely substitute anything at lookup time.
fn resolve_own_symlinks(config: &mut Map) {
    static SYMLINK: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"MRSM\{([A-Za-z0-9_\-]+(?::[^}:\n]+)*)\}").expect("valid regex")
    });
    fn lookup(root: &Map, path: &str) -> Option<Value> {
        let mut current = Value::Map(root.clone());
        for key in path.split(':') {
            current = current.as_map()?.get(key)?.clone();
        }
        (!current.is_null()).then_some(current)
    }
    fn walk(value: &Value, root: &Map) -> Value {
        match value {
            Value::Str(text) if text.contains("MRSM{") => {
                if let Some(caps) = SYMLINK.captures(text)
                    && caps.get(0).is_some_and(|m| m.as_str() == text.as_str())
                {
                    let path = &caps[1];
                    if !crate::config::secrets::RESERVED_KEYS
                        .iter()
                        .any(|k| path.starts_with(&format!("{k}:")))
                        && let Some(found) = lookup(root, path)
                        && !matches!(&found, Value::Str(s) if s.contains("MRSM{"))
                    {
                        return found;
                    }
                    return value.clone();
                }
                let replaced = SYMLINK.replace_all(text, |caps: &regex::Captures| {
                    match lookup(root, &caps[1]) {
                        Some(Value::Str(s)) if !s.contains("MRSM{") => s,
                        Some(v) if !matches!(v, Value::Map(_) | Value::List(_)) => v.py_str(),
                        _ => caps[0].to_string(),
                    }
                });
                Value::Str(replaced.into_owned())
            }
            Value::Map(map) => Value::Map(
                map.iter()
                    .map(|(k, v)| (k.clone(), walk(v, root)))
                    .collect(),
            ),
            Value::List(items) => Value::List(items.iter().map(|v| walk(v, root)).collect()),
            other => other.clone(),
        }
    }
    for _ in 0..4 {
        let snapshot = config.clone();
        let resolved: Map = snapshot
            .iter()
            .map(|(k, v)| (k.clone(), walk(v, &snapshot)))
            .collect();
        if resolved == *config {
            break;
        }
        *config = resolved;
    }
}

/// A fingerprint of a team's configuration (and scope): it changes when a connector is granted
/// or revoked, the team is renamed or deleted, or a password changes.
pub fn fingerprint(scope: &TeamScope, config: &Map) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(serde_json::to_string(scope).unwrap_or_default().as_bytes());
    hasher.update(b"\0");
    hasher.update(Value::Map(config.clone()).to_json_string().as_bytes());
    hasher
        .finalize()
        .iter()
        .take(16)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Where a team's scope lives under `root` (`team_scopes/<key>/`).
pub fn scope_dir(root: &Path, key: &str) -> PathBuf {
    root.join(SCOPES_DIR).join(key)
}

/// The files of a team's processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeFiles {
    /// The team's directory.
    pub dir: PathBuf,
    /// Its (empty) config directory: `MRSM_CONFIG_DIR`.
    pub config_dir: PathBuf,
    /// Its configuration: `MRSM_CONFIG_FILE`.
    pub config_file: PathBuf,
}

/// Create `dir` (and its parents) readable by its owner only.
pub fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Write `contents` to `path`, readable by its owner only, atomically, unless it already holds
/// exactly that.
pub fn write_private(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    if std::fs::read_to_string(path).is_ok_and(|current| current == contents) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        return Ok(());
    }
    let dir = path.parent().unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temporary = dir.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let written = (|| {
        let mut file = options.open(&temporary)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    written
}

/// Write a team's configuration to `dir` (created owner-only, with an empty config directory):
/// the files its processes are started with.
pub fn write_scope_files(dir: &Path, config: &Map) -> std::io::Result<ScopeFiles> {
    let files = ScopeFiles {
        dir: dir.to_path_buf(),
        config_dir: dir.join(CONFIG_DIR),
        config_file: dir.join(CONFIG_FILE),
    };
    create_private_dir(&files.dir)?;
    create_private_dir(&files.config_dir)?;
    write_private(
        &files.config_file,
        &Value::Map(config.clone()).to_json_string(),
    )?;
    Ok(files)
}

/// Record a team's scope under `root` (`team_scopes/<key>/scope.json`, and the name in
/// `team_scopes/index.json`), so that its jobs are launched with it, also after a restart.
pub fn write_scope_record(root: &Path, scope: &TeamScope) -> std::io::Result<()> {
    let dir = scope_dir(root, &scope.key());
    create_private_dir(&dir)?;
    write_private(
        &dir.join(SCOPE_FILE),
        &serde_json::to_string_pretty(scope).unwrap_or_default(),
    )?;
    let index_path = root.join(SCOPES_DIR).join(INDEX_FILE);
    let mut index: BTreeMap<String, String> = std::fs::read_to_string(&index_path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default();
    let name = scope.team.to_lowercase();
    if !scope.deleted && !scope.open {
        index.retain(|_, key| key != &scope.key());
        index.insert(name, scope.key());
    } else if index.get(&name) == Some(&scope.key()) {
        index.remove(&name);
    }
    write_private(
        &index_path,
        &serde_json::to_string_pretty(&index).unwrap_or_default(),
    )
}

/// The scope recorded under `root` for `key`.
pub fn read_scope_record(root: &Path, key: &str) -> Option<TeamScope> {
    let text = std::fs::read_to_string(scope_dir(root, key).join(SCOPE_FILE)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Whether an isolated team is recorded under `root` (the index holds isolated teams only).
pub fn isolated_teams_recorded(root: &Path) -> bool {
    std::fs::read_to_string(root.join(SCOPES_DIR).join(INDEX_FILE))
        .ok()
        .and_then(|text| serde_json::from_str::<BTreeMap<String, String>>(&text).ok())
        .is_some_and(|index| !index.is_empty())
}

/// The isolated teams recorded under `root` whose members may install their own plugins
/// ([`TeamScope::plugins_write`]; a record that predates the field counts), by name: the teams
/// whose processes run code the team itself chose.
pub fn plugin_installing_isolated_teams(root: &Path) -> Vec<String> {
    let Some(index) = std::fs::read_to_string(root.join(SCOPES_DIR).join(INDEX_FILE))
        .ok()
        .and_then(|text| serde_json::from_str::<BTreeMap<String, String>>(&text).ok())
    else {
        return Vec::new();
    };
    index
        .values()
        .filter_map(|key| read_scope_record(root, key))
        .filter(|scope| !scope.open && !scope.deleted && scope.plugins_write.unwrap_or(true))
        .map(|scope| scope.team)
        .collect()
}

/// The scope key recorded under `root` for the team named `team` (any case).
pub fn scope_key_of(root: &Path, team: &str) -> Option<String> {
    let text = std::fs::read_to_string(root.join(SCOPES_DIR).join(INDEX_FILE)).ok()?;
    let index: BTreeMap<String, String> = serde_json::from_str(&text).ok()?;
    index.get(&team.trim().to_lowercase()).cloned()
}

/// The connector variables (`MRSM_<TYPE>_<LABEL>` of a built-in or plugin connector type, and
/// any `MRSM_SQL_*`, `MRSM_API_*`, `MRSM_VALKEY_*`) in `env`.
pub fn connector_variables(
    env: &[(String, String)],
    root: &Path,
    plugins_dirs: &[PathBuf],
) -> Vec<String> {
    use crate::config::connector_types::{is_builtin_connector_type, plugin_connector_types};
    use crate::config::{get_connector_env_vars, parse_connector_env_var};
    let mut plugin_types: Option<BTreeSet<String>> = None;
    get_connector_env_vars(env)
        .into_iter()
        .filter(|var| {
            let Some((typ, _)) = parse_connector_env_var(var) else {
                return false;
            };
            is_builtin_connector_type(&typ)
                || plugin_types
                    .get_or_insert_with(|| plugin_connector_types(root, plugins_dirs))
                    .contains(&typ)
        })
        .collect()
}

pub mod sandbox;

mod environment;
pub use environment::{
    FINGERPRINT_VAR, JOB_NAMES, MEERSCHAUM_NAMES, PASSTHROUGH_KEY, Passthrough, SYSTEM_NAMES,
    is_allowed, is_job_name, is_refused, may_record, team_env,
};

#[cfg(test)]
mod tests;
