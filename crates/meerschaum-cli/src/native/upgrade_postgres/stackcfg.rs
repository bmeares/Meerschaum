//! The stack config edit of the switch: the `db` service's image, its data volume entry and the
//! top-level volume declaration, and nothing else (every other customization stays as it is).
//!
//! The edit is made on the `stack` key's RAW value (as read from `stack.yaml`, `MRSM{}` symlinks
//! and `<DOLLAR>` spellings untouched). The switch writes it as an edit of those lines of the file
//! ([`super::yamledit`]) when it can, and through the config layer otherwise.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value as Json;

use meerschaum_core::config::{ConfigMap, ConfigValue};

/// The `db` service's data volume, as Compose resolved it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataVolume {
    /// The key under the top-level `volumes:` (`meerschaum_db_data`).
    pub key: String,
    /// The Docker volume's name (`<project>_meerschaum_db_data`).
    pub name: String,
    /// The mount point in the container (`/home/postgres/pgdata`).
    pub target: String,
    /// Declared `external: true`.
    pub external: bool,
}

/// Whether `ancestor` is `path` or one of its parent directories (POSIX paths).
pub fn is_ancestor(ancestor: &str, path: &str) -> bool {
    let a = ancestor.trim_end_matches('/');
    let p = path.trim_end_matches('/');
    a.is_empty() || p == a || p.starts_with(&format!("{a}/"))
}

/// The named volume of the resolved `db` service that holds `data_directory`.
pub fn data_volume(
    service: &Json,
    volumes: &Json,
    data_directory: &str,
) -> Result<DataVolume, String> {
    let mounts = service
        .get("volumes")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default();
    let holding: Vec<&Json> = mounts
        .iter()
        .filter(|m| {
            m.get("target")
                .and_then(Json::as_str)
                .is_some_and(|t| is_ancestor(t, data_directory))
        })
        .collect();
    // The deepest mount wins (a bind mount inside a volume would shadow it).
    let Some(mount) = holding.into_iter().max_by_key(|m| {
        m.get("target")
            .and_then(Json::as_str)
            .map(str::len)
            .unwrap_or(0)
    }) else {
        return Err(format!(
            "The database's data directory ({data_directory}) is not on a volume of the `db` \
             service: its data lives in the container itself, so it cannot be moved safely."
        ));
    };
    let target = mount
        .get("target")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string();
    if mount.get("type").and_then(Json::as_str) != Some("volume") {
        return Err(format!(
            "The database's data directory is a bind mount ({} -> {target}), not a named volume; \
             `upgrade postgres` only moves named volumes. Follow the manual procedure in the docs \
             (\"Upgrading the stack\").",
            mount
                .get("source")
                .and_then(Json::as_str)
                .unwrap_or_default()
        ));
    }
    let key = mount
        .get("source")
        .and_then(Json::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "The database's data volume is anonymous (it has no name), so it cannot be kept \
             apart from the new one."
                .to_string()
        })?
        .to_string();
    let declaration = volumes.get(&key);
    let name = declaration
        .and_then(|d| d.get("name"))
        .and_then(Json::as_str)
        .unwrap_or(&key)
        .to_string();
    let external = declaration
        .and_then(|d| d.get("external"))
        .and_then(Json::as_bool)
        .unwrap_or(false);
    Ok(DataVolume {
        key,
        name,
        target,
        external,
    })
}

static PG_SUFFIX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)_pg\d+(_\d{8}t?\d{6}z?)?$").expect("regex"));

/// `meerschaum_db_data` -> `meerschaum_db_data_pg18`; an earlier `_pg17` suffix is replaced.
/// For the same major (another image of it) the run's timestamp keeps the names apart.
pub fn new_volume_key(old_key: &str, major: u32, run_id: &str) -> String {
    let stem = PG_SUFFIX.replace(old_key, "").to_string();
    let candidate = format!("{stem}_pg{major}");
    if candidate == old_key {
        restore_volume_key(old_key, major, run_id)
    } else {
        candidate
    }
}

/// A restore's new volume: always unique to the run (`meerschaum_db_data_pg18_20261018t120000z`).
pub fn restore_volume_key(old_key: &str, major: u32, run_id: &str) -> String {
    let stem = PG_SUFFIX.replace(old_key, "").to_string();
    format!("{stem}_pg{major}_{}", run_id.to_lowercase())
}

/// The Docker name of the new volume: Compose's `<project>_<key>` (the prefix the old name
/// carries), or the declared `name:` with the same suffix rule.
pub fn new_volume_name(old: &DataVolume, new_key: &str, declared_name: bool) -> String {
    if declared_name {
        let suffix = new_key.strip_prefix(&PG_SUFFIX.replace(&old.key, "").to_string());
        let stem = PG_SUFFIX.replace(&old.name, "").to_string();
        return format!("{stem}{}", suffix.unwrap_or(""));
    }
    match old.name.strip_suffix(&old.key) {
        Some(prefix) => format!("{prefix}{new_key}"),
        None => new_key.to_string(),
    }
}

/// The volume keys and Docker names the resolved Compose config declares.
#[derive(Debug, Clone, Default)]
pub struct Declared {
    pub keys: std::collections::BTreeSet<String>,
    pub names: std::collections::BTreeSet<String>,
}

impl Declared {
    pub fn from_resolved(resolved: &Json) -> Declared {
        let mut declared = Declared::default();
        if let Some(volumes) = resolved.get("volumes").and_then(Json::as_object) {
            for (key, decl) in volumes {
                declared.keys.insert(key.clone());
                if let Some(name) = decl.get("name").and_then(Json::as_str) {
                    declared.names.insert(name.to_string());
                }
            }
        }
        declared
    }
}

/// Why a volume name cannot be the new volume's, if it cannot.
fn taken(
    name: &str,
    key: &str,
    old: &DataVolume,
    declared: &Declared,
    exists: &dyn Fn(&str) -> bool,
) -> Option<String> {
    if name == old.name {
        return Some(format!("'{name}' is the current data volume"));
    }
    if declared.keys.contains(key) || declared.names.contains(name) {
        return Some(format!("the stack config already declares '{name}'"));
    }
    exists(name).then(|| format!("the volume '{name}' exists"))
}

/// The new volume's key and Docker name: the first of `keys` (in order of preference) whose name
/// is free: not the current data volume, not declared by the config, not an existing volume.
/// Also returns why the preferred name was passed over. `Err` when none is free.
pub fn choose_new_volume(
    old: &DataVolume,
    keys: &[String],
    declared_name: bool,
    declared: &Declared,
    exists: &dyn Fn(&str) -> bool,
) -> Result<(String, String, Option<String>), Vec<String>> {
    let mut passed = Vec::new();
    for key in keys {
        let name = new_volume_name(old, key, declared_name);
        match taken(&name, key, old, declared, exists) {
            Some(why) => {
                if !passed.contains(&why) {
                    passed.push(why);
                }
            }
            None => return Ok((key.clone(), name, passed.into_iter().next())),
        }
    }
    Err(passed)
}

/// The data volume's declaration must be a plain local volume: the new volume is created with
/// Docker's default driver, so a `driver` or `driver_opts` (a bind-backed or network volume)
/// could not be reproduced.
pub fn check_declaration(resolved: &Json, key: &str) -> Result<(), String> {
    let Some(decl) = resolved.get("volumes").and_then(|v| v.get(key)) else {
        return Ok(());
    };
    let driver = decl.get("driver").and_then(Json::as_str).unwrap_or("local");
    let options = decl
        .get("driver_opts")
        .and_then(Json::as_object)
        .is_some_and(|o| !o.is_empty());
    if driver != "local" || options {
        return Err(format!(
            "The data volume `{key}` is declared with {} in the stack config; this command \
             creates the new volume with Docker's default local driver and cannot reproduce \
             that. Follow the manual procedure in the docs (\"Upgrading PostgreSQL\").",
            if driver != "local" {
                format!("the driver '{driver}'")
            } else {
                "`driver_opts`".to_string()
            }
        ));
    }
    Ok(())
}

/// The labels the declaration gives its volume (Compose would put them on it).
pub fn declared_labels(resolved: &Json, key: &str) -> Vec<(String, String)> {
    match resolved
        .get("volumes")
        .and_then(|v| v.get(key))
        .and_then(|d| d.get("labels"))
    {
        Some(Json::Object(m)) => m
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
            .collect(),
        Some(Json::Array(a)) => a
            .iter()
            .filter_map(Json::as_str)
            .map(|kv| match kv.split_once('=') {
                Some((k, v)) => (k.to_string(), v.to_string()),
                None => (kv.to_string(), String::new()),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Where the new image keeps its data: the old mount point when it still holds the new image's
/// `PGDATA`; else the declared image volume that does (PostgreSQL 18's official images moved
/// `PGDATA` to `/var/lib/postgresql/<major>/docker` under a `/var/lib/postgresql` volume); else
/// `PGDATA` itself.
pub fn new_mount_target(old_target: &str, new_pgdata: &str, image_volumes: &[String]) -> String {
    if is_ancestor(old_target, new_pgdata) {
        return old_target.to_string();
    }
    image_volumes
        .iter()
        .filter(|v| is_ancestor(v, new_pgdata))
        .max_by_key(|v| v.len())
        .cloned()
        .unwrap_or_else(|| new_pgdata.to_string())
}

/// The planned edit of the `stack` config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Switch {
    pub image: String,
    pub old_key: String,
    pub old_target: String,
    pub new_key: String,
    pub new_target: String,
    /// The new volume's declared `name:` when the old declaration had one.
    pub new_declared_name: Option<String>,
}

fn compose_map(stack: &ConfigValue) -> Option<&ConfigMap> {
    stack.get("docker-compose.yaml")?.as_map()
}

/// A short-syntax volume entry: `(source, target, options)`.
fn split_short(entry: &str) -> Option<(&str, &str, Option<&str>)> {
    let mut parts = entry.splitn(3, ':');
    let source = parts.next()?;
    let target = parts.next()?;
    Some((source, target, parts.next()))
}

fn entry_matches(entry: &ConfigValue, key: &str, target: &str) -> bool {
    match entry {
        ConfigValue::Str(s) => split_short(s).is_some_and(|(src, tgt, _)| {
            src == key && tgt.trim_end_matches('/') == target.trim_end_matches('/')
        }),
        ConfigValue::Map(m) => {
            m.get("source").and_then(ConfigValue::as_str) == Some(key)
                && m.get("target")
                    .and_then(ConfigValue::as_str)
                    .is_some_and(|t| t.trim_end_matches('/') == target.trim_end_matches('/'))
        }
        _ => false,
    }
}

/// Whether a service other than `db` mounts volume `key` (its declaration then stays).
pub fn volume_shared(stack: &ConfigValue, key: &str) -> bool {
    compose_map(stack)
        .and_then(|c| c.get("services"))
        .and_then(ConfigValue::as_map)
        .is_some_and(|services| {
            services
                .iter()
                .any(|(name, svc)| name != "db" && references_volume(svc, key))
        })
}

fn references_volume(service: &ConfigValue, key: &str) -> bool {
    service
        .get("volumes")
        .and_then(|v| match v {
            ConfigValue::List(l) => Some(l),
            _ => None,
        })
        .is_some_and(|entries| {
            entries.iter().any(|e| match e {
                ConfigValue::Str(s) => split_short(s).is_some_and(|(src, _, _)| src == key),
                ConfigValue::Map(m) => m.get("source").and_then(ConfigValue::as_str) == Some(key),
                _ => false,
            })
        })
}

/// Check that the raw `stack` config holds the `db` image and the data volume entry exactly as
/// Compose resolved them (else something else, e.g. an `MRSM_CONFIG` patch or an override file,
/// decides them, and editing `stack.yaml` would not move the stack).
pub fn check_editable(
    stack: &ConfigValue,
    resolved_image: &str,
    volume: &DataVolume,
) -> Result<(), String> {
    let db = compose_map(stack)
        .and_then(|c| c.get("services"))
        .and_then(|s| s.get("db"))
        .ok_or("The stack config has no `db` service under `docker-compose.yaml: services`.")?;
    let image = db.get("image").and_then(ConfigValue::as_str).unwrap_or("");
    if image != resolved_image {
        return Err(format!(
            "The stack config's `db` image is '{image}' but Docker Compose runs \
             '{resolved_image}': something else sets it (an override file next to the compose \
             file, or a config patch). Remove that, then run this again."
        ));
    }
    let found = db
        .get("volumes")
        .and_then(|v| match v {
            ConfigValue::List(l) => Some(l),
            _ => None,
        })
        .is_some_and(|l| {
            l.iter()
                .any(|e| entry_matches(e, &volume.key, &volume.target))
        });
    if !found {
        return Err(format!(
            "The stack config's `db` service does not list the volume `{}:{}` that Docker \
             Compose mounts, so it cannot be edited safely.",
            volume.key, volume.target
        ));
    }
    Ok(())
}

/// The `stack` config with the switch applied.
pub fn apply(stack: &ConfigValue, switch: &Switch) -> Result<ConfigValue, String> {
    // Is the old volume used by another service? Then its declaration stays.
    let shared = volume_shared(stack, &switch.old_key);
    let mut stack = stack.clone();
    let ConfigValue::Map(root) = &mut stack else {
        return Err("The stack config is not a mapping.".into());
    };
    let Some(ConfigValue::Map(compose)) = root.get_mut("docker-compose.yaml") else {
        return Err("The stack config has no `docker-compose.yaml` mapping.".into());
    };
    {
        let Some(ConfigValue::Map(services)) = compose.get_mut("services") else {
            return Err("The stack config has no `services` mapping.".into());
        };
        let Some(ConfigValue::Map(db)) = services.get_mut("db") else {
            return Err("The stack config has no `db` service.".into());
        };
        db.insert("image".into(), ConfigValue::str(switch.image.clone()));
        let Some(ConfigValue::List(entries)) = db.get_mut("volumes") else {
            return Err("The `db` service has no `volumes` list.".into());
        };
        let Some(entry) = entries
            .iter_mut()
            .find(|e| entry_matches(e, &switch.old_key, &switch.old_target))
        else {
            return Err(format!(
                "The `db` service does not list the volume `{}:{}`.",
                switch.old_key, switch.old_target
            ));
        };
        match entry {
            ConfigValue::Str(s) => {
                let options = split_short(s).and_then(|(_, _, o)| o).map(str::to_string);
                *s = match options {
                    Some(o) => format!("{}:{}:{o}", switch.new_key, switch.new_target),
                    None => format!("{}:{}", switch.new_key, switch.new_target),
                };
            }
            ConfigValue::Map(m) => {
                m.insert("source".into(), ConfigValue::str(switch.new_key.clone()));
                m.insert("target".into(), ConfigValue::str(switch.new_target.clone()));
            }
            _ => unreachable!("matched a string or a mapping"),
        }
    }
    // The top-level declaration: the new volume right where the old one was declared.
    let volumes = compose
        .entry("volumes".to_string())
        .or_insert_with(ConfigValue::new_map);
    if volumes.is_null() {
        *volumes = ConfigValue::new_map();
    }
    let ConfigValue::Map(volumes) = volumes else {
        return Err("The stack config's top-level `volumes` is not a mapping.".into());
    };
    let old_decl = volumes.get(&switch.old_key).cloned();
    let mut new_decl = old_decl.clone().unwrap_or(ConfigValue::Null);
    if let (ConfigValue::Map(m), Some(name)) = (&mut new_decl, &switch.new_declared_name) {
        m.insert("name".into(), ConfigValue::str(name.clone()));
    }
    let position = volumes.get_index_of(&switch.old_key);
    if !shared {
        volumes.shift_remove(&switch.old_key);
    }
    match position {
        Some(i) if !shared => {
            volumes.shift_insert(i, switch.new_key.clone(), new_decl);
        }
        Some(i) => {
            volumes.shift_insert(i + 1, switch.new_key.clone(), new_decl);
        }
        None => {
            volumes.insert(switch.new_key.clone(), new_decl);
        }
    }
    Ok(stack)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn stack() -> ConfigValue {
        let json = json!({
            "project_name": "mrsm",
            "docker-compose.yaml": {
                "services": {
                    "db": {
                        "environment": {"POSTGRES_USER": "<DOLLAR>POSTGRES_USER"},
                        "image": "timescale/timescaledb-ha:pg17",
                        "ports": ["MRSM{meerschaum:connectors:sql:main:port}:5432"],
                        "volumes": [
                            "meerschaum_db_data:/home/postgres/pgdata",
                            "/root/db/initdb:/docker-entrypoint-initdb.d:z,ro"
                        ],
                        "shm_size": "1024m"
                    },
                    "api": {"image": "bmeares/meerschaum:api", "volumes": ["api_root:/meerschaum"]}
                },
                "networks": {"frontend": null, "backend": null},
                "volumes": {
                    "api_root": null,
                    "meerschaum_db_data": null,
                    "grafana_storage": null
                }
            },
            "filetype": "yaml"
        });
        ConfigValue::from_value(&serde_json::from_value(json).unwrap())
    }

    fn switch() -> Switch {
        Switch {
            image: "timescale/timescaledb-ha:pg18".into(),
            old_key: "meerschaum_db_data".into(),
            old_target: "/home/postgres/pgdata".into(),
            new_key: "meerschaum_db_data_pg18".into(),
            new_target: "/home/postgres/pgdata".into(),
            new_declared_name: None,
        }
    }

    fn resolved_db() -> Json {
        json!({
            "image": "timescale/timescaledb-ha:pg17",
            "volumes": [
                {"type": "volume", "source": "meerschaum_db_data", "target": "/home/postgres/pgdata", "volume": {}},
                {"type": "bind", "source": "/root/db/initdb", "target": "/docker-entrypoint-initdb.d", "read_only": true}
            ]
        })
    }

    #[test]
    fn the_data_volume_is_the_named_volume_holding_the_data_directory() {
        let volumes = json!({"meerschaum_db_data": {"name": "mrsm_meerschaum_db_data"}});
        let v = data_volume(&resolved_db(), &volumes, "/home/postgres/pgdata/data").unwrap();
        assert_eq!(
            v,
            DataVolume {
                key: "meerschaum_db_data".into(),
                name: "mrsm_meerschaum_db_data".into(),
                target: "/home/postgres/pgdata".into(),
                external: false,
            }
        );
        // Data outside every mount, or on a bind mount: refused.
        assert!(data_volume(&resolved_db(), &volumes, "/var/lib/postgresql/data").is_err());
        let bind = json!({"volumes": [{"type": "bind", "source": "/srv/pg", "target": "/home/postgres/pgdata"}]});
        assert!(
            data_volume(&bind, &volumes, "/home/postgres/pgdata/data")
                .unwrap_err()
                .contains("bind mount")
        );
    }

    #[test]
    fn new_volume_names_follow_the_major() {
        assert_eq!(
            new_volume_key("meerschaum_db_data", 18, "20261018T120000Z"),
            "meerschaum_db_data_pg18"
        );
        assert_eq!(
            new_volume_key("meerschaum_db_data_pg17", 18, "20261018T120000Z"),
            "meerschaum_db_data_pg18"
        );
        assert_eq!(
            new_volume_key("meerschaum_db_data_pg18", 18, "20261018T120000Z"),
            "meerschaum_db_data_pg18_20261018t120000z"
        );
        assert_eq!(
            restore_volume_key("meerschaum_db_data", 18, "20261018T120000Z"),
            "meerschaum_db_data_pg18_20261018t120000z"
        );
        assert_eq!(
            restore_volume_key(
                "meerschaum_db_data_pg18_20261001t000000z",
                18,
                "20261018T120000Z"
            ),
            "meerschaum_db_data_pg18_20261018t120000z"
        );
        assert_eq!(
            new_volume_key(
                "meerschaum_db_data_pg17_20261001t000000z",
                18,
                "20261018T120000Z"
            ),
            "meerschaum_db_data_pg18"
        );
        let old = DataVolume {
            key: "meerschaum_db_data".into(),
            name: "mrsm_meerschaum_db_data".into(),
            target: "/home/postgres/pgdata".into(),
            external: false,
        };
        assert_eq!(
            new_volume_name(&old, "meerschaum_db_data_pg18", false),
            "mrsm_meerschaum_db_data_pg18"
        );
        let named = DataVolume {
            name: "pgdata".into(),
            ..old
        };
        assert_eq!(
            new_volume_name(&named, "meerschaum_db_data_pg18", true),
            "pgdata_pg18"
        );
    }

    #[test]
    fn a_new_volume_never_takes_a_name_in_use() {
        // The declared name already carries the target major's suffix: the computed name would
        // be the LIVE data volume, so the run's own name is used.
        let live = DataVolume {
            key: "meerschaum_db_data".into(),
            name: "proj_pg18".into(),
            target: "/home/postgres/pgdata".into(),
            external: false,
        };
        let keys = vec![
            new_volume_key(&live.key, 18, "20261018T120000Z"),
            restore_volume_key(&live.key, 18, "20261018T120000Z"),
        ];
        let none = |_: &str| false;
        let (key, name, why) =
            choose_new_volume(&live, &keys, true, &Declared::default(), &none).unwrap();
        assert_eq!(key, "meerschaum_db_data_pg18_20261018t120000z");
        assert_eq!(name, "proj_pg18_20261018t120000z");
        assert_eq!(
            why.as_deref(),
            Some("'proj_pg18' is the current data volume")
        );

        // A volume left by an earlier run, or a name the config declares, is passed over too.
        let old = DataVolume {
            key: "meerschaum_db_data".into(),
            name: "mrsm_meerschaum_db_data".into(),
            target: "/home/postgres/pgdata".into(),
            external: false,
        };
        let exists = |n: &str| n == "mrsm_meerschaum_db_data_pg18";
        let (key, name, why) =
            choose_new_volume(&old, &keys, false, &Declared::default(), &exists).unwrap();
        assert_eq!(name, "mrsm_meerschaum_db_data_pg18_20261018t120000z");
        assert_eq!(key, keys[1]);
        assert!(why.unwrap().contains("exists"));
        let declared = Declared::from_resolved(&json!({"volumes": {
            "meerschaum_db_data_pg18": {"name": "mrsm_meerschaum_db_data_pg18"}
        }}));
        let (_, name, _) = choose_new_volume(&old, &keys, false, &declared, &none).unwrap();
        assert_eq!(name, "mrsm_meerschaum_db_data_pg18_20261018t120000z");
        // The preferred name when it is free.
        let (key, name, why) =
            choose_new_volume(&old, &keys, false, &Declared::default(), &none).unwrap();
        assert_eq!(
            (key.as_str(), name.as_str(), why),
            (
                "meerschaum_db_data_pg18",
                "mrsm_meerschaum_db_data_pg18",
                None
            )
        );
        // Nothing free: the reasons.
        let all = |_: &str| true;
        assert!(choose_new_volume(&old, &keys, false, &Declared::default(), &all).is_err());
    }

    #[test]
    fn only_plain_local_volume_declarations_are_moved() {
        let resolved = json!({"volumes": {
            "plain": {"name": "p"},
            "bound": {"driver": "local", "driver_opts": {"type": "none", "o": "bind", "device": "/srv/pg"}},
            "nfs": {"driver": "rexray/ebs"},
            "labelled": {"labels": {"backup": "nightly"}}
        }});
        assert!(check_declaration(&resolved, "plain").is_ok());
        assert!(check_declaration(&resolved, "missing").is_ok());
        assert!(
            check_declaration(&resolved, "bound")
                .unwrap_err()
                .contains("driver_opts")
        );
        assert!(
            check_declaration(&resolved, "nfs")
                .unwrap_err()
                .contains("rexray/ebs")
        );
        assert_eq!(
            declared_labels(&resolved, "labelled"),
            vec![("backup".to_string(), "nightly".to_string())]
        );
    }

    #[test]
    fn mount_targets_follow_the_new_images_pgdata() {
        // timescaledb-ha: the volume's mount point still holds PGDATA.
        assert_eq!(
            new_mount_target("/home/postgres/pgdata", "/home/postgres/pgdata/data", &[]),
            "/home/postgres/pgdata"
        );
        // postgres:18 moved PGDATA under its /var/lib/postgresql volume.
        assert_eq!(
            new_mount_target(
                "/var/lib/postgresql/data",
                "/var/lib/postgresql/18/docker",
                &["/var/lib/postgresql".to_string()]
            ),
            "/var/lib/postgresql"
        );
        // postgres:17 and earlier.
        assert_eq!(
            new_mount_target(
                "/var/lib/postgresql/data",
                "/var/lib/postgresql/data",
                &["/var/lib/postgresql/data".to_string()]
            ),
            "/var/lib/postgresql/data"
        );
        assert!(is_ancestor("/a/b", "/a/b/c"));
        assert!(!is_ancestor("/a/b", "/a/bc"));
    }

    #[test]
    fn the_switch_edits_only_the_image_the_volume_and_its_declaration() {
        let before = stack();
        check_editable(
            &before,
            "timescale/timescaledb-ha:pg17",
            &DataVolume {
                key: "meerschaum_db_data".into(),
                name: "mrsm_meerschaum_db_data".into(),
                target: "/home/postgres/pgdata".into(),
                external: false,
            },
        )
        .unwrap();
        let after = apply(&before, &switch()).unwrap();
        let db = after
            .get_path(&["docker-compose.yaml", "services", "db"])
            .unwrap();
        assert_eq!(
            db.get("image").and_then(ConfigValue::as_str),
            Some("timescale/timescaledb-ha:pg18")
        );
        let ConfigValue::List(vols) = db.get("volumes").unwrap() else {
            panic!()
        };
        assert_eq!(
            vols[0].as_str(),
            Some("meerschaum_db_data_pg18:/home/postgres/pgdata")
        );
        assert_eq!(
            vols[1].as_str(),
            Some("/root/db/initdb:/docker-entrypoint-initdb.d:z,ro")
        );
        // Everything else is untouched, the symlinks and `<DOLLAR>` spellings included.
        assert_eq!(
            db.get("ports"),
            before.get_path(&["docker-compose.yaml", "services", "db", "ports"])
        );
        assert_eq!(
            db.get_path(&["environment", "POSTGRES_USER"])
                .and_then(ConfigValue::as_str),
            Some("<DOLLAR>POSTGRES_USER")
        );
        let keys: Vec<&String> = after
            .get_path(&["docker-compose.yaml", "volumes"])
            .and_then(ConfigValue::as_map)
            .unwrap()
            .keys()
            .collect();
        assert_eq!(
            keys,
            vec!["api_root", "meerschaum_db_data_pg18", "grafana_storage"]
        );
        assert_eq!(after.get("project_name"), before.get("project_name"));
        assert_eq!(
            after.get_path(&["docker-compose.yaml", "services", "api"]),
            before.get_path(&["docker-compose.yaml", "services", "api"])
        );
    }

    #[test]
    fn a_volume_another_service_mounts_stays_declared() {
        let mut before = stack();
        if let ConfigValue::Map(root) = &mut before
            && let Some(ConfigValue::Map(compose)) = root.get_mut("docker-compose.yaml")
            && let Some(ConfigValue::Map(services)) = compose.get_mut("services")
        {
            services.insert(
                "backup".into(),
                ConfigValue::from_value(
                    &serde_json::from_value(json!({"volumes": ["meerschaum_db_data:/data:ro"]}))
                        .unwrap(),
                ),
            );
        }
        let after = apply(&before, &switch()).unwrap();
        let keys: Vec<&String> = after
            .get_path(&["docker-compose.yaml", "volumes"])
            .and_then(ConfigValue::as_map)
            .unwrap()
            .keys()
            .collect();
        assert_eq!(
            keys,
            vec![
                "api_root",
                "meerschaum_db_data",
                "meerschaum_db_data_pg18",
                "grafana_storage"
            ]
        );
    }

    #[test]
    fn long_syntax_entries_and_named_declarations_are_edited() {
        let json = json!({"docker-compose.yaml": {
            "services": {"db": {"image": "postgres:17", "volumes": [
                {"type": "volume", "source": "pg", "target": "/var/lib/postgresql/data"}
            ]}},
            "volumes": {"pg": {"name": "shared_pg"}}
        }});
        let before = ConfigValue::from_value(&serde_json::from_value(json).unwrap());
        let after = apply(
            &before,
            &Switch {
                image: "postgres:18".into(),
                old_key: "pg".into(),
                old_target: "/var/lib/postgresql/data".into(),
                new_key: "pg_pg18".into(),
                new_target: "/var/lib/postgresql".into(),
                new_declared_name: Some("shared_pg_pg18".into()),
            },
        )
        .unwrap();
        let entry = after
            .get_path(&["docker-compose.yaml", "services", "db", "volumes"])
            .and_then(|v| match v {
                ConfigValue::List(l) => l.first().cloned(),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            entry.get("source").and_then(ConfigValue::as_str),
            Some("pg_pg18")
        );
        assert_eq!(
            entry.get("target").and_then(ConfigValue::as_str),
            Some("/var/lib/postgresql")
        );
        assert_eq!(
            after
                .get_path(&["docker-compose.yaml", "volumes", "pg_pg18", "name"])
                .and_then(ConfigValue::as_str),
            Some("shared_pg_pg18")
        );
        assert!(
            after
                .get_path(&["docker-compose.yaml", "volumes", "pg"])
                .is_none()
        );
    }

    #[test]
    fn a_config_that_does_not_hold_the_running_values_is_refused() {
        let v = DataVolume {
            key: "meerschaum_db_data".into(),
            name: "x".into(),
            target: "/home/postgres/pgdata".into(),
            external: false,
        };
        assert!(check_editable(&stack(), "timescale/timescaledb-ha:pg16", &v).is_err());
        let other = DataVolume {
            key: "elsewhere".into(),
            ..v
        };
        assert!(check_editable(&stack(), "timescale/timescaledb-ha:pg17", &other).is_err());
    }
}
