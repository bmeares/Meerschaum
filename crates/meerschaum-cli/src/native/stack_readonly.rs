//! The stack's Grafana reads the main database through a read-only role.
//!
//! The stack provisions a data source named "Meerschaum Main" in Grafana's main organization
//! (`stack:grafana:datasource`, written to Grafana's provisioning directory). Up to 5.0 it
//! connected with `sql:main`'s credentials: the database's owner, a superuser in the stack's own
//! database. Whenever `mrsm stack` writes the stack's files:
//!
//! - **The data source.** A "Meerschaum Main" whose `user` is `sql:main`'s user (whether the
//!   config holds the `MRSM{...}` references of the default or the literal values), or has no
//!   user, is written with the read-only role and its generated password instead
//!   ([`meerschaum_core::config::grafana_readonly`], which every writer of the file applies).
//!   One whose user is anyone else was customized: it is written as it is, with a warning while
//!   Grafana's anonymous access is on. The user's config file is never rewritten; the switch
//!   happens each time the files are written.
//! - **The role.** The compose file gets a one-shot service ([`SERVICE`]) built from the `db`
//!   service's image that runs the role's setup with `psql`
//!   ([`meerschaum_connector_sql::team_database::main_readonly_psql_script`]) once `db` is
//!   healthy, and `grafana` waits for it to finish successfully: Grafana never starts with a
//!   data source whose role was not set up, and a failed setup stops `stack up` with an error.
//!   It runs on every `stack up` and is idempotent.
//! - **Grafana picks it up.** Grafana reads its provisioning files only when it starts, so the
//!   `grafana` service carries a hash of the provisioned data sources ([`REVISION_VARIABLE`]): a
//!   change recreates the container, and Grafana updates "Meerschaum Main" in place (same name,
//!   same uid: dashboards keep working).
//!
//! The role reads the instance's own schemas; more schemas are listed in
//! `stack:grafana:readonly_schemas`, never a team's.

use meerschaum_connector_sql::team_database::{self, MAIN_READONLY_ROLE};
use meerschaum_core::value::{Map, Value};
use sha2::Digest;

/// The one-shot compose service that sets the role up before Grafana starts.
pub const SERVICE: &str = "grafana_readonly";

/// The `grafana` service's variable that changes with the provisioned data sources.
pub const REVISION_VARIABLE: &str = "MRSM_DATASOURCE_REVISION";

/// The [`SERVICE`] variable holding the read-only role's password (moved to a file by
/// [`super::stack_secrets`] with the owner's `PGPASSWORD`).
pub const PASSWORD_VARIABLE: &str = "MRSM_READONLY_PASSWORD";

/// The [`SERVICE`] command when its passwords are in the environment.
const ENV_COMMAND: &str = "printf '%s\\n' \"$MRSM_READONLY_SQL\" | psql -X -v ro_password=\"$MRSM_READONLY_PASSWORD\" -f -";

/// The [`SERVICE`] command when its two passwords are in files ([`super::stack_secrets`]):
/// the owner's at `owner_path`, the role's at `password_path`. Neither is in the container's
/// configuration (`docker inspect`), and a file that cannot be read stops the setup (and so
/// Grafana) before `psql` runs.
pub fn file_command(owner_path: &str, password_path: &str) -> String {
    format!(
        "PGPASSWORD=\"$(cat '{owner_path}')\" && ro_password=\"$(cat '{password_path}')\" && \
         export PGPASSWORD && \
         printf '%s\\n' \"$MRSM_READONLY_SQL\" | psql -X -v ro_password=\"$ro_password\" -f -"
    )
}

/// The provisioned data source's name.
pub const MAIN_DATASOURCE: &str = meerschaum_core::config::grafana_readonly::MAIN_DATASOURCE_NAME;

use meerschaum_core::config::grafana_readonly::trimmed_text;
pub use meerschaum_core::config::grafana_readonly::{MainSource, switch_main_datasource};

/// The schemas `stack:grafana:readonly_schemas` asks the role to read too (a list or a
/// comma-separated string), and those refused: the schema of a connector reserved for teams
/// (`meerschaum:connectors:sql:<label>` with `teams`), `sql_connectors`.
pub fn readonly_schemas(stack_grafana: &Map, sql_connectors: &Map) -> (Vec<String>, Vec<String>) {
    let wanted: Vec<String> = match stack_grafana.get("readonly_schemas") {
        Some(Value::List(items)) => items
            .iter()
            .filter(|v| !v.is_null())
            .map(|v| v.py_str().trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Some(Value::Str(s)) => s
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => Vec::new(),
    };
    let team_schemas: Vec<String> = sql_connectors
        .values()
        .filter_map(|attrs| attrs.as_map())
        .filter(|attrs| attrs.get("teams").is_some_and(|t| !t.is_null()))
        .map(|attrs| trimmed_text(attrs.get("schema")))
        .filter(|s| !s.is_empty())
        .collect();
    let mut granted = Vec::new();
    let mut refused = Vec::new();
    for schema in wanted {
        if team_schemas.contains(&schema) {
            refused.push(schema);
        } else if !granted.contains(&schema) {
            granted.push(schema);
        }
    }
    (granted, refused)
}

/// Whether a compose service's `environment` sets `name` to a true value.
fn environment_true(service: Option<&Value>, name: &str) -> bool {
    let value = match service.and_then(|s| s.get("environment")) {
        Some(Value::Map(map)) => map.get(name).map(|v| v.py_str()),
        Some(Value::List(items)) => items.iter().find_map(|item| match item {
            Value::Str(s) => s.strip_prefix(&format!("{name}=")).map(str::to_string),
            _ => None,
        }),
        _ => None,
    };
    value.is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
}

/// Whether the compose file's `grafana` service turns anonymous access on.
pub fn anonymous_enabled(compose: &Value) -> bool {
    environment_true(
        compose.get("services").and_then(|s| s.get("grafana")),
        "GF_AUTH_ANONYMOUS_ENABLED",
    )
}

/// What sets the role up: `(owner's user, owner's password)` connect to `source`'s database.
pub struct RoleSetup<'a> {
    pub owner_user: &'a str,
    pub owner_password: &'a str,
    pub password: &'a str,
    pub schemas: &'a [String],
    /// [`revision`] of the provisioned data sources.
    pub revision: &'a str,
}

/// A short hash of the provisioned data sources (and the setup they need).
pub fn revision(datasource: &Value, schemas: &[String]) -> String {
    let text = meerschaum_core::yaml::dump(datasource).unwrap_or_default();
    let mut hash = sha2::Sha256::new();
    hash.update(text.as_bytes());
    hash.update(team_database::main_readonly_psql_script(schemas).as_bytes());
    hex::encode(hash.finalize())[..16].to_string()
}

/// Non-empty Docker Compose `profiles` entries on a service (`None` when absent or empty).
fn service_profiles(service: &Map) -> Option<Vec<String>> {
    let items: Vec<String> = match service.get("profiles") {
        Some(Value::List(values)) => values
            .iter()
            .filter(|v| !v.is_null())
            .map(Value::py_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Some(Value::Str(single)) => {
            let trimmed = single.trim();
            if trimmed.is_empty() {
                Vec::new()
            } else {
                vec![trimmed.to_string()]
            }
        }
        _ => Vec::new(),
    };
    (!items.is_empty()).then_some(items)
}

/// Choose `profiles` for `grafana_readonly` so Docker Compose never validates it when `db` is
/// excluded by profiles, and does not run it unnecessarily when `grafana` is gated by profiles.
fn resolved_profiles(db: &Map, grafana: &Map) -> Option<Vec<String>> {
    match (service_profiles(db), service_profiles(grafana)) {
        (Some(db_profiles), Some(grafana_profiles)) => {
            let shared: Vec<String> = db_profiles
                .iter()
                .filter(|p| grafana_profiles.contains(p))
                .cloned()
                .collect();
            Some(if shared.is_empty() {
                db_profiles
            } else {
                shared
            })
        }
        (Some(db_profiles), None) => Some(db_profiles),
        (None, Some(grafana_profiles)) => Some(grafana_profiles),
        (None, None) => None,
    }
}

/// Add [`SERVICE`] to the compose config and make `grafana` wait for it. `Err`: the stack has no
/// `db` service to take `psql` from (or no `grafana`).
pub fn add_role_setup(
    compose: &mut Value,
    source: &MainSource,
    setup: &RoleSetup<'_>,
) -> Result<(), String> {
    let MainSource::Switched {
        host,
        port,
        database,
    } = source
    else {
        return Ok(());
    };
    let Some(services) = compose
        .as_map_mut()
        .and_then(|c| c.get_mut("services"))
        .and_then(Value::as_map_mut)
    else {
        return Err("the stack has no services".into());
    };
    let Some(grafana_ref) = services.get("grafana").and_then(Value::as_map).cloned() else {
        return Err("the stack has no `grafana` service".into());
    };
    let Some(db) = services.get("db").and_then(Value::as_map).cloned() else {
        return Err("the stack has no `db` service to set the role up with".into());
    };
    let Some(image) = db.get("image").map(Value::py_str).filter(|i| !i.is_empty()) else {
        return Err("the stack's `db` service has no `image`".into());
    };
    let s = |v: &str| Value::Str(v.to_string());
    let mut environment = Map::new();
    environment.insert("PGHOST".into(), s(host));
    environment.insert("PGPORT".into(), s(&port.to_string()));
    environment.insert("PGUSER".into(), s(setup.owner_user));
    environment.insert("PGPASSWORD".into(), s(setup.owner_password));
    environment.insert("PGDATABASE".into(), s(database));
    environment.insert("PGCONNECT_TIMEOUT".into(), s("10"));
    environment.insert(PASSWORD_VARIABLE.into(), s(setup.password));
    environment.insert(
        "MRSM_READONLY_SQL".into(),
        s(&team_database::main_readonly_psql_script(setup.schemas)),
    );
    let mut service = Map::new();
    service.insert("image".into(), s(&image));
    service.insert(
        "entrypoint".into(),
        Value::List(vec![s("/bin/sh"), s("-c")]),
    );
    service.insert("command".into(), Value::List(vec![s(ENV_COMMAND)]));
    service.insert("environment".into(), Value::Map(environment));
    service.insert("restart".into(), s("no"));
    let mut condition = Map::new();
    condition.insert(
        "condition".into(),
        s(if db.contains_key("healthcheck") {
            "service_healthy"
        } else {
            "service_started"
        }),
    );
    let mut depends = Map::new();
    depends.insert("db".into(), Value::Map(condition));
    service.insert("depends_on".into(), Value::Map(depends));
    match db.get("networks") {
        Some(Value::Map(networks)) => {
            service.insert(
                "networks".into(),
                Value::List(networks.keys().map(|k| s(k)).collect()),
            );
        }
        Some(Value::List(networks)) => {
            service.insert("networks".into(), Value::List(networks.clone()));
        }
        _ => {}
    }
    let readonly_profiles = resolved_profiles(&db, &grafana_ref);
    // `grafana` may be active while `grafana_readonly` is not (its profiles follow `db`'s):
    // then `grafana`'s dependency on it must be optional, or Compose rejects the project.
    let optional_dependency = readonly_profiles
        .as_ref()
        .is_some_and(|profiles| service_profiles(&grafana_ref).as_ref() != Some(profiles));
    if let Some(profiles) = readonly_profiles {
        service.insert(
            "profiles".into(),
            Value::List(profiles.into_iter().map(|p| s(&p)).collect()),
        );
    }
    services.insert(SERVICE.into(), Value::Map(service));

    let Some(Value::Map(grafana)) = services.get_mut("grafana") else {
        return Err("the stack has no `grafana` service".into());
    };
    let mut depends = match grafana.get("depends_on") {
        Some(Value::Map(map)) => map.clone(),
        Some(Value::List(items)) => items
            .iter()
            .map(|item| {
                let mut condition = Map::new();
                condition.insert("condition".into(), s("service_started"));
                (item.py_str(), Value::Map(condition))
            })
            .collect(),
        _ => Map::new(),
    };
    let mut completed = Map::new();
    completed.insert("condition".into(), s("service_completed_successfully"));
    if optional_dependency {
        completed.insert("required".into(), Value::Bool(false));
    }
    depends.insert(SERVICE.into(), Value::Map(completed));
    grafana.insert("depends_on".into(), Value::Map(depends));
    let environment = grafana
        .entry("environment".to_string())
        .or_insert_with(|| Value::Map(Map::new()));
    super::os_sql_stack::set_environment(environment, REVISION_VARIABLE, setup.revision, true);
    Ok(())
}

/// The warning for a stack whose Grafana lets anonymous visitors query data sources Meerschaum
/// did not switch (`None`: nothing to say).
pub fn anonymous_warning(datasource: &Value, source: &MainSource) -> Option<String> {
    let mut listed: Vec<String> = Vec::new();
    if let MainSource::Customized { user } = source {
        listed.push(format!("'{MAIN_DATASOURCE}' (user '{user}')"));
    }
    if let Some(Value::List(sources)) = datasource.get("datasources") {
        for source in sources {
            let name = trimmed_text(source.get("name"));
            if name == MAIN_DATASOURCE {
                continue;
            }
            let user = trimmed_text(source.get("user"));
            listed.push(if user.is_empty() {
                format!("'{name}'")
            } else {
                format!("'{name}' (user '{user}')")
            });
        }
    }
    if listed.is_empty() {
        return None;
    }
    Some(format!(
        "The stack's Grafana has anonymous access on: anyone who can open a public dashboard can \
         run any query with the credentials of {}. Meerschaum switches only an unmodified \
         '{MAIN_DATASOURCE}' to its read-only role '{MAIN_READONLY_ROLE}'; give these read-only \
         credentials that reach public data only.",
        listed.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(json: &str) -> Value {
        Value::from_json_str(json).unwrap()
    }

    /// The 5.0 default, as `mrsm stack` substitutes it (or as a config file holds it literally).
    fn old_default(user: &str, password: &str) -> Value {
        value(&format!(
            r#"{{"apiVersion": 1, "datasources": [{{"name": "Meerschaum Main", "type": "postgres",
                "jsonData": {{"sslmode": "disable", "postgresVersion": 1800, "timescaledb": true,
                "database": "meerschaum"}}, "user": "{user}",
                "secureJsonData": {{"password": "{password}"}}, "url": "db:5432",
                "isDefault": true, "editable": true}}]}}"#
        ))
    }

    fn main_source(datasource: &Value) -> Map {
        datasource.get("datasources").unwrap().as_list().unwrap()[0]
            .as_map()
            .unwrap()
            .clone()
    }

    #[test]
    fn the_default_and_a_materialized_old_default_switch_to_the_role() {
        // Fresh default and a stack.yaml holding the old default (substituted to the owner).
        for owner in ["mrsm", "postgres"] {
            let mut datasource = old_default(owner, "owner-secret");
            let found = switch_main_datasource(&mut datasource, Some(owner), "ro-secret", None);
            assert_eq!(
                found,
                MainSource::Switched {
                    host: "db".into(),
                    port: 5432,
                    database: "meerschaum".into()
                }
            );
            let source = main_source(&datasource);
            assert_eq!(source["user"].py_str(), MAIN_READONLY_ROLE);
            assert_eq!(
                source["secureJsonData"].get("password").unwrap().py_str(),
                "ro-secret"
            );
            // Everything else (name, url, jsonData) is kept: Grafana updates it in place.
            assert_eq!(source["name"].py_str(), "Meerschaum Main");
            assert_eq!(source["url"].py_str(), "db:5432");
            assert!(!format!("{datasource:?}").contains("owner-secret"));
        }
        // No user at all is not left to Grafana's default: it is switched too.
        let mut datasource = value(
            r#"{"datasources": [{"name": "Meerschaum Main", "url": "pg:6432", "password": "x"}]}"#,
        );
        assert_eq!(
            switch_main_datasource(&mut datasource, Some("mrsm"), "ro", Some("main_db")),
            MainSource::Switched {
                host: "pg".into(),
                port: 6432,
                database: "main_db".into()
            }
        );
        assert!(!main_source(&datasource).contains_key("password"));
    }

    #[test]
    fn a_customized_data_source_is_left_as_written() {
        let mut datasource = old_default("grafana_reader", "theirs");
        let before = datasource.clone();
        assert_eq!(
            switch_main_datasource(&mut datasource, Some("mrsm"), "ro", None),
            MainSource::Customized {
                user: "grafana_reader".into()
            }
        );
        assert_eq!(datasource, before);
        let warning = anonymous_warning(
            &datasource,
            &MainSource::Customized {
                user: "grafana_reader".into(),
            },
        )
        .unwrap();
        assert!(warning.contains("grafana_reader"), "{warning}");
        // Nothing to switch without the data source.
        let mut other = value(r#"{"datasources": [{"name": "Other", "user": "mrsm"}]}"#);
        assert_eq!(
            switch_main_datasource(&mut other, Some("mrsm"), "ro", None),
            MainSource::Absent
        );
        assert!(
            anonymous_warning(&other, &MainSource::Absent)
                .unwrap()
                .contains("'Other'")
        );
        // A switched default alone needs no warning.
        let mut datasource = old_default("mrsm", "s");
        let found = switch_main_datasource(&mut datasource, Some("mrsm"), "ro", None);
        assert!(anonymous_warning(&datasource, &found).is_none());
    }

    #[test]
    fn additional_schemas_never_include_a_teams() {
        let connectors = value(
            r#"{"main": {"flavor": "timescaledb"},
                "wetlands": {"flavor": "timescaledb", "schema": "wetlands", "teams": ["Wetlands"]},
                "shared": {"flavor": "postgresql", "schema": "shared"}}"#,
        )
        .to_map();
        let (granted, refused) = readonly_schemas(
            &value(r#"{"readonly_schemas": ["reports", "wetlands", "shared", "reports"]}"#)
                .to_map(),
            &connectors,
        );
        assert_eq!(granted, ["reports", "shared"]);
        assert_eq!(refused, ["wetlands"]);
        let (granted, _) = readonly_schemas(
            &value(r#"{"readonly_schemas": "a, b"}"#).to_map(),
            &connectors,
        );
        assert_eq!(granted, ["a", "b"]);
        assert!(readonly_schemas(&Map::new(), &connectors).0.is_empty());
        // The schemas reach the setup script.
        assert!(
            team_database::main_readonly_psql_script(&granted).contains("ARRAY['a', 'b']::text[]")
        );
    }

    #[test]
    fn the_role_is_set_up_before_grafana_starts() {
        let mut compose = value(
            r#"{"services": {
                "db": {"image": "timescale/timescaledb-ha:pg18", "networks": ["backend"],
                       "healthcheck": {"test": ["CMD", "true"]}},
                "grafana": {"image": "grafana/grafana:latest",
                            "depends_on": {"db": {"condition": "service_healthy"}},
                            "environment": {"GF_AUTH_ANONYMOUS_ENABLED": "true"}}}}"#,
        );
        assert!(anonymous_enabled(&compose));
        let schemas = vec!["reports".to_string()];
        let source = MainSource::Switched {
            host: "db".into(),
            port: 5432,
            database: "meerschaum".into(),
        };
        let setup = RoleSetup {
            owner_user: "mrsm",
            owner_password: "owner",
            password: "ro",
            schemas: &schemas,
            revision: "abc",
        };
        add_role_setup(&mut compose, &source, &setup).unwrap();
        let services = compose.get("services").unwrap();
        let service = services.get(SERVICE).unwrap();
        assert_eq!(
            service.get("image").unwrap().py_str(),
            "timescale/timescaledb-ha:pg18"
        );
        let environment = service.get("environment").unwrap();
        assert_eq!(environment.get("PGUSER").unwrap().py_str(), "mrsm");
        assert_eq!(
            environment.get("PGDATABASE").unwrap().py_str(),
            "meerschaum"
        );
        assert!(
            environment
                .get("MRSM_READONLY_SQL")
                .unwrap()
                .py_str()
                .contains("'reports'")
        );
        assert_eq!(
            service
                .get("depends_on")
                .unwrap()
                .get("db")
                .unwrap()
                .get("condition")
                .unwrap()
                .py_str(),
            "service_healthy"
        );
        let grafana = services.get("grafana").unwrap();
        assert_eq!(
            grafana
                .get("depends_on")
                .unwrap()
                .get(SERVICE)
                .unwrap()
                .get("condition")
                .unwrap()
                .py_str(),
            "service_completed_successfully"
        );
        assert_eq!(
            grafana
                .get("environment")
                .unwrap()
                .get(REVISION_VARIABLE)
                .unwrap()
                .py_str(),
            "abc"
        );
        // Without `db` the role cannot be set up here: an error to warn with.
        let mut bare = value(r#"{"services": {"grafana": {"image": "g"}}}"#);
        assert!(add_role_setup(&mut bare, &source, &setup).is_err());
        // A customized data source needs no role.
        let mut untouched = bare.clone();
        add_role_setup(&mut untouched, &MainSource::Absent, &setup).unwrap();
        assert_eq!(untouched, bare);
        // The revision follows the provisioned data sources.
        let a = revision(&old_default("x", "1"), &schemas);
        assert_eq!(a, revision(&old_default("x", "1"), &schemas));
        assert_ne!(a, revision(&old_default("x", "2"), &schemas));
        assert_ne!(a, revision(&old_default("x", "1"), &[]));
    }

    #[test]
    fn grafana_readonly_inherits_compose_profiles() {
        let source = MainSource::Switched {
            host: "db".into(),
            port: 5432,
            database: "meerschaum".into(),
        };
        let schemas: Vec<String> = Vec::new();
        let setup = RoleSetup {
            owner_user: "mrsm",
            owner_password: "owner",
            password: "ro",
            schemas: &schemas,
            revision: "rev1",
        };
        let profiles_of = |compose: &Value| -> Option<Vec<String>> {
            compose
                .get("services")
                .and_then(|s| s.get(SERVICE))
                .and_then(|s| s.get("profiles"))
                .and_then(Value::as_list)
                .map(|items| items.iter().map(Value::py_str).collect())
        };

        // Both `db` and `grafana` disabled via `profiles: [unused]`.
        let mut both = value(
            r#"{"services": {
                "db": {"image": "timescale/timescaledb-ha:pg18", "profiles": ["unused"]},
                "grafana": {"image": "grafana/grafana:latest", "profiles": ["unused"]}
            }}"#,
        );
        add_role_setup(&mut both, &source, &setup).unwrap();
        assert_eq!(profiles_of(&both), Some(vec!["unused".into()]));

        // Only `db` has profiles: `grafana_readonly` must inherit `db`'s profiles so
        // `depends_on: db` never fails compose validation.
        let mut db_only = value(
            r#"{"services": {
                "db": {"image": "timescale/timescaledb-ha:pg18", "profiles": ["legacy", "unused"]},
                "grafana": {"image": "grafana/grafana:latest"}
            }}"#,
        );
        add_role_setup(&mut db_only, &source, &setup).unwrap();
        assert_eq!(
            profiles_of(&db_only),
            Some(vec!["legacy".into(), "unused".into()])
        );
        let required_of = |compose: &Value| -> Option<Value> {
            compose
                .get("services")
                .and_then(|s| s.get("grafana"))
                .and_then(|g| g.get("depends_on"))
                .and_then(|d| d.get(SERVICE))
                .and_then(|d| d.get("required"))
                .cloned()
        };
        // `grafana` stays active without `db`'s profile: its dependency must be optional.
        assert_eq!(required_of(&db_only), Some(Value::Bool(false)));
        assert_eq!(required_of(&both), None);

        // Only `grafana` has profiles: `grafana_readonly` only activates with `grafana`.
        let mut grafana_only = value(
            r#"{"services": {
                "db": {"image": "timescale/timescaledb-ha:pg18"},
                "grafana": {"image": "grafana/grafana:latest", "profiles": ["observability"]}
            }}"#,
        );
        add_role_setup(&mut grafana_only, &source, &setup).unwrap();
        assert_eq!(
            profiles_of(&grafana_only),
            Some(vec!["observability".into()])
        );

        // Overlapping profiles pick the intersection; disjoint profiles fall back to `db`'s.
        let mut overlap = value(
            r#"{"services": {
                "db": {"image": "timescale/timescaledb-ha:pg18", "profiles": ["core", "unused"]},
                "grafana": {"image": "grafana/grafana:latest", "profiles": ["unused", "obs"]}
            }}"#,
        );
        add_role_setup(&mut overlap, &source, &setup).unwrap();
        assert_eq!(profiles_of(&overlap), Some(vec!["unused".into()]));

        let mut disjoint = value(
            r#"{"services": {
                "db": {"image": "timescale/timescaledb-ha:pg18", "profiles": ["db_only"]},
                "grafana": {"image": "grafana/grafana:latest", "profiles": ["obs"]}
            }}"#,
        );
        add_role_setup(&mut disjoint, &source, &setup).unwrap();
        assert_eq!(profiles_of(&disjoint), Some(vec!["db_only".into()]));

        // Neither service has profiles: `grafana_readonly` omits `profiles`.
        let mut none = value(
            r#"{"services": {
                "db": {"image": "timescale/timescaledb-ha:pg18"},
                "grafana": {"image": "grafana/grafana:latest"}
            }}"#,
        );
        add_role_setup(&mut none, &source, &setup).unwrap();
        assert_eq!(profiles_of(&none), None);
    }
}
