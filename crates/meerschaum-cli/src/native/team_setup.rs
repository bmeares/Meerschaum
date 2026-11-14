//! `bootstrap teams [NAME]` (5.1): create a team step by step, with what makes it self-contained.
//!
//! - **Isolation** (`isolated`): the team's members do not see what belongs to Everyone (pipes,
//!   jobs, connectors without `teams`, plugins the team was not granted).
//! - **A database of its own** (`database`: `database`, `schema` or `none`) on the server of an
//!   existing PostgreSQL-family connector (`base`, `sql:main` by default): a role for the team,
//!   a read-only role for dashboards, and a new connector `sql:<label>` reserved for the team,
//!   which becomes the team's default instance (`meerschaum_connector_sql::team_database`).
//! - **Grants**: the connectors and plugins the team may use, and whether its members may
//!   install, edit and upload plugins (`plugins_write`).
//! - **Grafana** (`api:grafana`): the team's organization, with a data source reading the team's
//!   database through the read-only role.
//!
//! Every answer may be given up front in `--params` (`isolated`, `database`, `base`, `name`,
//! `label`, `instance`, `connectors`, `plugins`, `plugins_write`), with `--members` and
//! `--managers`; `--yes` / `--noask` take the defaults for the rest.
//!
//! The team is registered on the instance (`-i`), where the users are. Locally the instance's
//! credentials are the trust boundary, as for `register teams`.

use meerschaum_connector_sql::team_database::{self, TeamDatabase, TeamDatabaseKind};
use meerschaum_core::SuccessTuple;
use meerschaum_core::access::{
    TEAM_CONNECTORS_ATTRIBUTE, TEAM_DATABASE_ATTRIBUTE, TEAM_GRAFANA_ATTRIBUTE,
    TEAM_INSTANCE_ATTRIBUTE, TEAM_ISOLATED_ATTRIBUTE, TEAM_PLUGINS_ATTRIBUTE,
    TEAM_PLUGINS_WRITE_ATTRIBUTE, valid_team_name,
};
use meerschaum_core::value::{Map, Value};
pub use meerschaum_web::grafana;
use meerschaum_web::grafana::{Datasource, GrafanaSettings};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::ArgsMap;

use super::ask::{Ask, ask, choose};
use super::teams::{TeamInfo, is_everyone, list_arg};
use super::{flag, info, print_tuple, session, warn, words, yes_no};

/// What a team is created with (the answers of `bootstrap teams`, or the console's New team).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TeamOptions {
    /// Hide what belongs to Everyone from the team's members.
    pub isolated: bool,
    /// A database or a schema of its own (`None`: neither).
    pub database: Option<TeamDatabaseKind>,
    /// The connector to branch from (`sql:main`).
    pub base: Option<String>,
    /// The database's or schema's name (default: the team's slug).
    pub name: Option<String>,
    /// The label of the team's new connector (default: the team's slug).
    pub label: Option<String>,
    /// The team's default instance, when it gets no database of its own.
    pub instance: Option<String>,
    /// The connectors granted to the team.
    pub connectors: Vec<String>,
    /// The plugins granted to the team.
    pub plugins: Vec<String>,
    /// Whether the team's members may install, edit and upload plugins.
    pub plugins_write: bool,
}

fn names_of(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::List(items)) => items
            .iter()
            .filter(|v| !v.is_null())
            .map(Value::py_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Some(Value::Str(text)) => text
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

fn text_of(value: Option<&Value>) -> Option<String> {
    value
        .filter(|v| !v.is_null())
        .map(Value::py_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

impl TeamOptions {
    /// The options given in `--params` (`Err`: an unknown `database` kind).
    pub fn from_params(params: &Map) -> Result<TeamOptions, String> {
        let database = match text_of(params.get("database")) {
            None => None,
            Some(kind) if matches!(kind.to_lowercase().as_str(), "none" | "false" | "no") => None,
            Some(kind) => Some(TeamDatabaseKind::parse(&kind).ok_or_else(|| {
                format!("Unknown `database` '{kind}': use 'database', 'schema' or 'none'.")
            })?),
        };
        Ok(TeamOptions {
            isolated: params.get("isolated").is_some_and(Value::truthy),
            database,
            base: text_of(params.get("base")),
            name: text_of(params.get("name")),
            label: text_of(params.get("label")),
            instance: text_of(params.get("instance")),
            connectors: names_of(params.get("connectors")),
            plugins: names_of(params.get("plugins")),
            plugins_write: params.get("plugins_write").is_some_and(Value::truthy),
        })
    }

    /// The team's attributes for these options; `database` is what was provisioned
    /// (`(connector keys, base keys, what was made)`).
    pub fn attributes(&self, database: Option<(&str, &str, &TeamDatabase)>) -> Map {
        let mut attrs = Map::new();
        if self.isolated {
            attrs.insert(TEAM_ISOLATED_ATTRIBUTE.into(), Value::Bool(true));
        }
        let instance = database
            .map(|(keys, _, _)| keys.to_string())
            .or_else(|| self.instance.clone());
        if let Some(instance) = &instance {
            attrs.insert(TEAM_INSTANCE_ATTRIBUTE.into(), Value::Str(instance.clone()));
        }
        let mut connectors: Vec<String> = Vec::new();
        for keys in instance.iter().chain(&self.connectors) {
            if !connectors.contains(keys) {
                connectors.push(keys.clone());
            }
        }
        if !connectors.is_empty() {
            attrs.insert(
                TEAM_CONNECTORS_ATTRIBUTE.into(),
                Value::List(connectors.into_iter().map(Value::Str).collect()),
            );
        }
        if !self.plugins.is_empty() {
            attrs.insert(
                TEAM_PLUGINS_ATTRIBUTE.into(),
                Value::List(self.plugins.iter().cloned().map(Value::Str).collect()),
            );
        }
        if self.plugins_write {
            attrs.insert(TEAM_PLUGINS_WRITE_ATTRIBUTE.into(), Value::Bool(true));
        }
        if let Some((keys, base, made)) = database {
            attrs.insert(
                TEAM_DATABASE_ATTRIBUTE.into(),
                Value::Map(made.summary(keys, base)),
            );
        }
        attrs
    }
}

/// The data source of a team's database for Grafana: the read-only role, at `database_url` (how
/// Grafana reaches the server) or the connector's own host and port.
pub fn grafana_datasource(
    settings: &GrafanaSettings,
    team: &str,
    made: &TeamDatabase,
) -> Datasource {
    let url = settings.database_url.clone().unwrap_or_else(|| {
        format!(
            "{}:{}",
            made.host.as_deref().unwrap_or("localhost"),
            made.port.unwrap_or(5432)
        )
    });
    Datasource {
        name: team.to_string(),
        url,
        database: made.database.clone(),
        user: made.readonly_role.clone(),
        password: made.readonly_password.clone(),
        timescaledb: matches!(
            made.flavor,
            meerschaum_connector_sql::flavor::Flavor::Timescaledb
                | meerschaum_connector_sql::flavor::Flavor::TimescaledbHa
        ),
    }
}

/// `api:grafana` when the integration is enabled (no warning when the section is absent).
pub(crate) fn grafana_settings(rt: &mut Runtime<'_>) -> Option<GrafanaSettings> {
    let (valid, section) = rt.config().get_with(
        &["api", "grafana"],
        meerschaum_core::config::GetOptions {
            patch: true,
            substitute: true,
            write_missing: false,
            warn: false,
        },
    );
    if !valid {
        return None;
    }
    GrafanaSettings::from_section(&section.to_map())
}

fn sql_connectors(rt: &mut Runtime<'_>) -> Vec<String> {
    rt.config()
        .get(&["meerschaum", "connectors", "sql"])
        .map(|v| v.to_map())
        .unwrap_or_default()
        .keys()
        .filter(|label| *label != "default")
        .map(|label| format!("sql:{label}"))
        .collect()
}

fn connector_exists(rt: &mut Runtime<'_>, typ: &str, label: &str) -> bool {
    // The section is read whole: asking for a label that is not there warns about the keys.
    rt.config()
        .get(&["meerschaum", "connectors", typ])
        .map(|v| v.to_map())
        .is_some_and(|labels| labels.get(label).is_some_and(|v| !v.is_null()))
}

fn fail(message: impl Into<String>) -> ActionResult {
    Ok(SuccessTuple::fail(message.into()))
}

/// `bootstrap teams [NAME] [--members a,b] [--managers c] [--params JSON]`.
pub fn bootstrap_teams(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let yes = flag(args, "yes");
    let noask = flag(args, "noask");
    let quiet = yes || noask;
    let params = super::params(args);
    let mut options = match TeamOptions::from_params(params.as_ref().unwrap_or(&Map::new())) {
        Ok(options) => options,
        Err(e) => return fail(e),
    };
    let given = |key: &str| params.as_ref().is_some_and(|p| p.contains_key(key));

    let names = words(args, "action");
    if names.len() > 1 {
        return fail("Bootstrap one team at a time.");
    }
    let name = match names.first() {
        Some(name) => name.clone(),
        None if quiet => return fail("Give the team's name: `bootstrap teams NAME`."),
        None => ask(rt, "What is the team's name?", &Ask::new())?
            .trim()
            .to_string(),
    };
    if is_everyone(&name) {
        return fail("'Everyone' is the built-in team every user belongs to; choose another name.");
    }
    let valid = valid_team_name(&name);
    if !valid.success {
        return Ok(valid);
    }
    let session = session(rt);
    match session.block_on(super::teams::get_team(&inst, &name)) {
        Ok(Some(_)) => return fail(format!("Team '{name}' already exists.")),
        Ok(None) => {}
        Err(e) => return fail(format!("Failed to look up team '{name}': {e}")),
    }
    let members = list_arg(args, "members").unwrap_or_default();
    let managers = list_arg(args, "managers").unwrap_or_default();
    let wanted: Vec<String> = members.iter().chain(&managers).cloned().collect();
    let missing = super::teams::unregistered(rt, &inst, &wanted)?;
    if !missing.is_empty() {
        return Ok(super::teams::unregistered_failure(&inst, &missing));
    }

    // Isolation.
    if !given("isolated") && !quiet {
        options.isolated = yes_no(
            rt,
            &format!(
                "Hide what belongs to Everyone (its pipes, jobs, connectors and plugins) from \
                 the members of '{name}'?"
            ),
            "y",
            false,
            false,
        )?;
    }

    // A database of its own.
    let bases = sql_connectors(rt);
    if !given("database") && !quiet {
        let choices = vec![
            "A new database".to_string(),
            "A new schema in an existing database".to_string(),
            "Neither".to_string(),
        ];
        let answer = choose(
            rt,
            &format!("What should '{name}' store its data in?"),
            &choices,
            Some("A new database"),
            true,
        )?;
        options.database = match answer.as_deref() {
            Some("A new database") => Some(TeamDatabaseKind::Database),
            Some("A new schema in an existing database") => Some(TeamDatabaseKind::Schema),
            _ => None,
        };
    }
    let default_slug = team_database::slug(&name);
    if let Some(kind) = options.database {
        let default_base = if inst.sql().is_some() {
            inst.keys()
        } else {
            "sql:main".to_string()
        };
        if options.base.is_none() && !quiet && bases.len() > 1 {
            options.base = choose(
                rt,
                "Which connector's server should it be made on?",
                &bases,
                Some(&default_base),
                true,
            )?;
        }
        let base = options.base.clone().unwrap_or(default_base);
        options.base = Some(base);
        if options.name.is_none() && !quiet {
            let default = default_slug.clone().unwrap_or_default();
            let answer = ask(
                rt,
                &format!("What is the new {}'s name?", kind.name()),
                &Ask::new().default(default),
            )?;
            options.name = Some(answer.trim().to_string()).filter(|n| !n.is_empty());
        }
    } else if options.instance.is_none() && !quiet && !bases.is_empty() {
        let mut choices = vec!["None".to_string()];
        choices.extend(bases.iter().cloned());
        let answer = choose(
            rt,
            &format!("Which connector is the default instance of '{name}'?"),
            &choices,
            Some("None"),
            true,
        )?;
        options.instance = answer.filter(|a| a != "None");
    }

    // Grants.
    if !given("connectors") && !quiet {
        let answer = ask(
            rt,
            "Which other connectors may the team use? (comma-separated keys, or nothing)",
            &Ask::new().default(""),
        )?;
        options.connectors = names_of(Some(&Value::Str(answer)));
    }
    if !given("plugins") && !quiet {
        let answer = ask(
            rt,
            "Which plugins may the team use? (comma-separated names, or nothing)",
            &Ask::new().default(""),
        )?;
        options.plugins = names_of(Some(&Value::Str(answer)));
    }
    if !given("plugins_write") && !quiet {
        options.plugins_write = yes_no(
            rt,
            &format!(
                "May the members of '{name}' install, edit and upload plugins? Plugins run on \
                 the server with its credentials."
            ),
            "n",
            false,
            false,
        )?;
    }

    // The database, its connector, the team, then Grafana.
    let mut provisioned: Option<(String, String, TeamDatabase)> = None;
    if let Some(kind) = options.database {
        let base = options.base.clone().unwrap_or_else(|| "sql:main".into());
        let Some(base_label) = base.strip_prefix("sql:") else {
            return fail(format!(
                "A team database is made on a `sql:` connector, not '{base}'."
            ));
        };
        let label = match options.label.clone().or_else(|| default_slug.clone()) {
            Some(label) => label,
            None => return fail(format!("The team name '{name}' has no letters or digits.")),
        };
        if let Err(e) = team_database::valid_identifier(&label) {
            return fail(format!("The connector label is not usable: {e}"));
        }
        if connector_exists(rt, "sql", &label) {
            return fail(format!(
                "The connector 'sql:{label}' already exists: give another `label` in --params."
            ));
        }
        let conn = session.sql_connector(base_label, rt.config())?;
        info(
            rt,
            &format!(
                "Creating the {} of team '{name}' on '{base}'...",
                kind.name()
            ),
        );
        let made = match session.block_on(team_database::create(
            &conn,
            &name,
            kind,
            options.name.as_deref(),
        )) {
            Ok(made) => made,
            Err(e) => return fail(format!("Failed to create the team's {}: {e}", kind.name())),
        };
        for note in &made.notes {
            warn(rt, note);
        }
        let mut section = super::connectors::meerschaum_section(rt);
        let attributes = Value::Map(made.connector_attributes(&name));
        let connectors = section
            .entry("connectors".to_string())
            .or_insert_with(|| Value::Map(Map::new()));
        if let Value::Map(connectors) = connectors {
            let sql = connectors
                .entry("sql".to_string())
                .or_insert_with(|| Value::Map(Map::new()));
            if let Value::Map(sql) = sql {
                sql.insert(label.clone(), attributes);
            }
        }
        if !super::connectors::write_meerschaum(rt, section) {
            return fail(format!(
                "The {} '{}' and the role '{}' were created, but the connector 'sql:{label}' \
                 could not be written to the configuration.",
                kind.name(),
                options.name.clone().unwrap_or_else(|| label.clone()),
                made.role
            ));
        }
        rt.config().refresh_connectors();
        provisioned = Some((format!("sql:{label}"), base, made));
    }

    let mut attributes = options.attributes(
        provisioned
            .as_ref()
            .map(|(keys, base, made)| (keys.as_str(), base.as_str(), made)),
    );

    if let Some(settings) = grafana_settings(rt) {
        let datasource = provisioned
            .as_ref()
            .map(|(_, _, made)| grafana_datasource(&settings, &name, made));
        match session.block_on(grafana::ensure_team(&settings, &name, datasource.as_ref())) {
            Ok((org, _)) => {
                let mut record = Map::new();
                record.insert("org_id".into(), Value::Int(org));
                attributes.insert(TEAM_GRAFANA_ATTRIBUTE.into(), Value::Map(record));
            }
            Err(e) => warn(
                rt,
                &format!("{e}\nThe team's organization is made when a member opens Grafana."),
            ),
        }
    }

    let team = super::teams::normalized(TeamInfo {
        name: name.clone(),
        members,
        managers,
        id: None,
        attributes,
    });
    info(
        rt,
        &format!(
            "Registering team '{name}' to Meerschaum instance '{}'...",
            inst.keys()
        ),
    );
    let res = session.block_on(super::teams::register_team(&inst, &team));
    if !res.success {
        print_tuple(rt, &res, TupleArgs::default());
        return fail(match &provisioned {
            Some((keys, _, _)) => format!(
                "The connector '{keys}' and its database were created, but team '{name}' was \
                 not registered: {}",
                res.message
            ),
            None => format!("Team '{name}' was not registered: {}", res.message),
        });
    }
    let mut message = format!("Created team '{name}'");
    if let Some((keys, _, made)) = &provisioned {
        message.push_str(&format!(
            " with the {} '{}' (connector '{keys}')",
            made.kind.name(),
            made.schema.as_deref().unwrap_or(&made.database)
        ));
    }
    message.push('.');
    if options.isolated {
        message.push_str("\n    Its members do not see what belongs to Everyone.");
    }
    Ok(SuccessTuple::ok(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_connector_sql::flavor::Flavor;

    fn params(json: &str) -> Map {
        Value::from_json_str(json).unwrap().to_map()
    }

    fn made() -> TeamDatabase {
        TeamDatabase {
            kind: TeamDatabaseKind::Database,
            database: "wetlands".into(),
            schema: None,
            role: "mrsm_wetlands".into(),
            password: "pw".into(),
            readonly_role: "mrsm_wetlands_ro".into(),
            readonly_password: "ro-pw".into(),
            host: Some("db".into()),
            port: Some(5432),
            flavor: Flavor::TimescaledbHa,
            notes: Vec::new(),
        }
    }

    #[test]
    fn options_come_from_params() {
        let options = TeamOptions::from_params(&params(
            r#"{"isolated": true, "database": "Schema", "base": "sql:main", "name": "wet",
                "connectors": "plugin:noaa, sql:shared", "plugins": ["noaa"], "plugins_write": true}"#,
        ))
        .unwrap();
        assert!(options.isolated && options.plugins_write);
        assert_eq!(options.database, Some(TeamDatabaseKind::Schema));
        assert_eq!(options.connectors, ["plugin:noaa", "sql:shared"]);
        assert_eq!(options.plugins, ["noaa"]);
        assert_eq!(
            TeamOptions::from_params(&params(r#"{"database": "none"}"#))
                .unwrap()
                .database,
            None
        );
        assert!(TeamOptions::from_params(&params(r#"{"database": "table"}"#)).is_err());
        assert_eq!(
            TeamOptions::from_params(&Map::new()).unwrap(),
            TeamOptions::default()
        );
    }

    #[test]
    fn attributes_name_the_database_without_credentials() {
        let options = TeamOptions {
            isolated: true,
            connectors: vec!["plugin:noaa".into(), "sql:wetlands".into()],
            plugins: vec!["noaa".into()],
            ..Default::default()
        };
        let made = made();
        let attrs = options.attributes(Some(("sql:wetlands", "sql:main", &made)));
        assert_eq!(attrs.get("isolated"), Some(&Value::Bool(true)));
        assert_eq!(attrs.get("instance").unwrap().py_str(), "sql:wetlands");
        assert_eq!(
            names_of(attrs.get("connectors")),
            ["sql:wetlands", "plugin:noaa"]
        );
        assert!(!attrs.contains_key("plugins_write"));
        let text = format!("{attrs:?}");
        assert!(!text.contains("pw"), "{text}");
        // Without a database: the chosen instance, and nothing for an open team.
        let plain = TeamOptions {
            instance: Some("sql:shared".into()),
            ..Default::default()
        };
        let attrs = plain.attributes(None);
        assert_eq!(attrs.get("instance").unwrap().py_str(), "sql:shared");
        assert!(!attrs.contains_key("isolated") && !attrs.contains_key("database"));
        assert!(TeamOptions::default().attributes(None).is_empty());
    }

    #[test]
    fn the_grafana_data_source_uses_the_read_only_role() {
        let settings = GrafanaSettings::from_section(&params(
            r#"{"enabled": true, "url": "http://grafana:3000"}"#,
        ))
        .unwrap();
        let source = grafana_datasource(&settings, "Wetlands", &made());
        assert_eq!(source.url, "db:5432");
        assert_eq!(source.user, "mrsm_wetlands_ro");
        assert_eq!(source.password, "ro-pw");
        assert!(source.timescaledb);
        let settings = GrafanaSettings::from_section(&params(
            r#"{"enabled": true, "url": "http://grafana:3000", "database_url": "pg.internal:6432"}"#,
        ))
        .unwrap();
        assert_eq!(
            grafana_datasource(&settings, "Wetlands", &made()).url,
            "pg.internal:6432"
        );
    }
}
