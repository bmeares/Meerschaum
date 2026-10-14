//! [`InstanceOps`] over the Rust instance connectors: what `mrsm compose explain`, `init` and
//! `up --dry` use to read (and, for `up`, edit) the project's pipe registrations
//! (`meerschaum/compose/subactions/{explain,up}.py`).
//!
//! Python runs a subaction inside `replace_config(<config: block>)` and `replace_env(<project
//! env>)`, so the instance connectors are the project's: its `config:` connectors, its
//! `{MRSM_ROOT_DIR}`, and the `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` variables of the project
//! environment layered over the file. [`ComposeInstance::configure`] builds that view
//! ([`ProjectConfig`]) and every connector is resolved from it by keys through a private
//! [`Session`] (so the host's worker pool, config and connectors are never touched).
//!
//! **Registered parameters are RAW** (`PARAMETER_CALL_SITES.md`): `get_pipes`, `get_parameters`
//! and `get_pipe_attributes` return the stored map, and `edit_pipe` persists `raw_parameters()`.
//! `{{ Pipe() }}` references are resolved by the compose crate's resolver, which asks
//! [`ComposeInstance::table_reference`] for the quoted, schema-qualified target.
//!
//! **Unreachable instances behave like v4:** `get_pipes` raises (`error(str(e))`), failing the
//! subaction with `Exception`; `get_parameters` / `get_id` of an unreachable instance answer
//! "not registered"; `table_reference` fails, which leaves the template as written.
//!
//! **Deviation (intended): file instances are never created.** v4's `explain` creates the
//! SQLite / GeoPackage file of a project-defined connector (and its `mrsm_*` tables and cache
//! files) just by asking which pipes are registered. Here a SQLite-family instance whose file
//! does not exist is an empty registry: nothing is connected to, so nothing is created.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use meerschaum_compose::ops::InstanceOps;
use meerschaum_compose::pipes::ProjectConfig;
use meerschaum_compose::project::{Host, Project};
use meerschaum_connector::{InstanceConnector, PipeId, PipesKeys, PipesKeysQuery};
use meerschaum_core::config::Config;
use meerschaum_core::pipe::{Pipe, PipeDefaults};
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{SuccessTuple, pyfmt};
use meerschaum_sql::SqlConnector;
use meerschaum_sql::flavor::Flavor;
use meerschaum_sql::instance::InstanceSettings;
use meerschaum_sql::uri::{ConnectionSpec, ConnectorAttributes};

use super::pipes::{SelectedPipe, build_pipe, group_pipes};
use super::session::{Instance, Session, connector_attributes, split_keys};

/// How an instance's keys resolved.
enum Resolved {
    /// A connector to query.
    Live(Instance),
    /// A SQLite-family connector whose file does not exist: an empty registry that must not be
    /// connected to (connecting would create the file).
    Absent(Arc<SqlConnector>),
    /// The connector could not be built or reached (`str(exception)`).
    Failed(String),
}

/// The project scope set by [`InstanceOps::configure`].
struct Scope {
    config: ProjectConfig,
    defaults: PipeDefaults,
}

/// Pipe registrations on the project's instance connectors.
pub struct ComposeInstance {
    session: Session,
    scope: Option<Scope>,
    /// Failures by instance keys (an unreachable instance is tried once per run).
    failed: HashMap<String, String>,
    /// Lazy connectors for absent SQLite files, by keys (only used to render table names).
    absent: HashMap<String, Arc<SqlConnector>>,
}

impl Default for ComposeInstance {
    fn default() -> Self {
        Self::new()
    }
}

impl ComposeInstance {
    /// An instance with its own Tokio runtime; call [`InstanceOps::configure`] (compose does)
    /// before use. Unconfigured, every instance is an empty registry.
    pub fn new() -> ComposeInstance {
        ComposeInstance {
            session: Session::new(),
            scope: None,
            failed: HashMap::new(),
            absent: HashMap::new(),
        }
    }

    fn config(&self) -> Option<&Config> {
        self.scope.as_ref().map(|s| s.config.registry())
    }

    /// Resolve `keys` (`parse_instance_keys`) in the project's config.
    fn resolve(&mut self, keys: &str) -> Resolved {
        let Some(config) = self.config() else {
            return Resolved::Failed("No project is configured for the instance connector.".into());
        };
        let (typ, label) = split_keys(keys);
        let full = format!("{typ}:{label}");
        if let Some(error) = self.failed.get(&full) {
            return Resolved::Failed(error.clone());
        }
        if typ == "sql" {
            if let Some((attributes, path)) = sqlite_file(config, &label) {
                if !path.exists() {
                    return match self.lazy_connector(&full, &label, &attributes) {
                        Ok(conn) => Resolved::Absent(conn),
                        Err(e) => Resolved::Failed(e),
                    };
                }
            }
        }
        // `Session::instance` caches the connector it builds.
        let config = self.config().expect("scope");
        match self.session.instance(keys, config) {
            Ok(instance) => Resolved::Live(instance),
            Err(e) => {
                self.failed.insert(full, e.message.clone());
                Resolved::Failed(e.message)
            }
        }
    }

    fn lazy_connector(
        &mut self,
        full: &str,
        label: &str,
        attributes: &ConnectorAttributes,
    ) -> Result<Arc<SqlConnector>, String> {
        if let Some(conn) = self.absent.get(full) {
            return Ok(conn.clone());
        }
        let spec = ConnectionSpec::from_attributes(attributes).map_err(|e| e.to_string())?;
        let settings = InstanceSettings::defaults(spec.flavor);
        let _guard = self.session.handle().enter();
        let conn =
            Arc::new(SqlConnector::connect_lazy(label, spec, settings).map_err(|e| e.to_string())?);
        self.absent.insert(full.to_string(), conn.clone());
        Ok(conn)
    }

    fn pipes_from_keys(&self, instance_keys: &str, result: PipesKeys) -> Result<Vec<Pipe>, String> {
        let defaults = self
            .scope
            .as_ref()
            .map(|s| s.defaults.clone())
            .ok_or_else(|| "No project is configured for the instance connector.".to_string())?;
        let items: Vec<(Option<PipeId>, meerschaum_connector::PipeKeysEntry)> = match result {
            PipesKeys::List(l) => l.into_iter().map(|e| (None, e)).collect(),
            PipesKeys::Dict(d) => d.into_iter().map(|(id, e)| (Some(id), e)).collect(),
        };
        let mut pipes = Vec::new();
        for (id, entry) in items {
            let lk = entry
                .location_key
                .as_deref()
                .filter(|l| *l != "None")
                .map(str::to_string);
            let parameters = match &entry.extra {
                Some(Value::Map(m)) => Some(m.clone()),
                Some(Value::Str(s)) => Value::from_json_str(s).ok().map(|v| v.to_map()),
                _ => None,
            };
            let tags = match &entry.extra {
                Some(Value::List(l)) => Some(Value::List(l.clone())),
                _ => None,
            };
            let hydrated = parameters.is_some();
            let mut params = parameters.unwrap_or_default();
            if let Some(t) = tags {
                params.entry("tags".to_string()).or_insert(t);
            }
            let pipe = build_pipe(
                &entry.connector_keys,
                &entry.metric_key,
                lk.as_deref(),
                instance_keys,
                Some(params),
                &defaults,
            )
            .map_err(|e| e.message)?;
            pipes.push(SelectedPipe { pipe, id, hydrated });
        }
        Ok(group_pipes(pipes)
            .into_iter()
            .map(|selected| selected.pipe)
            .collect())
    }
}

impl Drop for ComposeInstance {
    fn drop(&mut self) {
        self.session.shutdown();
    }
}

/// The SQLite-family connector `label` resolves to in `config` and its database file, or `None`
/// for any other flavor, an in-memory database or attributes that do not resolve (the session
/// reports those when it builds the connector).
fn sqlite_file(config: &Config, label: &str) -> Option<(ConnectorAttributes, PathBuf)> {
    let merged = connector_attributes(config, "sql", label);
    if merged.is_empty() {
        return None;
    }
    let flavor_of = |attrs: &Map| -> Option<Flavor> {
        let uri = attrs.get("uri").map(pyfmt::py_str);
        let named = attrs
            .get("flavor")
            .map(pyfmt::py_str)
            .and_then(|f| f.parse::<Flavor>().ok());
        ConnectionSpec::from_attributes(&ConnectorAttributes {
            label: Some(label.to_string()),
            flavor: named,
            uri,
            // A placeholder: only the flavor matters here.
            database: Some("w9".into()),
            host: Some("w9".into()),
            username: Some("w9".into()),
            password: Some("w9".into()),
            ..Default::default()
        })
        .ok()
        .map(|spec| spec.flavor)
    };
    if !matches!(
        flavor_of(&merged),
        Some(Flavor::Sqlite | Flavor::Geopackage)
    ) {
        return None;
    }
    // PARITY: SQLite / GeoPackage connectors do not inherit `sql:default`, and `{SQLITE_DB_PATH}`
    // (or a missing database for `sql:local`) is the root's SQLite file (`Session::sql_connector`).
    let mut attrs = config
        .get(&["meerschaum", "connectors", "sql", label])
        .map(|v| v.to_map())
        .unwrap_or_default();
    let database = attrs.get("database").map(pyfmt::py_str);
    if database.as_deref() == Some("{SQLITE_DB_PATH}") || (label == "local" && database.is_none()) {
        let path = config.paths().path("SQLITE_DB_PATH");
        attrs = apply_patch_maps(
            &attrs,
            &Map::from_iter([(
                "database".to_string(),
                Value::Str(path.to_string_lossy().into_owned()),
            )]),
        );
    }
    let flavor = attrs
        .get("flavor")
        .map(pyfmt::py_str)
        .and_then(|f| f.parse::<Flavor>().ok());
    let attributes = ConnectorAttributes {
        label: Some(label.to_string()),
        flavor,
        uri: attrs.get("uri").map(pyfmt::py_str),
        database: attrs.get("database").map(pyfmt::py_str),
        schema: attrs.get("schema").map(pyfmt::py_str),
        ..Default::default()
    };
    let spec = ConnectionSpec::from_attributes(&attributes).ok()?;
    if spec.is_memory() {
        return None;
    }
    let path = spec.resolved_database().ok().flatten()?;
    if path.is_empty() {
        return None;
    }
    Some((attributes, PathBuf::from(path)))
}

/// What Python's `error(str(e))` raises.
fn raised(message: &str) -> String {
    format!("\n 🛑 {message}")
}

impl InstanceOps for ComposeInstance {
    fn configure(&mut self, host: &Host, project: &Project) {
        // A new project scope replaces everything built for the previous one.
        if self.scope.is_some() {
            self.session.shutdown();
            self.session = Session::new();
        }
        self.failed.clear();
        self.absent.clear();
        self.scope = ProjectConfig::new(host, project).ok().map(|config| {
            let defaults = config.pipe_defaults();
            Scope { config, defaults }
        });
    }

    fn get_pipes(&mut self, instance_keys: &str, tags: &[String]) -> Vec<Pipe> {
        self.try_get_pipes(instance_keys, tags).unwrap_or_default()
    }

    fn try_get_pipes(&mut self, instance_keys: &str, tags: &[String]) -> Result<Vec<Pipe>, String> {
        match self.resolve(instance_keys) {
            Resolved::Absent(_) => Ok(Vec::new()),
            Resolved::Failed(e) => Err(raised(&e)),
            Resolved::Live(instance) => {
                let query = PipesKeysQuery {
                    tags: tags.to_vec(),
                    ..Default::default()
                };
                let result = self
                    .session
                    .block_on(instance.conn().fetch_pipes_keys(&query))
                    .map_err(|e| raised(&e.to_string()))?;
                self.pipes_from_keys(instance_keys, result)
            }
        }
    }

    fn get_parameters(&mut self, pipe: &Pipe) -> Option<Map> {
        let attributes = self.get_pipe_attributes(pipe).ok()?;
        match attributes.get("parameters") {
            Some(Value::Map(parameters)) => Some(parameters.clone()),
            _ => None,
        }
    }

    fn get_id(&mut self, pipe: &Pipe) -> Option<i64> {
        let Resolved::Live(instance) = self.resolve(pipe.instance_keys()) else {
            return None;
        };
        match self.session.block_on(instance.conn().get_pipe_id(pipe)) {
            Ok(Some(PipeId::Int(id))) => Some(id),
            // A string ID (custom instances): compose only asks whether the pipe has one.
            Ok(Some(PipeId::Str(_))) => Some(0),
            _ => None,
        }
    }

    fn edit_pipe(&mut self, pipe: &Pipe) -> SuccessTuple {
        // PARITY: `Pipe.edit()` persists `_attributes['parameters']`, which the pipe's own
        // `validate_indices()` call has already merged over the registered parameters
        // (`Pipe.parameters` = registered patched with the local ones): keys that only the
        // registration has survive an edit, and the local keys win.
        let registered = self.get_parameters(pipe);
        let mut edited = pipe.clone();
        if let Some(registered) = registered {
            edited.set_parameters(apply_patch_maps(&registered, pipe.raw_parameters()));
        }
        match self.resolve(pipe.instance_keys()) {
            Resolved::Live(instance) => self
                .session
                .block_on(instance.conn().edit_pipe(&edited, false)),
            Resolved::Absent(_) => SuccessTuple::fail(format!(
                "The database of '{}' does not exist; cannot edit {pipe}.",
                pipe.instance_keys()
            )),
            Resolved::Failed(e) => SuccessTuple::fail(e),
        }
    }

    fn delete_pipe(&mut self, pipe: &Pipe) -> SuccessTuple {
        match self.resolve(pipe.instance_keys()) {
            Resolved::Live(instance) => self.session.block_on(instance.conn().delete_pipe(pipe)),
            Resolved::Absent(_) => SuccessTuple::fail(format!(
                "The database of '{}' does not exist; cannot delete {pipe}.",
                pipe.instance_keys()
            )),
            Resolved::Failed(e) => SuccessTuple::fail(e),
        }
    }

    fn table_reference(&mut self, pipe: &Pipe) -> Result<String, String> {
        match self.resolve(pipe.instance_keys()) {
            Resolved::Live(Instance::Sql(conn)) => {
                // The lookup of the registered parameters (an explicit `target`) is synchronous
                // on a multi-threaded runtime.
                let _guard = self.session.handle().enter();
                conn.pipe_table_reference(pipe)
                    .ok_or_else(|| format!("Cannot resolve the table of {pipe}."))
            }
            Resolved::Live(Instance::Other(conn)) => match conn.pipe_table_reference(pipe) {
                Some(table) => Ok(table),
                None => pipe.target(None).map_err(|e| e.to_string()),
            },
            Resolved::Absent(conn) => {
                let sql_pipe = conn.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(conn.pipe_table_name(&sql_pipe))
            }
            Resolved::Failed(e) => Err(e),
        }
    }

    fn get_pipe_attributes(&mut self, pipe: &Pipe) -> Result<Map, String> {
        match self.resolve(pipe.instance_keys()) {
            Resolved::Live(instance) => self
                .session
                .block_on(instance.conn().get_pipe_attributes(pipe))
                .map_err(|e| e.to_string()),
            Resolved::Absent(_) => Ok(Map::new()),
            Resolved::Failed(e) => Err(e),
        }
    }

    // `sync_pipe` stays the trait default (a failure): the natively served subactions
    // (`explain`, `init`, `up --dry`) never sync in-process.
}
