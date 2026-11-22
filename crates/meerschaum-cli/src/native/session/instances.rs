//! Instance and SQL connectors of a session: connectors registered by a host, connectors
//! built from the config registry, and the configured SQL connector provider.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes, InvalidPort};
use meerschaum_core::config::Config;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{pyfmt, static_config};
use meerschaum_plugin::source::compiler::CompilerPolicy;

use crate::builtin::ActionError;

use super::*;

impl Session {
    /// An already built connector of this session (never builds one).
    pub fn cached_instance(&self, keys: &str) -> Option<Instance> {
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full_keys(keys))
            .cloned()
    }

    /// Register a connector the host built for `keys` (`type:label`; a bare type means the
    /// default label). Later [`Session::instance`] lookups of those keys return it instead of
    /// building one from the config; registering again replaces it. Returns the connector it
    /// replaced (registered or built from config), if any.
    pub fn register_instance(&self, keys: &str, instance: Instance) -> Option<Instance> {
        let full = full_keys(keys);
        self.registered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(full.clone());
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(full, instance)
    }

    /// [`Session::register_instance`] for a SQL connector.
    pub fn register_sql(&self, keys: &str, conn: Arc<SqlConnector>) -> Option<Instance> {
        self.register_instance(keys, Instance::Sql(conn))
    }

    /// Forget the connector for `keys` (registered or built); the next lookup builds it again.
    pub fn unregister(&self, keys: &str) -> Option<Instance> {
        let full = full_keys(keys);
        self.registered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&full);
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&full)
    }

    /// Whether the host registered the connector for `keys`.
    pub fn is_registered(&self, keys: &str) -> bool {
        self.registered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&full_keys(keys))
    }

    /// When `true`, [`Session::instance`] fails for keys nobody registered (and never reads
    /// the config or starts a worker to build them). Off by default.
    pub fn set_registered_only(&self, on: bool) {
        self.registered_only.store(on, Ordering::SeqCst);
    }

    /// `parse_instance_keys(keys)` → the connector (built once per keys).
    pub fn instance(&self, keys: &str, config: &Config) -> Result<Instance, ActionError> {
        let (typ, label) = split_keys(keys);
        let full = format!("{typ}:{label}");
        // Read the exact descriptor before a cached native driver or default attributes can
        // adopt these logical keys. API execution keeps the identity but carries no DB client.
        if let Some(alias) =
            meerschaum_connector_api::APIConnector::execution_alias_from_config(config, &full)
                .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?
        {
            return Ok(Instance::Other(Arc::new(alias)));
        }
        if let Some(i) = self
            .instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full)
        {
            return Ok(i.clone());
        }
        if self.registered_only.load(Ordering::SeqCst) {
            return Err(ActionError::new(
                "ConnectorNotRegistered",
                format!("No connector was registered for '{full}'."),
            ));
        }
        if matches!(typ.as_str(), "api" | "valkey")
            && connector_attributes(config, &typ, &label).is_empty()
        {
            return Err(ActionError::new(
                "ValueError",
                super::connectors::undefined_connector_message(&typ, &label),
            ));
        }
        let inst = match typ.as_str() {
            "sql" => Instance::Sql(Arc::new(self.sql_connector(&label, config)?)),
            "api" => {
                let c = meerschaum_connector_api::APIConnector::from_config(config, &label)
                    .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?;
                Instance::Other(Arc::new(c))
            }
            "valkey" => {
                let attrs = meerschaum_connector_valkey::ValkeyConnector::attributes_from_config(
                    config, &label,
                );
                let c = self
                    .block_on(meerschaum_connector_valkey::ValkeyConnector::connect(
                        &label, attrs,
                    ))
                    .map_err(|e| ActionError::new("ConnectionError", e.to_string()))?;
                Instance::Other(Arc::new(c))
            }
            _ if !self.worker_enabled() => {
                return Err(ActionError::new(
                    "WorkerError",
                    format!("Cannot build '{full}' without the Python worker."),
                ));
            }
            _ => {
                let natives = self.native_plugins(config)?;
                let attributes = connector_attributes(config, &typ, &label);
                if let Some(connector) =
                    crate::native_plugins::instance_connector(&natives, keys, &attributes)
                {
                    let connector = connector.map_err(|e| ActionError::new("ImportError", e))?;
                    let inst = Instance::Other(connector);
                    self.instances
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(full, inst.clone());
                    return Ok(inst);
                }
                let connector = self
                    .block_on(meerschaum_worker::PythonConnector::connect(
                        keys,
                        self.pool(config),
                    ))
                    .map_err(|e| {
                        // A type no plugin declares: say where connectors come from.
                        use meerschaum_core::config::connector_types;
                        let paths = config.paths();
                        let known = connector_types::is_builtin_connector_type(&typ)
                            || connector_types::plugin_connector_types(
                                &paths.root_dir,
                                &paths.plugins_dirs,
                            )
                            .contains(&typ);
                        if known {
                            ActionError::new("WorkerError", e.to_string())
                        } else {
                            ActionError::new(
                                "ValueError",
                                super::connectors::undefined_connector_message(&typ, &label),
                            )
                        }
                    })?;
                if !connector.info().is_instance {
                    return Err(ActionError::new(
                        "Exception",
                        format!("'{keys}' is not an instance connector."),
                    ));
                }
                Instance::Other(Arc::new(connector))
            }
        };
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(full, inst.clone());
        Ok(inst)
    }

    /// A `sql:<label>` connector from the config registry.
    pub fn sql_connector(&self, label: &str, config: &Config) -> Result<SqlConnector, ActionError> {
        if meerschaum_connector_api::APIConnector::execution_alias_from_config(
            config,
            &format!("sql:{label}"),
        )
        .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?
        .is_some()
        {
            return Err(ActionError::new(
                "InvalidAttributesError",
                "This logical SQL connector uses the API; a native database client is unavailable.",
            ));
        }
        let spec = sql_spec(label, config)?;
        let settings = InstanceSettings::from_config(spec.flavor, config);
        let conn = self
            .block_on(SqlConnector::connect_with(label, spec, settings))
            .map_err(|e| {
                ActionError::new(
                    "OperationalError",
                    super::connectors::connect_failure_message(label, config, &e),
                )
            })?;
        // Cross-instance `{{ Pipe(..., instance='sql:other') }}` references resolve against the
        // configured connectors, not only `MRSM_SQL_<LABEL>` environment variables.
        conn.set_instance_provider(Arc::new(ConfiguredSqlProvider::new(config)));
        Ok(conn)
    }
}

/// The connection spec of `sql:<label>` from the config registry, without connecting.
pub fn sql_spec(label: &str, config: &Config) -> Result<ConnectionSpec, ActionError> {
    let mut attrs = connector_attributes(config, "sql", label);
    // PARITY: SQLite / GeoPackage connectors do not inherit `sql:default`, and
    // `{SQLITE_DB_PATH}` (or a missing database for `sql:local`) is the root's SQLite file
    // (`SQLConnector.__init__`, `create_engine`).
    let flavor = attrs.get("flavor").map(pyfmt::py_str).unwrap_or_default();
    if matches!(flavor.as_str(), "sqlite" | "geopackage") {
        attrs = config
            .get(&["meerschaum", "connectors", "sql", label])
            .map(|v| v.to_map())
            .unwrap_or_default();
        let db = attrs.get("database").map(pyfmt::py_str);
        let is_placeholder = db.as_deref() == Some("{SQLITE_DB_PATH}");
        if is_placeholder || (label == "local" && db.is_none()) {
            let path = config.paths().path("SQLITE_DB_PATH");
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            attrs.insert(
                "database".into(),
                Value::Str(path.to_string_lossy().into_owned()),
            );
        }
    }
    if attrs.is_empty() {
        // v4 raised "Missing flavor" here; the connector is defined nowhere.
        return Err(ActionError::new(
            "ValueError",
            super::connectors::undefined_connector_message("sql", label),
        ));
    }
    ConnectorAttributes::from_attribute_map(label, &attrs, InvalidPort::Reject)
        .and_then(|ca| ConnectionSpec::from_attributes(&ca))
        .map_err(|e| ActionError::new("ValueError", e.to_string()))
}

/// `type:label` with the default label filled in (the session's map key).
/// What native plugin discovery sees (names, trees and skipped directories): when it is
/// unchanged the opened [`NativePlugins`] and the warnings printed for it are still current.
pub(super) fn native_signature(
    plugins_dirs: &[std::path::PathBuf],
    cache: &std::path::Path,
    compiler: &CompilerPolicy,
) -> Vec<String> {
    let discovery = meerschaum_plugin::discovery::discover_report_in_with_policy(
        plugins_dirs,
        &meerschaum_plugin::discovery::build_dir_for_cache(cache),
        compiler,
    );
    discovery
        .candidates
        .iter()
        .map(|c| format!("{}:{}:{}", c.name, c.version, c.fingerprint))
        .chain(discovery.skipped.iter().map(|s| s.message()))
        .collect()
}

pub(super) fn full_keys(keys: &str) -> String {
    let (typ, label) = split_keys(keys);
    format!("{typ}:{label}")
}

/// Split `type:label` (the label defaults to `main`).
pub fn split_keys(keys: &str) -> (String, String) {
    let mut parts = keys.split(':');
    let typ = parts.next().unwrap_or_default().to_string();
    let label = parts
        .next()
        .map(str::to_string)
        .unwrap_or_else(|| static_config().connectors.default_label.clone());
    (typ, label)
}

/// `meerschaum:connectors:<type>:default` patched with the label's section.
pub fn connector_attributes(config: &Config, typ: &str, label: &str) -> Map {
    let specific = config
        .get(&["meerschaum", "connectors", typ, label])
        .map(|v| v.to_map())
        .unwrap_or_default();
    if specific.is_empty() {
        return Map::new();
    }
    let base = config
        .get(&["meerschaum", "connectors", typ, "default"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    apply_patch_maps(&base, &specific)
}

/// Builds `sql:<label>` connectors from the configured `meerschaum:connectors:sql` section
/// (falling back to `MRSM_SQL_<LABEL>` variables), lazily and once per label.
pub struct ConfiguredSqlProvider {
    sql: Map,
    sqlite_path: String,
    cache: Mutex<HashMap<String, Arc<SqlConnector>>>,
}

impl ConfiguredSqlProvider {
    /// A provider over the connectors in `config`.
    pub fn new(config: &Config) -> Self {
        Self {
            sql: config
                .get(&["meerschaum", "connectors", "sql"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            sqlite_path: config
                .paths()
                .path("SQLITE_DB_PATH")
                .to_string_lossy()
                .into_owned(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn build(&self, label: &str) -> Option<SqlConnector> {
        let section = |key: &str| self.sql.get(key).map(|v| v.to_map()).unwrap_or_default();
        let specific = section(label);
        if specific.is_empty() {
            return None;
        }
        let mut attrs = apply_patch_maps(&section("default"), &specific);
        let flavor = attrs.get("flavor").map(pyfmt::py_str).unwrap_or_default();
        if matches!(flavor.as_str(), "sqlite" | "geopackage") {
            attrs = specific;
            let db = attrs.get("database").map(pyfmt::py_str);
            if db.as_deref() == Some("{SQLITE_DB_PATH}") || (label == "local" && db.is_none()) {
                attrs.insert("database".into(), Value::Str(self.sqlite_path.clone()));
            }
        }
        let ca =
            ConnectorAttributes::from_attribute_map(label, &attrs, InvalidPort::Reject).ok()?;
        let spec = ConnectionSpec::from_attributes(&ca).ok()?;
        let settings = InstanceSettings::defaults(spec.flavor);
        SqlConnector::connect_lazy(label, spec, settings).ok()
    }
}

impl meerschaum_connector::InstanceProvider for ConfiguredSqlProvider {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        let label = keys.strip_prefix("sql:")?;
        {
            let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(conn) = cache.get(label) {
                return Some(conn.clone() as Arc<dyn InstanceConnector>);
            }
        }
        match self.build(label) {
            Some(conn) => {
                let conn = Arc::new(conn);
                self.cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(label.to_string(), conn.clone());
                Some(conn as Arc<dyn InstanceConnector>)
            }
            None => meerschaum_connector_sql::instance::env_provider::EnvSqlProvider::shared()
                .instance(keys),
        }
    }
}
