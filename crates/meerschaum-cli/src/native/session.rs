//! The connectors the native actions use: a Tokio runtime and the instance and source
//! connectors resolved from the config registry (`meerschaum/connectors/__init__.py`
//! `get_connector` and `connectors/parse.py` `parse_instance_keys`).
//!
//! Connectors are built from `meerschaum:connectors:<type>:default` patched with the label's
//! section, which includes the `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` environment connectors
//! (the config registry adds them when it is created).

use meerschaum_worker::manifest::{Manifest, ensure_manifest, manifest_path};
use meerschaum_worker::{PoolConfig, WorkerPool};
use std::collections::HashMap;
use std::future::Future;
use std::io::IsTerminal;
use std::sync::{Arc, Mutex};

use meerschaum_connector::InstanceConnector;
use meerschaum_core::config::Config;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{pyfmt, static_config};
use meerschaum_plugin::NativePlugins;
use meerschaum_sql::SqlConnector;
use meerschaum_sql::flavor::Flavor;
use meerschaum_sql::instance::InstanceSettings;
use meerschaum_sql::uri::{ConnectionOptions, ConnectionSpec, ConnectorAttributes};

use crate::builtin::ActionError;

/// An instance connector and its concrete type (SQL methods without a trait counterpart, such
/// as `get_partition_info` and the in-place sync, need the concrete connector).
#[derive(Clone)]
pub enum Instance {
    /// A `sql:` instance.
    Sql(Arc<SqlConnector>),
    /// Any other instance (`api:`, `valkey:`).
    Other(Arc<dyn InstanceConnector>),
}

impl Instance {
    /// The trait object.
    pub fn conn(&self) -> Arc<dyn InstanceConnector> {
        match self {
            Instance::Sql(c) => c.clone(),
            Instance::Other(c) => c.clone(),
        }
    }

    /// `str(connector)`.
    pub fn keys(&self) -> String {
        self.conn().connector_keys()
    }

    /// `connector.type`.
    pub fn type_name(&self) -> String {
        self.conn().connector_type().to_string()
    }

    /// The SQL connector, if this is a SQL instance.
    pub fn sql(&self) -> Option<&Arc<SqlConnector>> {
        match self {
            Instance::Sql(c) => Some(c),
            Instance::Other(_) => None,
        }
    }
}

/// The Tokio runtime and the connectors built so far (`meerschaum.connectors.connectors`).
#[derive(Clone)]
pub struct Session {
    rt: Arc<tokio::runtime::Runtime>,
    instances: Arc<Mutex<HashMap<String, Instance>>>,
    pool: Arc<Mutex<Option<WorkerPool>>>,
    manifest: Arc<Mutex<Option<Arc<Manifest>>>>,
    python_manifest: Arc<Mutex<Option<Arc<Manifest>>>>,
    natives: Arc<Mutex<Option<Arc<NativePlugins>>>>,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    /// A new session with a multi-threaded runtime.
    pub fn new() -> Session {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        Session {
            rt: Arc::new(rt),
            instances: Arc::new(Mutex::new(HashMap::new())),
            pool: Arc::new(Mutex::new(None)),
            manifest: Arc::new(Mutex::new(None)),
            python_manifest: Arc::new(Mutex::new(None)),
            natives: Arc::new(Mutex::new(None)),
        }
    }

    /// The single lazily-created Python pool shared by this process session.
    pub fn pool(&self, config: &Config) -> WorkerPool {
        self.pool
            .lock()
            .expect("worker pool")
            .get_or_insert_with(|| {
                let mut settings = PoolConfig::from_config(config);
                settings.worker.inherit_stdin = std::io::stdin().is_terminal();
                WorkerPool::new(settings)
            })
            .clone()
    }

    /// Refresh the fingerprinted manifest (Python plugins merged with native plugins) without
    /// starting Python for an empty plugin tree.
    pub fn manifest(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        let python = self.python_manifest_refresh(config)?;
        let paths = config.paths();
        let natives = match NativePlugins::open(
            &paths.plugins_dirs,
            &crate::native_plugins::cache_path(&paths.root_dir),
        ) {
            Ok(natives) => natives,
            Err(error) => {
                // A broken native plugin must not disable the Python plugins.
                let mut manifest = (*python).clone();
                manifest
                    .load_errors
                    .insert("native plugins".into(), error.to_string());
                *self.natives.lock().expect("natives") = Some(Arc::new(NativePlugins::empty()));
                let manifest = Arc::new(manifest);
                *self.manifest.lock().expect("manifest") = Some(manifest.clone());
                return Ok(manifest);
            }
        };
        let natives = Arc::new(natives);
        let manifest = Arc::new(crate::native_plugins::merge(&python, &natives));
        *self.natives.lock().expect("natives") = Some(natives);
        *self.manifest.lock().expect("manifest") = Some(manifest.clone());
        Ok(manifest)
    }

    fn python_manifest_refresh(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        let paths = config.paths();
        let pool = self.pool(config);
        let (manifest, _) = self
            .block_on(ensure_manifest(
                &pool,
                &manifest_path(&paths.root_dir),
                &paths.plugins_dirs,
            ))
            .map_err(|e| ActionError::new("WorkerError", e.to_string()))?;
        let manifest = Arc::new(manifest);
        *self.python_manifest.lock().expect("python manifest") = Some(manifest.clone());
        Ok(manifest)
    }

    /// The Python-only manifest (what the Python worker adapters may claim).
    pub fn python_manifest(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        if let Some(manifest) = self
            .python_manifest
            .lock()
            .expect("python manifest")
            .clone()
        {
            return Ok(manifest);
        }
        self.manifest(config)?;
        self.python_manifest_refresh(config)
    }

    /// The native plugins (discovered with the manifest).
    pub fn native_plugins(&self, config: &Config) -> Result<Arc<NativePlugins>, ActionError> {
        if let Some(natives) = self.natives.lock().expect("natives").clone() {
            return Ok(natives);
        }
        self.manifest(config)?;
        Ok(self
            .natives
            .lock()
            .expect("natives")
            .clone()
            .unwrap_or_else(|| Arc::new(NativePlugins::empty())))
    }

    /// Shut down pooled children before dropping the Tokio runtime.
    pub fn shutdown(&self) {
        let pool = self.pool.lock().expect("worker pool").clone();
        if let Some(pool) = pool {
            self.block_on(pool.shutdown());
        }
    }

    /// Run a future to completion.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.rt.block_on(fut)
    }

    /// Spawn a background task on the runtime.
    pub fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.rt.spawn(fut);
    }

    /// The labels of connectors built so far, by type (`show connectors`' "Active connectors").
    pub fn active(&self) -> Vec<(String, String)> {
        let mut keys: Vec<String> = self
            .instances
            .lock()
            .expect("instances")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys.into_iter()
            .filter_map(|k| {
                k.split_once(':')
                    .map(|(t, l)| (t.to_string(), l.to_string()))
            })
            .collect()
    }

    /// `parse_instance_keys(keys)` → the connector (built once per keys).
    pub fn instance(&self, keys: &str, config: &Config) -> Result<Instance, ActionError> {
        let (typ, label) = split_keys(keys);
        let full = format!("{typ}:{label}");
        if let Some(i) = self.instances.lock().expect("instances").get(&full) {
            return Ok(i.clone());
        }
        let inst = match typ.as_str() {
            "sql" => Instance::Sql(Arc::new(self.sql_connector(&label, config)?)),
            "api" => {
                let c = meerschaum_api::APIConnector::from_config(config, &label)
                    .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?;
                Instance::Other(Arc::new(c))
            }
            "valkey" => {
                let attrs =
                    meerschaum_valkey::ValkeyConnector::attributes_from_config(config, &label);
                let c = self
                    .block_on(meerschaum_valkey::ValkeyConnector::connect(&label, attrs))
                    .map_err(|e| ActionError::new("ConnectionError", e.to_string()))?;
                Instance::Other(Arc::new(c))
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
                        .expect("instances")
                        .insert(full, inst.clone());
                    return Ok(inst);
                }
                let connector = self
                    .block_on(meerschaum_worker::PythonConnector::connect(
                        keys,
                        self.pool(config),
                    ))
                    .map_err(|e| ActionError::new("WorkerError", e.to_string()))?;
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
            .expect("instances")
            .insert(full, inst.clone());
        Ok(inst)
    }

    /// A `sql:<label>` connector from the config registry.
    pub fn sql_connector(&self, label: &str, config: &Config) -> Result<SqlConnector, ActionError> {
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
            // PARITY: `SQLConnector.__init__` raises when neither `flavor` nor `uri` is set.
            return Err(ActionError::new(
                "ValueError",
                format!("    Missing flavor. Provide flavor as a key for 'sql:{label}'."),
            ));
        }
        let ca = sql_attributes(label, &attrs)?;
        let spec = ConnectionSpec::from_attributes(&ca)
            .map_err(|e| ActionError::new("ValueError", e.to_string()))?;
        let settings = InstanceSettings::from_config(spec.flavor, config);
        let conn = self
            .block_on(SqlConnector::connect_with(label, spec, settings))
            .map_err(|e| ActionError::new("OperationalError", e.to_string()))?;
        // Cross-instance `{{ Pipe(..., instance='sql:other') }}` references resolve against the
        // configured connectors, not only `MRSM_SQL_<LABEL>` environment variables.
        conn.set_instance_provider(Arc::new(ConfiguredSqlProvider::new(config)));
        Ok(conn)
    }
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

fn sql_attributes(label: &str, attrs: &Map) -> Result<ConnectorAttributes, ActionError> {
    let mut ca = ConnectorAttributes {
        label: Some(label.to_string()),
        ..Default::default()
    };
    let s = |v: &Value| pyfmt::py_str(v);
    for (k, v) in attrs {
        if v.is_null() {
            continue;
        }
        match k.as_str() {
            "flavor" => {
                ca.flavor = Some(
                    s(v).parse::<Flavor>()
                        .map_err(|e| ActionError::new("ValueError", e.to_string()))?,
                )
            }
            "uri" => ca.uri = Some(s(v)),
            "host" => ca.host = Some(s(v)),
            "port" => ca.port = pyfmt::py_int(v).and_then(|p| u16::try_from(p).ok()),
            "username" => ca.username = Some(s(v)),
            "password" => ca.password = Some(s(v)),
            "database" => ca.database = Some(s(v)),
            "schema" => ca.schema = Some(s(v)),
            "options" => {
                ca.options = Some(match v {
                    Value::Map(m) => {
                        ConnectionOptions::Pairs(m.iter().map(|(k, v)| (k.clone(), s(v))).collect())
                    }
                    other => ConnectionOptions::Str(s(other)),
                })
            }
            "pool_size" => ca.pool_size = pyfmt::py_int(v).and_then(|p| u32::try_from(p).ok()),
            "max_overflow" => {
                ca.max_overflow = pyfmt::py_int(v).and_then(|p| u32::try_from(p).ok())
            }
            other => ca.extra.push((other.to_string(), s(v))),
        }
    }
    Ok(ca)
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
        let ca = sql_attributes(label, &attrs).ok()?;
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
            None => meerschaum_sql::instance::env_provider::EnvSqlProvider::shared().instance(keys),
        }
    }
}

#[cfg(test)]
mod provider_tests {
    use super::*;
    use meerschaum_connector::InstanceProvider;
    use meerschaum_core::config::ConfigOptions;

    #[test]
    fn provider_resolves_configured_connectors() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let config = Config::new(ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                plugins.path().display().to_string(),
            ),
        ]))
        .unwrap();
        let provider = ConfiguredSqlProvider::new(&config);
        // `sql:local` is a configured SQLite connector in the default config.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = rt.enter();
        let conn = provider
            .instance("sql:local")
            .expect("configured connector");
        assert_eq!(conn.connector_keys(), "sql:local");
        assert!(provider.instance("sql:not_configured_anywhere").is_none());
        assert!(provider.instance("api:local").is_none());
    }
}
