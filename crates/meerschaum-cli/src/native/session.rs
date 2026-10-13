//! The connectors the native actions use: a Tokio runtime and the instance and source
//! connectors resolved from the config registry (`meerschaum/connectors/__init__.py`
//! `get_connector` and `connectors/parse.py` `parse_instance_keys`).
//!
//! Connectors are built from `meerschaum:connectors:<type>:default` patched with the label's
//! section, which includes the `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` environment connectors
//! (the config registry adds them when it is created).

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use meerschaum_connector::InstanceConnector;
use meerschaum_core::config::Config;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{pyfmt, static_config};
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
    instances: Rc<RefCell<HashMap<String, Instance>>>,
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
            instances: Rc::new(RefCell::new(HashMap::new())),
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
        let mut keys: Vec<String> = self.instances.borrow().keys().cloned().collect();
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
        if let Some(i) = self.instances.borrow().get(&full) {
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
                return Err(ActionError::new(
                    "Exception",
                    format!("Unable to parse connector keys '{keys}'"),
                ));
            }
        };
        self.instances.borrow_mut().insert(full, inst.clone());
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
        self.block_on(SqlConnector::connect_with(label, spec, settings))
            .map_err(|e| ActionError::new("OperationalError", e.to_string()))
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
