//! Pipes: keys, parameters, and the pure-data attributes of `meerschaum.Pipe`
//! (`meerschaum/core/Pipe/__init__.py`, `_attributes.py`, and `get_chunk_interval` from
//! `_data.py`).
//!
//! Constructing a [`Pipe`] performs no I/O. Where Python reads configuration while constructing
//! (the default instance, `pipes:cache:enabled`) or while computing an attribute (the default
//! chunk size, the autotime column name), the values come from [`PipeDefaults`].
//!
//! Accessors share a lazy parameter snapshot. Instance-backed resolution is supplied through
//! [`Pipe::resolve_parameters_with`]; raw metadata remains available for persistence and planning.
//! Python's `dtypes` property additionally merges dtypes inferred from the instance table;
//! [`Pipe::dtypes`] is the configured part (`get_dtypes(infer=False)`).

pub mod chunk;
pub mod refs;
pub mod resolve;
pub mod target;

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::config::Config;
use crate::dtypes::{
    PrecisionUnit, are_dtypes_equal, pd_dtype, precision_unit_alias, resolve_alias,
};
use crate::pyfmt::py_str;
use crate::static_config::static_config;
use crate::value::{Map, Value, apply_patch_maps};

pub use chunk::{ChunkInterval, ChunkIntervalArg, DEFAULT_BACKTRACK_MINUTES};
pub use refs::{pipe_from_string, pipe_from_value};
pub use target::{TruncateError, truncate_item_name, truncate_string_sections_best_effort};

/// Errors from constructing a pipe or computing an attribute.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PipeError {
    /// The connector or metric key is missing.
    #[error(
        "Please provide strings for the connector and metric\n    (first two positional arguments)."
    )]
    MissingKeys,
    /// A key or tag starts with the negation prefix.
    #[error("A pipe's keys and tags cannot start with the prefix '{0}'.")]
    NegationPrefix(String),
    /// A constructor argument problem (Python raises `TypeError`).
    #[error("{0}")]
    TypeError(String),
    /// A value problem (Python raises `ValueError`).
    #[error("{0}")]
    ValueError(String),
    /// The target name could not be truncated.
    #[error(transparent)]
    Truncate(#[from] TruncateError),
}

/// Configuration values the Python `Pipe` reads with `get_config`.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeDefaults {
    /// `meerschaum:instance` (default `sql:main`).
    pub instance_keys: String,
    /// `pipes:parameters:verify:chunk_minutes` (default 43200).
    pub chunk_minutes: i64,
    /// `pipes:autotime:column_name_if_datetime_missing` (default `ts`).
    pub autotime_column: String,
    /// `pipes:cache:enabled` (default `true`).
    pub cache_enabled: bool,
}

impl Default for PipeDefaults {
    fn default() -> Self {
        PipeDefaults {
            instance_keys: "sql:main".to_string(),
            chunk_minutes: 43200,
            autotime_column: "ts".to_string(),
            cache_enabled: true,
        }
    }
}

impl PipeDefaults {
    /// Read the defaults from a config registry.
    pub fn from_config(config: &Config) -> PipeDefaults {
        let d = PipeDefaults::default();
        let opts = crate::config::GetOptions {
            warn: false,
            ..Default::default()
        };
        let get = |keys: &[&str]| -> Option<Value> {
            let (valid, v) = config.get_with(keys, opts);
            valid.then_some(v)
        };
        PipeDefaults {
            instance_keys: get(&["meerschaum", "instance"])
                .map(|v| py_str(&v))
                .unwrap_or(d.instance_keys),
            chunk_minutes: get(&["pipes", "parameters", "verify", "chunk_minutes"])
                .and_then(|v| crate::pyfmt::py_int(&v))
                .unwrap_or(d.chunk_minutes),
            autotime_column: get(&["pipes", "autotime", "column_name_if_datetime_missing"])
                .map(|v| py_str(&v))
                .unwrap_or(d.autotime_column),
            cache_enabled: get(&["pipes", "cache", "enabled"])
                .map(|v| v.truthy())
                .unwrap_or(d.cache_enabled),
        }
    }
}

/// The resolved timestamp precision of a pipe's datetime axis (`Pipe.precision`).
#[derive(Debug, Clone, PartialEq)]
pub struct Precision {
    /// The unit.
    pub unit: PrecisionUnit,
    /// The optional rounding interval (`precision.interval`), as configured.
    pub interval: Option<Value>,
}

impl Precision {
    /// The Python dict form (`{'unit': 'microsecond', 'interval': ...}`).
    pub fn to_value(&self) -> Value {
        let mut m = Map::new();
        m.insert("unit".into(), Value::Str(self.unit.name().to_string()));
        if let Some(i) = &self.interval {
            m.insert("interval".into(), i.clone());
        }
        Value::Map(m)
    }
}

/// The positional parameter names of `Pipe.__init__`, in order.
pub const PIPE_INIT_PARAMS: &[&str] = &[
    "connector",
    "metric",
    "location",
    "parameters",
    "columns",
    "indices",
    "tags",
    "target",
    "dtypes",
    "instance",
    "upsert",
    "autoincrement",
    "autotime",
    "precision",
    "static",
    "enforce",
    "null_indices",
    "mixed_numerics",
    "compress",
    "temporary",
    "cache",
    "cache_connector_keys",
    "references",
    "reference",
    "parents",
    "parent",
    "children",
    "child",
    "mrsm_instance",
    "connector_keys",
    "metric_key",
    "location_key",
    "instance_keys",
    "indexes",
    "debug",
];

/// A Meerschaum pipe: three keys, an instance, and a parameters dictionary.
///
/// Equality and hashing use the connector, metric, location and instance keys only, like
/// `Pipe.__eq__` / `Pipe.__hash__`.
#[derive(Debug, Clone)]
pub struct Pipe {
    connector_keys: String,
    metric_key: String,
    location_key: Option<String>,
    instance_keys: String,
    parameters: Map,
    resolved_cache: std::sync::OnceLock<Map>,
    pub(crate) resolution_snapshot: bool,
    /// `temporary`: never create instance tables.
    pub temporary: bool,
    /// `cache`: whether metadata is cached to disk.
    pub cache: bool,
    /// `cache_connector_keys`.
    pub cache_connector_keys: Option<String>,
    default_instance_keys: String,
    warnings: Vec<String>,
}

impl PartialEq for Pipe {
    fn eq(&self, other: &Self) -> bool {
        self.connector_keys == other.connector_keys
            && self.metric_key == other.metric_key
            && self.location_key == other.location_key
            && self.instance_keys == other.instance_keys
    }
}

impl Eq for Pipe {}

impl Hash for Pipe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Python hashes the keys joined with an esoteric separator.
        let sep = "[\"']";
        let loc = self.location_key.as_deref().unwrap_or("None");
        format!(
            "{}{sep}{}{sep}{}{sep}{}{sep}",
            self.connector_keys, self.metric_key, loc, self.instance_keys
        )
        .hash(state);
    }
}

impl fmt::Display for Pipe {
    /// `str(pipe)` (`pipe_repr` without ANSI): `Pipe('ck', 'mk'[, 'lk'][, instance='ik'])`, where
    /// the instance is shown only when it differs from the configured default instance.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pipe('{}', '{}'", self.connector_keys, self.metric_key)?;
        if let Some(loc) = &self.location_key {
            write!(f, ", '{loc}'")?;
        }
        if self.instance_keys != self.default_instance_keys {
            write!(f, ", instance='{}'", self.instance_keys)?;
        }
        f.write_str(")")
    }
}

/// A builder for [`Pipe`] with typed arguments (see [`Pipe::from_call`] for the Python-style
/// argument form).
#[derive(Debug, Clone, Default)]
pub struct PipeBuilder {
    kwargs: Map,
    defaults: PipeDefaults,
}

impl PipeBuilder {
    fn set(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.kwargs.insert(key.to_string(), value.into());
        self
    }

    /// The location key.
    pub fn location(self, location: Option<&str>) -> Self {
        self.set("location", location.map(String::from))
    }

    /// The instance keys (`instance=`).
    pub fn instance(self, instance: &str) -> Self {
        self.set("instance", instance)
    }

    /// The initial parameters.
    pub fn parameters(self, parameters: Map) -> Self {
        self.set("parameters", parameters)
    }

    /// Any other `Pipe.__init__` keyword argument (`columns`, `dtypes`, `upsert`, `parents`, ...).
    pub fn kwarg(self, key: &str, value: impl Into<Value>) -> Self {
        self.set(key, value)
    }

    /// The configuration defaults to use.
    pub fn defaults(mut self, defaults: PipeDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// Construct the pipe.
    pub fn build(self) -> Result<Pipe, PipeError> {
        Pipe::from_call(&[], &self.kwargs, &self.defaults)
    }
}

fn kw<'a>(kwargs: &'a Map, key: &str) -> &'a Value {
    static NULL: Value = Value::Null;
    kwargs.get(key).unwrap_or(&NULL)
}

impl Pipe {
    /// `Pipe(connector, metric)` with the default instance and no location.
    pub fn new(connector_keys: &str, metric_key: &str) -> Result<Pipe, PipeError> {
        Pipe::builder(connector_keys, metric_key).build()
    }

    /// Start building `Pipe(connector, metric, ...)`.
    pub fn builder(connector_keys: &str, metric_key: &str) -> PipeBuilder {
        PipeBuilder::default()
            .set("connector", connector_keys)
            .set("metric", metric_key)
    }

    /// Port of `Pipe.__init__(*args, **kwargs)`: map positional and keyword arguments onto the
    /// constructor's parameters and apply its logic.
    pub fn from_call(
        args: &[Value],
        kwargs: &Map,
        defaults: &PipeDefaults,
    ) -> Result<Pipe, PipeError> {
        if args.len() > PIPE_INIT_PARAMS.len() {
            return Err(PipeError::TypeError(format!(
                "Pipe.__init__() takes from 1 to {} positional arguments but {} were given",
                PIPE_INIT_PARAMS.len() + 1,
                args.len() + 1
            )));
        }
        let mut all = Map::new();
        for (name, value) in PIPE_INIT_PARAMS.iter().zip(args.iter()) {
            all.insert(name.to_string(), value.clone());
        }
        for (key, value) in kwargs {
            if !PIPE_INIT_PARAMS.contains(&key.as_str()) {
                return Err(PipeError::TypeError(format!(
                    "Pipe.__init__() got an unexpected keyword argument '{key}'"
                )));
            }
            if all.contains_key(key) {
                return Err(PipeError::TypeError(format!(
                    "Pipe.__init__() got multiple values for argument '{key}'"
                )));
            }
            all.insert(key.clone(), value.clone());
        }
        Pipe::init(&all, defaults)
    }

    fn init(a: &Map, defaults: &PipeDefaults) -> Result<Pipe, PipeError> {
        let mut warnings = Vec::new();
        let mut connector = kw(a, "connector").clone();
        let mut metric = kw(a, "metric").clone();
        let mut location = kw(a, "location").clone();
        if (!connector.truthy() && !kw(a, "connector_keys").truthy())
            || (!metric.truthy() && !kw(a, "metric_key").truthy())
        {
            return Err(PipeError::MissingKeys);
        }
        if !location.truthy() {
            location = kw(a, "location_key").clone();
        }
        if !connector.truthy() {
            connector = kw(a, "connector_keys").clone();
        }
        if !metric.truthy() {
            metric = kw(a, "metric_key").clone();
        }
        if matches!(location.as_str(), Some("[None]") | Some("None")) {
            location = Value::Null;
        }

        let negation_prefix = &static_config().system.fetch_pipes_keys.negation_prefix;
        let tags_arg = kw(a, "tags");
        let mut to_check: Vec<String> =
            vec![py_str(&connector), py_str(&metric), py_str(&location)];
        if tags_arg.truthy() {
            match tags_arg {
                Value::List(items) => to_check.extend(items.iter().map(py_str)),
                Value::Str(s) => to_check.extend(s.chars().map(String::from)),
                Value::Map(m) => to_check.extend(m.keys().cloned()),
                _ => {}
            }
        }
        if to_check
            .iter()
            .any(|k| k.starts_with(negation_prefix.as_str()))
        {
            return Err(PipeError::NegationPrefix(negation_prefix.clone()));
        }

        let connector_keys = py_str(&connector);
        // PARITY: Python keeps a non-string metric/location as-is (e.g. an int); keys are
        // stringified here.
        let metric_key = py_str(&metric);
        let location_key = if location.is_null() {
            None
        } else {
            Some(py_str(&location))
        };

        let temporary = kw(a, "temporary").truthy();
        let mut cache = match kw(a, "cache") {
            Value::Null => !temporary && defaults.cache_enabled,
            v => v.truthy(),
        };
        let cache_connector_keys = match kw(a, "cache_connector_keys") {
            Value::Null => None,
            v => Some(py_str(v)),
        };

        let mut params = match kw(a, "parameters") {
            Value::Map(m) => m.clone(),
            Value::Null => Map::new(),
            other => {
                warnings.push(format!(
                    "The provided parameters are of invalid type '{}'.",
                    other.type_name()
                ));
                Map::new()
            }
        };

        let columns_arg = kw(a, "columns");
        let columns = if columns_arg.truthy() {
            columns_arg.clone()
        } else {
            params.get("columns").cloned().unwrap_or(Value::Null)
        };
        match columns {
            Value::List(items) => {
                let m: Map = items
                    .iter()
                    .map(|c| (py_str(c), Value::Str(py_str(c))))
                    .collect();
                params.insert("columns".into(), Value::Map(m));
            }
            Value::Map(m) => {
                params.insert("columns".into(), Value::Map(m));
            }
            Value::Str(s) if s.contains("Pipe(") => {}
            Value::Null => {}
            other => warnings.push(format!(
                "The provided columns are of invalid type '{}'.",
                other.type_name()
            )),
        }

        let indices = [
            kw(a, "indices").clone(),
            kw(a, "indexes").clone(),
            params.get("indices").cloned().unwrap_or(Value::Null),
            params.get("indexes").cloned().unwrap_or(Value::Null),
        ]
        .into_iter()
        .reduce(|acc, v| if acc.truthy() { acc } else { v })
        .unwrap_or(Value::Null);
        if let Value::Map(_) = indices {
            let key = if params.contains_key("indexes") {
                "indexes"
            } else {
                "indices"
            };
            params.insert(key.into(), indices);
        }

        match tags_arg {
            Value::List(_) => {
                params.insert("tags".into(), tags_arg.clone());
            }
            Value::Null => {}
            other => warnings.push(format!(
                "The provided tags are of invalid type '{}'.",
                other.type_name()
            )),
        }
        match kw(a, "target") {
            Value::Str(s) => {
                params.insert("target".into(), Value::Str(s.clone()));
            }
            Value::Null => {}
            other => warnings.push(format!(
                "The provided target is of invalid type '{}'.",
                other.type_name()
            )),
        }
        match kw(a, "dtypes") {
            Value::Map(m) => {
                params.insert("dtypes".into(), Value::Map(m.clone()));
            }
            Value::Null => {}
            other => warnings.push(format!(
                "The provided dtypes are of invalid type '{}'.",
                other.type_name()
            )),
        }
        for key in ["upsert", "autoincrement", "autotime"] {
            if let Value::Bool(b) = kw(a, key) {
                params.insert(key.into(), Value::Bool(*b));
            }
        }
        match kw(a, "precision") {
            Value::Map(m) => {
                params.insert("precision".into(), Value::Map(m.clone()));
            }
            Value::Str(s) => {
                let mut m = Map::new();
                m.insert("unit".into(), Value::Str(s.clone()));
                params.insert("precision".into(), Value::Map(m));
            }
            _ => {}
        }
        for key in ["static", "enforce", "null_indices", "mixed_numerics"] {
            if let Value::Bool(b) = kw(a, key) {
                params.insert(key.into(), Value::Bool(*b));
            }
        }
        if let v @ (Value::Bool(_) | Value::Map(_)) = kw(a, "compress") {
            params.insert("compress".into(), v.clone());
        }

        let instance = match kw(a, "mrsm_instance") {
            Value::Null => {
                let i = kw(a, "instance");
                if i.truthy() {
                    i.clone()
                } else {
                    kw(a, "instance_keys").clone()
                }
            }
            v => v.clone(),
        };
        let instance_keys = match instance {
            Value::Null => defaults.instance_keys.clone(),
            v => py_str(&v),
        };
        if instance_keys == "sql:memory" {
            cache = false;
        }

        let mut pipe = Pipe {
            connector_keys,
            metric_key,
            location_key,
            instance_keys,
            parameters: params,
            resolved_cache: std::sync::OnceLock::new(),
            resolution_snapshot: false,
            temporary,
            cache,
            cache_connector_keys,
            default_instance_keys: defaults.instance_keys.clone(),
            warnings,
        };

        for (plural, singular) in [
            ("references", "reference"),
            ("parents", "parent"),
            ("children", "child"),
        ] {
            let p = kw(a, plural);
            let s = kw(a, singular);
            if !p.is_null() || !s.is_null() {
                let vals = if !p.is_null() { p.clone() } else { s.clone() };
                pipe.set_relation(plural, vals);
            }
        }
        Ok(pipe)
    }

    /// The connector keys (`pipe.connector_keys`).
    pub fn connector_keys(&self) -> &str {
        &self.connector_keys
    }

    /// The metric key (`pipe.metric_key` / `pipe.metric`).
    pub fn metric_key(&self) -> &str {
        &self.metric_key
    }

    /// The location key (`pipe.location_key` / `pipe.location`).
    pub fn location_key(&self) -> Option<&str> {
        self.location_key.as_deref()
    }

    /// The instance keys (`pipe.instance_keys`).
    pub fn instance_keys(&self) -> &str {
        &self.instance_keys
    }

    /// The configured default instance at construction (used by `str(pipe)`).
    pub fn default_instance_keys(&self) -> &str {
        &self.default_instance_keys
    }

    /// Warnings Python would have emitted while constructing this pipe.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// `pipe.meta` / `pipe.keys()`: the four keys needed to reconstruct the pipe.
    pub fn keys(&self) -> Map {
        let mut m = Map::new();
        m.insert(
            "connector_keys".into(),
            Value::Str(self.connector_keys.clone()),
        );
        m.insert("metric_key".into(), Value::Str(self.metric_key.clone()));
        m.insert("location_key".into(), self.location_key.clone().into());
        m.insert(
            "instance_keys".into(),
            Value::Str(self.instance_keys.clone()),
        );
        m
    }

    /// `Pipe.__getstate__`: the keys plus the in-memory parameters.
    pub fn to_state(&self) -> Map {
        let mut m = Map::new();
        m.insert(
            "connector_keys".into(),
            Value::Str(self.connector_keys.clone()),
        );
        m.insert("metric_key".into(), Value::Str(self.metric_key.clone()));
        m.insert("location_key".into(), self.location_key.clone().into());
        m.insert("parameters".into(), Value::Map(self.parameters.clone()));
        m.insert(
            "instance_keys".into(),
            Value::Str(self.instance_keys.clone()),
        );
        m
    }

    /// `Pipe.__setstate__`: rebuild a pipe from [`Pipe::to_state`].
    pub fn from_state(state: &Map, defaults: &PipeDefaults) -> Result<Pipe, PipeError> {
        Pipe::from_call(&[], state, defaults)
    }

    /// Lazily resolved parameters. Instance-backed references may be supplied with
    /// [`Pipe::resolve_parameters_with`]; ordinary access resolves self templates and config.
    pub fn parameters(&self) -> &Map {
        if self.resolution_snapshot {
            return &self.parameters;
        }
        self.resolved_cache.get_or_init(|| {
            let resolver = crate::template::InMemoryResolver::default();
            let config = Value::Map(self.parameters.clone())
                .to_json_string()
                .contains("MRSM{")
                .then(crate::config::Config::from_env)
                .and_then(Result::ok);
            self.resolved_parameters(&resolver, config.as_ref())
                .map(|r| r.parameters)
                .unwrap_or_else(|_| self.parameters.clone())
        })
    }

    /// Unresolved metadata for persistence, definition hashes, and dependency extraction.
    pub fn raw_parameters(&self) -> &Map {
        &self.parameters
    }

    /// Resolve a run's instance-backed references and cache the snapshot for all consumers.
    /// Mutating parameters invalidates this snapshot; call again after registered state changes.
    pub fn resolve_parameters_with(
        &mut self,
        resolver: &dyn crate::template::TemplateResolver,
        config: Option<&crate::config::Config>,
    ) -> Result<Vec<String>, PipeError> {
        let result = self.resolved_parameters(resolver, config)?;
        self.resolved_cache.take();
        let _ = self.resolved_cache.set(result.parameters);
        Ok(result.warnings)
    }

    /// Mutable access to the in-memory parameters (the live `parameters` dict).
    pub fn parameters_mut(&mut self) -> &mut Map {
        self.resolved_cache.take();
        &mut self.parameters
    }

    /// `pipe.parameters = {...}`.
    pub fn set_parameters(&mut self, parameters: Map) {
        self.resolved_cache.take();
        self.parameters = parameters;
    }

    /// `pipe.update_parameters(patch, persist=False)`: deep-merge a patch.
    pub fn update_parameters(&mut self, patch: Map) {
        self.resolved_cache.take();
        self.parameters = apply_patch_maps(&self.parameters, &patch);
    }

    fn update_one(&mut self, key: &str, value: Value) {
        let mut patch = Map::new();
        patch.insert(key.to_string(), value);
        self.update_parameters(patch);
    }

    /// A top-level parameter (`pipe.parameters.get(key)`).
    pub fn param(&self, key: &str) -> Option<&Value> {
        self.parameters().get(key)
    }

    /// `pipe.columns`: the `columns` dict without falsy names or roles.
    pub fn columns(&self) -> Map {
        match self.param("columns") {
            Some(Value::Map(cols)) => cols
                .iter()
                .filter(|(ix, col)| col.truthy() && !ix.is_empty())
                .map(|(ix, col)| (ix.clone(), col.clone()))
                .collect(),
            _ => Map::new(),
        }
    }

    /// The name of a column role (`pipe.columns.get(role)`), if it is a string.
    pub fn column(&self, role: &str) -> Option<String> {
        self.columns().get(role).map(py_str)
    }

    /// `pipe.columns = ...`: a list becomes `{col: col}`.
    pub fn set_columns(&mut self, columns: Value) -> Result<(), PipeError> {
        let columns = match columns {
            Value::List(items) => {
                Value::Map(items.iter().map(|c| (py_str(c), c.clone())).collect())
            }
            m @ Value::Map(_) => m,
            other => {
                return Err(PipeError::TypeError(format!(
                    "{self}.columns must be a dictionary, received {}.",
                    other.type_name()
                )));
            }
        };
        self.update_one("columns", columns);
        Ok(())
    }

    fn indices_key(&self) -> &'static str {
        if self.parameters().contains_key("indexes") {
            "indexes"
        } else {
            "indices"
        }
    }

    /// `pipe.indices` / `pipe.indexes`: a `unique` composite of the index columns (when more
    /// than one), every column role, then the configured `indices` (or `indexes`).
    ///
    /// PARITY: Python builds `unique` from a `set`, so its order varies between processes; here
    /// it is the datetime column first, then the other roles in order.
    pub fn indices(&self) -> Map {
        let configured = match self.param(self.indices_key()) {
            Some(Value::Map(m)) => m.clone(),
            _ => Map::new(),
        };
        let columns = self.columns();
        let mut unique: Vec<Value> = Vec::new();
        if let Some(dt) = columns.get("datetime") {
            unique.push(dt.clone());
        }
        for (ix, col) in &columns {
            if ix != "datetime" && !unique.contains(col) {
                unique.push(col.clone());
            }
        }
        let mut out = Map::new();
        if unique.len() > 1 {
            out.insert("unique".into(), Value::List(unique));
        }
        for (ix, col) in &columns {
            out.insert(ix.clone(), col.clone());
        }
        for (k, v) in configured {
            out.insert(k, v);
        }
        out
    }

    /// `pipe.indices = {...}` (stored under `indexes` if that key is already used).
    pub fn set_indices(&mut self, indices: Map) {
        let key = self.indices_key();
        self.update_one(key, Value::Map(indices));
    }

    /// `pipe.tags`: the raw `tags` value (a list unless misconfigured), default `[]`.
    pub fn tags(&self) -> Value {
        self.param("tags")
            .cloned()
            .unwrap_or_else(|| Value::List(Vec::new()))
    }

    /// The tags as strings.
    pub fn tags_list(&self) -> Vec<String> {
        match self.tags() {
            Value::List(items) => items.iter().map(py_str).collect(),
            _ => Vec::new(),
        }
    }

    /// `pipe.tags = [...]`; tags may not start with the negation prefix.
    pub fn set_tags(&mut self, tags: Vec<String>) -> Result<(), PipeError> {
        let prefix = &static_config().system.fetch_pipes_keys.negation_prefix;
        if tags.iter().any(|t| t.starts_with(prefix.as_str())) {
            return Err(PipeError::ValueError(format!(
                "Tags cannot begin with '{prefix}'."
            )));
        }
        self.update_one(
            "tags",
            Value::List(tags.into_iter().map(Value::Str).collect()),
        );
        Ok(())
    }

    /// `pipe.get_dtypes(infer=False)`: configured dtypes with aliases resolved, the datetime
    /// column defaulting to `datetime`, and an autoincrement primary key defaulting to `int`.
    pub fn dtypes(&self) -> Map {
        let configured = self.param("dtypes").cloned().unwrap_or(Value::Null);
        let configured_map = configured.as_map().cloned().unwrap_or_default();
        let raw_columns = self.param("columns");
        let col = |role: &str| -> Option<Value> {
            raw_columns
                .and_then(|c| c.get(role))
                .filter(|v| v.truthy())
                .cloned()
        };
        let mut out = Map::new();
        for (c, typ) in &configured_map {
            if c.is_empty() || !typ.truthy() {
                continue;
            }
            let t = match typ {
                Value::Str(s) => Value::Str(resolve_alias(s).to_string()),
                other => other.clone(),
            };
            out.insert(c.clone(), t);
        }
        if let Some(dt) = col("datetime") {
            let dt = py_str(&dt);
            if !configured_map.contains_key(&dt) {
                out.insert(dt, Value::Str("datetime".into()));
            }
        }
        if let Some(primary) = col("primary") {
            let primary = py_str(&primary);
            if self.autoincrement() && !out.contains_key(&primary) {
                out.insert(primary, Value::Str("int".into()));
            }
        }
        out
    }

    /// The dtype of one column (`pipe.dtypes.get(col)`), as a string.
    pub fn dtype(&self, column: &str) -> Option<String> {
        self.dtypes().get(column).map(py_str)
    }

    /// `pipe.dtypes = {...}`.
    pub fn set_dtypes(&mut self, dtypes: Map) {
        self.update_one("dtypes", Value::Map(dtypes));
    }

    fn bool_param(&self, key: &str, default: bool) -> bool {
        self.param(key).map_or(default, Value::truthy)
    }

    /// `pipe.upsert` (default `False`).
    pub fn upsert(&self) -> bool {
        self.bool_param("upsert", false)
    }

    /// `pipe.static` (default `False`).
    pub fn is_static(&self) -> bool {
        self.bool_param("static", false)
    }

    /// `pipe.autoincrement` (default `False`).
    pub fn autoincrement(&self) -> bool {
        self.bool_param("autoincrement", false)
    }

    /// `pipe.autotime` (default `False`).
    pub fn autotime(&self) -> bool {
        self.bool_param("autotime", false)
    }

    /// `pipe.enforce` (default `True`).
    pub fn enforce(&self) -> bool {
        self.bool_param("enforce", true)
    }

    /// `pipe.null_indices` (default `True`).
    pub fn null_indices(&self) -> bool {
        self.bool_param("null_indices", true)
    }

    /// `pipe.mixed_numerics` (default `True`).
    pub fn mixed_numerics(&self) -> bool {
        self.bool_param("mixed_numerics", true)
    }

    /// Set one of the boolean flags (`upsert`, `static`, `autoincrement`, `autotime`, `enforce`,
    /// `null_indices`, `mixed_numerics`), like `pipe.upsert = True`.
    pub fn set_flag(&mut self, key: &str, value: bool) {
        self.update_one(key, Value::Bool(value));
    }

    /// `pipe.tzinfo` reduced to "is the datetime axis timezone-aware (UTC)?".
    pub fn is_utc(&self, defaults: &PipeDefaults) -> bool {
        let dtypes = self.dtypes();
        let mut dt_typ = self
            .column("datetime")
            .map(|dt| dtypes.get(&dt).map_or("datetime".to_string(), py_str));
        if self.autotime() {
            dt_typ = Some(
                dtypes
                    .get(&defaults.autotime_column)
                    .map_or("datetime".to_string(), py_str),
            );
        }
        match dt_typ {
            Some(t) => t.to_lowercase().contains("utc") || t == "datetime",
            None => false,
        }
    }

    /// `pipe._target_legacy()`: `{connector_keys with ':' -> '_'}_{metric}[_{location}]`.
    pub fn target_legacy(&self) -> String {
        let mut name = format!(
            "{}_{}",
            self.connector_keys.replace(':', "_"),
            self.metric_key
        );
        if let Some(loc) = &self.location_key {
            name.push('_');
            name.push_str(loc);
        }
        name
    }

    /// `pipe.target` without side effects. `sql_flavor` is the instance connector's flavor when
    /// the instance is a SQL connector (enables per-flavor truncation).
    ///
    /// Order: a truthy `target`; else the first present of `target_name`, `target_table`,
    /// `target_table_name` (if truthy); else the legacy default. A truthy `target` is never
    /// truncated.
    pub fn target(&self, sql_flavor: Option<&str>) -> Result<String, PipeError> {
        Ok(self.target_and_default(sql_flavor)?.0)
    }

    fn target_and_default(&self, sql_flavor: Option<&str>) -> Result<(String, bool), PipeError> {
        if let Some(t) = self.param("target").filter(|v| v.truthy()) {
            return Ok((py_str(t), true));
        }
        let default_target = self.target_legacy();
        let mut defaults = vec![default_target.clone()];
        let mut target = Value::Null;
        for k in ["target_name", "target_table", "target_table_name"] {
            if let Some(v) = self.param(k) {
                target = v.clone();
                break;
            }
        }
        let mut target = if target.truthy() {
            py_str(&target)
        } else {
            default_target
        };
        if let Some(flavor) = sql_flavor {
            let truncated = truncate_item_name(&target, flavor)?;
            defaults.push(truncated.clone());
            target = truncated;
        }
        let is_default = defaults.contains(&target);
        Ok((target, is_default))
    }

    /// `pipe.target` with Python's side effect: a non-default target (e.g. from
    /// `target_name`) is written back to `parameters['target']`.
    pub fn resolve_target(&mut self, sql_flavor: Option<&str>) -> Result<String, PipeError> {
        let (target, is_default) = self.target_and_default(sql_flavor)?;
        if !is_default && self.param("target").filter(|v| v.truthy()).is_none() {
            self.update_one("target", Value::Str(target.clone()));
        }
        Ok(target)
    }

    /// `pipe.target = name`.
    pub fn set_target(&mut self, target: &str) {
        self.update_one("target", Value::Str(target.to_string()));
    }

    /// Port of `get_precision`: the configured `precision` (string or dict), or one derived from
    /// the datetime column's dtype, with aliases resolved.
    pub fn precision(&self, defaults: &PipeDefaults) -> Result<Precision, PipeError> {
        let default_unit = static_config()
            .dtypes
            .datetime
            .default_precision_unit
            .clone();
        let mut precision = self
            .param("precision")
            .cloned()
            .unwrap_or_else(Value::new_map);
        if let Value::Str(s) = &precision {
            let mut m = Map::new();
            m.insert("unit".into(), Value::Str(s.clone()));
            precision = Value::Map(m);
        }
        if !precision.truthy() {
            let mut dt_col = self
                .param("columns")
                .and_then(|c| c.get("datetime"))
                .filter(|v| v.truthy())
                .map(py_str);
            if dt_col.is_none() && self.autotime() {
                dt_col = Some(defaults.autotime_column.clone());
            }
            let Some(dt_col) = dt_col else {
                // PARITY: returned without validation or caching (`_attributes.py:1250`).
                return Ok(Precision {
                    unit: PrecisionUnit::parse(&default_unit).unwrap_or(PrecisionUnit::Microsecond),
                    interval: None,
                });
            };
            let mut dt_typ = self
                .dtypes()
                .get(&dt_col)
                .map_or("datetime".to_string(), py_str);
            let mut m = Map::new();
            if are_dtypes_equal(&dt_typ, "datetime") {
                if dt_typ == "datetime" {
                    dt_typ = pd_dtype("datetime")
                        .unwrap_or("datetime64[us, UTC]")
                        .to_string();
                }
                let unit = dt_typ
                    .split_once('[')
                    .map_or(dt_typ.as_str(), |(_, rest)| rest);
                let unit = unit.split(',').next().unwrap_or(unit);
                let unit = unit.split(' ').next().unwrap_or(unit);
                m.insert(
                    "unit".into(),
                    Value::Str(unit.trim_end_matches(']').to_string()),
                );
            } else if are_dtypes_equal(&dt_typ, "int") {
                let unit = if dt_typ.contains("32") {
                    "second".to_string()
                } else {
                    default_unit.clone()
                };
                m.insert("unit".into(), Value::Str(unit));
            } else if are_dtypes_equal(&dt_typ, "date") {
                m.insert("unit".into(), Value::Str("day".into()));
            }
            precision = Value::Map(m);
        }
        let Value::Map(pm) = &precision else {
            return Err(PipeError::TypeError(format!(
                "'{}' object has no attribute 'get'",
                precision.type_name()
            )));
        };
        let unit = pm
            .get("unit")
            .cloned()
            .unwrap_or_else(|| Value::Str(default_unit.clone()));
        let interval = pm.get("interval").filter(|v| v.truthy()).cloned();
        let unit_str = match &unit {
            Value::Null => default_unit.clone(),
            v => py_str(v),
        };
        let true_unit = precision_unit_alias(&unit_str).unwrap_or(&unit_str);
        let parsed = PrecisionUnit::from_name(true_unit).ok_or_else(|| {
            PipeError::ValueError(format!(
                "Invalid precision unit '{true_unit}'.\nAccepted values are 'nanosecond', 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'ns', 'us', 'ms', 's', 'sec', 'm', 'min', 'h', 'hr', 'd', and 'D'."
            ))
        })?;
        Ok(Precision {
            unit: parsed,
            interval,
        })
    }

    /// `pipe.precision = unit_or_dict`: a string keeps an existing `interval`.
    pub fn set_precision(&mut self, precision: Value) {
        let existing = self.param("precision").cloned();
        let existing = match existing {
            Some(Value::Str(s)) => {
                let mut m = Map::new();
                m.insert("unit".into(), Value::Str(s));
                Some(Value::Map(m))
            }
            other => other,
        };
        let value = match precision {
            m @ Value::Map(_) => m,
            other => {
                let mut m = Map::new();
                m.insert("unit".into(), other);
                if let Some(interval) = existing
                    .as_ref()
                    .and_then(|e| e.get("interval"))
                    .filter(|v| v.truthy())
                {
                    m.insert("interval".into(), interval.clone());
                }
                Value::Map(m)
            }
        };
        self.update_one("precision", value);
    }

    /// `pipe.guess_datetime()` over the configured dtypes.
    pub fn guess_datetime(&self) -> Option<String> {
        let dtypes = self.dtypes();
        if let Some(Value::Null) = dtypes.get("datetime") {
            return None;
        }
        dtypes
            .iter()
            .find(|(_, t)| are_dtypes_equal(&py_str(t), "datetime"))
            .map(|(c, _)| c.clone())
    }

    /// `pipe[key]` (`Pipe.__getitem__`) for the pure-data attributes. Unknown keys are `None`.
    pub fn get_item(&self, key: &str) -> Value {
        match key {
            "connector_keys" | "connector" | "connector_key" => {
                Value::Str(self.connector_keys.clone())
            }
            "metric_key" | "metric" => Value::Str(self.metric_key.clone()),
            "location_key" | "location" => self.location_key.clone().into(),
            "parameters" => Value::Map(self.parameters().clone()),
            "instance" | "instance_key" | "instance_keys" => Value::Str(self.instance_keys.clone()),
            other => self.attribute(other).unwrap_or(Value::Null),
        }
    }

    /// A pure-data attribute by name (`getattr(pipe, name)`), or `None` if it is not one.
    pub fn attribute(&self, name: &str) -> Option<Value> {
        Some(match name {
            "connector_keys" | "connector_key" | "connector" => {
                Value::Str(self.connector_keys.clone())
            }
            "metric_key" | "metric" => Value::Str(self.metric_key.clone()),
            "location_key" | "location" => self.location_key.clone().into(),
            "instance_keys" | "instance" => Value::Str(self.instance_keys.clone()),
            "parameters" => Value::Map(self.parameters().clone()),
            "columns" => Value::Map(self.columns()),
            "dtypes" => Value::Map(self.dtypes()),
            "indices" | "indexes" => Value::Map(self.indices()),
            "tags" => self.tags(),
            "target" => Value::Str(self.target(None).ok()?),
            "upsert" => Value::Bool(self.upsert()),
            "static" => Value::Bool(self.is_static()),
            "autoincrement" => Value::Bool(self.autoincrement()),
            "autotime" => Value::Bool(self.autotime()),
            "enforce" => Value::Bool(self.enforce()),
            "null_indices" => Value::Bool(self.null_indices()),
            "mixed_numerics" => Value::Bool(self.mixed_numerics()),
            "temporary" => Value::Bool(self.temporary),
            "keys" | "meta" => Value::Map(self.keys()),
            _ => return None,
        })
    }

    /// The configured value of `parents`/`children`/`references` (falling back to the singular
    /// key), as a list of reference values.
    pub fn relation_values(&self, plural: &str) -> Vec<Value> {
        let singular = match plural {
            "parents" => "parent",
            "children" => "child",
            "references" => "reference",
            other => other,
        };
        let key = if self.parameters().contains_key(plural) {
            plural
        } else {
            singular
        };
        let refs = self.param(key).cloned().unwrap_or(Value::Null);
        if !refs.truthy() {
            return Vec::new();
        }
        match refs {
            Value::List(items) => items,
            other => vec![other],
        }
    }

    fn relation_pipes(
        &self,
        plural: &str,
        defaults: &PipeDefaults,
    ) -> Result<Vec<Option<Pipe>>, PipeError> {
        self.relation_values(plural)
            .iter()
            .map(|v| pipe_from_value(v, Some(self), defaults))
            .collect()
    }

    /// `pipe.parents`: one entry per configured reference; `None` where Python's
    /// `get_pipe_from_value` failed (it warns and returns `None`).
    pub fn parents(&self, defaults: &PipeDefaults) -> Result<Vec<Option<Pipe>>, PipeError> {
        self.relation_pipes("parents", defaults)
    }

    /// `pipe.children`.
    pub fn children(&self, defaults: &PipeDefaults) -> Result<Vec<Option<Pipe>>, PipeError> {
        self.relation_pipes("children", defaults)
    }

    /// `pipe.references` (parameter inheritance, not a data edge).
    pub fn references(&self, defaults: &PipeDefaults) -> Result<Vec<Option<Pipe>>, PipeError> {
        self.relation_pipes("references", defaults)
    }

    /// `pipe.parent`: the first of [`Pipe::parents`].
    pub fn parent(&self, defaults: &PipeDefaults) -> Result<Option<Pipe>, PipeError> {
        Ok(self.parents(defaults)?.into_iter().next().flatten())
    }

    /// `pipe.child`: the first of [`Pipe::children`].
    pub fn child(&self, defaults: &PipeDefaults) -> Result<Option<Pipe>, PipeError> {
        Ok(self.children(defaults)?.into_iter().next().flatten())
    }

    /// `pipe.reference`: the first of [`Pipe::references`].
    pub fn reference(&self, defaults: &PipeDefaults) -> Result<Option<Pipe>, PipeError> {
        Ok(self.references(defaults)?.into_iter().next().flatten())
    }

    /// The setters for `parents`, `children` and `references` (and their singular aliases): a
    /// single value becomes a one-item list, stored under the plural key.
    pub fn set_relation(&mut self, plural: &str, values: Value) {
        let list = match values {
            Value::List(items) => items,
            other => vec![other],
        };
        self.update_one(plural, Value::List(list));
    }

    /// `pipe.parents = [...]` with pipes (stored as their `keys()` dicts).
    pub fn set_parent_pipes(&mut self, pipes: &[Pipe]) {
        let list = pipes.iter().map(|p| Value::Map(p.keys())).collect();
        self.set_relation("parents", Value::List(list));
    }

    /// `pipe.children = [...]` with pipes (stored as their `keys()` dicts).
    pub fn set_children_pipes(&mut self, pipes: &[Pipe]) {
        let list = pipes.iter().map(|p| Value::Map(p.keys())).collect();
        self.set_relation("children", Value::List(list));
    }

    /// The `pipe_ref` string used by the engine (§5.1):
    /// `"{instance_keys}|{connector_keys}|{metric_key}|{location_key}"`.
    pub fn pipe_ref(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.instance_keys,
            self.connector_keys,
            self.metric_key,
            self.location_key.as_deref().unwrap_or("None")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(s: &str) -> Value {
        Value::from_json_str(s).unwrap()
    }

    #[test]
    fn construct_and_display() {
        let p = Pipe::builder("sql:main", "weather")
            .location(Some("us"))
            .build()
            .unwrap();
        assert_eq!(p.to_string(), "Pipe('sql:main', 'weather', 'us')");
        assert_eq!(p.target(None).unwrap(), "sql_main_weather_us");
        let p2 = Pipe::builder("sql:main", "weather")
            .location(Some("us"))
            .instance("sql:local")
            .build()
            .unwrap();
        assert_ne!(p, p2);
        assert_eq!(
            p2.to_string(),
            "Pipe('sql:main', 'weather', 'us', instance='sql:local')"
        );
        assert!(Pipe::new("_sql", "x").is_err());
        assert!(Pipe::new("", "x").is_err());
        let none_loc = Pipe::builder("a", "b")
            .location(Some("None"))
            .build()
            .unwrap();
        assert_eq!(none_loc.location_key(), None);
    }

    #[test]
    fn attributes() {
        let params = json(
            r#"{"columns": {"datetime": "ts", "id": "station", "value": null}, "dtypes": {"val": "decimal"}, "indexes": {"geo": ["lat", "lon"]}, "autoincrement": true}"#,
        );
        let p = Pipe::builder("plugin:noaa", "weather")
            .parameters(params.as_map().unwrap().clone())
            .build()
            .unwrap();
        assert_eq!(
            Value::Map(p.columns()),
            json(r#"{"datetime": "ts", "id": "station"}"#)
        );
        assert_eq!(
            Value::Map(p.dtypes()),
            json(r#"{"val": "numeric", "ts": "datetime"}"#)
        );
        assert_eq!(
            Value::Map(p.indices()),
            json(
                r#"{"unique": ["ts", "station"], "datetime": "ts", "id": "station", "geo": ["lat", "lon"]}"#
            )
        );
        assert!(p.enforce() && p.null_indices() && !p.upsert());
        let prec = p.precision(&PipeDefaults::default()).unwrap();
        assert_eq!(prec.unit, PrecisionUnit::Microsecond);
    }
}
