//! `meerschaum-connector-sql` bound into `meerschaum._core`: the `SqlConnector` instance methods behind
//! the v4 `SQLConnector` façade (design §4.4, D12, D16).
//!
//! `_core.SqlConnector(label, attributes)` is built from the same attributes the Python
//! `SQLConnector` resolved (its `__dict__`: `flavor`, `uri`, `host`, `port`, `username`,
//! `password`, `database`, `schema`, ...), so `connectors.yaml`, keyword arguments and
//! `MRSM_SQL_<LABEL>` URIs all work unchanged. Each method takes the Python arguments of the v4
//! method it replaces; pipes, users and plugins are read by attribute (duck typing). Database
//! I/O runs on the extension's Tokio runtime with the GIL released; frames cross as Arrow
//! ([`crate::pandas_bridge`]). `read` / `exec` / `value` / `to_sql` / `.engine` stay
//! SQLAlchemy in the façade (D16) and are not bound here.

use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_connector::{PipesKeys, PipesKeysQuery, SyncOptions};
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::instance::data::DataQueryOptions;
use meerschaum_connector_sql::pipe::ChunkInterval;
use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes};
use meerschaum_connector_sql::{Flavor, SqlConnector as RsSqlConnector};
use meerschaum_core::{Map, Value};
use meerschaum_kernel::Frame;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::convert::{PyCtx, cell_to_py, core_to_py, py_to_core};
use crate::pandas_bridge::{
    ColumnMode, IndexSpec, OutSpec, frame_from_py, frame_to_pandas, frame_to_polars,
};
use crate::pipe_bridge::{
    ParamsView, bound_to_py, cached_columns_types, core_pipe, core_user, data_query, date_bound,
    params_map, plugin_record, str_list, success_to_py,
};
use crate::runtime::block_on;

fn backend_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// `get_config(*keys)` through the Python façade (which serves the Rust registry), as a core
/// value.
fn py_config(py: Python<'_>, keys: &[&str]) -> Option<Value> {
    let get_config = py
        .import("meerschaum.config")
        .and_then(|m| m.getattr("get_config"))
        .ok()?;
    let kw = PyDict::new(py);
    kw.set_item("warn", false).ok()?;
    let args = PyTuple::new(py, keys).ok()?;
    let v = get_config.call(args, Some(&kw)).ok()?;
    if v.is_none() {
        return None;
    }
    py_to_core(&v).ok()
}

fn py_int(v: Option<Value>) -> Option<i64> {
    v.and_then(|v| meerschaum_core::pyfmt::py_int(&v))
}

/// Tell the DuckDB loader which Meerschaum root is in effect (`meerschaum.config.paths.ROOT_DIR_PATH`),
/// so a connection opened later looks for and installs the library under the same root as
/// `mrsm install driver duckdb` and `show drivers`, also after `set_root()` /
/// `replace_root_dir()` changed it. Set every time a connector is built, not once.
fn follow_python_root(py: Python<'_>) {
    let root = py
        .import("meerschaum.config.paths")
        .and_then(|paths| paths.getattr("ROOT_DIR_PATH"))
        .and_then(|root| root.str().map(|s| s.to_string()))
        .ok()
        .filter(|root| !root.is_empty())
        .map(std::path::PathBuf::from);
    if root.is_some() {
        meerschaum_connector_sql::duckdb_lib::set_root_override(root);
    }
}

/// The instance settings, read from the Python config (`system:connectors:sql:*`, `pipes:*`).
fn settings_from_py(py: Python<'_>, flavor: Flavor) -> InstanceSettings {
    let mut s = InstanceSettings::defaults(flavor);
    if let Some(c) = py_int(py_config(py, &["system", "connectors", "sql", "chunksize"])) {
        s.chunksize = c;
    }
    s.bulk_insert = py_config(
        py,
        &[
            "system",
            "connectors",
            "sql",
            "bulk_insert",
            flavor.as_str(),
        ],
    )
    .map(|v| v.truthy())
    .unwrap_or(false);
    let tt = |key: &str| {
        py_config(
            py,
            &[
                "system",
                "connectors",
                "sql",
                "instance",
                "temporary_target",
                key,
            ],
        )
    };
    if let Some(p) = tt("prefix") {
        s.temporary_target.prefix = p.py_str();
    }
    if let Some(n) = py_int(tt("transaction_id_length")) {
        s.temporary_target.transaction_id_length = n.max(0) as usize;
    }
    if let Some(sep) = tt("separator") {
        s.temporary_target.separator = sep.py_str();
    }
    if let Some(n) = py_int(py_config(
        py,
        &[
            "system",
            "connectors",
            "sql",
            "instance",
            "max_partitions_per_sync",
        ],
    )) {
        s.max_partitions_per_sync = n.max(0) as usize;
    }
    if let Some(n) = py_int(py_config(
        py,
        &[
            "system",
            "connectors",
            "sql",
            "instance",
            "stale_temporary_tables_minutes",
        ],
    )) {
        s.stale_temporary_tables_minutes = n;
    }
    if let Some(n) = py_int(py_config(
        py,
        &["pipes", "sync", "filter_params_index_limit"],
    )) {
        s.filter_params_index_limit = n.max(0) as usize;
    }
    if let Some(n) = py_int(py_config(
        py,
        &["pipes", "parameters", "fetch", "backtrack_minutes"],
    )) {
        s.backtrack_minutes = n;
    }
    if let Some(v) = py_config(py, &["meerschaum", "instance"]) {
        s.pipe_defaults.instance_keys = v.py_str();
    }
    if let Some(n) = py_int(py_config(
        py,
        &["pipes", "parameters", "verify", "chunk_minutes"],
    )) {
        s.pipe_defaults.chunk_minutes = n;
    }
    if let Some(v) = py_config(
        py,
        &["pipes", "autotime", "column_name_if_datetime_missing"],
    ) {
        s.pipe_defaults.autotime_column = v.py_str();
    }
    if let Some(v) = py_config(py, &["pipes", "cache", "enabled"]) {
        s.pipe_defaults.cache_enabled = v.truthy();
    }
    s
}

/// The connector attributes from the Python connector's `__dict__`-like mapping.
fn attributes_from_py(label: &str, attrs: &Bound<'_, PyDict>) -> PyResult<ConnectorAttributes> {
    let mut out = ConnectorAttributes {
        label: Some(label.to_string()),
        ..Default::default()
    };
    for (k, v) in attrs.iter() {
        if v.is_none() {
            continue;
        }
        let key: String = k.str()?.extract()?;
        let text = || -> PyResult<String> { Ok(v.str()?.to_str()?.to_string()) };
        match key.as_str() {
            "flavor" => {
                out.flavor = Some(
                    text()?
                        .parse()
                        .map_err(|e| PyValueError::new_err(format!("{e:?}")))?,
                )
            }
            "uri" => out.uri = Some(text()?),
            "host" => out.host = Some(text()?),
            "port" => {
                out.port = Some(
                    text()?
                        .parse()
                        .map_err(|_| PyValueError::new_err(format!("Invalid port '{v}'.")))?,
                )
            }
            "username" => out.username = Some(text()?),
            "password" => out.password = Some(text()?),
            "database" => out.database = Some(text()?),
            "schema" => out.schema = Some(text()?),
            "label" => {}
            _ => {
                if v.is_instance_of::<pyo3::types::PyString>()
                    || v.is_instance_of::<pyo3::types::PyInt>()
                {
                    out.extra.push((key, text()?));
                }
            }
        }
    }
    Ok(out)
}

/// The pandas dtype hints of a read (`read(dtype=...)` in v4: `to_pandas_dtype()` of the
/// pipe's dtypes).
fn read_hints(dtypes: &IndexMap<String, String>) -> IndexMap<String, String> {
    dtypes
        .iter()
        .map(|(c, t)| (c.clone(), meerschaum_kernel::dtypes::to_pandas_dtype(t)))
        .collect()
}

fn pandas_out<'py>(
    ctx: &PyCtx<'py>,
    frame: &Frame,
    hints: IndexMap<String, String>,
) -> PyResult<Bound<'py, PyAny>> {
    let spec = OutSpec {
        index: IndexSpec::Range,
        hints,
        arrow_native: false,
        arrow_columns: Vec::new(),
        geometry: None,
        labels: IndexMap::new(),
        reuse: IndexMap::new(),
        placeholders: Vec::new(),
        polars_objects: Vec::new(),
    };
    frame_to_pandas(ctx, frame, &spec)
}

/// Frames as a list of dicts, the rows v4 builds from `result.mappings()`: database `NULL`s are
/// `None` (not the pandas null flavor of the column type).
fn frame_docs_py<'py>(ctx: &PyCtx<'py>, frame: &Frame) -> PyResult<Bound<'py, PyList>> {
    let py = ctx.py;
    let names = frame.names();
    let columns: Vec<(Vec<meerschaum_kernel::Value>, Vec<bool>)> = frame
        .columns()
        .iter()
        .map(|c| (c.data.values(), c.data.is_null_mask()))
        .collect();
    let out = PyList::empty(py);
    for i in 0..frame.height() {
        let d = PyDict::new(py);
        for (n, (vals, nulls)) in names.iter().zip(&columns) {
            // A `Decimal('NaN')` read from a `NUMERIC` column is a value, not a null (v4 keeps it
            // in the documents; a database NULL is `None`).
            if nulls[i] && !matches!(vals[i], meerschaum_kernel::Value::Decimal(_)) {
                d.set_item(n, py.None())?;
            } else {
                d.set_item(n, cell_to_py(ctx, &vals[i])?)?;
            }
        }
        out.append(d)?;
    }
    Ok(out)
}

/// `apply_patch_to_config`'s difference: the top-level keys whose values changed.
fn changed_keys(before: &Map, after: &Map) -> Map {
    after
        .iter()
        .filter(|(k, v)| before.get(*k) != Some(*v))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// `pipe_metadata_epoch()`: the number of pipe metadata writes (`register_pipe`, `edit_pipe`,
/// `delete_pipe`, a sync's dtype edits) this process has made through any `_core.SqlConnector`.
/// `Pipe.get_parameters` keys its memo of cross-pipe `{{ Pipe() }}` templates on it.
#[pyfunction]
pub fn pipe_metadata_epoch() -> u64 {
    meerschaum_connector_sql::instance::pipes::pipe_metadata_epoch()
}

/// The `sql:<label>` instance connector.
#[pyclass(frozen, name = "SqlConnector", module = "meerschaum._core")]
pub struct PySqlConnector {
    pub(crate) inner: Arc<RsSqlConnector>,
}

impl PySqlConnector {
    fn conn(&self) -> Arc<RsSqlConnector> {
        self.inner.clone()
    }
}

#[pymethods]
impl PySqlConnector {
    /// `SqlConnector(label, attributes)`: `attributes` is the Python connector's resolved
    /// attributes (`flavor`, `uri`, `host`, `port`, `username`, `password`, `database`,
    /// `schema`, ...). The pool connects lazily.
    #[new]
    fn new(py: Python<'_>, label: &str, attributes: &Bound<'_, PyDict>) -> PyResult<Self> {
        let attrs = attributes_from_py(label, attributes)?;
        let spec = ConnectionSpec::from_attributes(&attrs).map_err(backend_err)?;
        let settings = settings_from_py(py, spec.flavor);
        follow_python_root(py);
        let label = label.to_string();
        let conn = py.allow_threads(|| {
            let _guard = crate::runtime::runtime().enter();
            RsSqlConnector::connect_lazy(&label, spec, settings)
        });
        Ok(PySqlConnector {
            inner: Arc::new(conn.map_err(backend_err)?),
        })
    }

    /// `self.flavor`.
    #[getter]
    fn flavor(&self) -> &'static str {
        self.inner.flavor().as_str()
    }

    /// `self.schema`.
    #[getter]
    fn schema(&self) -> Option<String> {
        self.inner.schema().map(str::to_string)
    }

    /// Ask the database for its default schema when none is configured (`current_schema()` /
    /// `SCHEMA_NAME()`, once, cached): a role's `search_path` or a login's default schema is
    /// what v4's `sqlalchemy.inspect(engine).default_schema_name` reports. Returns the schema
    /// in effect; raises when the database is unreachable (the flavor default stays until a
    /// later call succeeds).
    fn discover_schema(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let c = self.conn();
        block_on(py, async move { c.discover_default_schema().await }).map_err(backend_err)
    }

    /// `test_connection()`: whether a trivial query (`SELECT 1`) succeeds. One attempt;
    /// the façade retries.
    #[pyo3(signature = (**_kw))]
    fn test_connection(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> PyResult<bool> {
        let c = self.conn();
        Ok(block_on(py, async move {
            c.value(c.flavor().test_query()).await.is_some()
        }))
    }

    /// `self.internal_schema`.
    #[getter]
    fn internal_schema(&self) -> Option<String> {
        self.inner.internal_schema().map(str::to_string)
    }

    fn __repr__(&self) -> String {
        format!("_core.SqlConnector('{}')", self.inner.keys())
    }

    // -----------------------------------------------------------------------------------------
    // Registration (`mrsm_pipes`)

    #[pyo3(signature = (pipe, **_kw))]
    fn register_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Unresolved)?;
        let c = self.conn();
        let res = block_on(py, async move { c.register_pipe(&p).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, patch=false, **_kw))]
    fn edit_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        patch: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Raw)?;
        let c = self.conn();
        let res = block_on(py, async move { c.edit_pipe(&p, patch).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn delete_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Keys)?;
        let c = self.conn();
        let res = block_on(py, async move { c.delete_pipe(&p).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_id(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<i64>> {
        let p = core_pipe(pipe, ParamsView::Keys)?;
        let c = self.conn();
        block_on(py, async move { c.get_pipe_id(&p).await }).map_err(backend_err)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_attributes<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = core_pipe(pipe, ParamsView::Keys)?;
        let c = self.conn();
        let attrs =
            block_on(py, async move { c.get_pipe_attributes(&p).await }).map_err(backend_err)?;
        core_to_py(py, &Value::Map(attrs))
    }

    #[pyo3(signature = (
        connector_keys=None, metric_keys=None, location_keys=None, tags=None, params=None, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn fetch_pipes_keys<'py>(
        &self,
        py: Python<'py>,
        connector_keys: Option<&Bound<'py, PyAny>>,
        metric_keys: Option<&Bound<'py, PyAny>>,
        location_keys: Option<&Bound<'py, PyAny>>,
        tags: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut lks: Vec<Option<String>> = Vec::new();
        if let Some(obj) = location_keys.filter(|o| !o.is_none()) {
            for item in obj.try_iter()? {
                let item = item?;
                lks.push(if item.is_none() {
                    None
                } else {
                    Some(item.str()?.to_str()?.to_string())
                });
            }
        }
        let query = PipesKeysQuery {
            connector_keys: str_list(connector_keys)?.unwrap_or_default(),
            metric_keys: str_list(metric_keys)?.unwrap_or_default(),
            location_keys: lks,
            tags: str_list(tags)?.unwrap_or_default(),
            params: params_map(params)?,
        };
        let c = self.conn();
        let keys =
            block_on(py, async move { c.fetch_pipes_keys(&query).await }).map_err(backend_err)?;
        let entry_tuple =
            |e: &meerschaum_connector::PipeKeysEntry| -> PyResult<Bound<'py, PyTuple>> {
                let mut items: Vec<Bound<'py, PyAny>> = vec![
                    e.connector_keys.clone().into_pyobject(py)?.into_any(),
                    e.metric_key.clone().into_pyobject(py)?.into_any(),
                    match &e.location_key {
                        Some(l) => l.clone().into_pyobject(py)?.into_any(),
                        None => py.None().into_bound(py),
                    },
                ];
                if let Some(extra) = &e.extra {
                    items.push(core_to_py(py, extra)?);
                }
                PyTuple::new(py, items)
            };
        match keys {
            PipesKeys::Dict(m) => {
                let out = PyDict::new(py);
                for (id, e) in &m {
                    out.set_item(core_to_py(py, &id.to_value())?, entry_tuple(e)?)?;
                }
                Ok(out.into_any())
            }
            PipesKeys::List(v) => {
                let out = PyList::empty(py);
                for e in &v {
                    out.append(entry_tuple(e)?)?;
                }
                Ok(out.into_any())
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Tables and data

    #[pyo3(signature = (pipe, **_kw))]
    fn pipe_exists(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        block_on(py, async move { c.pipe_exists(&p).await }).map_err(backend_err)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_columns_types<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let types =
            block_on(py, async move { c.get_pipe_columns_types(&p).await }).map_err(backend_err)?;
        let out = PyDict::new(py);
        for (k, v) in types {
            out.set_item(k, v)?;
        }
        Ok(out)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_columns_indices<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let indices = block_on(py, async move { c.get_pipe_columns_indices(&p).await })
            .map_err(backend_err)?;
        let out = PyDict::new(py);
        for (col, docs) in indices {
            let list = PyList::empty(py);
            for d in docs {
                let doc = PyDict::new(py);
                doc.set_item("name", d.name)?;
                doc.set_item("type", d.type_)?;
                if let Some(cl) = d.clustered {
                    doc.set_item("clustered", cl)?;
                }
                list.append(doc)?;
            }
            out.set_item(col, list)?;
        }
        Ok(out)
    }

    #[pyo3(signature = (pipe, params=None, newest=true, remote=false, **_kw))]
    fn get_sync_time<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        params: Option<&Bound<'py, PyAny>>,
        newest: bool,
        remote: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let params = params_map(params)?;
        let cached = cached_columns_types(pipe);
        let c = self.conn();
        let st = block_on(py, async move {
            c.get_sync_time_cached(&p, params.as_ref(), newest, remote, cached)
                .await
        })
        .map_err(backend_err)?;
        bound_to_py(&ctx, st.as_ref())
    }

    #[pyo3(signature = (pipe, begin=None, end=None, params=None, remote=false, **_kw))]
    #[allow(clippy::too_many_arguments)]
    fn get_pipe_rowcount(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        begin: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        params: Option<&Bound<'_, PyAny>>,
        remote: bool,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<i64>> {
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let mut q = data_query(&ctx, None, None, begin, end, params, None, None)?;
        q.remote = remote;
        let cached = cached_columns_types(pipe);
        let c = self.conn();
        block_on(py, async move {
            c.get_pipe_rowcount_cached(&p, &q, cached).await
        })
        .map_err(backend_err)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn drop_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let res = block_on(py, async move { c.drop_pipe(&p).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, begin=None, end=None, params=None, **_kw))]
    fn clear_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let q = data_query(&ctx, None, None, begin, end, params, None, None)?;
        let c = self.conn();
        let res = block_on(py, async move { c.clear_pipe(&p, &q).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, begin=None, end=None, params=None, **_kw))]
    fn deduplicate_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let q = data_query(&ctx, None, None, begin, end, params, None, None)?;
        let c = self.conn();
        let res = block_on(py, async move { c.deduplicate_pipe(&p, &q).await });
        success_to_py(py, &res)
    }

    /// `get_pipe_data(...)`: a pandas DataFrame (Polars with `as_polars=True`), `None` when the
    /// table does not exist, or an iterator of chunks with `as_iterator=True`.
    #[pyo3(signature = (
        pipe, select_columns=None, omit_columns=None, begin=None, end=None, params=None,
        order=Some("asc".to_string()), limit=None, chunksize=Some(-1), as_iterator=false,
        as_polars=false, begin_add_minutes=0, end_add_minutes=0, replace_nulls=None,
        skip_existing_cols_check=false, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn get_pipe_data<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        select_columns: Option<&Bound<'py, PyAny>>,
        omit_columns: Option<&Bound<'py, PyAny>>,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
        order: Option<String>,
        limit: Option<i64>,
        chunksize: Option<i64>,
        as_iterator: bool,
        as_polars: bool,
        begin_add_minutes: i64,
        end_add_minutes: i64,
        replace_nulls: Option<String>,
        skip_existing_cols_check: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = PyCtx::new(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        // v4: `order=None` omits the `ORDER BY` (and `Pipe.get_data` does not sort).
        let no_order = order.is_none();
        // v4: `begin=''` reads from the sync time minus the backtrack interval.
        let begin_default = begin
            .and_then(|b| b.downcast::<pyo3::types::PyString>().ok())
            .is_some_and(|s| s.to_str().map(|s| s.is_empty()).unwrap_or(false));
        let q = data_query(
            &ctx,
            select_columns,
            omit_columns,
            begin,
            end,
            params,
            limit,
            order,
        )?;
        let opts = DataQueryOptions {
            begin_default,
            begin_add_minutes,
            end_add_minutes,
            replace_nulls,
            skip_existing_cols_check,
            no_order,
        };
        let cached = cached_columns_types(pipe);
        let c = self.conn();
        let default_chunksize = c.settings.chunksize;
        let (df, state) = {
            let _t = crate::trace::Stage::new("sql: get_pipe_data");
            block_on(py, async move {
                c.get_pipe_data_and_state_with(&p, &q, &opts, cached).await
            })
            .map_err(backend_err)?
        };
        let Some(df) = df else {
            return Ok(py.None().into_bound(py));
        };
        let _t = crate::trace::Stage::new("sql: frame -> python");
        if as_polars && !as_iterator {
            return frame_to_polars(py, &df, &state.dtypes);
        }
        let hints = read_hints(&state.dtypes);
        if !as_iterator {
            return pandas_out(&ctx, &df, hints);
        }
        let size = match chunksize {
            Some(n) if n > 0 => n as usize,
            _ => default_chunksize.max(1) as usize,
        };
        let chunks = PyList::empty(py);
        let height = df.height();
        let mut start = 0usize;
        while start < height {
            let stop = (start + size).min(height);
            let chunk = df.take(&(start..stop).collect::<Vec<_>>());
            chunks.append(pandas_out(&ctx, &chunk, hints.clone())?)?;
            start = stop;
        }
        if height == 0 {
            chunks.append(pandas_out(&ctx, &df, hints)?)?;
        }
        chunks.call_method0("__iter__")
    }

    /// `get_pipe_docs(...)`: the rows as dictionaries.
    #[pyo3(signature = (
        pipe, select_columns=None, omit_columns=None, begin=None, end=None, params=None,
        order=Some("asc".to_string()), limit=None, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn get_pipe_docs<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        select_columns: Option<&Bound<'py, PyAny>>,
        omit_columns: Option<&Bound<'py, PyAny>>,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
        order: Option<String>,
        limit: Option<i64>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Documents never need pandas: with `PyCtx::new` the first `get_data(as_docs=True)` of
        // a process imported it (or asked `attempt_import` to install it).
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let no_order = order.is_none();
        let q = data_query(
            &ctx,
            select_columns,
            omit_columns,
            begin,
            end,
            params,
            limit,
            order,
        )?;
        let cached = cached_columns_types(pipe);
        let c = self.conn();
        let df = block_on(py, async move {
            let sp = c.sql_pipe(&p)?;
            let state = c.pipe_state_cached(&sp, cached).await;
            if !state.exists {
                return Ok::<_, meerschaum_connector::ConnectorError>(None);
            }
            let opts = DataQueryOptions {
                no_order,
                ..Default::default()
            };
            let sql = c.build_data_query(&sp, &state, &q, &opts).await?;
            Ok(c.driver().read_docs_frame(&sql).await.ok())
        })
        .map_err(backend_err)?;
        match df {
            Some(df) => frame_docs_py(&ctx, &df),
            None => Ok(PyList::empty(py)),
        }
    }

    #[pyo3(signature = (
        pipe, select_columns=None, omit_columns=None, begin=None, end=None, params=None,
        order=Some("asc".to_string()), sort_datetimes=false, limit=None, begin_add_minutes=0,
        end_add_minutes=0, replace_nulls=None, skip_existing_cols_check=false, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn get_pipe_data_query(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        select_columns: Option<&Bound<'_, PyAny>>,
        omit_columns: Option<&Bound<'_, PyAny>>,
        begin: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        params: Option<&Bound<'_, PyAny>>,
        order: Option<String>,
        sort_datetimes: bool,
        limit: Option<i64>,
        begin_add_minutes: i64,
        end_add_minutes: i64,
        replace_nulls: Option<String>,
        skip_existing_cols_check: bool,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let _ = sort_datetimes;
        let ctx = PyCtx::scalars(py)?;
        let begin_default = begin
            .and_then(|b| b.downcast::<pyo3::types::PyString>().ok())
            .is_some_and(|s| s.to_str().map(|s| s.is_empty()).unwrap_or(false));
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let no_order = order.is_none();
        let q = data_query(
            &ctx,
            select_columns,
            omit_columns,
            begin,
            end,
            params,
            limit,
            order,
        )?;
        let opts = DataQueryOptions {
            begin_default,
            begin_add_minutes,
            end_add_minutes,
            replace_nulls,
            skip_existing_cols_check,
            no_order,
        };
        let c = self.conn();
        block_on(
            py,
            async move { c.get_pipe_data_query(&p, &q, &opts).await },
        )
        .map_err(backend_err)
    }

    /// `sync_pipe(pipe, df, begin, end, chunksize, check_existing, ...)`. The pipe's in-memory
    /// parameters receive the changes v4's `sync_pipe` makes (`autoincrement`, numeric dtypes).
    #[pyo3(signature = (
        pipe, df=None, begin=None, end=None, chunksize=Some(-1), check_existing=true, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn sync_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        df: Option<&Bound<'py, PyAny>>,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        chunksize: Option<i64>,
        check_existing: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let Some(df) = df.filter(|d| !d.is_none()) else {
            let msg = format!("DataFrame is None. Cannot sync {}.", pipe.str()?);
            return success_to_py(py, &meerschaum_core::SuccessTuple::fail(msg));
        };
        let ctx = PyCtx::new(py)?;
        // The raw parameters: `sync_pipe_mut_cached` resolves the templates through the instance
        // on every call and returns only the raw keys the sync changed.
        let mut p = core_pipe(pipe, ParamsView::Raw)?;
        let before = p.raw_parameters().clone();
        let frame = {
            let _t = crate::trace::Stage::new("sql: sync_pipe df -> frame");
            frame_from_py(&ctx, df, ColumnMode::Full)?.frame
        };
        let mut extra = Map::new();
        if let Ok(cached) = pipe.call_method1("_get_cached_value", ("new_special_cols",)) {
            if !cached.is_none() {
                extra.insert("new_special_cols".into(), py_to_core(&cached)?);
            }
        }
        let opts = SyncOptions {
            check_existing,
            chunksize,
            begin: date_bound(&ctx, begin)?,
            end: date_bound(&ctx, end)?,
            extra,
        };
        let c = self.conn();
        let cached = cached_columns_types(pipe);
        let (res, p) = {
            let _t = crate::trace::Stage::new("sql: sync_pipe");
            block_on(py, async move {
                let res = c.sync_pipe_mut_cached(&mut p, frame, &opts, cached).await;
                (res, p)
            })
        };
        let patch = changed_keys(&before, p.raw_parameters());
        if !patch.is_empty() {
            // v4 mutates the Python pipe (`pipe.update_parameters(...)`); Rust already
            // persisted the change, so only the in-memory raw parameters are patched here.
            let kw = PyDict::new(py);
            kw.set_item("persist", false)?;
            pipe.call_method(
                "update_parameters",
                (core_to_py(py, &Value::Map(patch))?,),
                Some(&kw),
            )?;
        }
        success_to_py(py, &res.as_success_tuple())
    }

    // -----------------------------------------------------------------------------------------
    // Indices

    #[pyo3(signature = (pipe, columns=None, indices=None, **_kw))]
    fn create_indices(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        columns: Option<&Bound<'_, PyAny>>,
        indices: Option<&Bound<'_, PyAny>>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let columns = str_list(columns)?;
        let indices = str_list(indices)?;
        let c = self.conn();
        Ok(block_on(py, async move {
            c.create_indices(&p, columns.as_deref(), indices.as_deref())
                .await
        }))
    }

    #[pyo3(signature = (pipe, columns=None, indices=None, **_kw))]
    fn drop_indices(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        columns: Option<&Bound<'_, PyAny>>,
        indices: Option<&Bound<'_, PyAny>>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let columns = str_list(columns)?;
        let indices = str_list(indices)?;
        let c = self.conn();
        Ok(block_on(py, async move {
            c.drop_indices(&p, columns.as_deref(), indices.as_deref())
                .await
        }))
    }

    #[pyo3(signature = (pipe, columns=None, **_kw))]
    fn create_pipe_indices<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        columns: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let columns = str_list(columns)?;
        let c = self.conn();
        let res = block_on(py, async move {
            c.create_pipe_indices(&p, columns.as_deref()).await
        });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, columns=None, **_kw))]
    fn drop_pipe_indices<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        columns: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let columns = str_list(columns)?;
        let c = self.conn();
        let res = block_on(py, async move {
            c.drop_pipe_indices(&p, columns.as_deref()).await
        });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_create_index_queries<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let q = block_on(py, async move { c.get_create_index_queries(&p).await })
            .map_err(backend_err)?;
        let out = PyDict::new(py);
        for (k, v) in q {
            out.set_item(k, v)?;
        }
        Ok(out)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_drop_index_queries<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let q =
            block_on(py, async move { c.get_drop_index_queries(&p).await }).map_err(backend_err)?;
        let out = PyDict::new(py);
        for (k, v) in q {
            out.set_item(k, v)?;
        }
        Ok(out)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_index_names<'py>(
        &self,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let py = pipe.py();
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let names = self.inner.get_pipe_index_names(&p).map_err(backend_err)?;
        let out = PyDict::new(py);
        for (k, v) in names {
            out.set_item(k, v)?;
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------------------------
    // Maintenance

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_size(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<i64>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        Ok(block_on(py, async move { c.get_pipe_size(&p).await }))
    }

    #[pyo3(signature = (pipe, no_policy=false, **_kw))]
    fn compress_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        no_policy: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let res = block_on(py, async move { c.compress_pipe(&p, no_policy).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, no_policy=false, **_kw))]
    fn decompress_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        no_policy: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let res = block_on(py, async move { c.decompress_pipe(&p, no_policy).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn apply_compression_policy<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let res = block_on(py, async move { c.apply_compression_policy(&p).await });
        success_to_py(py, &res)
    }

    /// `set_integer_now_func(pipe)`: register an `integer_now` function on an integer-axis
    /// TimescaleDB hypertable (`True` when nothing needed doing or it was registered).
    #[pyo3(signature = (pipe, **_kw))]
    fn set_integer_now_func(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let sp = c.sql_pipe(&p).map_err(backend_err)?;
        Ok(block_on(
            py,
            async move { c.set_integer_now_func(&sp).await },
        ))
    }

    #[pyo3(signature = (pipe, full=false, **_kw))]
    fn vacuum_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        full: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let res = block_on(py, async move { c.vacuum_pipe(&p, full).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn analyze_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let res = block_on(py, async move { c.analyze_pipe(&p).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, chunk_minutes=None, **_kw))]
    fn partition_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        chunk_minutes: Option<i64>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let (res, patch) = block_on(py, async move {
            c.partition_pipe_with_parameters(&p, chunk_minutes).await
        });
        if !patch.is_empty() {
            let kw = PyDict::new(py);
            kw.set_item("persist", false)?;
            pipe.call_method(
                "update_parameters",
                (core_to_py(py, &Value::Map(patch))?,),
                Some(&kw),
            )?;
        }
        success_to_py(py, &res)
    }

    /// `get_partition_info(pipe)`: `interval` is a `timedelta` on datetime axes, as in v4.
    #[pyo3(signature = (pipe, **_kw))]
    fn get_partition_info<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn();
        let interval = c.sql_pipe(&p).ok().and_then(|sp| sp.chunk_interval().ok());
        let info = block_on(py, async move { c.get_partition_info(&p).await });
        let out = core_to_py(py, &Value::Map(info))?.downcast_into::<PyDict>()?;
        if out.get_item("interval")?.is_some_and(|v| !v.is_none()) {
            if let Some(ChunkInterval::Delta(d)) = interval {
                let td = py
                    .import("datetime")?
                    .getattr("timedelta")?
                    .call1((0, d.num_seconds()))?;
                out.set_item("interval", td)?;
            }
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------------------------
    // Users (`mrsm_users`)

    #[pyo3(signature = (user, **_kw))]
    fn register_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut u = core_user(user)?;
        let c = self.conn();
        let res = block_on(py, async move { c.register_user(&mut u).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (user, **_kw))]
    fn get_user_id<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let u = core_user(user)?;
        let c = self.conn();
        let id = block_on(py, async move { c.get_user_id(&u).await }).map_err(backend_err)?;
        core_to_py(py, &id.unwrap_or(Value::Null))
    }

    #[pyo3(signature = (**_kw))]
    fn get_users(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<String>> {
        let c = self.conn();
        block_on(py, async move { c.get_users().await }).map_err(backend_err)
    }

    #[pyo3(signature = (user, **_kw))]
    fn edit_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut u = core_user(user)?;
        let c = self.conn();
        if u.user_id.is_none() {
            let lookup = u.clone();
            let c2 = c.clone();
            let id =
                block_on(py, async move { c2.get_user_id(&lookup).await }).map_err(backend_err)?;
            if let Some(id) = &id {
                // v4 sets `user.user_id` before editing.
                user.setattr("user_id", core_to_py(py, id)?)?;
            }
            u.user_id = id;
        }
        let res = block_on(py, async move { c.edit_user(&u).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (user, **_kw))]
    fn delete_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let u = core_user(user)?;
        let c = self.conn();
        let res = block_on(py, async move { c.delete_user(&u).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (user, **_kw))]
    fn get_user_password_hash(
        &self,
        py: Python<'_>,
        user: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let u = core_user(user)?;
        let c = self.conn();
        block_on(py, async move { c.get_user_password_hash(&u).await }).map_err(backend_err)
    }

    #[pyo3(signature = (user, **_kw))]
    fn get_user_type(
        &self,
        py: Python<'_>,
        user: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let u = core_user(user)?;
        let c = self.conn();
        block_on(py, async move { c.get_user_type(&u).await }).map_err(backend_err)
    }

    #[pyo3(signature = (user, **_kw))]
    fn get_user_attributes<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let u = core_user(user)?;
        let c = self.conn();
        let attrs =
            block_on(py, async move { c.get_user_attributes(&u).await }).map_err(backend_err)?;
        match attrs {
            Some(m) => core_to_py(py, &Value::Map(m)),
            None => Ok(py.None().into_bound(py)),
        }
    }

    // -----------------------------------------------------------------------------------------
    // Plugins (`mrsm_plugins`)

    #[pyo3(signature = (plugin, force=false, **_kw))]
    fn register_plugin<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        force: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let rec = plugin_record(plugin, true)?;
        let c = self.conn();
        let res = block_on(py, async move { c.register_plugin(&rec, force).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_id(
        &self,
        py: Python<'_>,
        plugin: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<i64>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn();
        block_on(py, async move { c.get_plugin_id(&rec).await }).map_err(backend_err)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_version(
        &self,
        py: Python<'_>,
        plugin: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn();
        block_on(py, async move { c.get_plugin_version(&rec).await }).map_err(backend_err)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_user_id(
        &self,
        py: Python<'_>,
        plugin: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<i64>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn();
        block_on(py, async move { c.get_plugin_user_id(&rec).await }).map_err(backend_err)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_username(
        &self,
        py: Python<'_>,
        plugin: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn();
        block_on(py, async move { c.get_plugin_username(&rec).await }).map_err(backend_err)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_attributes<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn();
        let attrs = block_on(py, async move { c.get_plugin_attributes(&rec).await })
            .map_err(backend_err)?;
        core_to_py(py, &Value::Map(attrs))
    }

    #[pyo3(signature = (user_id=None, search_term=None, **_kw))]
    fn get_plugins(
        &self,
        py: Python<'_>,
        user_id: Option<&Bound<'_, PyAny>>,
        search_term: Option<String>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<String>> {
        let user_id = match user_id.filter(|u| !u.is_none()) {
            Some(u) => Some(py_to_core(u)?),
            None => None,
        };
        let c = self.conn();
        block_on(py, async move {
            c.get_plugins(user_id.as_ref(), search_term.as_deref())
                .await
        })
        .map_err(backend_err)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn delete_plugin<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn();
        let res = block_on(py, async move { c.delete_plugin(&rec).await });
        success_to_py(py, &res)
    }
}
