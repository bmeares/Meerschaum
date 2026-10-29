//! `Pipe.sync()` for explicit data in Rust (design §4.4, D14, D20): `_core.sync_pipe` runs the
//! engine's ordinary one-pipe flow ([`meerschaum_engine::sync::sync_pipe`]: registration check,
//! dtype enforcement, `autotime`, newly detected special columns, `filter_existing`, the
//! instance's `sync_pipe`) on a `_core.SqlConnector` the Python façade built.
//!
//! **Input.** `list[dict]` and `dict[str, list]` cross as kernel [`PipeInput::Docs`] /
//! [`PipeInput::Columns`] straight from the Python objects ([`PyCtx::scalars`] and
//! [`py_to_cell`]): no pandas, Polars, pyarrow or JSON round trip, so UUIDs, `bytes`,
//! `Decimal`, nested JSON and timezone-aware datetimes keep their types, and an interpreter
//! with none of the dataframe packages can sync. pandas, GeoPandas and Polars frames go through
//! [`frame_from_py`] (they need the package, which they imported to exist). Anything else
//! (a string, a generator, a tuple, a list of non-dicts, a dict with scalar values, keys that
//! are not strings, Dask) is *declined*, not guessed at: the result says why and the caller
//! keeps its Python path. Nothing is read or written before the decline.
//!
//! **Session.** The call is *worker-less* and *registered-only*, like [`crate::verify_api`]'s
//! façade session: the registry holds exactly the one connector the caller passed, so no
//! configured connector is built, no config file is read and no Python process starts. (The
//! one-instance [`StaticRegistry`] is all an explicit-data sync of one SQL pipe needs; the
//! façade [`Session`](meerschaum_cli::native::Session) of `verify_pipe` also holds the source
//! connectors of a chunked verification, which a sync of given data never reads.) It is **not**
//! a graph run: no ledger, no run history, no `mrsm:engine_*` pipe is touched (R1).
//!
//! **What the caller gets back.** A dict: `success`, `message`, the row counts of the
//! [`SyncResult`] (`inserted`, `updated`, `upserted`, `deleted`), `duration` (seconds), the
//! sync's `begin` / `end` bounds and `parameters` (the top-level raw parameters the sync
//! changed on the instance: `autoincrement`, numeric and newly detected special `dtypes`; the
//! façade applies them to the in-memory pipe like v4's `sync_pipe` does). `{'declined': reason}`
//! for an input shape that is not handled, `{'invalid': message}` for data pandas would not
//! turn into a DataFrame (the façade reports it as v4 does).

use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_connector::{DateBound, InstanceConnector};
use meerschaum_core::{Map, Value};
use meerschaum_engine::sync::{SyncArgs, SyncBegin, SyncEnv, sync_pipe as engine_sync_pipe};
use meerschaum_kernel::Value as Cell;
use meerschaum_kernel::enforce::PipeInput;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyType};

use crate::convert::{PyCtx, core_to_py, py_to_cell, py_to_core, qualname};
use crate::facade::{self, RegistryInput};
use crate::pandas_bridge::{ColumnMode, frame_from_py};
use crate::pipe_bridge::{ParamsView, bound_to_py, core_pipe, date_bound, params_map};
use crate::sql_api::PySqlConnector;

/// What [`input_from_py`] made of the caller's data.
#[derive(Debug)]
pub enum NativeInput {
    /// Data the native flow takes.
    Data(PipeInput),
    /// Data of a shape this path does not handle (the reason, for `debug` output).
    Declined(String),
    /// Data pandas would reject when building a DataFrame (the message pandas gives).
    Invalid(String),
}

/// A binding called with `debug=True`: debug output is on while it runs (see
/// `meerschaum_core::debug::guard` and [`crate::logging`]).
pub(crate) struct DebugGuard(#[allow(dead_code)] meerschaum_core::debug::DebugGuard);

impl DebugGuard {
    pub(crate) fn new(on: bool) -> Self {
        DebugGuard(meerschaum_core::debug::guard(on))
    }
}

/// The name of a document key: a string as it is, anything else (`pd.DataFrame` accepts
/// integer labels, which the table then names by their text) through `str()`.
fn key_name(key: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(match key.cast::<PyString>() {
        Ok(s) => s.to_str()?.to_string(),
        Err(_) => key.str()?.to_str()?.to_string(),
    })
}

/// `type(obj).__name__`.
fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "object".into())
}

/// A non-text sequence the way `pd.DataFrame` takes one as a column or a row: lists, tuples,
/// ranges, NumPy arrays and pandas / Polars Series (their index is not used).
fn sequence_items<'py>(obj: &Bound<'py, PyAny>) -> PyResult<Option<Vec<Bound<'py, PyAny>>>> {
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyDict>()
    {
        return Ok(None);
    }
    if let Ok(list) = obj.cast::<PyList>() {
        return Ok(Some(list.iter().collect()));
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        return Ok(Some(tuple.iter().collect()));
    }
    let name = qualname(obj)?;
    let is_array_like = name == "range"
        || name.starts_with("numpy.")
        || name.starts_with("pandas.")
        || name.starts_with("polars.");
    if is_array_like && !name.ends_with("DataFrame") && obj.hasattr("__iter__")? {
        let mut out = Vec::new();
        for item in obj.try_iter()? {
            out.push(item?);
        }
        return Ok(Some(out));
    }
    Ok(None)
}

/// The documents `pd.DataFrame(items)` builds from a list or tuple: dictionaries are the rows;
/// rows given as sequences name their columns `0`, `1`, ...; scalars are one column `0`.
fn docs_from_items(ctx: &PyCtx<'_>, items: &[Bound<'_, PyAny>]) -> PyResult<NativeInput> {
    let mut docs: Vec<IndexMap<String, Cell>> = Vec::with_capacity(items.len());
    let first_is_dict = items.first().is_some_and(|i| i.is_instance_of::<PyDict>());
    let first_is_row = items
        .first()
        .is_some_and(|i| !first_is_dict && sequence_items(i).ok().flatten().is_some());
    for item in items {
        if first_is_dict {
            let Ok(dict) = item.cast::<PyDict>() else {
                return Ok(NativeInput::Invalid(format!(
                    "'{}' object has no attribute 'keys'",
                    type_name(item)
                )));
            };
            let mut doc = IndexMap::with_capacity(dict.len());
            for (key, value) in dict.iter() {
                doc.insert(key_name(&key)?, py_to_cell(ctx, &value)?);
            }
            docs.push(doc);
        } else if first_is_row {
            let Some(row) = sequence_items(item)? else {
                return Ok(NativeInput::Invalid(format!(
                    "Expected a sequence for every row, got a '{}'.",
                    type_name(item)
                )));
            };
            let mut doc = IndexMap::with_capacity(row.len());
            for (index, value) in row.iter().enumerate() {
                doc.insert(index.to_string(), py_to_cell(ctx, value)?);
            }
            docs.push(doc);
        } else {
            let mut doc = IndexMap::with_capacity(1);
            doc.insert("0".to_string(), py_to_cell(ctx, item)?);
            docs.push(doc);
        }
    }
    Ok(NativeInput::Data(PipeInput::Docs(docs)))
}

/// A dictionary as `PipeInput::Columns` (`pd.DataFrame(columns)`): sequences are columns,
/// scalars repeat down the column when another column has a length, and mappings are columns
/// aligned on their keys (the index).
fn columns_from_dict(ctx: &PyCtx<'_>, dict: &Bound<'_, PyDict>) -> PyResult<NativeInput> {
    enum Raw<'py> {
        Seq(Vec<Bound<'py, PyAny>>),
        Mapping(Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)>),
        Scalar(Bound<'py, PyAny>),
    }
    let mut raws: Vec<(String, Raw<'_>)> = Vec::with_capacity(dict.len());
    for (key, values) in dict.iter() {
        let name = key_name(&key)?;
        let raw = if let Some(items) = sequence_items(&values)? {
            Raw::Seq(items)
        } else if let Ok(mapping) = values.cast::<PyDict>() {
            Raw::Mapping(mapping.iter().collect())
        } else if values.is_instance_of::<PyBytes>()
            || values.is_instance_of::<PyString>()
            || values.is_instance_of::<PyBool>()
            || values.is_instance_of::<PyInt>()
            || values.is_instance_of::<PyFloat>()
            || values.is_none()
            || !values.hasattr("__iter__")?
        {
            Raw::Scalar(values)
        } else {
            return Ok(NativeInput::Declined(format!(
                "column '{name}' is a '{}'",
                type_name(&values)
            )));
        };
        raws.push((name, raw));
    }
    // The index of mapping columns: the union of their keys, in order of first appearance.
    let mut index: Vec<String> = Vec::new();
    for (_, raw) in &raws {
        if let Raw::Mapping(pairs) = raw {
            for (k, _) in pairs {
                let k = k.str()?.to_str()?.to_string();
                if !index.contains(&k) {
                    index.push(k);
                }
            }
        }
    }
    let mut length: Option<usize> = None;
    for (_, raw) in &raws {
        let n = match raw {
            Raw::Seq(items) => Some(items.len()),
            Raw::Mapping(_) => Some(index.len()),
            Raw::Scalar(_) => None,
        };
        match (length, n) {
            (Some(a), Some(b)) if a != b => {
                return Ok(NativeInput::Invalid(
                    "All arrays must be of the same length".into(),
                ));
            }
            (None, Some(b)) => length = Some(b),
            _ => {}
        }
    }
    let Some(length) = length else {
        if raws.is_empty() {
            return Ok(NativeInput::Data(PipeInput::Columns(IndexMap::new())));
        }
        return Ok(NativeInput::Invalid(
            "If using all scalar values, you must pass an index".into(),
        ));
    };
    let mut columns: IndexMap<String, Vec<Cell>> = IndexMap::with_capacity(raws.len());
    for (name, raw) in raws {
        let cells = match raw {
            Raw::Seq(items) => items
                .iter()
                .map(|v| py_to_cell(ctx, v))
                .collect::<PyResult<Vec<_>>>()?,
            Raw::Scalar(value) => {
                let cell = py_to_cell(ctx, &value)?;
                vec![cell; length]
            }
            Raw::Mapping(pairs) => {
                let mut by_key: IndexMap<String, Cell> = IndexMap::with_capacity(pairs.len());
                for (k, v) in pairs {
                    by_key.insert(k.str()?.to_str()?.to_string(), py_to_cell(ctx, &v)?);
                }
                index
                    .iter()
                    .map(|k| by_key.get(k).cloned().unwrap_or(Cell::Null))
                    .collect()
            }
        };
        columns.insert(name, cells);
    }
    Ok(NativeInput::Data(PipeInput::Columns(columns)))
}

/// Python's `str.splitlines()` line boundaries.
fn py_splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        let is_break = matches!(
            c,
            '\n' | '\r'
                | '\u{0b}'
                | '\u{0c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        );
        if !is_break {
            continue;
        }
        lines.push(&text[start..i]);
        let mut end = i + c.len_utf8();
        if c == '\r' && chars.peek().is_some_and(|(_, n)| *n == '\n') {
            chars.next();
            end += 1;
        }
        start = end;
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// `parse_simple_lines(text)`: one `string_to_dict` document per line.
fn docs_from_text(text: &str) -> NativeInput {
    let mut docs: Vec<IndexMap<String, Cell>> = Vec::new();
    for line in py_splitlines(text) {
        match meerschaum_core::pyfmt::string_to_dict(line) {
            Ok(map) => docs.push(
                map.iter()
                    .map(|(k, v)| (k.clone(), Cell::from(v)))
                    .collect(),
            ),
            Err(_) => {
                return NativeInput::Invalid("Cannot parse simple lines into a dataframe.".into());
            }
        }
    }
    NativeInput::Data(PipeInput::Docs(docs))
}

/// Convert the data `Pipe.sync()` was given. Documents, dictionaries of lists and text never
/// touch pandas or Polars; a DataFrame object needs its own package (which created it).
pub fn input_from_py<'py>(py: Python<'py>, df: &Bound<'py, PyAny>) -> PyResult<NativeInput> {
    if let Ok(text) = df.cast::<PyString>() {
        return Ok(docs_from_text(text.to_str()?));
    }
    if let Ok(list) = df.cast::<PyList>() {
        let ctx = PyCtx::scalars(py)?;
        let items: Vec<_> = list.iter().collect();
        return docs_from_items(&ctx, &items);
    }
    if let Ok(tuple) = df.cast::<PyTuple>() {
        let ctx = PyCtx::scalars(py)?;
        let items: Vec<_> = tuple.iter().collect();
        return docs_from_items(&ctx, &items);
    }
    if let Ok(dict) = df.cast::<PyDict>() {
        let ctx = PyCtx::scalars(py)?;
        return columns_from_dict(&ctx, dict);
    }
    let name = qualname(df)?;
    if name.starts_with("pandas.") && name.ends_with("DataFrame") || name.starts_with("geopandas.")
    {
        let ctx = PyCtx::new(py)?;
        let frame = frame_from_py(&ctx, df, ColumnMode::Full)?.frame;
        return Ok(NativeInput::Data(PipeInput::Frame(frame)));
    }
    if name.starts_with("polars.") && (name.ends_with("DataFrame") || name.ends_with("LazyFrame")) {
        // `scalars`: a Polars frame never needs pandas (nor its import, nor an install).
        let ctx = PyCtx::scalars(py)?;
        let frame = frame_from_py(&ctx, df, ColumnMode::Full)?.frame;
        return Ok(NativeInput::Data(PipeInput::Frame(frame)));
    }
    if name.starts_with("pyarrow.") && (name.ends_with("Table") || name.ends_with("RecordBatch")) {
        // Arrow tables and record batches cross through the C stream interface.
        let table = if name.ends_with("RecordBatch") {
            py.import("pyarrow")?
                .getattr("Table")?
                .call_method1("from_batches", (vec![df.clone()],))?
        } else {
            df.clone()
        };
        let frame = crate::pandas_bridge::frame_from_arrow(&table)?;
        return Ok(NativeInput::Data(PipeInput::Frame(frame)));
    }
    if name == "numpy.ndarray" {
        // `pd.DataFrame(array)`: the rows of a 2-D array, or the one column of a 1-D array.
        return input_from_py(py, &df.call_method0("tolist")?);
    }
    Ok(NativeInput::Declined(format!(
        "the data is a '{name}'; `Pipe.sync()` takes a list of dictionaries, a dictionary of lists, \
         text of simple lines, a pandas, GeoPandas or Polars DataFrame, a pyarrow Table or \
         RecordBatch, a NumPy array, or an iterator of those chunks"
    )))
}

/// `begin` as `Pipe.sync` received it: `''` is "the sync time minus the backtrack", `None`
/// no lower bound, anything else an explicit bound.
fn sync_begin(ctx: &PyCtx<'_>, begin: Option<&Bound<'_, PyAny>>) -> PyResult<SyncBegin> {
    let Some(begin) = begin.filter(|b| !b.is_none()) else {
        return Ok(SyncBegin::Unbounded);
    };
    Ok(match date_bound(ctx, Some(begin))? {
        Some(b) => SyncBegin::At(b),
        None => SyncBegin::Default,
    })
}

/// The top-level raw parameters whose values differ between two maps (`after`'s values).
fn changed_keys(before: &Map, after: &Map) -> Map {
    after
        .iter()
        .filter(|(k, v)| before.get(*k) != Some(*v))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// The registered parameters of a pipe, `None` when it is not registered or the read fails.
async fn registered_parameters(
    instance: &Arc<dyn InstanceConnector>,
    pipe: &meerschaum_core::Pipe,
) -> Option<Map> {
    match instance.get_pipe_attributes(pipe).await {
        Ok(attrs) => match attrs.get("parameters") {
            Some(Value::Map(m)) => Some(m.clone()),
            _ => None,
        },
        Err(_) => None,
    }
}

/// Plugin keyword arguments as a core map: callables and objects that are not data cannot
/// cross to a worker and are dropped (a worker rebuilds the rest with `filter_keywords`).
pub(crate) fn kwargs_map(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Map> {
    let mut out = Map::new();
    let Some(kwargs) = kwargs else {
        return Ok(out);
    };
    for (key, value) in kwargs.iter() {
        if value.is_callable() && !value.is_instance_of::<PyType>() {
            continue;
        }
        let converted = py_to_core(&value)?;
        if facade::is_forwardable(&converted) {
            out.insert(key.str()?.to_str()?.to_string(), converted);
        }
    }
    Ok(out)
}

/// `{keys: attributes}` of connectors the Python process defined ad hoc.
pub(crate) fn attribute_maps(
    attributes: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<(String, Map)>> {
    let mut out = Vec::new();
    let Some(attributes) = attributes else {
        return Ok(out);
    };
    for (keys, attrs) in attributes.iter() {
        out.push((
            key_name(&keys)?,
            params_map(Some(&attrs))?.unwrap_or_default(),
        ));
    }
    Ok(out)
}

/// `_core.sync_pipe(pipe, connector, df, begin=None, end=None, check_existing=True,
/// enforce_dtypes=True, chunksize=-1, infer=False, params=None, force=False, retries=10,
/// min_seconds=1.0, workers=None, kwargs=None, connectors=None, attributes=None, inplace=True,
/// replace=False, debug=False)`: sync the data into the pipe's table through the engine's
/// ordinary flow (`replace`: the rows replace the window `[begin, end)`, rc15)
/// (module docs). `connector` is the pipe instance's `_core.SqlConnector` (`None` for an
/// instance that is not SQL: `api:`, `valkey:`, custom); `infer` syncs from the pipe's source
/// instead of `df` (v4's `pipe.sync()` without data); `connectors` maps keys to the other
/// `_core.SqlConnector`s of the call (a SQL source) and `attributes` the keys of connectors the
/// Python process defined ad hoc to their attributes. The façade has already registered the
/// pipe (a plugin connector's `register()` is Python) and validated its indices.
#[pyfunction]
#[pyo3(signature = (
    pipe, connector, df=None, begin=None, end=None, check_existing=true, enforce_dtypes=true,
    chunksize=Some(-1), infer=false, params=None, force=false, retries=10, min_seconds=1.0,
    workers=None, kwargs=None, connectors=None, attributes=None, inplace=true, replace=false,
    debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn sync_pipe<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    connector: Option<&Bound<'py, PyAny>>,
    df: Option<&Bound<'py, PyAny>>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    check_existing: bool,
    enforce_dtypes: bool,
    chunksize: Option<i64>,
    infer: bool,
    params: Option<&Bound<'py, PyAny>>,
    force: bool,
    retries: u32,
    min_seconds: f64,
    workers: Option<i64>,
    kwargs: Option<&Bound<'py, PyDict>>,
    connectors: Option<&Bound<'py, PyDict>>,
    attributes: Option<&Bound<'py, PyDict>>,
    inplace: bool,
    replace: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    let _debug = DebugGuard::new(debug);
    let rust_connector =
        |obj: &Bound<'py, PyAny>| -> PyResult<Arc<meerschaum_connector_sql::SqlConnector>> {
            Ok(obj
                .cast::<PySqlConnector>()
                .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?
                .get()
                .inner
                .clone())
        };
    let input = if infer {
        None
    } else {
        let Some(df) = df else {
            return Err(PyValueError::new_err(
                "`df` is required unless `infer=True`.",
            ));
        };
        match input_from_py(py, df)? {
            NativeInput::Data(input) => Some(input),
            NativeInput::Declined(reason) => {
                out.set_item("declined", reason)?;
                return Ok(out);
            }
            NativeInput::Invalid(message) => {
                out.set_item("invalid", message)?;
                return Ok(out);
            }
        }
    };

    let ctx = PyCtx::scalars(py)?;
    let extra = kwargs_map(kwargs)?;
    let args = SyncArgs {
        begin: sync_begin(&ctx, begin)?,
        end: date_bound(&ctx, end)?,
        params: params_map(params)?,
        check_existing,
        enforce_dtypes,
        chunksize,
        force,
        retries,
        min_seconds,
        workers,
        extra,
        replace,
        ..Default::default()
    };
    // The raw parameters: the engine resolves them against the registered ones.
    let core = core_pipe(pipe, ParamsView::Raw)?;
    let instance_keys = core.instance_keys().to_string();
    let raw_before = core.raw_parameters().clone();

    let mut sql: Vec<(String, Arc<meerschaum_connector_sql::SqlConnector>)> = Vec::new();
    let home_sql = match connector.filter(|c| !c.is_none()) {
        Some(c) => {
            let conn = rust_connector(c)?;
            sql.push((instance_keys.clone(), conn.clone()));
            Some(conn)
        }
        None => None,
    };
    if let Some(connectors) = connectors {
        for (keys, conn) in connectors.iter() {
            sql.push((key_name(&keys)?, rust_connector(&conn)?));
        }
    }
    facade::install_instance_provider(&sql);
    let connector_keys = core.connector_keys().to_string();
    let need_python = !facade::is_native(&instance_keys)
        || (infer && connector_keys.contains(':') && !facade::is_native(&connector_keys));
    let built = facade::build_registry(
        py,
        RegistryInput {
            sql,
            attributes: attribute_maps(attributes)?,
            instance_keys: instance_keys.clone(),
            connector_keys,
            inplace,
        },
        need_python,
    );
    let built = match built {
        Ok(built) => built,
        // A worker is not started while a worker imports a plugin (a plugin whose import syncs a
        // `plugin:` pipe would recurse): the sync fails like any failed sync, and the import
        // goes on.
        Err(message) if meerschaum_worker::worker::spawn_refused() => {
            return failed_sync(py, out, &message);
        }
        Err(message) => return Err(pyo3::exceptions::PyRuntimeError::new_err(message)),
    };
    let registry = built.registry;
    let engine_config = built.config;
    let settings = home_sql.as_ref().map(|c| c.settings.clone());

    let (result, patch) = {
        let _t = crate::trace::Stage::new("sync_pipe: engine");
        let core = core.clone();
        facade::spawn_block_on(py, async move {
            let instance: Arc<dyn InstanceConnector> = match registry.instance(&instance_keys) {
                Ok(i) => i,
                Err(e) => {
                    return (
                        meerschaum_core::success::SyncResult::new(false, e),
                        Map::new(),
                    );
                }
            };
            let (defaults, backtrack, limit) = match (&engine_config, &settings) {
                (Some(c), _) => (
                    c.defaults.clone(),
                    c.default_backtrack_minutes,
                    c.filter_params_index_limit,
                ),
                (None, Some(s)) => (
                    s.pipe_defaults.clone(),
                    s.backtrack_minutes,
                    s.filter_params_index_limit,
                ),
                (None, None) => {
                    let d = meerschaum_engine::EngineConfig::default();
                    (
                        d.defaults,
                        d.default_backtrack_minutes,
                        d.filter_params_index_limit,
                    )
                }
            };
            let env = SyncEnv {
                registry: registry.as_ref(),
                defaults: &defaults,
                default_backtrack_minutes: backtrack,
                filter_params_index_limit: limit,
                known: None,
            };
            let registered_before = if core.temporary {
                None
            } else {
                registered_parameters(&instance, &core).await
            };
            let result = engine_sync_pipe(&env, &core, input, &args, None).await;
            let mut patch = Map::new();
            if result.success
                && !core.temporary
                && let Some(after) = registered_parameters(&instance, &core).await
            {
                // What the sync wrote to the registered parameters (`autoincrement`,
                // numeric and special `dtypes`): v4 mutates the Python pipe the same way.
                // Unsaved in-memory edits are not in either side of the comparison.
                patch = match registered_before {
                    Some(before) => changed_keys(&before, &after),
                    None => changed_keys(&raw_before, &after),
                };
            }
            if result.success
                && core.param("compress").is_some_and(|v| v.truthy())
                && let Some(sql) = &home_sql
            {
                // `Pipe.sync` applies the compression policy of a `compress` pipe afterwards.
                // A failed policy is not a failed sync (v4 only prints it under `debug`).
                let _ = sql.apply_compression_policy(&core).await;
            }
            (result, patch)
        })
    };

    out.set_item("success", result.success)?;
    out.set_item("message", &result.message)?;
    out.set_item("inserted", result.inserted)?;
    out.set_item("updated", result.updated)?;
    out.set_item("upserted", result.upserted)?;
    out.set_item("deleted", result.deleted)?;
    out.set_item("duration", result.duration.map(|d| d.as_secs_f64()))?;
    let bound = |b: &Option<meerschaum_core::success::Bound>| -> PyResult<Bound<'py, PyAny>> {
        let converted = b.as_ref().map(|b| match b {
            meerschaum_core::success::Bound::Int(i) => DateBound::Int(*i),
            meerschaum_core::success::Bound::Datetime(dt) => {
                DateBound::Datetime(meerschaum_kernel::value::DateTimeValue::utc(dt.naive_utc()))
            }
        });
        bound_to_py(&ctx, converted.as_ref())
    };
    out.set_item("begin", bound(&result.begin)?)?;
    out.set_item("end", bound(&result.end)?)?;
    let intervals = PyList::empty(py);
    for interval in &result.changed_intervals {
        // A grid cell edge outside Python's datetime range (a row at year 1: its cell starts
        // before year 1) is an open end.
        let edge = |b: &Option<meerschaum_core::success::Bound>| -> Bound<'py, PyAny> {
            bound(b).unwrap_or_else(|_| py.None().into_bound(py))
        };
        intervals.append((edge(&interval.begin), edge(&interval.end)))?;
    }
    out.set_item("changed_intervals", intervals)?;
    out.set_item("parameters", core_to_py(py, &Value::Map(patch))?)?;
    Ok(out)
}

/// The dictionary `sync_pipe` returns for a sync that could not start: a failed result with
/// nothing inserted.
fn failed_sync<'py>(
    py: Python<'py>,
    out: Bound<'py, PyDict>,
    message: &str,
) -> PyResult<Bound<'py, PyDict>> {
    out.set_item("success", false)?;
    out.set_item("message", message)?;
    for key in ["inserted", "updated", "upserted", "deleted"] {
        out.set_item(key, 0)?;
    }
    out.set_item("duration", py.None())?;
    out.set_item("begin", py.None())?;
    out.set_item("end", py.None())?;
    out.set_item("changed_intervals", PyList::empty(py))?;
    out.set_item("parameters", PyDict::new(py))?;
    Ok(out)
}

/// `_core.parse_sql_uri(uri)`: `SQLConnector.parse_uri(uri)` without SQLAlchemy (the Rust
/// URL grammar is SQLAlchemy 2.0's, golden-tested against `fixtures/uris.json`). Keys:
/// `host`, `database`, `username`, `password`, `port` (an `int`), `flavor` and the query
/// parameters. A malformed URI raises `ValueError`.
#[pyfunction]
pub fn parse_sql_uri<'py>(py: Python<'py>, uri: &str) -> PyResult<Bound<'py, PyDict>> {
    let params = meerschaum_connector_sql::uri::parse_uri(uri)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let out = PyDict::new(py);
    for (key, value) in params {
        match value {
            meerschaum_connector_sql::uri::UriValue::Str(s) => out.set_item(key, s)?,
            meerschaum_connector_sql::uri::UriValue::Int(i) => out.set_item(key, i)?,
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uris_parse_like_sqlalchemy() {
        with_py(|py| {
            let parsed = parse_sql_uri(py, "sqlite:////tmp/a.db").unwrap();
            assert_eq!(
                parsed
                    .get_item("database")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "/tmp/a.db"
            );
            assert_eq!(
                parsed
                    .get_item("flavor")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "sqlite"
            );
            let pg =
                parse_sql_uri(py, "postgres://u:p@h:5432/d?options=--search_path%3Dw13h").unwrap();
            assert_eq!(
                pg.get_item("port")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                5432
            );
            assert_eq!(
                pg.get_item("flavor")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "postgresql"
            );
            assert!(parse_sql_uri(py, "not a uri").is_err());
        });
    }

    fn with_py<R>(f: impl FnOnce(Python<'_>) -> R) -> R {
        pyo3::Python::initialize();
        Python::attach(f)
    }

    fn eval<'py>(py: Python<'py>, code: &str) -> Bound<'py, PyAny> {
        py.eval(&std::ffi::CString::new(code).unwrap(), None, None)
            .unwrap()
    }

    fn data(input: NativeInput) -> PipeInput {
        match input {
            NativeInput::Data(input) => input,
            other => panic!("expected data, got {other:?}"),
        }
    }

    #[test]
    fn docs_keep_first_seen_order_and_special_scalars() {
        with_py(|py| {
            let df = eval(
                py,
                "[{'id': 1, 'u': __import__('uuid').UUID(int=7), 'b': b'\\x00\\x01'},\
                  {'id': 2, 'later': __import__('decimal').Decimal('1.50'), 'j': {'a': [1, 2]}}]",
            );
            let PipeInput::Docs(docs) = data(input_from_py(py, &df).unwrap()) else {
                panic!("a list of dictionaries is documents");
            };
            assert_eq!(docs.len(), 2);
            assert_eq!(
                docs[0].keys().cloned().collect::<Vec<_>>(),
                ["id", "u", "b"]
            );
            assert!(matches!(docs[0]["u"], Cell::Uuid(_)));
            assert!(matches!(&docs[0]["b"], Cell::Bytes(b) if b == &[0, 1]));
            assert!(matches!(docs[1]["later"], Cell::Decimal(_)));
            assert!(matches!(docs[1]["j"], Cell::Json(_)));
        });
    }

    #[test]
    fn docs_keep_timezone_awareness() {
        with_py(|py| {
            let df = eval(
                py,
                "[{'dt': __import__('datetime').datetime(2026, 1, 2, 3, 4, 5, 6, \
                  tzinfo=__import__('datetime').timezone.utc)},\
                  {'dt': __import__('datetime').datetime(2026, 1, 2, 3, 4, 5, 6)}]",
            );
            let PipeInput::Docs(docs) = data(input_from_py(py, &df).unwrap()) else {
                panic!("documents");
            };
            let Cell::Datetime(aware) = &docs[0]["dt"] else {
                panic!("a datetime");
            };
            let Cell::Datetime(naive) = &docs[1]["dt"] else {
                panic!("a datetime");
            };
            assert!(aware.tz.is_some());
            assert!(naive.tz.is_none());
            assert_eq!(aware.utc, naive.utc);
        });
    }

    #[test]
    fn empty_input_is_an_empty_frame_not_a_decline() {
        with_py(|py| {
            let docs = data(input_from_py(py, &eval(py, "[]")).unwrap());
            assert!(matches!(docs, PipeInput::Docs(d) if d.is_empty()));
            let columns = data(input_from_py(py, &eval(py, "{}")).unwrap());
            assert!(matches!(columns, PipeInput::Columns(c) if c.is_empty()));
        });
    }

    #[test]
    fn dict_of_lists_converts_by_column() {
        with_py(|py| {
            let df = eval(py, "{'a': [1, 2, None], 'b': ('x', 'y', 'z')}");
            let PipeInput::Columns(columns) = data(input_from_py(py, &df).unwrap()) else {
                panic!("columns");
            };
            assert_eq!(columns["a"].len(), 3);
            assert!(matches!(columns["a"][2], Cell::Null));
            assert!(matches!(&columns["b"][1], Cell::Str(s) if s == "y"));
        });
    }

    #[test]
    fn unequal_columns_are_invalid_like_pandas() {
        with_py(|py| {
            let df = eval(py, "{'a': [1, 2], 'b': [1]}");
            match input_from_py(py, &df).unwrap() {
                NativeInput::Invalid(message) => {
                    assert_eq!(message, "All arrays must be of the same length");
                }
                other => panic!("expected invalid, got {other:?}"),
            }
        });
    }

    #[test]
    fn other_shapes_follow_pandas() {
        with_py(|py| {
            // Shapes `pd.DataFrame(...)` takes: documents, rows, scalars, broadcast columns.
            for code in [
                "({'a': 1},)",
                "[[1, 2], [3, 4]]",
                "[1, 2, 3]",
                "{'a': [1], 'b': 2}",
                "{1: [1]}",
                "[{1: 'a'}]",
                "'id:1,val:2.5\\nid:2,val:3.5'",
            ] {
                assert!(
                    matches!(
                        input_from_py(py, &eval(py, code)).unwrap(),
                        NativeInput::Data(_)
                    ),
                    "{code}"
                );
            }
            // Shapes pandas rejects (the message is v4's; the façade reports it).
            for code in ["[{'a': 1}, 5]", "{'a': 1}"] {
                assert!(
                    matches!(
                        input_from_py(py, &eval(py, code)).unwrap(),
                        NativeInput::Invalid(_)
                    ),
                    "{code}"
                );
            }
            // Objects with no DataFrame meaning are declined with a reason.
            for code in [
                "object()",
                "(x for x in [{'a': 1}])",
                "{'a': [1], 'b': (x for x in [1])}",
            ] {
                match input_from_py(py, &eval(py, code)).unwrap() {
                    NativeInput::Declined(reason) => assert!(!reason.is_empty(), "{code}"),
                    other => panic!("{code}: expected a decline, got {other:?}"),
                }
            }
        });
    }

    #[test]
    fn the_caller_input_is_not_modified() {
        with_py(|py| {
            let df = eval(py, "[{'a': 1, 'b': {'c': [1]}}, {'a': 2}]");
            let before = df.repr().unwrap().to_string();
            let _ = input_from_py(py, &df).unwrap();
            assert_eq!(df.repr().unwrap().to_string(), before);
        });
    }
}
