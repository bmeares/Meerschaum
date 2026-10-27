//! `Pipe` methods bound into `meerschaum._core` (design §4.4, D14): the Python `Pipe` keeps
//! signatures and argument normalization, and each method calls one of these functions
//! (`pipe_get_data`, `pipe_get_backtrack_data`, `pipe_get_sync_time`, `pipe_get_rowcount`,
//! `pipe_infer_dtypes`, `pipe_enforce_dtypes`, `pipe_filter_existing`, `pipe_deduplicate`,
//! `pipe_fetch`, `pipe_get_backtrack_interval`). The algorithms are [`crate::pipe_methods`].
//!
//! **The context.** Every function takes the Python pipe (duck typed, [`core_pipe`]) and a
//! `context` dictionary the façade built (`Pipe._native_context`): `connector` (the pipe
//! instance's `_core.SqlConnector`, `None` for an `api:`, `valkey:` or custom instance, which are
//! built from `attributes` and reached through the Rust clients or the worker, D12),
//! `connectors` (the other Rust SQL connectors of the call, a SQL source), `attributes` (the
//! connectors the Python process defined ad hoc) and `columns_types` (the Python pipe's cached
//! `get_columns_types()`, or `None`). A function that cannot handle a call returns
//! `NotImplemented` and the façade keeps its Python body (listed in `PARITY.md`; none today).
//!
//! **Frames.** Reads come back as pandas (Polars with `as_polars`) frames whose backends follow
//! the instance read of v4 (`read_hints`); the frames of `filter_existing` and
//! `enforce_dtypes` follow the kernel bindings' output rules.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::data::ChunkBounds;
use meerschaum_core::pipe::{ChunkInterval, ChunkIntervalArg, PipeDefaults};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_engine::ConnectorRegistry;
use meerschaum_engine::registry::StaticRegistry;
use meerschaum_engine::sync::SyncBegin;
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyTuple};

use crate::convert::{PyCtx, py_to_core};
use crate::facade::{self, RegistryInput};
use crate::pandas_bridge::{
    ColumnMode, FrameKind, IndexSpec, OutSpec, frame_from_py, frame_to_pandas, frame_to_polars,
    frame_to_py,
};
use crate::pipe_bridge::{ParamsView, bound_to_py, core_pipe, date_bound, params_map, str_list};
use crate::pipe_methods::{
    ChunkHook, ChunkPlan, Data, DedupArgs, FetchMode, Fetched, FilterOpts, GetDataArgs,
    HookOutcome, Reader,
};
use crate::sql_api::PySqlConnector;
use crate::sync_api::{DebugGuard, NativeInput, attribute_maps, input_from_py, kwargs_map};

fn run_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// A `get_config(*keys)` value from the live Python config.
fn live_config(py: Python<'_>, keys: &[&str]) -> Option<Value> {
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

fn live_int(py: Python<'_>, keys: &[&str]) -> Option<i64> {
    live_config(py, keys).and_then(|v| meerschaum_core::pyfmt::py_int(&v))
}

/// The pipe defaults and the two numbers the v4 bodies read from the live config.
fn live_env(py: Python<'_>) -> PyResult<(PipeDefaults, i64, usize)> {
    let mut defaults = crate::core_api::py_defaults(py)?;
    if let Some(minutes) = live_int(py, &["pipes", "parameters", "verify", "chunk_minutes"]) {
        defaults.chunk_minutes = minutes;
    }
    if let Some(Value::Str(col)) = live_config(
        py,
        &["pipes", "autotime", "column_name_if_datetime_missing"],
    ) {
        defaults.autotime_column = col;
    }
    let backtrack = live_int(py, &["pipes", "parameters", "fetch", "backtrack_minutes"])
        .unwrap_or(meerschaum_core::pipe::DEFAULT_BACKTRACK_MINUTES);
    let limit = live_int(py, &["pipes", "sync", "filter_params_index_limit"])
        .unwrap_or(250)
        .max(0) as usize;
    Ok((defaults, backtrack, limit))
}

/// What one call holds besides the Python objects (everything is `Send`).
#[derive(Clone)]
pub struct Opened {
    /// The pipe, with its resolved parameters.
    pub core: Pipe,
    registry: Arc<dyn ConnectorRegistry>,
    sql: Option<Arc<SqlConnector>>,
    sources: HashMap<String, Arc<SqlConnector>>,
    defaults: PipeDefaults,
    backtrack_minutes: i64,
    filter_limit: usize,
    columns_types: Option<IndexMap<String, String>>,
}

impl Opened {
    /// The [`Reader`]; resolves the instance (which may build a Python-worker connector, so
    /// call it inside the runtime).
    pub fn reader(&self) -> Result<Reader, String> {
        let keys = self.core.instance_keys().to_string();
        let instance: Arc<dyn InstanceConnector> = match &self.sql {
            Some(sql) => sql.clone(),
            None => self.registry.instance(&keys)?,
        };
        Ok(Reader {
            instance,
            sql: self.sql.clone(),
            sources: self.sources.clone(),
            registry: self.registry.clone(),
            defaults: self.defaults.clone(),
            backtrack_minutes: self.backtrack_minutes,
            filter_limit: self.filter_limit,
            columns_types: self.columns_types.clone(),
        })
    }
}

fn rust_sql(obj: &Bound<'_, PyAny>) -> PyResult<Arc<SqlConnector>> {
    Ok(obj
        .cast::<PySqlConnector>()
        .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?
        .get()
        .inner
        .clone())
}

fn context_item<'py>(
    context: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(context.get_item(key)?.filter(|v| !v.is_none()))
}

fn columns_types_of(context: &Bound<'_, PyDict>) -> PyResult<Option<IndexMap<String, String>>> {
    let Some(types) = context_item(context, "columns_types")? else {
        return Ok(None);
    };
    let dict = types.cast::<PyDict>()?;
    let mut out = IndexMap::with_capacity(dict.len());
    for (k, v) in dict.iter() {
        out.insert(k.str()?.to_string(), v.str()?.to_string());
    }
    Ok(Some(out))
}

/// Open a call (module docs). `full_registry`: build the whole registry (a call that reads
/// sources, resolves references on other instances or syncs); otherwise a Rust SQL instance is
/// reached directly and nothing is read from the config files.
pub(crate) fn open(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    full_registry: bool,
) -> PyResult<Opened> {
    let core = core_pipe(pipe, ParamsView::Resolved)?;
    let instance_keys = core.instance_keys().to_string();
    let connector_keys = core.connector_keys().to_string();
    let mut sql: Vec<(String, Arc<SqlConnector>)> = Vec::new();
    let home = match context_item(context, "connector")? {
        Some(c) => {
            let conn = rust_sql(&c)?;
            sql.push((instance_keys.clone(), conn.clone()));
            Some(conn)
        }
        None => None,
    };
    let mut sources = HashMap::new();
    if let Some(connectors) = context_item(context, "connectors")? {
        for (keys, conn) in connectors.cast::<PyDict>()?.iter() {
            let keys = keys.str()?.to_string();
            let conn = rust_sql(&conn)?;
            sources.insert(facade::full_keys(&keys), conn.clone());
            sql.push((keys, conn));
        }
    }
    if let Some(home) = &home {
        sources.insert(facade::full_keys(&instance_keys), home.clone());
    }
    facade::install_instance_provider(&sql);
    let (defaults, backtrack_minutes, filter_limit) = live_env(py)?;
    let registry: Arc<dyn ConnectorRegistry> = match (&home, full_registry) {
        (Some(conn), false) => {
            Arc::new(StaticRegistry::new().with_instance(&instance_keys, conn.clone() as Arc<_>))
        }
        _ => {
            let attributes = match context_item(context, "attributes")? {
                Some(a) => attribute_maps(Some(a.cast::<PyDict>()?))?,
                None => Vec::new(),
            };
            let need_python = !facade::is_native(&instance_keys)
                || (connector_keys.contains(':') && !facade::is_native(&connector_keys));
            facade::build_registry(
                py,
                RegistryInput {
                    sql,
                    attributes,
                    instance_keys,
                    connector_keys,
                    inplace: false,
                },
                need_python,
            )
            .map_err(run_err)?
            .registry
        }
    };
    Ok(Opened {
        core,
        registry,
        sql: home,
        sources,
        defaults,
        backtrack_minutes,
        filter_limit,
        columns_types: columns_types_of(context)?,
    })
}

// ---------------------------------------------------------------------------------------------
// Frames and documents back into Python

/// The pandas dtype hints of a read (`read(dtype=...)` in v4: `to_pandas_dtype()` of the pipe's
/// dtypes).
fn read_hints(dtypes: &IndexMap<String, String>) -> IndexMap<String, String> {
    dtypes
        .iter()
        .map(|(c, t)| (c.clone(), meerschaum_kernel::dtypes::to_pandas_dtype(t)))
        .collect()
}

fn out_spec<'py>(hints: IndexMap<String, String>) -> OutSpec<'py> {
    OutSpec {
        index: IndexSpec::Range,
        hints,
        arrow_native: false,
        arrow_columns: Vec::new(),
        geometry: None,
        labels: IndexMap::new(),
        reuse: IndexMap::new(),
        placeholders: Vec::new(),
        polars_objects: Vec::new(),
    }
}

fn frame_out<'py>(
    py: Python<'py>,
    frame: &Frame,
    dtypes: &IndexMap<String, String>,
    as_polars: bool,
) -> PyResult<Bound<'py, PyAny>> {
    frame_out_with(py, frame, dtypes, as_polars, true)
}

/// [`frame_out`]; `hinted` false builds the pandas frame the way v4 built one from JSON documents
/// (nullable dtypes, [`crate::api_client_api::nullable_hints`]): what an instance whose reads are
/// not typed by the pipe's dtypes hands back, see [`hinted_reads`].
fn frame_out_with<'py>(
    py: Python<'py>,
    frame: &Frame,
    dtypes: &IndexMap<String, String>,
    as_polars: bool,
    hinted: bool,
) -> PyResult<Bound<'py, PyAny>> {
    if as_polars {
        return frame_to_polars(py, frame, dtypes);
    }
    let ctx = PyCtx::new(py)?;
    let hints = if hinted {
        read_hints(dtypes)
    } else {
        crate::api_client_api::nullable_hints(frame)
    };
    frame_to_pandas(&ctx, frame, &out_spec(hints))
}

/// Whether the instance's reads carry the pipe's dtypes before `Pipe.enforce_dtypes` (a SQL
/// instance reads with `dtype=`, custom instances are as the worker returned them). An `api:` /
/// `valkey:` instance's v4 frame is built from JSON documents (`parse_df_datetimes`: the nullable
/// `Int64`, `Float64`, `boolean`, `string`), and the enforcement then casts what the pipe's
/// dtypes declare.
fn hinted_reads(core: &Pipe) -> bool {
    !core.instance_keys().starts_with("api:") && !core.instance_keys().starts_with("valkey:")
}

/// Documents as dictionaries (database `NULL`s are `None`).
fn docs_out<'py>(
    py: Python<'py>,
    docs: &[meerschaum_connector::pipe_ops::Doc],
) -> PyResult<Bound<'py, PyList>> {
    docs_out_typed(py, docs, &HashSet::new())
}

fn docs_out_typed<'py>(
    py: Python<'py>,
    docs: &[meerschaum_connector::pipe_ops::Doc],
    integers: &HashSet<String>,
) -> PyResult<Bound<'py, PyList>> {
    let ctx = PyCtx::scalars(py)?;
    let out = PyList::empty(py);
    for doc in docs {
        let d = PyDict::new(py);
        for (key, value) in doc {
            // A database `NULL` is `None` for every dtype (the kernel carries a float `NULL` as
            // `NaN`); only a `Decimal('NaN')` is a value (PostgreSQL `NUMERIC` specials).
            let is_null = match value {
                meerschaum_kernel::Value::Null
                | meerschaum_kernel::Value::NA
                | meerschaum_kernel::Value::NaT => true,
                meerschaum_kernel::Value::Float(f) => f.is_nan(),
                _ => false,
            };
            if is_null {
                d.set_item(key, py.None())?;
            } else {
                d.set_item(
                    key,
                    crate::convert::native_integer_cell_to_py(&ctx, value, integers.contains(key))?,
                )?;
            }
        }
        out.append(d)?;
    }
    Ok(out)
}

/// A read's frame the way v4's `Pipe.get_data()` returned it: the instance's pandas frame
/// (`read_hints`) through `Pipe.enforce_dtypes(df, dtypes=pipe_dtypes)`, whose output rules
/// (Arrow-backed native columns, the kernel binding's hints) decide the pandas backends.
fn enforced_frame<'py>(
    py: Python<'py>,
    frame: &Frame,
    dtypes: &IndexMap<String, String>,
    ctx: Option<&PipeDtypesContext>,
    as_polars: bool,
    hinted: bool,
) -> PyResult<Bound<'py, PyAny>> {
    // A column `select_columns` named that the table does not have comes back as nulls of the
    // `string` dtype (`add_missing_cols_to_df`); it is not one of the pipe's dtypes.
    let mut hint_dtypes = dtypes.clone();
    if frame.height() > 0 {
        for col in frame.columns() {
            if !hint_dtypes.contains_key(&col.name) && col.data.is_null_mask().iter().all(|n| *n) {
                hint_dtypes.insert(col.name.clone(), "string".into());
            }
        }
    }
    let df = frame_out_with(py, frame, &hint_dtypes, as_polars, hinted)?;
    let Some(ctx) = ctx.filter(|c| !c.dtypes.is_empty() && !as_polars) else {
        return Ok(df);
    };
    let explicit = if ctx.enforce {
        ctx.explicit_dtypes.clone()
    } else {
        IndexMap::new()
    };
    crate::kernel_api::enforce_dtypes(
        py,
        df,
        dtypes_dict(py, &ctx.dtypes)?.into_any(),
        Some(dtypes_dict(py, &explicit)?.into_any()),
        true,
        ctx.mixed_numerics,
        ctx.enforce,
        !ctx.tz_aware,
        false,
        false,
    )
}

fn data_out<'py>(
    py: Python<'py>,
    data: Data,
    ctx: Option<&PipeDtypesContext>,
    as_docs: bool,
    as_polars: bool,
    hinted: bool,
) -> PyResult<Bound<'py, PyAny>> {
    match data {
        Data::Missing if as_docs => Ok(PyList::empty(py).into_any()),
        Data::Missing => Ok(py.None().into_bound(py)),
        Data::Docs(docs) => Ok(docs_out(py, &docs)?.into_any()),
        Data::TypedDocs(docs, integers) => Ok(docs_out_typed(py, &docs, &integers)?.into_any()),
        Data::Frame(frame, dtypes) => enforced_frame(py, &frame, &dtypes, ctx, as_polars, hinted),
    }
}

/// [`Reader::get_data`] and, for a frame, the context its enforcement needs.
async fn read_with_context(
    reader: &Reader,
    pipe: &Pipe,
    args: &GetDataArgs,
) -> Result<(Data, Option<PipeDtypesContext>), String> {
    let data = reader.get_data(pipe, args).await?;
    with_context(reader, pipe, data).await
}

async fn with_context(
    reader: &Reader,
    pipe: &Pipe,
    data: Data,
) -> Result<(Data, Option<PipeDtypesContext>), String> {
    let ctx = match &data {
        Data::Frame(_, dtypes) => Some(reader.context(pipe, Some(dtypes.clone()), true).await?),
        _ => None,
    };
    Ok((data, ctx))
}

fn data_len(data: &Data) -> usize {
    match data {
        Data::Missing => 0,
        Data::Docs(d) | Data::TypedDocs(d, _) => d.len(),
        Data::Frame(f, _) => f.height(),
    }
}

// ---------------------------------------------------------------------------------------------
// get_data

#[allow(clippy::too_many_arguments)]
fn get_data_args(
    py: Python<'_>,
    select_columns: Option<&Bound<'_, PyAny>>,
    omit_columns: Option<&Bound<'_, PyAny>>,
    begin: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
    params: Option<&Bound<'_, PyAny>>,
    as_docs: bool,
    add_missing_columns: bool,
    order: Option<String>,
    limit: Option<i64>,
) -> PyResult<GetDataArgs> {
    let ctx = PyCtx::scalars(py)?;
    // `select_columns='*'` is every column; a bare string is one column.
    let select = match select_columns.filter(|s| !s.is_none()) {
        Some(s) if s.str()?.to_str()? == "*" && s.is_instance_of::<pyo3::types::PyString>() => None,
        other => str_list(other)?,
    };
    Ok(GetDataArgs {
        select_columns: select,
        omit_columns: str_list(omit_columns)?,
        begin: date_bound(&ctx, begin)?,
        end: date_bound(&ctx, end)?,
        params: params_map(params)?,
        as_docs,
        add_missing_columns,
        order,
        limit,
        ..Default::default()
    })
}

/// `_core.pipe_get_data(pipe, context, select_columns=None, omit_columns=None, begin=None,
/// end=None, params=None, as_docs=False, as_polars=False, add_missing_columns=False,
/// order='asc', limit=None, debug=False)`: `Pipe.get_data()` (a frame, documents, or `None`
/// for a pipe whose table does not exist). `begin` and `end` are already parsed.
#[pyfunction]
#[pyo3(signature = (
    pipe, context, select_columns=None, omit_columns=None, begin=None, end=None, params=None,
    as_docs=false, as_polars=false, add_missing_columns=false, order=Some("asc".to_string()),
    limit=None, kwargs=None, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_get_data<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    select_columns: Option<&Bound<'py, PyAny>>,
    omit_columns: Option<&Bound<'py, PyAny>>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    params: Option<&Bound<'py, PyAny>>,
    as_docs: bool,
    as_polars: bool,
    add_missing_columns: bool,
    order: Option<String>,
    limit: Option<i64>,
    kwargs: Option<&Bound<'py, PyDict>>,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let args = get_data_args(
        py,
        select_columns,
        omit_columns,
        begin,
        end,
        params,
        as_docs,
        add_missing_columns,
        order,
        limit,
    )?;
    let args = with_kwargs(args, begin, kwargs)?;
    let opened = open(py, pipe, context, false)?;
    let core = opened.core.clone();
    let hinted = hinted_reads(&core);
    let (data, ctx) = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        read_with_context(&reader, &core, &args).await
    })
    .map_err(run_err)?;
    data_out(py, data, ctx.as_ref(), as_docs, as_polars, hinted)
}

/// The chunks of `Pipe.get_data(as_iterator=True)`: one read per `get_chunk_bounds` chunk
/// (or one read when the data span less than the chunk interval), as v4's generator did.
#[pyclass(name = "PipeChunks", module = "meerschaum._core")]
pub struct PipeChunks {
    opened: Opened,
    args: GetDataArgs,
    as_polars: bool,
    chunk_interval: Option<ChunkIntervalArg>,
    state: Mutex<ChunkState>,
}

enum ChunkState {
    Fresh,
    Single,
    Bounds(std::vec::IntoIter<ChunkBounds>),
    Done,
}

#[pymethods]
impl PipeChunks {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        loop {
            let step = {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                match &mut *state {
                    ChunkState::Done => return Ok(None),
                    ChunkState::Fresh => None,
                    ChunkState::Single => {
                        *state = ChunkState::Done;
                        Some((self.args.begin.clone(), self.args.end.clone(), true))
                    }
                    ChunkState::Bounds(bounds) => match bounds.next() {
                        Some((b, e)) => Some((b, e, false)),
                        None => {
                            *state = ChunkState::Done;
                            return Ok(None);
                        }
                    },
                }
            };
            let Some((begin, end, single)) = step else {
                let opened = self.opened.clone();
                let args = self.args.clone();
                let interval = self.chunk_interval;
                let plan = facade::spawn_block_on(py, async move {
                    let reader = opened.reader()?;
                    reader
                        .plan_chunks(
                            &opened.core,
                            args.begin.as_ref(),
                            args.end.as_ref(),
                            args.params.as_ref(),
                            interval,
                        )
                        .await
                })
                .map_err(run_err)?;
                *self.state.lock().unwrap_or_else(|e| e.into_inner()) = match plan {
                    ChunkPlan::Empty => ChunkState::Done,
                    ChunkPlan::Single => ChunkState::Single,
                    ChunkPlan::Bounds(b) => ChunkState::Bounds(b.into_iter()),
                };
                continue;
            };
            let opened = self.opened.clone();
            let args = GetDataArgs {
                begin,
                end,
                ..self.args.clone()
            };
            let as_docs = args.as_docs;
            let hinted = hinted_reads(&self.opened.core);
            let (data, ctx) = facade::spawn_block_on(py, async move {
                let reader = opened.reader()?;
                read_with_context(&reader, &opened.core, &args).await
            })
            .map_err(run_err)?;
            // The single chunk is yielded as it is (`None` included); chunks of a chunked read
            // are yielded when they have rows.
            if !single && data_len(&data) == 0 {
                continue;
            }
            return data_out(py, data, ctx.as_ref(), as_docs, self.as_polars, hinted).map(Some);
        }
    }
}

/// `_core.pipe_get_data_chunks(pipe, context, select_columns=None, omit_columns=None,
/// begin=None, end=None, params=None, as_docs=False, as_polars=False, chunk_interval=None,
/// order='asc', limit=None, debug=False)`: the iterator of `Pipe.get_data(as_iterator=True)`.
#[pyfunction]
#[pyo3(signature = (
    pipe, context, select_columns=None, omit_columns=None, begin=None, end=None, params=None,
    as_docs=false, as_polars=false, chunk_interval=None, order=Some("asc".to_string()),
    limit=None, kwargs=None, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_get_data_chunks(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    select_columns: Option<&Bound<'_, PyAny>>,
    omit_columns: Option<&Bound<'_, PyAny>>,
    begin: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
    params: Option<&Bound<'_, PyAny>>,
    as_docs: bool,
    as_polars: bool,
    chunk_interval: Option<&Bound<'_, PyAny>>,
    order: Option<String>,
    limit: Option<i64>,
    kwargs: Option<&Bound<'_, PyDict>>,
    debug: bool,
) -> PyResult<PipeChunks> {
    let _ = debug;
    let args = get_data_args(
        py,
        select_columns,
        omit_columns,
        begin,
        end,
        params,
        as_docs,
        false,
        order,
        limit,
    )?;
    let args = with_kwargs(args, begin, kwargs)?;
    Ok(PipeChunks {
        opened: open(py, pipe, context, false)?,
        args,
        as_polars,
        chunk_interval: chunk_interval_arg(py, chunk_interval)?,
        state: Mutex::new(ChunkState::Fresh),
    })
}

/// `chunk_interval`: a `timedelta`, an `int`, or `None`.
fn chunk_interval_arg(
    py: Python<'_>,
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<ChunkIntervalArg>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(None);
    };
    let timedelta = py.import("datetime")?.getattr("timedelta")?;
    if obj.is_instance(&timedelta)? {
        return Ok(Some(ChunkIntervalArg::Delta(py_timedelta(obj)?)));
    }
    Ok(Some(ChunkIntervalArg::Int(obj.extract::<i64>()?)))
}

fn py_timedelta(obj: &Bound<'_, PyAny>) -> PyResult<chrono::TimeDelta> {
    let days: i64 = obj.getattr("days")?.extract()?;
    let seconds: i64 = obj.getattr("seconds")?.extract()?;
    let micros: i64 = obj.getattr("microseconds")?.extract()?;
    Ok(chrono::TimeDelta::days(days)
        + chrono::TimeDelta::seconds(seconds)
        + chrono::TimeDelta::microseconds(micros))
}

// ---------------------------------------------------------------------------------------------
// get_backtrack_data, get_sync_time, get_rowcount

/// `_core.pipe_get_backtrack_data(pipe, context, backtrack_minutes=None, begin=None,
/// params=None, limit=None, order='desc', select_columns=None, omit_columns=None, end=None,
/// as_docs=False, as_polars=False, add_missing_columns=False, debug=False)`:
/// `Pipe.get_backtrack_data()` (the other arguments are `get_data()`'s).
#[pyfunction]
#[pyo3(signature = (
    pipe, context, backtrack_minutes=None, begin=None, params=None, limit=None,
    order=Some("desc".to_string()), select_columns=None, omit_columns=None, end=None,
    as_docs=false, as_polars=false, add_missing_columns=false, kwargs=None, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_get_backtrack_data<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    backtrack_minutes: Option<f64>,
    begin: Option<&Bound<'py, PyAny>>,
    params: Option<&Bound<'py, PyAny>>,
    limit: Option<i64>,
    order: Option<String>,
    select_columns: Option<&Bound<'py, PyAny>>,
    omit_columns: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    as_docs: bool,
    as_polars: bool,
    add_missing_columns: bool,
    kwargs: Option<&Bound<'py, PyDict>>,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let args = get_data_args(
        py,
        select_columns,
        omit_columns,
        begin,
        end,
        params,
        as_docs,
        add_missing_columns,
        order,
        limit,
    )?;
    let args = with_kwargs(args, begin, kwargs)?;
    let opened = open(py, pipe, context, false)?;
    let core = opened.core.clone();
    let hinted = hinted_reads(&core);
    let (data, ctx) = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        let data = reader
            .get_backtrack_data(&core, backtrack_minutes, args)
            .await?;
        with_context(&reader, &core, data).await
    })
    .map_err(run_err)?;
    data_out(py, data, ctx.as_ref(), as_docs, as_polars, hinted)
}

/// `_core.pipe_get_sync_time(pipe, context, params=None, newest=True,
/// apply_backtrack_interval=False, remote=False, round_down=False, debug=False)`: the newest
/// (oldest) value of the datetime axis (`remote`: the source's own, through the Rust SQL
/// connector, the `api:` / `valkey:` client or the worker).
#[pyfunction]
#[pyo3(signature = (
    pipe, context, params=None, newest=true, apply_backtrack_interval=false, remote=false,
    round_down=false, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_get_sync_time<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    params: Option<&Bound<'py, PyAny>>,
    newest: bool,
    apply_backtrack_interval: bool,
    remote: bool,
    round_down: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let params = params_map(params)?;
    let opened = open(py, pipe, context, remote)?;
    let core = opened.core.clone();
    let found = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        reader
            .get_sync_time(
                &core,
                params.as_ref(),
                newest,
                remote,
                apply_backtrack_interval,
                round_down,
            )
            .await
    })
    .map_err(run_err)?;
    let ctx = PyCtx::scalars(py)?;
    bound_to_py(&ctx, found.as_ref())
}

/// `_core.pipe_get_rowcount(pipe, context, begin=None, end=None, params=None, remote=False,
/// estimate=False, debug=False)`: the number of rows (`0` for a missing table; `remote`: the
/// source's count of the fetch definition; `estimate`: the database's estimate from catalog
/// statistics, exact where there is none or the table is small). Database errors raise.
#[pyfunction]
#[pyo3(signature = (
    pipe, context, begin=None, end=None, params=None, remote=false, estimate=false, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_get_rowcount<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    params: Option<&Bound<'py, PyAny>>,
    remote: bool,
    estimate: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let ctx = PyCtx::scalars(py)?;
    let (begin, end) = (date_bound(&ctx, begin)?, date_bound(&ctx, end)?);
    let params = params_map(params)?;
    let opened = open(py, pipe, context, remote)?;
    let core = opened.core.clone();
    let count = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        if estimate {
            reader
                .get_rowcount_estimate(&core, begin, end, params, remote)
                .await
        } else {
            reader.get_rowcount(&core, begin, end, params, remote).await
        }
    })
    .map_err(run_err)?;
    Ok(count.into_pyobject(py)?.into_any())
}

// ---------------------------------------------------------------------------------------------
// dtypes

fn dtypes_dict<'py>(
    py: Python<'py>,
    dtypes: &IndexMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    for (c, t) in dtypes {
        out.set_item(c, t)?;
    }
    Ok(out)
}

/// `_core.pipe_infer_dtypes(pipe, context, refresh=False, debug=False)`: `Pipe.infer_dtypes()`
/// (the pandas dtypes of the instance's columns; `{}` for a missing table).
#[pyfunction]
#[pyo3(signature = (pipe, context, refresh=false, debug=false))]
pub fn pipe_infer_dtypes<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    refresh: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let _debug = DebugGuard::new(debug);
    let opened = open(py, pipe, context, false)?;
    let core = opened.core.clone();
    let inferred = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        reader.infer_dtypes(&core, refresh).await
    })
    .map_err(run_err)?;
    dtypes_dict(py, &inferred)
}

/// `_core.pipe_enforce_dtypes(pipe, context, df, enforce=True, safe_copy=True, dtypes=None,
/// as_polars=False, debug=False)`: `Pipe.enforce_dtypes()`. The input is what `Pipe.sync()`
/// takes (a frame, a dictionary, a list, a tuple, text, an Arrow table, a NumPy array); anything
/// else is a `TypeError` naming them.
#[pyfunction]
#[pyo3(signature = (
    pipe, context, df, enforce=true, safe_copy=true, dtypes=None, as_polars=false, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_enforce_dtypes<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    df: &Bound<'py, PyAny>,
    enforce: bool,
    safe_copy: bool,
    dtypes: Option<&Bound<'py, PyAny>>,
    as_polars: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    if df.is_none() {
        return Ok(df.clone());
    }
    let override_dtypes = match dtypes.filter(|d| !d.is_none()) {
        Some(d) => {
            let mut map = IndexMap::new();
            for (k, v) in d.cast::<PyDict>()?.iter() {
                map.insert(k.str()?.to_string(), v.str()?.to_string());
            }
            Some(map).filter(|m: &IndexMap<String, String>| !m.is_empty())
        }
        None => None,
    };
    let opened = open(py, pipe, context, false)?;
    let core = opened.core.clone();
    let is_frame = {
        let name = crate::convert::qualname(df)?;
        name.contains("DataFrame") || name.contains("LazyFrame")
    };
    if is_frame {
        let ctx = {
            let opened = opened.clone();
            let core = core.clone();
            let override_dtypes = override_dtypes.clone();
            facade::spawn_block_on(py, async move {
                let reader = opened.reader()?;
                reader.context(&core, override_dtypes, enforce).await
            })
            .map_err(run_err)?
        };
        if ctx.dtypes.is_empty() {
            // `to_pandas(df)` / `to_polars(df)`: a pandas frame stays the very object.
            let name = crate::convert::qualname(df)?;
            if !as_polars && name.starts_with("pandas.") {
                return Ok(df.clone());
            }
        }
        let dtypes_py = dtypes_dict(py, &ctx.dtypes)?;
        let explicit_py = dtypes_dict(
            py,
            &if ctx.enforce {
                ctx.explicit_dtypes.clone()
            } else {
                IndexMap::new()
            },
        )?;
        return crate::kernel_api::enforce_dtypes(
            py,
            df.clone(),
            dtypes_py.into_any(),
            Some(explicit_py.into_any()),
            safe_copy,
            ctx.mixed_numerics,
            ctx.enforce,
            !ctx.tz_aware,
            as_polars,
            debug,
        );
    }
    let input = match input_from_py(py, df)? {
        NativeInput::Data(input) => input,
        NativeInput::Declined(reason) => {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "Cannot enforce the dtypes of {core}: {reason}."
            )));
        }
        NativeInput::Invalid(message) => {
            crate::core_api::emit_warnings(
                py,
                vec![format!(
                    "Unable to cast incoming data as a DataFrame...:\n{message}"
                )],
                2,
            )?;
            return Ok(py.None().into_bound(py));
        }
    };
    let (frame, ctx) = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        reader
            .enforce_dtypes(&core, input, override_dtypes, enforce)
            .await
    })
    .map_err(run_err)?;
    frame_out(py, &frame, &ctx.dtypes, as_polars)
}

// ---------------------------------------------------------------------------------------------
// filter_existing

/// Object columns with `Decimal('NaN')` cells as nulls.
fn nan_decimals_to_null(mut frame: Frame) -> Frame {
    for name in frame.names() {
        let Some(meerschaum_kernel::ColumnData::Object(values)) =
            frame.column(&name).map(|c| &c.data)
        else {
            continue;
        };
        if !values
            .iter()
            .any(|v| matches!(v, meerschaum_kernel::Value::Decimal(d) if d.is_nan()))
        {
            continue;
        }
        let cleaned = values
            .iter()
            .map(|v| match v {
                meerschaum_kernel::Value::Decimal(d) if d.is_nan() => {
                    meerschaum_kernel::Value::Null
                }
                other => other.clone(),
            })
            .collect();
        let _ = frame.set(&name, meerschaum_kernel::ColumnData::Object(cleaned));
    }
    frame
}

/// `_core.pipe_filter_existing(pipe, context, df, date_bound_only=False,
/// include_unchanged_columns=False, enforce_dtypes=False, debug=False)`: the `(unseen, update,
/// delta)` frames of `Pipe.filter_existing()` (Polars frames in, Polars frames out; the input
/// is what `Pipe.sync()` takes, anything else is a `TypeError`).
#[pyfunction]
#[pyo3(signature = (
    pipe, context, df, date_bound_only=false, include_unchanged_columns=false,
    enforce_dtypes=false, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_filter_existing<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    df: &Bound<'py, PyAny>,
    date_bound_only: bool,
    include_unchanged_columns: bool,
    enforce_dtypes: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let ctx = PyCtx::new(py)?;
    let opened = open(py, pipe, context, false)?;
    let core = opened.core.clone();
    let opts = FilterOpts {
        date_bound_only,
        include_unchanged_columns,
        enforce_dtypes,
    };
    let name = crate::convert::qualname(df)?;
    if name.starts_with("dask") {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "Dask DataFrames are not supported in Meerschaum 5. Convert the data to pandas or \
             Polars (`ddf.compute()`). See the migration guide: reference/migrating-to-v5.",
        ));
    }
    // What the three results are built from: the input's kind and its dtypes.
    // The columns that arrive as text (WKT): v4 casts a declared geometry column of text to a
    // GeoDataFrame geometry, while shapely objects stay `object`.
    let mut text_columns: Vec<String> = Vec::new();
    let (input, kind, input_dtypes) = if df.is_none() {
        (None, FrameKind::Pandas, IndexMap::new())
    } else if name.contains("DataFrame") || name.contains("LazyFrame") {
        let py_frame = frame_from_py(&ctx, df, ColumnMode::Full)?;
        let kind = py_frame.kind.clone();
        let dtypes = py_frame.dtypes.clone();
        for column in py_frame.frame.columns() {
            // An `object` column of text, or pandas' `str` / `string` dtype (a native column).
            if column
                .data
                .first_valid_index()
                .is_some_and(|i| matches!(column.data.get(i), meerschaum_kernel::Value::Str(_)))
            {
                text_columns.push(column.name.clone());
            }
        }
        (Some(PipeInput::Frame(py_frame.frame)), kind, dtypes)
    } else {
        match input_from_py(py, df)? {
            NativeInput::Data(input) => (Some(input), FrameKind::Pandas, IndexMap::new()),
            NativeInput::Declined(reason) => {
                return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                    "Cannot filter the existing rows of {core}: {reason}."
                )));
            }
            NativeInput::Invalid(message) => {
                // `Pipe.enforce_dtypes()` warns and returns `None`, which has no rows.
                crate::core_api::emit_warnings(
                    py,
                    vec![format!(
                        "Unable to cast incoming data as a DataFrame...:\n{message}"
                    )],
                    2,
                )?;
                (None, FrameKind::Pandas, IndexMap::new())
            }
        }
    };
    let (frames, dtypes) = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        match input {
            // `filter_existing(None)`: empty frames with the pipe's dtypes.
            None => {
                let pipe_dtypes = if core.enforce() {
                    reader.get_dtypes(&core, true, false).await?
                } else {
                    IndexMap::new()
                };
                let empty =
                    meerschaum_kernel::diff::add_missing_cols(&Frame::empty(), &pipe_dtypes);
                Ok((
                    (empty.clone(), empty.clone(), empty),
                    reader.get_dtypes(&core, true, false).await?,
                ))
            }
            Some(input) => {
                let dtypes = reader.get_dtypes(&core, true, false).await?;
                let out = reader.filter_existing(&core, input, &opts).await?;
                Ok::<_, String>((out.frames, dtypes))
            }
        }
    })
    .map_err(run_err)?;
    let (unseen, update, delta) = frames;
    // v4 builds `unseen` and `update` from a merge of `delta.fillna(pd.NA)`: a `Decimal('NaN')`
    // (a nullable `int` or a `numeric` null in an object column) comes out as a null.
    let (unseen, update) = (nan_decimals_to_null(unseen), nan_decimals_to_null(update));
    let mut hints = input_dtypes;
    for (col, typ) in &dtypes {
        hints.insert(col.clone(), meerschaum_kernel::dtypes::to_pandas_dtype(typ));
    }
    let mut geometry: Option<String> = None;
    for (col, typ) in &dtypes {
        let norm = typ.to_lowercase();
        if (norm.starts_with("geometry") || norm.starts_with("geography"))
            && text_columns.contains(col)
        {
            hints.insert(col.clone(), "geometry".into());
            geometry.get_or_insert_with(|| col.clone());
        }
    }
    let convert = |frame: &Frame| -> PyResult<Bound<'py, PyAny>> {
        let mut spec = out_spec(hints.clone());
        spec.geometry = geometry.clone().filter(|g| frame.contains(g));
        frame_to_py(&ctx, frame, &kind, &spec)
    };
    Ok(PyTuple::new(py, [convert(&unseen)?, convert(&update)?, convert(&delta)?])?.into_any())
}

// ---------------------------------------------------------------------------------------------
// deduplicate, fetch, backtrack interval

/// `_core.pipe_deduplicate(pipe, context, begin=None, end=None, params=None,
/// chunk_interval=None, bounded=None, use_instance_method=True, workers=None, debug=False)`:
/// `Pipe.deduplicate()` as a `SuccessTuple`.
#[pyfunction]
#[pyo3(signature = (
    pipe, context, begin=None, end=None, params=None, chunk_interval=None, bounded=None,
    use_instance_method=true, workers=None, kwargs=None, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_deduplicate<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    params: Option<&Bound<'py, PyAny>>,
    chunk_interval: Option<&Bound<'py, PyAny>>,
    bounded: Option<bool>,
    use_instance_method: bool,
    workers: Option<i64>,
    kwargs: Option<&Bound<'py, PyDict>>,
    debug: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let _debug = DebugGuard::new(debug);
    let ctx = PyCtx::scalars(py)?;
    let args = DedupArgs {
        begin: date_bound(&ctx, begin)?,
        end: date_bound(&ctx, end)?,
        params: params_map(params)?,
        chunk_interval: chunk_interval_arg(py, chunk_interval)?,
        bounded,
        use_instance_method,
        workers: workers.unwrap_or(1).max(1) as usize,
        extra: kwargs_map(kwargs)?,
    };
    let opened = open(py, pipe, context, true)?;
    let core = opened.core.clone();
    let result = facade::spawn_block_on(py, async move {
        match opened.reader() {
            Ok(reader) => reader.deduplicate(&core, &args).await,
            Err(e) => meerschaum_core::SuccessTuple::fail(e),
        }
    });
    crate::pipe_bridge::success_to_py(py, &result)
}

/// A caller's `chunk_hook` as a [`ChunkHook`]: the hook is given the chunk as a pandas frame and
/// the keywords it takes (`filter_arguments(hook, chunk, **kwargs)`, as `SQLConnector.read`
/// does); an exception is the `(False, traceback)` result v4 records.
fn python_hook(hook: Py<PyAny>, kwargs: Py<PyDict>, debug: bool) -> ChunkHook {
    Arc::new(move |chunk: Frame| -> Result<HookOutcome, String> {
        Python::attach(|py| -> PyResult<HookOutcome> {
            let ctx = PyCtx::new(py)?;
            let df = frame_to_pandas(&ctx, &chunk, &out_spec(IndexMap::new()))?;
            let kw = kwargs.bind(py).copy()?;
            kw.set_item("debug", debug)?;
            let filter = py
                .import("meerschaum.utils.misc")?
                .getattr("filter_arguments")?;
            let hook = hook.bind(py);
            let filtered = filter.call((hook.clone(), df), Some(&kw))?;
            let (call_args, call_kwargs): (Bound<'_, PyTuple>, Bound<'_, PyDict>) =
                filtered.extract()?;
            Ok(match hook.call(call_args, Some(&call_kwargs)) {
                Ok(result) => HookOutcome::Python(result.unbind()),
                Err(e) => {
                    let trace = e
                        .traceback(py)
                        .and_then(|t| t.format().ok())
                        .unwrap_or_default();
                    HookOutcome::Tuple(meerschaum_core::SuccessTuple::fail(format!("{trace}{e}")))
                }
            })
        })
        .map_err(|e| e.to_string())
    })
}

/// `_core.pipe_fetch(pipe, context, begin='', end=None, check_existing=True, params=None,
/// kwargs=None, as_polars=False, sync_chunks=False, chunk_hook=None, debug=False)`:
/// `Pipe.fetch()` (the source's newest data as one frame, a generator's chunks, or `None`).
/// `begin=''` is the sync time minus the backtrack interval. With `sync_chunks` every chunk is
/// synced as it arrives and the results are returned, as with a `chunk_hook`.
#[pyfunction]
#[pyo3(signature = (
    pipe, context, begin=None, end=None, check_existing=true, params=None, kwargs=None,
    as_polars=false, sync_chunks=false, chunk_hook=None, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn pipe_fetch<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    check_existing: bool,
    params: Option<&Bound<'py, PyAny>>,
    kwargs: Option<&Bound<'py, PyDict>>,
    as_polars: bool,
    sync_chunks: bool,
    chunk_hook: Option<&Bound<'py, PyAny>>,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let ctx = PyCtx::scalars(py)?;
    // `begin=''` is "the sync time minus the backtrack"; `None` no lower bound.
    let begin = match begin.filter(|b| !b.is_none()) {
        None => SyncBegin::Unbounded,
        Some(b) => match date_bound(&ctx, Some(b))? {
            Some(bound) => SyncBegin::At(bound),
            None => SyncBegin::Default,
        },
    };
    let end = date_bound(&ctx, end)?;
    let params = params_map(params)?;
    let extra = kwargs_map(kwargs)?;
    let mode = match chunk_hook.filter(|h| !h.is_none()) {
        Some(hook) => FetchMode::Hook(python_hook(
            hook.clone().unbind(),
            kwargs
                .map(|k| k.copy())
                .transpose()?
                .unwrap_or_else(|| PyDict::new(py))
                .unbind(),
            debug,
        )),
        None if sync_chunks => FetchMode::SyncChunks,
        None => FetchMode::Plain,
    };
    let opened = open(py, pipe, context, true)?;
    let core = opened.core.clone();
    let (fetched, dtypes) = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        let dtypes = reader.get_dtypes(&core, true, false).await?;
        let fetched = reader
            .fetch(&core, begin, end, check_existing, params, extra, mode)
            .await?;
        Ok::<_, String>((fetched, dtypes))
    })
    .map_err(run_err)?;
    match fetched {
        Fetched::Nothing => Ok(py.None().into_bound(py)),
        Fetched::True => Ok(PyBool::new(py, true).to_owned().into_any()),
        Fetched::Frame(frame) => frame_out(py, &frame, &dtypes, as_polars),
        Fetched::Chunks(chunks) => {
            let out = PyList::empty(py);
            for chunk in &chunks {
                out.append(frame_out(py, chunk, &dtypes, as_polars)?)?;
            }
            out.call_method0("__iter__")
        }
        Fetched::Hooked(results) => {
            let out = PyList::empty(py);
            for result in results {
                match result {
                    HookOutcome::Tuple(t) => {
                        out.append(crate::pipe_bridge::success_to_py(py, &t)?)?
                    }
                    HookOutcome::Python(object) => out.append(object.bind(py))?,
                }
            }
            Ok(out.into_any())
        }
    }
}

/// `_core.pipe_get_backtrack_interval(pipe, check_existing=True, dtypes=None)`:
/// `Pipe.get_backtrack_interval()` (`dtypes` is `pipe.dtypes`, so the integer-axis check sees
/// the real datetime dtype).
#[pyfunction]
#[pyo3(signature = (pipe, check_existing=true, dtypes=None))]
pub fn pipe_get_backtrack_interval<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    check_existing: bool,
    dtypes: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut core = core_pipe(pipe, ParamsView::Resolved)?;
    if let Some(d) = dtypes.filter(|d| !d.is_none()) {
        let mut patch = Map::new();
        patch.insert("dtypes".into(), py_to_core(d)?);
        core.update_parameters(patch);
    }
    let (defaults, backtrack_minutes, _) = live_env(py)?;
    let interval = core
        .get_backtrack_interval(check_existing, backtrack_minutes, &defaults)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    match interval {
        ChunkInterval::Int(i) => Ok(i.into_pyobject(py)?.into_any()),
        ChunkInterval::Delta(d) => {
            let kw = PyDict::new(py);
            kw.set_item("microseconds", d.num_microseconds().unwrap_or(i64::MAX))?;
            py.import("datetime")?
                .getattr("timedelta")?
                .call((), Some(&kw))
        }
    }
}

/// Register the functions and classes of this module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PipeChunks>()?;
    m.add_function(wrap_pyfunction!(pipe_get_data, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_get_data_chunks, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_get_backtrack_data, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_get_sync_time, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_get_rowcount, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_infer_dtypes, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_enforce_dtypes, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_filter_existing, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_deduplicate, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_fetch, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_get_backtrack_interval, m)?)?;
    Ok(())
}
/// The keywords `Pipe.get_data(**kw)` forwarded: a SQL instance reads `begin=''`,
/// `begin_add_minutes`, `end_add_minutes`, `replace_nulls` and `skip_existing_cols_check` (what
/// `SQLConnector.get_pipe_data` passes to `get_pipe_data_query`; the others it ignores, as v4's
/// `**kw` does); a custom instance's methods receive all of them.
fn with_kwargs(
    mut args: GetDataArgs,
    begin: Option<&Bound<'_, PyAny>>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<GetDataArgs> {
    args.sql_options.begin_default = begin
        .and_then(|b| b.cast::<pyo3::types::PyString>().ok())
        .is_some_and(|s| s.to_str().map(str::is_empty).unwrap_or(false));
    args.extra = kwargs_map(kwargs)?;
    let int = |key: &str| args.extra.get(key).and_then(Value::as_i64);
    if let Some(n) = int("begin_add_minutes") {
        args.sql_options.begin_add_minutes = n;
    }
    if let Some(n) = int("end_add_minutes") {
        args.sql_options.end_add_minutes = n;
    }
    if let Some(Value::Str(s)) = args.extra.get("replace_nulls") {
        args.sql_options.replace_nulls = Some(s.clone());
    }
    if let Some(v) = args
        .extra
        .get("skip_existing_cols_check")
        .and_then(Value::as_bool)
    {
        args.sql_options.skip_existing_cols_check = v;
    }
    Ok(args)
}
