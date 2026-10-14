//! D14 runway bindings: the remaining Python-core paths routed to Rust (design D12, D14, §4.4).
//!
//! - `sql_item_name` / `build_where`: the plugin-visible helpers of `meerschaum.utils.sql`.
//! - `d14_get_pipe_metadef` / `d14_sync_pipe_inplace`: the SQL source-side metadefinition builder
//!   and the same-instance sync, taking the Rust connector (`_core.SqlConnector`) as the first
//!   argument. The façade passes the connector it already built (`SQLConnector._rust_connector`).
//!
//! Everything that needs Python-side state (`pipe.parent`, `pipe.get_sync_time()`) receives it as
//! plain arguments, evaluated by the façade, so these functions never call back into the pipe.

use chrono::{FixedOffset, NaiveDate};
use indexmap::IndexMap;
use meerschaum_core::pipe::{ChunkInterval, ChunkIntervalArg};
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::{Map, Value};
use meerschaum_sql::Flavor;
use meerschaum_sql::instance::fetch::FetchArgs;
use meerschaum_sql::queries::metadef::{Bound as MBound, ParentSpec};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyString, PyTuple};

use crate::convert::{PyCtx, core_to_py, py_to_core};
use crate::pipe_bridge::{ParamsView, bound_to_py, core_pipe, date_bound, success_to_py};
use crate::runtime::block_on;
use crate::sql_api::PySqlConnector;

/// A flavor key that may not be a real flavor (Python falls back to the `default` wrappers).
fn flavor_opt(s: &str) -> Option<Flavor> {
    s.parse().ok()
}

/// `sql_item_name(item, flavor, schema=None)`.
#[pyfunction]
#[pyo3(signature = (item, flavor, schema=None))]
pub fn sql_item_name(
    item: &Bound<'_, PyAny>,
    flavor: &str,
    schema: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let item = item.str()?.to_string();
    let schema = match schema.filter(|s| !s.is_none()) {
        Some(s) => Some(s.str()?.to_string()),
        None => None,
    };
    Ok(meerschaum_sql::ident::sql_item_name_opt(
        &item,
        flavor_opt(flavor),
        schema.as_deref(),
    ))
}

/// `build_where(params, flavor=..., with_where=True)`. The façade resolves the connector's
/// flavor, so the `connector` argument is not bound.
#[pyfunction]
#[pyo3(signature = (params, flavor="postgresql", with_where=true))]
pub fn build_where(
    py: Python<'_>,
    params: &Bound<'_, PyAny>,
    flavor: &str,
    with_where: bool,
) -> PyResult<String> {
    let Value::Map(map) = py_to_core(params)? else {
        return Err(PyValueError::new_err("`params` must be a dictionary."));
    };
    let map: IndexMap<String, Value> = map.into_iter().collect();
    // The injection guard warns through Python's `warn()` (tracing has no subscriber here).
    let dumped = Value::Map(map.clone().into_iter().collect())
        .json_dumps()
        .to_lowercase();
    if ["drop ", "--", ";"].iter().any(|w| dumped.contains(w)) {
        let warn = py.import("meerschaum.utils.warnings")?.getattr("warn")?;
        warn.call1(("Aborting build_where() due to possible SQL injection.",))?;
        return Ok(String::new());
    }
    // PARITY: an unknown flavor key falls back to the `default` quoting like Python does.
    let flavor = flavor_opt(flavor).unwrap_or(Flavor::Postgresql);
    Ok(meerschaum_sql::where_clause::build_where(
        &map, flavor, with_where,
    ))
}

fn py_datetime(obj: &Bound<'_, PyAny>) -> PyResult<PyDateTime> {
    let py = obj.py();
    let get = |name: &str| -> PyResult<u32> { obj.getattr(name)?.extract() };
    let year: i32 = obj.getattr(intern!(py, "year"))?.extract()?;
    let naive = NaiveDate::from_ymd_opt(year, get("month")?, get("day")?)
        .and_then(|d| {
            d.and_hms_micro_opt(
                get("hour").ok()?,
                get("minute").ok()?,
                get("second").ok()?,
                get("microsecond").ok()?,
            )
        })
        .ok_or_else(|| PyValueError::new_err("Invalid datetime."))?;
    let offset = obj.call_method0(intern!(py, "utcoffset"))?;
    if offset.is_none() {
        return Ok(PyDateTime::Naive(naive));
    }
    let secs: f64 = offset.call_method0("total_seconds")?.extract()?;
    let fixed = FixedOffset::east_opt(secs as i32)
        .ok_or_else(|| PyValueError::new_err("Invalid UTC offset."))?;
    naive
        .and_local_timezone(fixed)
        .single()
        .map(PyDateTime::Aware)
        .ok_or_else(|| PyValueError::new_err("Invalid datetime."))
}

/// A `begin` / `end` argument as the metadefinition bound: `''` is the default (sync time minus
/// backtrack), `None` unbounded.
fn metadef_bound(obj: Option<&Bound<'_, PyAny>>) -> PyResult<MBound> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(MBound::None);
    };
    if let Ok(s) = obj.downcast::<PyString>() {
        let s = s.to_str()?;
        return Ok(if s.is_empty() {
            MBound::Default
        } else {
            MBound::Str(s.to_string())
        });
    }
    let py = obj.py();
    let datetime_cls = py.import("datetime")?.getattr("datetime")?;
    if obj.is_instance(&datetime_cls)? {
        return Ok(MBound::DateTime(py_datetime(obj)?));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(MBound::Int(i));
    }
    Err(PyValueError::new_err(format!(
        "Unsupported datetime bound '{}'.",
        obj.str()?
    )))
}

fn parent_spec(parent: Option<&Bound<'_, PyDict>>) -> PyResult<Option<ParentSpec>> {
    let Some(d) = parent else {
        return Ok(None);
    };
    let text = |k: &str| -> PyResult<Option<String>> {
        match d.get_item(k)? {
            Some(v) if !v.is_none() => Ok(Some(v.str()?.to_string())),
            _ => Ok(None),
        }
    };
    Ok(Some(ParentSpec {
        target: text("target")?.unwrap_or_default(),
        schema: text("schema")?,
        dt_col: text("dt_col")?,
        dt_typ: text("dt_typ")?,
        precision_unit: text("precision_unit")?.unwrap_or_else(|| "second".to_string()),
    }))
}

fn params_of(params: Option<&Bound<'_, PyAny>>) -> PyResult<Option<IndexMap<String, Value>>> {
    match params.filter(|p| !p.is_none()) {
        None => Ok(None),
        Some(p) => match py_to_core(p)? {
            Value::Map(m) => Ok(Some(m.into_iter().collect())),
            _ => Ok(None),
        },
    }
}

fn rust_conn(conn: &Bound<'_, PyAny>) -> PyResult<std::sync::Arc<meerschaum_sql::SqlConnector>> {
    let c = conn
        .downcast::<PySqlConnector>()
        .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?;
    Ok(c.get().inner.clone())
}

/// `get_pipe_metadef(pipe, params, begin, end, check_existing)` on the Rust connector `conn`.
///
/// `sync_time` is `pipe.get_sync_time()` (used when `begin == ''`), `parent` the `pipe.parent`
/// attributes (`target`, `schema`, `dt_col`, `dt_typ`, `precision_unit`) or `None`.
/// Returns `None` when the pipe has no SQL definition.
#[pyfunction]
#[pyo3(name = "get_pipe_metadef", signature = (conn, pipe, params=None, begin=None, end=None, check_existing=true, sync_time=None, parent=None))]
#[allow(clippy::too_many_arguments)]
pub fn d14_get_pipe_metadef(
    conn: &Bound<'_, PyAny>,
    pipe: &Bound<'_, PyAny>,
    params: Option<&Bound<'_, PyAny>>,
    begin: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
    check_existing: bool,
    sync_time: Option<&Bound<'_, PyAny>>,
    parent: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<String>> {
    let c = rust_conn(conn)?;
    let p = core_pipe(pipe, ParamsView::Resolved)?;
    let ctx = PyCtx::new(pipe.py())?;
    let args = FetchArgs {
        begin: metadef_bound(begin)?,
        end: metadef_bound(end)?,
        check_existing,
        params: params_of(params)?,
        sync_time: date_bound(&ctx, sync_time)?,
        parent: parent_spec(parent)?,
    };
    c.get_pipe_metadef(&p, &args)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

/// `sync_pipe_inplace(pipe, params, begin, end, check_existing)`.
#[pyfunction]
#[pyo3(name = "sync_pipe_inplace", signature = (conn, pipe, params=None, begin=None, end=None, check_existing=true))]
pub fn d14_sync_pipe_inplace<'py>(
    py: Python<'py>,
    conn: &Bound<'py, PyAny>,
    pipe: &Bound<'py, PyAny>,
    params: Option<&Bound<'py, PyAny>>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    check_existing: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let c = rust_conn(conn)?;
    let p = core_pipe(pipe, ParamsView::Resolved)?;
    let params: Option<Map> = params_of(params)?.map(|m| m.into_iter().collect());
    let begin = metadef_bound(begin)?;
    let end = metadef_bound(end)?;
    let res = block_on(py, async move {
        c.sync_pipe_inplace(&p, params.as_ref(), begin, end, check_existing)
            .await
    });
    success_to_py(py, &res)
}

/// One `{{ ... }}` template string found in the raw parameters, with its path.
fn collect_templates(
    value: &Value,
    path: &mut Vec<PathStep>,
    out: &mut Vec<(Vec<PathStep>, String)>,
) {
    match value {
        Value::Map(m) => {
            for (k, v) in m {
                path.push(PathStep::Key(k.clone()));
                collect_templates(v, path, out);
                path.pop();
            }
        }
        Value::List(items) => {
            for (i, v) in items.iter().enumerate() {
                path.push(PathStep::Index(i));
                collect_templates(v, path, out);
                path.pop();
            }
        }
        Value::Str(s) if s.contains("{{") => out.push((path.clone(), s.clone())),
        _ => {}
    }
}

#[derive(Clone)]
enum PathStep {
    Key(String),
    Index(usize),
}

fn value_at<'a>(root: &'a Map, path: &[PathStep]) -> Option<&'a Value> {
    let mut current: Option<&Value> = None;
    for (i, step) in path.iter().enumerate() {
        let next = match (i, step, current) {
            (0, PathStep::Key(k), _) => root.get(k),
            (_, PathStep::Key(k), Some(Value::Map(m))) => m.get(k),
            (_, PathStep::Index(n), Some(Value::List(l))) => l.get(*n),
            _ => None,
        };
        current = Some(next?);
    }
    current
}

/// The façade's template resolver with the pipe's own raw parameters read from its `_core.Pipe`
/// (the live map), not from the Python `attributes` cache, which can lag behind a direct
/// mutation of the core pipe.
struct SelfLiveResolver<'py> {
    inner: crate::core_api::PyResolver<'py>,
    me: meerschaum_core::Pipe,
}

impl meerschaum_core::template::TemplateResolver for SelfLiveResolver<'_> {
    fn defaults(&self) -> meerschaum_core::pipe::PipeDefaults {
        self.inner.defaults()
    }

    fn table_reference(&self, pipe: &meerschaum_core::Pipe) -> Result<String, String> {
        self.inner.table_reference(pipe)
    }

    fn attribute(&self, pipe: &meerschaum_core::Pipe, name: &str) -> Result<Value, String> {
        self.inner.attribute(pipe, name)
    }

    fn raw_parameters(&self, pipe: &meerschaum_core::Pipe) -> Map {
        if pipe == &self.me {
            self.me.raw_parameters().clone()
        } else {
            self.inner.raw_parameters(pipe)
        }
    }
}

/// `Pipe.get_parameters(apply_symlinks=True)`: the resolved parameters of a Python pipe
/// (reference pipes merged, `{{ Pipe() }}` substituted, by `meerschaum_core`) and the
/// `_symlinks` map v4 records on the pipe: `{path_tuple: {'original', 'substituted'}}` for every
/// string the `{{ }}` templating changed. `MRSM{}` symlinks are substituted by the caller.
#[pyfunction]
pub fn get_parameters<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyDict>)> {
    let mut me = core_pipe(pipe, ParamsView::Keys)?;
    // The live raw map, read from the pipe's `_core.Pipe` (the Python `attributes` view is a
    // `dict` subclass whose storage can lag behind the core).
    let raw = match pipe.getattr("_core_pipe") {
        Ok(core) => match py_to_core(&core.getattr("raw_parameters")?)? {
            Value::Map(m) => m,
            _ => Map::new(),
        },
        Err(_) => core_pipe(pipe, ParamsView::Raw)?.raw_parameters().clone(),
    };
    me.set_parameters(raw.clone());
    let resolver = SelfLiveResolver {
        inner: crate::core_api::PyResolver {
            py,
            defaults: crate::core_api::py_defaults(py)?,
            self_pipe: Some((me.clone(), pipe.clone())),
            built: Default::default(),
        },
        me: me.clone(),
    };
    let warnings = me
        .resolve_parameters_with(&resolver, None)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    crate::core_api::emit_warnings(py, warnings, 2)?;
    let resolved_map = me.parameters().clone();
    let resolved = core_to_py(py, &Value::Map(resolved_map.clone()))?;
    let symlinks = PyDict::new(py);
    let mut templates = Vec::new();
    for (k, v) in &raw {
        collect_templates(v, &mut vec![PathStep::Key(k.clone())], &mut templates);
    }
    for (path, original) in templates {
        // PARITY: v4 records `replace_pipes_syntax(original)` (before `MRSM{}` substitution).
        // The resolved value is that unless the string also holds a config symlink; a template
        // may substitute a non-string (an attribute chain such as `.columns`).
        let substituted: Bound<'py, PyAny> = match value_at(&resolved_map, &path) {
            Some(v) if !original.contains("MRSM{") => core_to_py(py, v)?,
            _ => crate::core_api::replace_pipes_syntax(py, &original, Some(pipe.clone()))?,
        };
        if substituted.eq(PyString::new(py, &original))? {
            continue;
        }
        let key = PyTuple::new(
            py,
            path.iter()
                .map(|s| match s {
                    PathStep::Key(k) => Ok(PyString::new(py, k).into_any()),
                    PathStep::Index(i) => Ok(i.into_pyobject(py)?.into_any()),
                })
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        let entry = PyDict::new(py);
        entry.set_item("original", &original)?;
        entry.set_item("substituted", substituted)?;
        symlinks.set_item(key, entry)?;
    }
    Ok((resolved, symlinks))
}

/// A Python `timedelta` as a chrono duration.
fn py_timedelta(obj: &Bound<'_, PyAny>) -> PyResult<chrono::TimeDelta> {
    let days: i64 = obj.getattr("days")?.extract()?;
    let seconds: i64 = obj.getattr("seconds")?.extract()?;
    let micros: i64 = obj.getattr("microseconds")?.extract()?;
    Ok(chrono::TimeDelta::days(days)
        + chrono::TimeDelta::seconds(seconds)
        + chrono::TimeDelta::microseconds(micros))
}

fn chunk_interval_to_py<'py>(
    py: Python<'py>,
    interval: ChunkInterval,
) -> PyResult<Bound<'py, PyAny>> {
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

/// `Pipe.get_chunk_interval(chunk_interval)` (`dtypes` is `pipe.dtypes`, which includes the
/// instance's inferred dtypes, so the integer-axis check sees the real datetime dtype).
#[pyfunction]
#[pyo3(signature = (pipe, chunk_interval=None, dtypes=None))]
pub fn get_chunk_interval<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    chunk_interval: Option<&Bound<'py, PyAny>>,
    dtypes: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut p = core_pipe(pipe, ParamsView::Resolved)?;
    if let Some(d) = dtypes.filter(|d| !d.is_none()) {
        let mut patch = Map::new();
        patch.insert("dtypes".into(), py_to_core(d)?);
        p.update_parameters(patch);
    }
    let arg = match chunk_interval.filter(|c| !c.is_none()) {
        None => None,
        Some(c) => {
            let timedelta = py.import("datetime")?.getattr("timedelta")?;
            Some(if c.is_instance(&timedelta)? {
                ChunkIntervalArg::Delta(py_timedelta(c)?)
            } else {
                ChunkIntervalArg::Int(c.extract::<i64>()?)
            })
        }
    };
    let defaults = crate::core_api::py_defaults(py)?;
    let interval = p
        .get_chunk_interval(arg, &defaults)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    chunk_interval_to_py(py, interval)
}

/// `Pipe.get_chunk_bounds(...)` over bounds the façade already resolved: `begin` / `end` are
/// parsed (`parse_date_bounds`), `oldest` / `newest` the sync times standing in for
/// `get_sync_time(newest=False/True)`, `chunk_interval` the resolved interval.
#[pyfunction]
#[pyo3(signature = (begin, end, bounded, chunk_interval, align, oldest=None, newest=None))]
#[allow(clippy::too_many_arguments)]
pub fn get_chunk_bounds<'py>(
    py: Python<'py>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    bounded: bool,
    chunk_interval: &Bound<'py, PyAny>,
    align: bool,
    oldest: Option<&Bound<'py, PyAny>>,
    newest: Option<&Bound<'py, PyAny>>,
) -> PyResult<Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)>> {
    let ctx = PyCtx::new(py)?;
    let timedelta = py.import("datetime")?.getattr("timedelta")?;
    let interval = if chunk_interval.is_instance(&timedelta)? {
        ChunkInterval::Delta(py_timedelta(chunk_interval)?)
    } else {
        ChunkInterval::Int(chunk_interval.extract::<i64>()?)
    };
    let bounds = meerschaum_sql::instance::data::get_chunk_bounds(
        date_bound(&ctx, begin)?,
        date_bound(&ctx, end)?,
        bounded,
        interval,
        align,
        date_bound(&ctx, oldest)?,
        date_bound(&ctx, newest)?,
    )
    .map_err(PyValueError::new_err)?;
    bounds
        .into_iter()
        .map(|(b, e)| {
            Ok((
                bound_to_py(&ctx, b.as_ref())?,
                bound_to_py(&ctx, e.as_ref())?,
            ))
        })
        .collect()
}
