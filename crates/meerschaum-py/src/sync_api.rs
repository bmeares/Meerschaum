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
use meerschaum_engine::StaticRegistry;
use meerschaum_engine::sync::{SyncArgs, SyncBegin, SyncEnv, sync_pipe as engine_sync_pipe};
use meerschaum_kernel::Value as Cell;
use meerschaum_kernel::enforce::PipeInput;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use crate::convert::{PyCtx, core_to_py, py_to_cell, qualname};
use crate::pandas_bridge::{ColumnMode, frame_from_py};
use crate::pipe_bridge::{ParamsView, bound_to_py, core_pipe, date_bound};
use crate::runtime::block_on;
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

/// Whether the object is a `dict` with `str` keys; the keys in order.
fn string_keys(dict: &Bound<'_, PyDict>) -> PyResult<Option<Vec<String>>> {
    let mut keys = Vec::with_capacity(dict.len());
    for key in dict.keys() {
        match key.downcast::<PyString>() {
            Ok(s) => keys.push(s.to_str()?.to_string()),
            Err(_) => return Ok(None),
        }
    }
    Ok(Some(keys))
}

/// A list of documents as `PipeInput::Docs` (`pd.DataFrame(docs)`): the columns appear in
/// order of first appearance, a missing key is a null, every value converts as a DataFrame
/// cell ([`py_to_cell`]).
fn docs_from_list(ctx: &PyCtx<'_>, list: &Bound<'_, PyList>) -> PyResult<NativeInput> {
    let mut docs: Vec<IndexMap<String, Cell>> = Vec::with_capacity(list.len());
    for item in list.iter() {
        let Ok(dict) = item.downcast::<PyDict>() else {
            return Ok(NativeInput::Declined(
                "the list holds items that are not dictionaries".into(),
            ));
        };
        let mut doc = IndexMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let Ok(key) = key.downcast::<PyString>() else {
                return Ok(NativeInput::Declined(
                    "a document has a key that is not a string".into(),
                ));
            };
            doc.insert(key.to_str()?.to_string(), py_to_cell(ctx, &value)?);
        }
        docs.push(doc);
    }
    Ok(NativeInput::Data(PipeInput::Docs(docs)))
}

/// A dictionary of lists as `PipeInput::Columns` (`pd.DataFrame(columns)`).
fn columns_from_dict(ctx: &PyCtx<'_>, dict: &Bound<'_, PyDict>) -> PyResult<NativeInput> {
    if string_keys(dict)?.is_none() {
        return Ok(NativeInput::Declined(
            "a column name is not a string".into(),
        ));
    }
    let mut columns: IndexMap<String, Vec<Cell>> = IndexMap::with_capacity(dict.len());
    let mut length: Option<usize> = None;
    for (key, values) in dict.iter() {
        let name = key.downcast::<PyString>()?.to_str()?.to_string();
        let cells = if let Ok(list) = values.downcast::<PyList>() {
            list.iter()
                .map(|v| py_to_cell(ctx, &v))
                .collect::<PyResult<Vec<_>>>()?
        } else if let Ok(tuple) = values.downcast::<PyTuple>() {
            tuple
                .iter()
                .map(|v| py_to_cell(ctx, &v))
                .collect::<PyResult<Vec<_>>>()?
        } else {
            // Scalars broadcast, Series and arrays carry an index: pandas decides.
            return Ok(NativeInput::Declined(format!(
                "column '{name}' is not a list"
            )));
        };
        match length {
            None => length = Some(cells.len()),
            Some(n) if n != cells.len() => {
                return Ok(NativeInput::Invalid(
                    "All arrays must be of the same length".into(),
                ));
            }
            Some(_) => {}
        }
        columns.insert(name, cells);
    }
    Ok(NativeInput::Data(PipeInput::Columns(columns)))
}

/// Convert the data `Pipe.sync()` was given. Documents and dictionaries of lists never touch
/// pandas or Polars; a DataFrame object needs its own package (which created it).
pub fn input_from_py<'py>(py: Python<'py>, df: &Bound<'py, PyAny>) -> PyResult<NativeInput> {
    if let Ok(list) = df.downcast::<PyList>() {
        let ctx = PyCtx::scalars(py)?;
        return docs_from_list(&ctx, list);
    }
    if let Ok(dict) = df.downcast::<PyDict>() {
        let ctx = PyCtx::scalars(py)?;
        return columns_from_dict(&ctx, dict);
    }
    let name = qualname(df)?;
    if name.starts_with("pandas.") || name.starts_with("geopandas.") {
        let ctx = PyCtx::new(py)?;
        let frame = frame_from_py(&ctx, df, ColumnMode::Full)?.frame;
        return Ok(NativeInput::Data(PipeInput::Frame(frame)));
    }
    if name.starts_with("polars.") && !name.ends_with("LazyFrame") {
        let ctx = PyCtx::new(py)?;
        let frame = frame_from_py(&ctx, df, ColumnMode::Full)?.frame;
        return Ok(NativeInput::Data(PipeInput::Frame(frame)));
    }
    Ok(NativeInput::Declined(format!(
        "the data is a '{name}', not a list of dictionaries, a dictionary of lists or a DataFrame"
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

/// `_core.sync_pipe(pipe, connector, df, begin='', end=None, check_existing=True,
/// enforce_dtypes=True, chunksize=-1)`: sync the data into the pipe's table through the
/// engine's ordinary flow (module docs). `connector` is the pipe instance's
/// `_core.SqlConnector`; the façade has already registered the pipe (a plugin connector's
/// `register()` is Python) and validated its indices.
#[pyfunction]
#[pyo3(signature = (
    pipe, connector, df, begin=None, end=None, check_existing=true, enforce_dtypes=true,
    chunksize=Some(-1)
))]
#[allow(clippy::too_many_arguments)]
pub fn sync_pipe<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    connector: &Bound<'py, PyAny>,
    df: &Bound<'py, PyAny>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    check_existing: bool,
    enforce_dtypes: bool,
    chunksize: Option<i64>,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    let conn = connector
        .downcast::<PySqlConnector>()
        .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?
        .get()
        .inner
        .clone();
    let input = match input_from_py(py, df)? {
        NativeInput::Data(input) => input,
        NativeInput::Declined(reason) => {
            out.set_item("declined", reason)?;
            return Ok(out);
        }
        NativeInput::Invalid(message) => {
            out.set_item("invalid", message)?;
            return Ok(out);
        }
    };

    let ctx = PyCtx::scalars(py)?;
    let args = SyncArgs {
        begin: sync_begin(&ctx, begin)?,
        end: date_bound(&ctx, end)?,
        check_existing,
        enforce_dtypes,
        chunksize,
        ..Default::default()
    };
    // The raw parameters: the engine resolves them against the registered ones.
    let core = core_pipe(pipe, ParamsView::Raw)?;
    let instance_keys = core.instance_keys().to_string();
    let raw_before = core.raw_parameters().clone();
    let settings = conn.settings.clone();
    let registry = StaticRegistry::new().with_instance(&instance_keys, conn.clone());
    let instance: Arc<dyn InstanceConnector> = conn.clone();

    let (result, patch) = {
        let _t = crate::trace::Stage::new("sync_pipe: engine");
        let core = core.clone();
        block_on(py, async move {
            let env = SyncEnv {
                registry: &registry,
                defaults: &settings.pipe_defaults,
                default_backtrack_minutes: settings.backtrack_minutes,
                filter_params_index_limit: settings.filter_params_index_limit,
                known: None,
            };
            let registered_before = if core.temporary {
                None
            } else {
                registered_parameters(&instance, &core).await
            };
            let result = engine_sync_pipe(&env, &core, Some(input), &args, None).await;
            let mut patch = Map::new();
            if result.success && !core.temporary {
                if let Some(after) = registered_parameters(&instance, &core).await {
                    // What the sync wrote to the registered parameters (`autoincrement`,
                    // numeric and special `dtypes`): v4 mutates the Python pipe the same way.
                    // Unsaved in-memory edits are not in either side of the comparison.
                    patch = match registered_before {
                        Some(before) => changed_keys(&before, &after),
                        None => changed_keys(&raw_before, &after),
                    };
                }
            }
            if result.success && core.param("compress").is_some_and(|v| v.truthy()) {
                // `Pipe.sync` applies the compression policy of a `compress` pipe afterwards.
                let res = conn.apply_compression_policy(&core).await;
                // A failed policy is not a failed sync (v4 only prints it under `debug`).
                let _ = res;
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
    out.set_item("parameters", core_to_py(py, &Value::Map(patch))?)?;
    Ok(out)
}

/// `_core.parse_sql_uri(uri)`: `SQLConnector.parse_uri(uri)` without SQLAlchemy (the Rust
/// URL grammar is SQLAlchemy 2.0's, golden-tested against `fixtures/uris.json`). Keys:
/// `host`, `database`, `username`, `password`, `port` (an `int`), `flavor` and the query
/// parameters. A malformed URI raises `ValueError`.
#[pyfunction]
pub fn parse_sql_uri<'py>(py: Python<'py>, uri: &str) -> PyResult<Bound<'py, PyDict>> {
    let params =
        meerschaum_sql::uri::parse_uri(uri).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let out = PyDict::new(py);
    for (key, value) in params {
        match value {
            meerschaum_sql::uri::UriValue::Str(s) => out.set_item(key, s)?,
            meerschaum_sql::uri::UriValue::Int(i) => out.set_item(key, i)?,
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
        pyo3::prepare_freethreaded_python();
        Python::with_gil(f)
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
    fn other_shapes_are_declined_with_a_reason() {
        with_py(|py| {
            for code in [
                "'a,b\\n1,2'",
                "(x for x in [{'a': 1}])",
                "({'a': 1},)",
                "[[1, 2], [3, 4]]",
                "[{'a': 1}, 5]",
                "{'a': 1}",
                "{'a': [1], 'b': 2}",
                "{1: [1]}",
                "[{1: 'a'}]",
                "None",
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
