//! Python objects at the instance boundary: duck-typed `Pipe`, `User` and `Plugin` objects,
//! datetime-axis bounds and `params` filters (design §4.4: the façade never `isinstance`-checks
//! user objects, so every attribute is read with `getattr`).

use meerschaum_connector::{DataQuery, DateBound, PluginRecord};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use crate::convert::{PyCtx, core_to_py, datetime_to_py, py_to_cell, py_to_core};

/// Which view of a Python pipe's parameters the instance method reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParamsView {
    /// `pipe.parameters` (references, `{{ Pipe() }}` and `MRSM{}` resolved): data methods.
    Resolved,
    /// `pipe.get_parameters(apply_symlinks=False)`: `register_pipe`.
    Unresolved,
    /// `pipe._attributes['parameters']` (the in-memory raw dict): `edit_pipe`.
    Raw,
    /// Keys only (`get_pipe_id`, `get_pipe_attributes`): never read the parameters, which
    /// would recurse into the instance.
    Keys,
}

fn opt_str(obj: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<String>> {
    match obj.getattr(name) {
        Ok(v) if v.is_none() => Ok(None),
        Ok(v) => Ok(Some(v.str()?.to_str()?.to_string())),
        Err(_) => Ok(None),
    }
}

fn truthy_attr(obj: &Bound<'_, PyAny>, name: &str) -> bool {
    obj.getattr(name)
        .and_then(|v| v.is_truthy())
        .unwrap_or(false)
}

fn map_of(obj: &Bound<'_, PyAny>) -> PyResult<Map> {
    if obj.is_none() {
        return Ok(Map::new());
    }
    Ok(match py_to_core(obj)? {
        Value::Map(m) => m,
        _ => Map::new(),
    })
}

/// A core [`Pipe`] for a Python pipe (or any object with the same attributes).
pub fn core_pipe(obj: &Bound<'_, PyAny>, view: ParamsView) -> PyResult<Pipe> {
    let py = obj.py();
    let ck = opt_str(obj, "connector_keys")?
        .ok_or_else(|| PyValueError::new_err("The pipe has no connector keys."))?;
    let mk = opt_str(obj, "metric_key")?
        .ok_or_else(|| PyValueError::new_err("The pipe has no metric key."))?;
    let lk = opt_str(obj, "location_key")?;
    let ik = opt_str(obj, "instance_keys")?;
    let temporary = truthy_attr(obj, "temporary");
    let parameters = match view {
        ParamsView::Keys => Map::new(),
        ParamsView::Resolved => map_of(&obj.getattr(intern!(py, "parameters"))?)?,
        ParamsView::Unresolved => {
            let kw = PyDict::new(py);
            kw.set_item("apply_symlinks", false)?;
            match obj
                .getattr("get_parameters")
                .and_then(|f| f.call((), Some(&kw)))
            {
                Ok(p) => map_of(&p)?,
                Err(_) => map_of(&obj.getattr(intern!(py, "parameters"))?)?,
            }
        }
        ParamsView::Raw => match obj
            .getattr("_attributes")
            .and_then(|a| a.get_item("parameters"))
        {
            Ok(p) => map_of(&p)?,
            Err(_) => map_of(&obj.getattr(intern!(py, "parameters"))?)?,
        },
    };
    let mut builder = Pipe::builder(&ck, &mk)
        .location(lk.as_deref())
        .parameters(parameters)
        .kwarg("temporary", temporary);
    if let Some(ik) = ik {
        builder = builder.instance(&ik);
    }
    builder
        .build()
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// The pipe's columns types from `pipe.get_columns_types()`, which v4's SQL methods consult
/// too (cached on the Python pipe for `pipes:dtypes:columns_types_cache_seconds`; a miss asks
/// the instance). `None` when the object has no such method or the call fails (duck typing).
pub fn cached_columns_types(pipe: &Bound<'_, PyAny>) -> Option<indexmap::IndexMap<String, String>> {
    let v = pipe.call_method0("get_columns_types").ok()?;
    let d = v.downcast::<PyDict>().ok()?;
    let mut out = indexmap::IndexMap::with_capacity(d.len());
    for (k, v) in d.iter() {
        out.insert(k.str().ok()?.to_string(), v.str().ok()?.to_string());
    }
    Some(out)
}

/// A datetime-axis bound from Python (`None`, `''`, an `int`, a `datetime`, an ISO string).
pub fn date_bound(ctx: &PyCtx<'_>, obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<DateBound>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(None);
    };
    if let Ok(s) = obj.downcast::<PyString>() {
        let s = s.to_str()?;
        if s.trim().is_empty() {
            return Ok(None);
        }
        return DateBound::parse(s)
            .map(Some)
            .ok_or_else(|| PyValueError::new_err(format!("Invalid datetime bound '{s}'.")));
    }
    let cell = py_to_cell(ctx, obj)?;
    match DateBound::from_value(&cell) {
        Some(b) => Ok(Some(b)),
        None if cell.is_na() => Ok(None),
        None => match &cell {
            meerschaum_kernel::Value::Float(f) if f.fract() == 0.0 => {
                Ok(Some(DateBound::Int(*f as i64)))
            }
            meerschaum_kernel::Value::Date(d) => Ok(Some(DateBound::Datetime(
                meerschaum_kernel::value::DateTimeValue {
                    utc: d.and_hms_opt(0, 0, 0).unwrap_or_default(),
                    tz: None,
                },
            ))),
            _ => Err(PyValueError::new_err(format!(
                "Invalid datetime bound '{}'.",
                obj.str()?
            ))),
        },
    }
}

/// A bound back into Python (`datetime` or `int`).
pub fn bound_to_py<'py>(ctx: &PyCtx<'py>, b: Option<&DateBound>) -> PyResult<Bound<'py, PyAny>> {
    let py = ctx.py;
    match b {
        None => Ok(py.None().into_bound(py)),
        Some(DateBound::Int(i)) => Ok(i.into_pyobject(py)?.into_any()),
        Some(DateBound::Datetime(dt)) => datetime_to_py(ctx, dt),
    }
}

/// A `params` filter dict as a core map (`None` stays `None`).
pub fn params_map(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Map>> {
    match obj.filter(|o| !o.is_none()) {
        None => Ok(None),
        Some(o) => Ok(Some(map_of(o)?)),
    }
}

/// A list of strings (`None` stays `None`; a bare string is one item).
pub fn str_list(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<String>>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(None);
    };
    if let Ok(s) = obj.downcast::<PyString>() {
        return Ok(Some(vec![s.to_str()?.to_string()]));
    }
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        out.push(item?.str()?.to_str()?.to_string());
    }
    Ok(Some(out))
}

/// The common `DataQuery` of `get_pipe_data`, `clear_pipe`, `get_pipe_rowcount`, ...
#[allow(clippy::too_many_arguments)]
pub fn data_query(
    ctx: &PyCtx<'_>,
    select_columns: Option<&Bound<'_, PyAny>>,
    omit_columns: Option<&Bound<'_, PyAny>>,
    begin: Option<&Bound<'_, PyAny>>,
    end: Option<&Bound<'_, PyAny>>,
    params: Option<&Bound<'_, PyAny>>,
    limit: Option<i64>,
    order: Option<String>,
) -> PyResult<DataQuery> {
    Ok(DataQuery {
        select_columns: str_list(select_columns)?,
        omit_columns: str_list(omit_columns)?,
        begin: date_bound(ctx, begin)?,
        end: date_bound(ctx, end)?,
        params: params_map(params)?,
        limit,
        order,
        as_chunks: false,
        remote: false,
    })
}

/// A `SuccessTuple` as Python's `(bool, str)`.
pub fn success_to_py<'py>(py: Python<'py>, t: &SuccessTuple) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(
        py,
        [
            t.success.into_pyobject(py)?.to_owned().into_any(),
            PyString::new(py, &t.message).into_any(),
        ],
    )
}

/// A core user for a Python `User` (duck-typed; private attributes read first so no property
/// triggers an instance call).
pub fn core_user(obj: &Bound<'_, PyAny>) -> PyResult<User> {
    let username = opt_str(obj, "username")?.unwrap_or_default();
    let password = opt_str(obj, "password")?.unwrap_or_default();
    let mut user = User::new(&username, Some(&password));
    user.email = opt_str(obj, "email")?;
    user.user_type = opt_str(obj, "type")?;
    let attrs = obj
        .getattr("_attributes")
        .or_else(|_| obj.getattr("attributes"));
    if let Ok(a) = attrs {
        if !a.is_none() {
            user.attributes = Some(map_of(&a)?);
        }
    }
    let user_id = obj.getattr("_user_id").or_else(|_| obj.getattr("user_id"));
    if let Ok(id) = user_id {
        if !id.is_none() {
            user.user_id = Some(py_to_core(&id)?);
        }
    }
    if let Ok(h) = obj.getattr("_password_hash") {
        if !h.is_none() {
            user.password_hash = Some(h.str()?.to_str()?.to_string());
        }
    }
    user.instance_keys = opt_str(obj, "_instance_keys")?;
    Ok(user)
}

/// A plugin record for a Python `Plugin`. `full` reads `version`, `user_id` and `attributes`
/// (which may import the plugin), else only the name.
pub fn plugin_record(obj: &Bound<'_, PyAny>, full: bool) -> PyResult<PluginRecord> {
    let name = opt_str(obj, "name")?
        .unwrap_or_else(|| obj.str().map(|s| s.to_string()).unwrap_or_default());
    let mut rec = PluginRecord {
        name,
        ..Default::default()
    };
    if full {
        rec.version = opt_str(obj, "version")?;
        if let Ok(id) = obj.getattr("user_id") {
            if !id.is_none() {
                rec.user_id = Some(py_to_core(&id)?);
            }
        }
        if let Ok(a) = obj.getattr("attributes") {
            rec.attributes = map_of(&a)?;
        }
    }
    Ok(rec)
}

/// A list of strings into a Python list.
pub fn strings_to_py<'py>(py: Python<'py>, items: &[String]) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, items)
}

/// A core value into Python (re-export for the instance bindings).
pub fn value_to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    core_to_py(py, v)
}
