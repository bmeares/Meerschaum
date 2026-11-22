//! Conversions between the API client's Rust values and Python: connector errors, frames,
//! documents, pipe keys and IDs, sync inputs, datetimes, UUIDs and tokens.

use super::*;

/// A connector error as the Python exception v4 would have let through.
pub(crate) fn conn_err(e: ConnectorError) -> PyErr {
    match e {
        ConnectorError::NotImplemented(m) => PyNotImplementedError::new_err(m),
        ConnectorError::Backend(m) => PyConnectionError::new_err(m),
        ConnectorError::Http { status, body } => {
            PyIOError::new_err(format!("{status} Error for url: {body}"))
        }
        ConnectorError::Detail(m) => PyRuntimeError::new_err(m),
        ConnectorError::Invalid(m) => PyValueError::new_err(m),
        ConnectorError::Json(e) => PyValueError::new_err(e.to_string()),
    }
}

/// An optional bound as a Python scalar (`datetime`, `int` or `None`).
pub(crate) fn bound_out<'py>(
    py: Python<'py>,
    b: Option<&DateBound>,
) -> PyResult<Bound<'py, PyAny>> {
    let ctx = PyCtx::scalars(py)?;
    bound_to_py(&ctx, b)
}

fn out_spec<'py>() -> OutSpec<'py> {
    OutSpec {
        index: IndexSpec::Range,
        hints: IndexMap::new(),
        arrow_native: false,
        arrow_columns: Vec::new(),
        geometry: None,
        labels: IndexMap::new(),
        reuse: IndexMap::new(),
        placeholders: Vec::new(),
        polars_objects: Vec::new(),
    }
}

/// The nullable pandas dtypes v4's `parse_df_datetimes(documents)` gave a frame built from JSON
/// (`convert_dtypes(dtype_backend='numpy_nullable')`): `Int64`, `Float64`, `boolean`, `string`;
/// datetimes and objects keep their default dtypes.
pub(crate) fn nullable_hints(frame: &Frame) -> IndexMap<String, String> {
    let mut hints = IndexMap::new();
    for column in frame.columns() {
        let dtype = column.data.dtype_str();
        let hint = if dtype.starts_with("int") || dtype.starts_with("uint") {
            "Int64"
        } else if dtype.starts_with("float") {
            "Float64"
        } else if dtype.starts_with("bool") {
            "boolean"
        } else if matches!(dtype.as_str(), "string" | "str" | "large_string" | "utf8") {
            "string"
        } else {
            continue;
        };
        hints.insert(column.name.clone(), hint.to_string());
    }
    hints
}

/// A frame as a pandas DataFrame with the nullable dtypes of a JSON-built v4 frame.
pub(crate) fn frame_out<'py>(py: Python<'py>, frame: &Frame) -> PyResult<Bound<'py, PyAny>> {
    let ctx = PyCtx::new(py)?;
    let mut spec = out_spec();
    spec.hints = nullable_hints(frame);
    frame_to_pandas(&ctx, frame, &spec)
}

/// Documents as dictionaries (`NULL`s are `None`, a `Decimal('NaN')` stays a value).
pub(crate) fn docs_out<'py>(
    py: Python<'py>,
    docs: &[meerschaum_connector::pipe_ops::Doc],
) -> PyResult<Bound<'py, PyList>> {
    let ctx = PyCtx::scalars(py)?;
    let out = PyList::empty(py);
    for doc in docs {
        let d = PyDict::new(py);
        for (key, value) in doc {
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
                d.set_item(key, cell_to_py(&ctx, value)?)?;
            }
        }
        out.append(d)?;
    }
    Ok(out)
}

/// `fetch_pipes_keys()`'s result: a dict of ID to key tuple, or a list of key tuples.
pub(crate) fn pipes_keys_out<'py>(
    py: Python<'py>,
    keys: &PipesKeys,
) -> PyResult<Bound<'py, PyAny>> {
    let entry_tuple = |e: &meerschaum_connector::PipeKeysEntry| -> PyResult<Bound<'py, PyTuple>> {
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
            for (id, e) in m {
                out.set_item(core_to_py(py, &id.to_value())?, entry_tuple(e)?)?;
            }
            Ok(out.into_any())
        }
        PipesKeys::List(v) => {
            let out = PyList::empty(py);
            for e in v {
                out.append(entry_tuple(e)?)?;
            }
            Ok(out.into_any())
        }
    }
}

/// The `PipesKeysQuery` of `fetch_pipes_keys(connector_keys, metric_keys, location_keys, tags,
/// params)`.
pub(crate) fn keys_query(
    connector_keys: Option<&Bound<'_, PyAny>>,
    metric_keys: Option<&Bound<'_, PyAny>>,
    location_keys: Option<&Bound<'_, PyAny>>,
    tags: Option<&Bound<'_, PyAny>>,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<PipesKeysQuery> {
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
    Ok(PipesKeysQuery {
        connector_keys: str_list(connector_keys)?.unwrap_or_default(),
        metric_keys: str_list(metric_keys)?.unwrap_or_default(),
        location_keys: lks,
        tags: str_list(tags)?.unwrap_or_default(),
        params: params_map(params)?,
    })
}

/// A pipe id as Python (`int` or `str`).
pub(crate) fn pipe_id_out<'py>(py: Python<'py>, id: Option<PipeId>) -> PyResult<Bound<'py, PyAny>> {
    match id {
        None => Ok(py.None().into_bound(py)),
        Some(PipeId::Int(i)) => Ok(i.into_pyobject(py)?.into_any()),
        Some(PipeId::Str(s)) => Ok(PyString::new(py, &s).into_any()),
    }
}

/// The rows a `sync_pipe(df)` call was given, as a frame (documents, dictionaries of lists,
/// pandas / Polars / Arrow frames).
pub(crate) fn sync_frame(py: Python<'_>, df: &Bound<'_, PyAny>) -> PyResult<Frame> {
    match input_from_py(py, df)? {
        NativeInput::Data(PipeInput::Frame(f)) => Ok(f),
        NativeInput::Data(PipeInput::Docs(docs)) => meerschaum_kernel::docs::frame_from_docs(&docs)
            .map_err(|e| PyValueError::new_err(e.to_string())),
        NativeInput::Data(PipeInput::Columns(cols)) => {
            meerschaum_kernel::docs::frame_from_columns(&cols)
                .map_err(|e| PyValueError::new_err(e.to_string()))
        }
        NativeInput::Declined(reason) => Err(PyTypeError::new_err(reason)),
        NativeInput::Invalid(reason) => Err(PyValueError::new_err(reason)),
    }
}

/// The keyword arguments `sync_pipe(**kw)` forwards as query parameters.
pub(crate) fn extra_map(kw: Option<&Bound<'_, PyDict>>) -> PyResult<Map> {
    let mut out = Map::new();
    if let Some(kw) = kw {
        for (k, v) in kw.iter() {
            if v.is_callable() {
                continue;
            }
            out.insert(k.str()?.to_str()?.to_string(), py_to_core(&v)?);
        }
    }
    Ok(out)
}

/// A query-string value the way `requests` encodes it.
pub(super) fn query_value(v: &Bound<'_, PyAny>) -> PyResult<QueryValue> {
    if v.is_none() {
        return Ok(QueryValue::None);
    }
    if v.is_instance_of::<pyo3::types::PyBool>() {
        return Ok(QueryValue::bool(v.extract::<bool>()?));
    }
    if v.is_instance_of::<PyList>() || v.is_instance_of::<PyTuple>() {
        let mut items = Vec::new();
        for item in v.try_iter()? {
            items.push(item?.str()?.to_str()?.to_string());
        }
        return Ok(QueryValue::Many(items));
    }
    Ok(QueryValue::One(v.str()?.to_str()?.to_string()))
}

pub(super) fn json_dumps(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<String> {
    py.import("json")?.call_method1("dumps", (obj,))?.extract()
}

/// A Python datetime as a UTC instant (naive values are UTC).
fn utc_datetime(ctx: &PyCtx<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Option<DateTime<Utc>>> {
    if obj.is_none() {
        return Ok(None);
    }
    let cell = py_to_cell(ctx, obj)?;
    Ok(match DateBound::from_value(&cell) {
        Some(DateBound::Datetime(dt)) => {
            Some(DateTime::<Utc>::from_naive_utc_and_offset(dt.utc, Utc))
        }
        _ => None,
    })
}

fn opt_attr<'py>(obj: &Bound<'py, PyAny>, name: &str) -> Option<Bound<'py, PyAny>> {
    obj.getattr(name).ok().filter(|v| !v.is_none())
}

pub(super) fn parse_uuid(obj: &Bound<'_, PyAny>) -> PyResult<Option<Uuid>> {
    if obj.is_none() {
        return Ok(None);
    }
    Uuid::parse_str(obj.str()?.to_str()?)
        .map(Some)
        .map_err(|e| PyValueError::new_err(format!("Invalid token ID '{obj}': {e}")))
}

/// A core token from a Python `Token` (`label`, `creation`, `expiration`, `scopes`,
/// `is_valid`, `id`, `secret`, the owner).
pub(super) fn core_token(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Token> {
    let ctx = PyCtx::scalars(py)?;
    let scopes: Vec<String> = match opt_attr(obj, "scopes") {
        Some(s) => str_list(Some(&s))?.unwrap_or_default(),
        None => Vec::new(),
    };
    let user = match opt_attr(obj, "user").or_else(|| opt_attr(obj, "_user")) {
        Some(u) => Some(core_user(&u)?),
        None => None,
    };
    let user_id = match opt_attr(obj, "_user_id") {
        Some(u) => Some(py_to_core(&u)?),
        None => None,
    };
    Ok(Token {
        label: opt_attr(obj, "label")
            .map(|l| l.str().map(|s| s.to_string()))
            .transpose()?,
        creation: match opt_attr(obj, "creation") {
            Some(c) => utc_datetime(&ctx, &c)?,
            None => None,
        },
        expiration: match opt_attr(obj, "expiration") {
            Some(c) => utc_datetime(&ctx, &c)?,
            None => None,
        },
        instance_keys: opt_attr(obj, "_instance_keys")
            .map(|l| l.str().map(|s| s.to_string()))
            .transpose()?,
        user,
        user_id,
        scopes,
        is_valid: obj
            .getattr("is_valid")
            .and_then(|v| v.is_truthy())
            .unwrap_or(true),
        id: match opt_attr(obj, "id") {
            Some(i) => parse_uuid(&i)?,
            None => None,
        },
        secret: opt_attr(obj, "secret")
            .map(|l| l.str().map(|s| s.to_string()))
            .transpose()?,
        secret_hash: None,
    })
}

/// `register_token` fills in what the server chose: label, secret, ID, expiration.
pub(super) fn token_back_to_py(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    token: &Token,
    uuid_cls: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let ctx = PyCtx::scalars(py)?;
    if let Some(l) = &token.label {
        obj.setattr("label", l)?;
    }
    if let Some(s) = &token.secret {
        obj.setattr("secret", s)?;
    }
    if let Some(id) = token.id {
        obj.setattr("id", uuid_cls.call1((id.to_string(),))?)?;
    }
    if let Some(exp) = token.expiration {
        let dt = meerschaum_kernel::value::DateTimeValue::utc(exp.naive_utc());
        obj.setattr("expiration", crate::convert::datetime_to_py(&ctx, &dt)?)?;
    }
    Ok(())
}

pub(super) fn serde_out<'py, T: serde::Serialize>(
    py: Python<'py>,
    v: &T,
) -> PyResult<Bound<'py, PyAny>> {
    let json = serde_json::to_value(v).map_err(|e| PyValueError::new_err(e.to_string()))?;
    json_to_py(py, &json)
}

pub(super) fn opt_string_out<'py>(
    py: Python<'py>,
    s: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    match s {
        Some(s) => Ok(PyString::new(py, &s).into_any()),
        None => Ok(py.None().into_bound(py)),
    }
}
