//! `meerschaum-connector-api` bound into `meerschaum._core`: the `APIConnector` client behind the v4
//! `meerschaum.connectors.api.APIConnector` façade (design §4.4, D12, D20).
//!
//! `_core.ApiConnector(label, attributes)` is built from the attributes the Python connector
//! resolved (`host`, `port`, `protocol`, `username` / `password`, `client_id` /
//! `client_secret`, `api_key`, `instance_keys`, `uri`, `verify`), so `connectors.yaml`, keyword
//! arguments and `MRSM_API_<LABEL>` URIs work unchanged. Each method takes the Python
//! arguments of the v4 method it replaces and runs the Rust client (`reqwest`, the same paths,
//! query parameters and bodies as v4) on the extension's Tokio runtime with the GIL released;
//! pipes, users, tokens and plugins are read by attribute (duck typing); frames cross as Arrow
//! ([`crate::pandas_bridge`]). Neither `requests` nor `httpx` is imported on this path (D20).
//!
//! The client's warnings (`Failed to get data for ...`) reach `meerschaum.utils.warnings.warn`
//! through the extension's `tracing` sink ([`crate::logging`]); callbacks (`monitor_logs`,
//! `do_action`) run on a runtime thread and take the interpreter lock for each call.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_connector::{
    ConnectorError, DataQuery, DateBound, PipeId, PipesKeys, PipesKeysQuery, SyncOptions,
};
use meerschaum_connector_api::APIConnector as RsApiConnector;
use meerschaum_connector_api::client::{ApiResponse, Body, LogsMonitor, Method, RequestOptions};
use meerschaum_connector_api::wire::QueryValue;
use meerschaum_core::users::Token;
use meerschaum_core::{Map, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use pyo3::exceptions::{
    PyConnectionError, PyIOError, PyNotImplementedError, PyRuntimeError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use uuid::Uuid;

use crate::convert::{PyCtx, cell_to_py, core_to_py, json_to_py, py_to_cell, py_to_core};
use crate::pandas_bridge::{IndexSpec, OutSpec, frame_to_pandas};
use crate::pipe_bridge::{
    ParamsView, bound_to_py, core_pipe, core_user, data_query, params_map, plugin_record, str_list,
    success_to_py,
};
use crate::runtime::{block_on, runtime};
use crate::sync_api::{NativeInput, input_from_py};

mod calls;
mod conversions;
mod response;

pub(crate) use calls::{install_warning_sink, run_interruptible};
pub(crate) use conversions::{
    bound_out, conn_err, docs_out, frame_out, keys_query, nullable_hints, pipe_id_out,
    pipes_keys_out, sync_frame,
};
use conversions::{
    core_token, extra_map, json_dumps, opt_string_out, parse_uuid, query_value, serde_out,
    token_back_to_py,
};
use response::PyApiResponse;

/// The error a Python callback raised inside a runtime thread: the first one is kept and
/// re-raised by the caller (`StopMonitoringLogs` and `KeyboardInterrupt` end the loop quietly).
#[derive(Default)]
struct CallbackError {
    error: Mutex<Option<PyErr>>,
    stopped: std::sync::atomic::AtomicBool,
}

impl CallbackError {
    fn handle(&self, py: Python<'_>, err: PyErr, stop: &tokio::sync::Notify) {
        let quiet = err
            .get_type(py)
            .name()
            .map(|n| {
                matches!(
                    n.to_string().as_str(),
                    "StopMonitoringLogs" | "KeyboardInterrupt"
                )
            })
            .unwrap_or(false);
        if !quiet && let Ok(mut slot) = self.error.lock() {
            slot.get_or_insert(err);
        }
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        stop.notify_one();
    }
}

/// The `api:<label>` instance connector.
#[pyclass(frozen, name = "ApiConnector", module = "meerschaum._core")]
pub struct PyApiConnector {
    pub(crate) inner: Arc<RsApiConnector>,
}

impl PyApiConnector {
    fn conn(&self) -> Arc<RsApiConnector> {
        self.inner.clone()
    }
}

fn raw_pipe(pipe: &Bound<'_, PyAny>) -> PyResult<meerschaum_core::Pipe> {
    core_pipe(pipe, ParamsView::Raw)
}

/// The pipe for calls which load the pipe's attributes (`get_pipe_id`, `get_pipe_attributes`):
/// the keys and whatever raw parameters the Python pipe already holds in `_attributes`, never
/// `pipe.parameters` (which asks the instance for them and would recurse).
pub(crate) fn id_pipe(pipe: &Bound<'_, PyAny>) -> PyResult<meerschaum_core::Pipe> {
    let mut p = core_pipe(pipe, ParamsView::Keys)?;
    if let Ok(params) = pipe
        .getattr("_attributes")
        .and_then(|a| a.get_item("parameters"))
        && let Some(Value::Map(m)) = (!params.is_none())
            .then(|| py_to_core(&params).ok())
            .flatten()
    {
        p.set_parameters(m);
    }
    Ok(p)
}

#[pymethods]
impl PyApiConnector {
    /// `ApiConnector(label, attributes)`.
    #[new]
    fn new(label: &str, attributes: &Bound<'_, PyDict>) -> PyResult<Self> {
        let attrs = match py_to_core(attributes.as_any())? {
            Value::Map(m) => m.into_iter().filter(|(_, v)| !v.is_null()).collect::<Map>(),
            _ => Map::new(),
        };
        let conn = Arc::new(RsApiConnector::new(Some(label), attrs.clone()).map_err(conn_err)?);
        crate::facade::hold_api(label, &attrs, &conn);
        Ok(PyApiConnector { inner: conn })
    }

    /// Build a logical connector without constructing a native SQL or Valkey client.
    #[staticmethod]
    fn from_execution_alias(keys: &str, attributes: &Bound<'_, PyDict>) -> PyResult<Self> {
        let attrs = py_to_core(attributes.as_any())?.to_map();
        let alias =
            meerschaum_core::team_scope::api_execution::Alias::from_attributes(&attrs, keys)
                .map_err(pyo3::exceptions::PyValueError::new_err)?
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("Missing API execution alias.")
                })?;
        let conn = RsApiConnector::from_execution_alias(&alias).map_err(conn_err)?;
        Ok(Self {
            inner: Arc::new(conn),
        })
    }

    fn __repr__(&self) -> String {
        format!("_core.ApiConnector('{}')", self.inner)
    }

    // -----------------------------------------------------------------------------------------
    // Attributes

    /// `self.url`: `protocol://host[:port]`.
    #[getter]
    fn url(&self) -> String {
        self.inner.url.clone()
    }

    /// `self.URI`.
    #[getter]
    fn uri(&self) -> String {
        self.inner.uri()
    }

    /// `self.instance_keys`.
    #[getter]
    fn instance_keys(&self) -> Option<String> {
        self.inner.instance_keys()
    }

    /// `self.login_scheme`.
    #[getter]
    fn login_scheme(&self) -> &'static str {
        self.inner.login_scheme()
    }

    /// `self.token`: the API key, or the session token (logging in when it is missing or
    /// about to expire).
    #[getter]
    fn token(&self, py: Python<'_>) -> Option<String> {
        let c = self.conn();
        block_on(py, async move { c.token().await })
    }

    /// v4's `self._token`: the stored session token, `None` before a login (never logs in).
    #[getter]
    fn stored_token(&self, py: Python<'_>) -> Option<String> {
        let c = self.conn();
        block_on(py, async move { c.stored_token().await.0 })
    }

    #[setter]
    fn set_stored_token(&self, py: Python<'_>, token: Option<String>) {
        let c = self.conn();
        block_on(py, async move { c.set_stored_token(token).await });
    }

    /// v4's `self._expires`: the stored expiration as an ISO naive timestamp, or `None`.
    #[getter]
    fn stored_expires(&self, py: Python<'_>) -> Option<String> {
        let c = self.conn();
        block_on(py, async move {
            c.stored_token()
                .await
                .1
                .map(|e| e.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        })
    }

    #[setter]
    fn set_stored_expires(&self, py: Python<'_>, expires: Option<String>) {
        let c = self.conn();
        let parsed = expires
            .as_deref()
            .and_then(meerschaum_connector_api::client::parse_expires);
        block_on(py, async move { c.set_stored_expires(parsed).await });
    }

    /// `get_pipe_instance_keys(pipe)`.
    fn get_pipe_instance_keys(&self, pipe: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
        let p = raw_pipe(pipe)?;
        Ok(self.inner.get_pipe_instance_keys(&p))
    }

    /// Forget the client-side caches of a pipe (attributes, `exists`, columns types).
    fn invalidate_pipe_cache(&self, pipe: &Bound<'_, PyAny>) -> PyResult<()> {
        let p = core_pipe(pipe, ParamsView::Keys)?;
        self.inner.invalidate_pipe_cache(&p);
        Ok(())
    }

    // -----------------------------------------------------------------------------------------
    // Requests

    /// `make_request(method, r_url, headers=None, use_token=True, params=None, json=None,
    /// data=None, files=None, timeout=None)`.
    #[pyo3(signature = (
        method, r_url, headers=None, use_token=true, params=None, json=None, data=None,
        files=None, timeout=None, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn make_request(
        &self,
        py: Python<'_>,
        method: &str,
        r_url: &str,
        headers: Option<&Bound<'_, PyDict>>,
        use_token: bool,
        params: Option<&Bound<'_, PyAny>>,
        json: Option<&Bound<'_, PyAny>>,
        data: Option<&Bound<'_, PyAny>>,
        files: Option<&Bound<'_, PyDict>>,
        timeout: Option<f64>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyApiResponse> {
        let method = match method.to_uppercase().as_str() {
            "GET" => Method::Get,
            "OPTIONS" => Method::Options,
            "HEAD" => Method::Head,
            "POST" => Method::Post,
            "PUT" => Method::Put,
            "PATCH" => Method::Patch,
            "DELETE" => Method::Delete,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Invalid HTTP method '{other}'."
                )));
            }
        };
        let mut query = Vec::new();
        if let Some(p) = params.filter(|p| !p.is_none()) {
            let d = p.cast::<PyDict>()?;
            for (k, v) in d.iter() {
                query.push((k.str()?.to_str()?.to_string(), query_value(&v)?));
            }
        }
        let mut header_pairs = Vec::new();
        if let Some(h) = headers {
            for (k, v) in h.iter() {
                header_pairs.push((
                    k.str()?.to_str()?.to_string(),
                    v.str()?.to_str()?.to_string(),
                ));
            }
        }
        let body = if let Some(j) = json.filter(|j| !j.is_none()) {
            Body::Json(json_dumps(py, j)?)
        } else if let Some(f) = files.filter(|f| !f.is_empty()) {
            let (field, fp) = f
                .iter()
                .next()
                .ok_or_else(|| PyValueError::new_err("No file."))?;
            let content: Vec<u8> = fp.call_method0("read")?.extract()?;
            let name = fp
                .getattr("name")
                .ok()
                .and_then(|n| n.str().ok().map(|s| s.to_string()))
                .unwrap_or_else(|| "archive".into());
            let file_name = std::path::Path::new(&name)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or(name);
            Body::File {
                field: field.str()?.to_str()?.to_string(),
                file_name,
                data: content,
            }
        } else if let Some(d) = data.filter(|d| !d.is_none()) {
            if let Ok(dict) = d.cast::<PyDict>() {
                let mut pairs = Vec::new();
                for (k, v) in dict.iter() {
                    if v.is_none() {
                        continue;
                    }
                    pairs.push((
                        k.str()?.to_str()?.to_string(),
                        v.str()?.to_str()?.to_string(),
                    ));
                }
                Body::Form(pairs)
            } else if let Ok(b) = d.cast::<PyBytes>() {
                Body::Text(String::from_utf8_lossy(b.as_bytes()).into_owned())
            } else {
                Body::Text(d.str()?.to_str()?.to_string())
            }
        } else {
            Body::None
        };
        let opts = RequestOptions {
            query,
            headers: header_pairs,
            body,
            use_token,
            timeout: timeout.map(Duration::from_secs_f64),
        };
        let c = self.conn();
        let url = c.request_url(r_url, &opts.query);
        let r = block_on(py, async move { c.make_request(method, r_url, opts).await })
            .map_err(conn_err)?;
        Ok(PyApiResponse { inner: r, url })
    }

    // -----------------------------------------------------------------------------------------
    // Login and misc

    /// `login(debug=False, warn=True)`: `warn=False` keeps the failure message to the result.
    #[pyo3(signature = (warn=true, **_kw))]
    fn login<'py>(
        &self,
        py: Python<'py>,
        warn: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let c = self.conn();
        let res = block_on(py, async move { c.login_with(warn).await });
        success_to_py(py, &res)
    }

    /// `get_mrsm_version()`.
    #[pyo3(signature = (**_kw))]
    fn get_mrsm_version(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> Option<String> {
        let c = self.conn();
        block_on(py, async move { c.get_mrsm_version().await })
    }

    /// `get_chaining_status()`.
    #[pyo3(signature = (**_kw))]
    fn get_chaining_status(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> Option<bool> {
        let c = self.conn();
        block_on(py, async move { c.get_chaining_status().await })
    }

    // -----------------------------------------------------------------------------------------
    // Pipes

    #[pyo3(signature = (pipe, **_kw))]
    fn register_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Unresolved)?;
        let c = self.conn();
        let c2 = c.clone();
        let p2 = p.clone();
        let res = block_on(py, async move { c.register_pipe(&p).await });
        c2.invalidate_pipe_cache(&p2);
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
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let c2 = c.clone();
        let p2 = p.clone();
        let res = block_on(py, async move { c.edit_pipe(&p, patch).await });
        c2.invalidate_pipe_cache(&p2);
        success_to_py(py, &res)
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
        let query = keys_query(connector_keys, metric_keys, location_keys, tags, params)?;
        let c = self.conn();
        let keys =
            block_on(py, async move { c.fetch_pipes_keys(&query).await }).map_err(conn_err)?;
        pipes_keys_out(py, &keys)
    }

    /// `sync_pipe(pipe, df=None, chunksize=-1, **kw)`: the keyword arguments are forwarded to
    /// the server as query parameters.
    #[pyo3(signature = (pipe, df=None, chunksize=Some(-1), **kw))]
    fn sync_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        df: Option<&Bound<'py, PyAny>>,
        chunksize: Option<i64>,
        kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let Some(df) = df.filter(|d| !d.is_none()) else {
            let msg = format!("DataFrame is `None`. Cannot sync {}.", pipe.str()?);
            return success_to_py(py, &SuccessTuple::fail(msg));
        };
        let p = raw_pipe(pipe)?;
        let frame = sync_frame(py, df)?;
        let mut extra = extra_map(kw)?;
        extra.shift_remove("debug");
        let check_existing = extra
            .get("check_existing")
            .map(|v| v.truthy())
            .unwrap_or(true);
        let opts = SyncOptions {
            check_existing,
            chunksize,
            begin: None,
            end: None,
            extra,
            ..Default::default()
        };
        let c = self.conn();
        let res = block_on(py, async move { c.sync_pipe(&p, frame, &opts).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn delete_pipe_cache<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let res = block_on(py, async move { c.delete_pipe_cache(&p).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn delete_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let c2 = c.clone();
        let p2 = p.clone();
        let res = block_on(py, async move { c.delete_pipe(&p).await });
        c2.invalidate_pipe_cache(&p2);
        success_to_py(py, &res)
    }

    /// Read only the main server's canonical registered source definition.
    #[pyo3(signature = (
        pipe, select_columns=None, omit_columns=None, begin=None, end=None, params=None,
        order=None, limit=None, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn fetch_registered_source<'py>(
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = PyCtx::scalars(py)?;
        // Source SQL, hooks and local parameters cannot influence the broker request.
        let p = core_pipe(pipe, ParamsView::Keys)?;
        let query = data_query(
            &ctx,
            select_columns,
            omit_columns,
            begin,
            end,
            params,
            limit,
            order,
        )?;
        let c = self.conn();
        match block_on(
            py,
            async move { c.fetch_registered_source(&p, &query).await },
        )
        .map_err(conn_err)?
        {
            Some(frame) => frame_out(py, &frame),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// `get_pipe_data(...)`: a pandas DataFrame, `None` when the request failed, or the tuple
    /// `(False, detail)` when the server answered with a `detail`.
    #[pyo3(signature = (
        pipe, select_columns=None, omit_columns=None, begin=None, end=None, params=None,
        as_chunks=false, **_kw
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
        as_chunks: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = PyCtx::scalars(py)?;
        let p = raw_pipe(pipe)?;
        let mut q: DataQuery = data_query(
            &ctx,
            select_columns,
            omit_columns,
            begin,
            end,
            params,
            None,
            None,
        )?;
        q.as_chunks = as_chunks;
        let c = self.conn();
        let res = block_on(py, async move { c.get_pipe_data(&p, &q).await });
        match res {
            Ok(Some(frame)) => frame_out(py, &frame),
            Ok(None) => Ok(py.None().into_bound(py)),
            // PARITY: v4 returns the tuple `(False, detail)` instead of a DataFrame.
            Err(ConnectorError::Detail(d)) => {
                Ok(success_to_py(py, &SuccessTuple::fail(d))?.into_any())
            }
            Err(e) => Err(conn_err(e)),
        }
    }

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
        let ctx = PyCtx::scalars(py)?;
        let p = raw_pipe(pipe)?;
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
        let c = self.conn();
        let docs = block_on(py, async move { c.get_pipe_docs(&p, &q).await }).map_err(conn_err)?;
        docs_out(py, &docs)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_id<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = id_pipe(pipe)?;
        let c = self.conn();
        let id = block_on(py, async move { c.get_pipe_id(&p).await }).map_err(conn_err)?;
        pipe_id_out(py, id)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_attributes<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = id_pipe(pipe)?;
        let c = self.conn();
        let attrs =
            block_on(py, async move { c.get_pipe_attributes(&p).await }).map_err(conn_err)?;
        core_to_py(py, &Value::Map(attrs))
    }

    #[pyo3(signature = (pipe, params=None, newest=true, **_kw))]
    fn get_sync_time<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        params: Option<&Bound<'py, PyAny>>,
        newest: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = raw_pipe(pipe)?;
        let params = params_map(params)?;
        let c = self.conn();
        let st = block_on(py, async move {
            c.get_sync_time(&p, params.as_ref(), newest).await
        })
        .map_err(conn_err)?;
        bound_out(py, st.as_ref())
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn pipe_exists(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        block_on(py, async move { c.pipe_exists(&p).await }).map_err(conn_err)
    }

    /// `create_metadata()`: `False` always, as in v4.
    #[pyo3(signature = (**_kw))]
    fn create_metadata(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> bool {
        let c = self.conn();
        block_on(py, async move { c.create_metadata().await })
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
    ) -> PyResult<i64> {
        let ctx = PyCtx::scalars(py)?;
        let p = raw_pipe(pipe)?;
        let mut q = data_query(&ctx, None, None, begin, end, params, None, None)?;
        q.remote = remote;
        let c = self.conn();
        block_on(py, async move { c.get_pipe_rowcount(&p, &q).await }).map_err(conn_err)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn drop_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = raw_pipe(pipe)?;
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
        let p = raw_pipe(pipe)?;
        let q = data_query(&ctx, None, None, begin, end, params, None, None)?;
        let c = self.conn();
        let res = block_on(py, async move { c.clear_pipe(&p, &q).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_columns_types<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let types = block_on(py, async move { c.get_pipe_columns_types_opt(&p).await })
            .map_err(conn_err)?;
        match types {
            None => Ok(py.None().into_bound(py)),
            Some(t) => {
                let out = PyDict::new(py);
                for (k, v) in t {
                    out.set_item(k, v)?;
                }
                Ok(out.into_any())
            }
        }
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_columns_indices<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let indices = block_on(py, async move { c.get_pipe_columns_indices_opt(&p).await })
            .map_err(conn_err)?;
        match indices {
            None => Ok(py.None().into_bound(py)),
            Some(idx) => {
                let out = PyDict::new(py);
                for (col, docs) in idx {
                    let list = PyList::empty(py);
                    for d in docs {
                        let doc = PyDict::new(py);
                        doc.set_item("name", d.name)?;
                        doc.set_item("type", d.index_type)?;
                        list.append(doc)?;
                    }
                    out.set_item(col, list)?;
                }
                Ok(out.into_any())
            }
        }
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_index_names<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let names =
            block_on(py, async move { c.get_pipe_index_names(&p).await }).map_err(conn_err)?;
        core_to_py(py, &Value::Map(names))
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_size(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<i64>> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        block_on(py, async move { c.get_pipe_size(&p).await }).map_err(conn_err)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn compress_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let res = block_on(py, async move { c.compress_pipe(&p).await });
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
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let res = block_on(py, async move { c.decompress_pipe(&p, no_policy).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, full=false, **_kw))]
    fn vacuum_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        full: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = raw_pipe(pipe)?;
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
        let p = raw_pipe(pipe)?;
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
        let p = raw_pipe(pipe)?;
        let c = self.conn();
        let res = block_on(py, async move { c.partition_pipe(&p, chunk_minutes).await });
        success_to_py(py, &res)
    }

    // -----------------------------------------------------------------------------------------
    // Users

    #[pyo3(signature = (**_kw))]
    fn get_users(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<String>> {
        let c = self.conn();
        block_on(py, async move { c.get_users().await }).map_err(conn_err)
    }

    #[pyo3(signature = (user, **_kw))]
    fn edit_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let u = core_user(user)?;
        let c = self.conn();
        let res = block_on(py, async move { c.edit_user(&u).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (user, **_kw))]
    fn register_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let u = core_user(user)?;
        let c = self.conn();
        let res = block_on(py, async move { c.register_user(&u).await });
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
        let id = block_on(py, async move { c.get_user_id(&u).await }).map_err(conn_err)?;
        core_to_py(py, &id.unwrap_or(Value::Null))
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
    fn get_user_attributes<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let u = core_user(user)?;
        let c = self.conn();
        let attrs =
            block_on(py, async move { c.get_user_attributes(&u).await }).map_err(conn_err)?;
        core_to_py(py, &attrs.unwrap_or(Value::Null))
    }

    #[pyo3(signature = (user, **_kw))]
    fn get_user_password_hash<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let u = core_user(user)?;
        let c = self.conn();
        let h =
            block_on(py, async move { c.get_user_password_hash(&u).await }).map_err(conn_err)?;
        opt_string_out(py, h)
    }

    #[pyo3(signature = (user, **_kw))]
    fn get_user_type<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let u = core_user(user)?;
        let c = self.conn();
        let t = block_on(py, async move { c.get_user_type(&u).await }).map_err(conn_err)?;
        opt_string_out(py, t)
    }

    // -----------------------------------------------------------------------------------------
    // Plugins

    /// `register_plugin(plugin, archive, archive_name)`: upload the plugin's archive bytes.
    #[pyo3(signature = (plugin, archive, archive_name, **_kw))]
    fn register_plugin<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        archive: Vec<u8>,
        archive_name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let rec = plugin_record(plugin, true)?;
        let name = archive_name.to_string();
        let c = self.conn();
        let res = block_on(
            py,
            async move { c.register_plugin(&rec, archive, &name).await },
        );
        success_to_py(py, &res)
    }

    /// Download and install a plugin through the CLI's archive transaction.
    #[pyo3(signature = (name, skip_deps=false, force=false, debug=false))]
    fn install_plugin<'py>(
        &self,
        py: Python<'py>,
        name: String,
        skip_deps: bool,
        force: bool,
        debug: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        use meerschaum_cli::native::lifecycle::install_repository_plugin;
        use meerschaum_cli::value::{ArgsMap, PyValue};
        let connector = self.conn();
        let args = ArgsMap::from([
            ("debug".into(), PyValue::Bool(debug)),
            ("force".into(), PyValue::Bool(force)),
            ("skip_deps".into(), PyValue::Bool(skip_deps)),
        ]);
        let res = crate::entry_api::run_in_session(py, move |rt| {
            install_repository_plugin(rt, &args, &connector, &name)
        })?;
        success_to_py(py, &res)
    }

    /// `download_plugin(name)`: the plugin archive's bytes (`ValueError` naming the failure
    /// when the server answered with a message instead).
    fn download_plugin<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyBytes>> {
        let c = self.conn();
        let n = name.to_string();
        let bytes = block_on(py, async move { c.download_plugin(&n).await })
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyBytes::new(py, &bytes))
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
        .map_err(conn_err)
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
        let attrs =
            block_on(py, async move { c.get_plugin_attributes(&rec).await }).map_err(conn_err)?;
        core_to_py(py, &Value::Map(attrs))
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_version<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn();
        let v = block_on(py, async move { c.get_plugin_version(&rec).await }).map_err(conn_err)?;
        opt_string_out(py, v)
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

    // -----------------------------------------------------------------------------------------
    // Tokens

    /// `register_token(token)`: the label, secret, ID and expiration the server chose are set
    /// on the Python token.
    #[pyo3(signature = (token, **_kw))]
    fn register_token<'py>(
        &self,
        py: Python<'py>,
        token: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut t = core_token(py, token)?;
        let c = self.conn();
        let (res, t) = block_on(py, async move {
            let res = c.register_token(&mut t).await;
            (res, t)
        });
        if res.success {
            let uuid_cls = py.import("uuid")?.getattr("UUID")?;
            token_back_to_py(py, token, &t, &uuid_cls)?;
        }
        success_to_py(py, &res)
    }

    /// `get_token_model(token_id)`: the model's fields, or `None`.
    #[pyo3(signature = (token_id, **_kw))]
    fn get_token_model<'py>(
        &self,
        py: Python<'py>,
        token_id: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = parse_uuid(token_id)?;
        let c = self.conn();
        let model = block_on(py, async move { c.get_token_model(id).await }).map_err(conn_err)?;
        match model {
            Some(m) => serde_out(py, &m),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// `get_tokens(labels=None)`: the tokens' fields.
    #[pyo3(signature = (labels=None, **_kw))]
    fn get_tokens<'py>(
        &self,
        py: Python<'py>,
        labels: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let labels = str_list(labels)?;
        let c = self.conn();
        let models =
            block_on(py, async move { c.get_tokens(labels.as_deref()).await }).map_err(conn_err)?;
        serde_out(py, &models)
    }

    #[pyo3(signature = (token, **_kw))]
    fn edit_token<'py>(
        &self,
        py: Python<'py>,
        token: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let t = core_token(py, token)?;
        let c = self.conn();
        let res = block_on(py, async move { c.edit_token(&t).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (token, **_kw))]
    fn invalidate_token<'py>(
        &self,
        py: Python<'py>,
        token: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let t = core_token(py, token)?;
        let c = self.conn();
        let res = block_on(py, async move { c.invalidate_token(&t).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (token_id, **_kw))]
    fn get_token_scopes(
        &self,
        py: Python<'_>,
        token_id: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<String>> {
        let id = parse_uuid(token_id)?;
        let c = self.conn();
        block_on(py, async move { c.get_token_scopes(id).await }).map_err(conn_err)
    }

    #[pyo3(signature = (token_id, **_kw))]
    fn token_exists(
        &self,
        py: Python<'_>,
        token_id: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let id = parse_uuid(token_id)?;
        let c = self.conn();
        block_on(py, async move { c.token_exists(id).await }).map_err(conn_err)
    }

    #[pyo3(signature = (token, **_kw))]
    fn delete_token<'py>(
        &self,
        py: Python<'py>,
        token: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let t = core_token(py, token)?;
        let c = self.conn();
        let res = block_on(py, async move { c.delete_token(&t).await });
        success_to_py(py, &res)
    }

    // -----------------------------------------------------------------------------------------
    // Actions

    /// `get_actions()`: the action names.
    fn get_actions(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let c = self.conn();
        block_on(py, async move { c.get_actions().await }).map_err(conn_err)
    }

    /// `do_action_legacy(action, kwargs)`: `POST /actions/{action[0]}`.
    #[pyo3(signature = (action, kwargs=None))]
    fn do_action_legacy<'py>(
        &self,
        py: Python<'py>,
        action: Vec<String>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let kw = extra_map(kwargs)?;
        let c = self.conn();
        let res = block_on(py, async move { c.do_action_legacy(&action, kw).await });
        success_to_py(py, &res)
    }

    /// `do_action(sysargs, callback_function=None)`: run the action as a temporary remote job,
    /// call `callback_function(line)` for each line of its output and return its result.
    #[pyo3(signature = (sysargs, callback_function=None))]
    fn do_action<'py>(
        &self,
        py: Python<'py>,
        sysargs: Vec<String>,
        callback_function: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let sysargs = meerschaum_cli::sysargs::remove_api_executor_keys(&sysargs);
        let stop = Arc::new(tokio::sync::Notify::new());
        let errors = Arc::new(CallbackError::default());
        let (stop_cb, errors_cb) = (stop.clone(), errors.clone());
        let on_line: Box<dyn FnMut(String) + Send> = Box::new(move |line| {
            let _ = Python::try_attach(|py| {
                let res = match &callback_function {
                    Some(cb) => cb.call1(py, (line,)).map(|_| ()),
                    None => py
                        .import("builtins")
                        .and_then(|b| {
                            let kw = PyDict::new(py);
                            kw.set_item("end", "")?;
                            kw.set_item("flush", true)?;
                            b.getattr("print")?.call((line,), Some(&kw))
                        })
                        .map(|_| ()),
                };
                if let Err(e) = res {
                    errors_cb.handle(py, e, &stop_cb);
                }
            });
        });
        let c = self.conn();
        let result = run_interruptible(
            py,
            async move { c.do_action(&sysargs, None, on_line).await },
            Some(stop),
            None,
        )?;
        if let Some(e) = errors.error.lock().ok().and_then(|mut s| s.take()) {
            return Err(e);
        }
        match result {
            Some(res) => success_to_py(py, &res),
            None => success_to_py(py, &SuccessTuple::fail("The action was interrupted.")),
        }
    }

    // -----------------------------------------------------------------------------------------
    // Jobs

    /// `get_jobs()`: every remote job's metadata by name (`{}` on errors).
    #[pyo3(signature = (**_kw))]
    fn get_jobs_metadata<'py>(
        &self,
        py: Python<'py>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let c = self.conn();
        let jobs = block_on(py, async move { c.get_jobs().await }).map_err(conn_err)?;
        serde_out(py, &jobs)
    }

    /// `get_job_metadata(name)`.
    #[pyo3(signature = (name, **_kw))]
    fn get_job_metadata<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let c = self.conn();
        let n = name.to_string();
        let meta = block_on(py, async move { c.get_job_metadata(&n).await }).map_err(conn_err)?;
        core_to_py(py, &Value::Map(meta))
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_properties<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let c = self.conn();
        let n = name.to_string();
        let props =
            block_on(py, async move { c.get_job_properties(&n).await }).map_err(conn_err)?;
        core_to_py(py, &Value::Map(props))
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_status(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_job_status(&n).await }).map_err(conn_err)
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_began(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_job_began(&n).await }).map_err(conn_err)
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_ended(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_job_ended(&n).await }).map_err(conn_err)
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_paused(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_job_paused(&n).await }).map_err(conn_err)
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_exists(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_job_exists(&n).await }).map_err(conn_err)
    }

    #[pyo3(signature = (name, **_kw))]
    fn delete_job<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let c = self.conn();
        let n = name.to_string();
        let res = block_on(py, async move { c.delete_job(&n).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (name, **_kw))]
    fn start_job<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let c = self.conn();
        let n = name.to_string();
        let res = block_on(py, async move { c.start_job(&n).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (name, **_kw))]
    fn stop_job<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let c = self.conn();
        let n = name.to_string();
        let res = block_on(py, async move { c.stop_job(&n).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (name, **_kw))]
    fn pause_job<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let c = self.conn();
        let n = name.to_string();
        let res = block_on(py, async move { c.pause_job(&n).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (name, sysargs, properties=None, **_kw))]
    fn create_job<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        sysargs: Vec<String>,
        properties: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let properties = params_map(properties)?;
        let c = self.conn();
        let n = name.to_string();
        let res = block_on(
            py,
            async move { c.create_job(&n, &sysargs, properties).await },
        );
        success_to_py(py, &res)
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_logs(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_logs(&n).await }).map_err(conn_err)
    }

    /// `get_job_stop_time(name)`: the ISO text (the façade parses it).
    #[pyo3(signature = (name, **_kw))]
    fn get_job_stop_time(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<String>> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_job_stop_time(&n).await }).map_err(conn_err)
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_is_blocking_on_stdin(
        &self,
        py: Python<'_>,
        name: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let c = self.conn();
        let n = name.to_string();
        block_on(py, async move { c.get_job_is_blocking_on_stdin(&n).await }).map_err(conn_err)
    }

    #[pyo3(signature = (name, **_kw))]
    fn get_job_prompt_kwargs<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let c = self.conn();
        let n = name.to_string();
        let kw =
            block_on(py, async move { c.get_job_prompt_kwargs(&n).await }).map_err(conn_err)?;
        core_to_py(py, &Value::Map(kw))
    }

    /// `monitor_logs(name, callback_function, input_callback_function=None,
    /// stop_callback_function=None, stop_on_exit=False, strip_timestamps=False)`: stream the
    /// job's logs over the websocket. The callbacks run on a runtime thread (the façade wraps
    /// coroutine functions); `StopMonitoringLogs` and `KeyboardInterrupt` end the stream quietly,
    /// any other exception ends it and is raised here.
    #[pyo3(signature = (
        name, callback_function, input_callback_function=None, stop_callback_function=None,
        stop_on_exit=false, strip_timestamps=false, stop_event=None, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn monitor_logs(
        &self,
        py: Python<'_>,
        name: &str,
        callback_function: Py<PyAny>,
        input_callback_function: Option<Py<PyAny>>,
        stop_callback_function: Option<Py<PyAny>>,
        stop_on_exit: bool,
        strip_timestamps: bool,
        stop_event: Option<Py<PyAny>>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let stop = Arc::new(tokio::sync::Notify::new());
        let errors = Arc::new(CallbackError::default());
        let line_cb = {
            let (stop, errors) = (stop.clone(), errors.clone());
            Box::new(move |line: String| {
                let _ = Python::try_attach(|py| {
                    if let Err(e) = callback_function.call1(py, (line,)) {
                        errors.handle(py, e, &stop);
                    }
                });
            }) as Box<dyn FnMut(String) + Send>
        };
        let input_cb = input_callback_function.map(|cb| {
            let (stop, errors) = (stop.clone(), errors.clone());
            Box::new(move || {
                Python::try_attach(|py| match cb.call0(py) {
                    Ok(v) => v.extract::<String>(py).unwrap_or_default(),
                    Err(e) => {
                        errors.handle(py, e, &stop);
                        String::new()
                    }
                })
                .unwrap_or_default()
            }) as Box<dyn FnMut() -> String + Send>
        });
        let stop_cb = stop_callback_function.map(|cb| {
            let (stop, errors) = (stop.clone(), errors.clone());
            Box::new(move |result: SuccessTuple| {
                let _ = Python::try_attach(|py| {
                    let tuple = match success_to_py(py, &result) {
                        Ok(t) => t,
                        Err(_) => return,
                    };
                    if let Err(e) = cb.call1(py, (tuple,)) {
                        errors.handle(py, e, &stop);
                    }
                });
            }) as Box<dyn FnMut(SuccessTuple) + Send>
        });
        let c = self.conn();
        let n = name.to_string();
        let outcome = run_interruptible(
            py,
            async move {
                let monitor = LogsMonitor {
                    on_line: line_cb,
                    on_input: input_cb,
                    on_stop: stop_cb,
                    stop_on_exit,
                    strip_timestamps,
                };
                c.monitor_logs(&n, monitor).await
            },
            Some(stop),
            stop_event.as_ref().map(|e| e.bind(py)),
        );
        match outcome {
            // PARITY: v4 swallows `KeyboardInterrupt` and ends the monitor.
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyKeyboardInterrupt>(py) => {}
            Err(e) => return Err(e),
            Ok(Some(Err(e))) => {
                if let Some(cb_err) = errors.error.lock().ok().and_then(|mut s| s.take()) {
                    return Err(cb_err);
                }
                return Err(conn_err(e));
            }
            Ok(_) => {}
        }
        if let Some(e) = errors.error.lock().ok().and_then(|mut s| s.take()) {
            return Err(e);
        }
        Ok(())
    }
}

/// `APIConnector.from_uri(uri, label)`'s attributes: the connector parameters a URI describes
/// (`protocol`, `host`, `port`, `username`, `password`, `label`).
#[pyfunction]
#[pyo3(signature = (uri, label=None))]
pub fn api_uri_params<'py>(
    py: Python<'py>,
    uri: &str,
    label: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let params =
        meerschaum_connector_api::client::uri::from_uri_params(uri, label).map_err(conn_err)?;
    core_to_py(py, &Value::Map(params))
}

/// Register the API client's bindings.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    install_warning_sink();
    m.add_class::<PyApiConnector>()?;
    m.add_class::<PyApiResponse>()?;
    m.add_function(wrap_pyfunction!(api_uri_params, m)?)?;
    Ok(())
}
