//! `meerschaum-connector-valkey` bound into `meerschaum._core`: the `ValkeyConnector` behind the v4
//! `meerschaum.connectors.valkey.ValkeyConnector` façade (design §4.4, D12, D20).
//!
//! `_core.ValkeyConnector(label, attributes)` is built from the attributes the Python connector
//! resolved (`host`, `port`, `username`, `password`, `db`, `socket_timeout`, `uri`). Like the
//! `valkey` client it replaces, it does not connect until the first command: the URI and the key
//! helpers never need a server. Commands run on the extension's Tokio runtime with the GIL
//! released; frames cross as Arrow ([`crate::pandas_bridge`]). The quirks the Rust crate keeps
//! on purpose (inclusive `end` through `ZRANGEBYSCORE`, unsorted documents, `get_sync_time`
//! ignoring `params`) are the crate's, not this file's.

use std::sync::{Arc, Mutex};

use meerschaum_connector::pipe_ops::Doc;
use meerschaum_connector::{ConnectorError, InstanceConnector, SyncOptions};
use meerschaum_connector_valkey::ValkeyConnector as RsValkeyConnector;
use meerschaum_connector_valkey::keys;
use meerschaum_core::{Map, SuccessTuple, Value};
use pyo3::exceptions::{PyConnectionError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

use crate::api_client_api::{
    bound_out, conn_err, docs_out, frame_out, id_pipe, keys_query, pipe_id_out, pipes_keys_out,
    sync_frame,
};
use crate::convert::{PyCtx, core_to_py, py_to_cell, py_to_core};
use crate::pandas_bridge::{ColumnMode, frame_from_py};
use crate::pipe_bridge::{
    ParamsView, core_pipe, core_user, data_query, date_bound, params_map, plugin_record, str_list,
    success_to_py,
};
use crate::runtime::block_on;

fn opt_string_out<'py>(py: Python<'py>, s: Option<String>) -> PyResult<Bound<'py, PyAny>> {
    match s {
        Some(s) => Ok(pyo3::types::PyString::new(py, &s).into_any()),
        None => Ok(py.None().into_bound(py)),
    }
}

/// The documents of a Python list of dictionaries, as kernel documents.
fn docs_in(ctx: &PyCtx<'_>, docs: &Bound<'_, PyAny>) -> PyResult<Vec<Doc>> {
    let mut out = Vec::new();
    for item in docs.try_iter()? {
        let item = item?;
        let d = item
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("A document must be a dictionary."))?;
        let mut doc = Doc::new();
        for (k, v) in d.iter() {
            doc.insert(k.str()?.to_str()?.to_string(), py_to_cell(ctx, &v)?);
        }
        out.push(doc);
    }
    Ok(out)
}

fn key_err(e: String) -> PyErr {
    PyValueError::new_err(e)
}

/// What a Python `ValkeyConnector` and the native calls on its keys share: the attributes and the
/// live connection, so one process connects once (the native context finds it through
/// [`crate::facade::held_valkey`]).
pub(crate) struct ValkeyShared {
    pub(crate) attributes: Map,
    /// The live connector, built (and connected) by the first command.
    live: Mutex<Option<Arc<RsValkeyConnector>>>,
}

impl ValkeyShared {
    /// The connection, if one was made.
    pub(crate) fn live(&self) -> Option<Arc<RsValkeyConnector>> {
        self.live.lock().ok().and_then(|slot| slot.clone())
    }

    /// Keep `connector` as the connection unless one exists; returns the one in use.
    pub(crate) fn set_live(&self, connector: Arc<RsValkeyConnector>) -> Arc<RsValkeyConnector> {
        match self.live.lock() {
            Ok(mut slot) => slot.get_or_insert(connector).clone(),
            Err(_) => connector,
        }
    }
}

/// The `valkey:<label>` instance connector.
#[pyclass(frozen, name = "ValkeyConnector", module = "meerschaum._core")]
pub struct PyValkeyConnector {
    label: String,
    shared: Arc<ValkeyShared>,
    /// The attribute-only connector (an in-memory backend): URIs and keys never connect.
    probe: RsValkeyConnector,
}

impl PyValkeyConnector {
    /// The connected connector (the first call connects; a failure is a `ConnectionError`
    /// and is retried by the next call).
    fn conn(&self, py: Python<'_>) -> PyResult<Arc<RsValkeyConnector>> {
        if let Some(c) = self.shared.live() {
            return Ok(c);
        }
        let (label, attributes) = (self.label.clone(), self.shared.attributes.clone());
        let built = block_on(py, async move {
            RsValkeyConnector::connect(&label, attributes).await
        })
        .map_err(conn_err)?;
        Ok(self.shared.set_live(Arc::new(built)))
    }
}

#[pymethods]
impl PyValkeyConnector {
    /// `ValkeyConnector(label, attributes)`.
    #[new]
    fn new(label: &str, attributes: &Bound<'_, PyDict>) -> PyResult<Self> {
        let attrs: Map = match py_to_core(attributes.as_any())? {
            Value::Map(m) => m.into_iter().filter(|(_, v)| !v.is_null()).collect(),
            _ => Map::new(),
        };
        if !attrs.contains_key("uri") && !attrs.contains_key("host") {
            return Err(PyValueError::new_err(format!(
                "Missing required attribute 'host' for connector 'valkey:{label}'."
            )));
        }
        let probe = RsValkeyConnector::with_backend(
            label,
            attrs.clone(),
            Arc::new(meerschaum_connector_valkey::MemoryBackend::new()),
        );
        let shared = Arc::new(ValkeyShared {
            attributes: attrs.clone(),
            live: Mutex::new(None),
        });
        crate::facade::hold_valkey(label, &attrs, &shared);
        Ok(PyValkeyConnector {
            label: label.to_string(),
            shared,
            probe,
        })
    }

    fn __repr__(&self) -> String {
        format!("_core.ValkeyConnector('{}')", self.label)
    }

    /// `self.URI`.
    #[getter]
    fn uri(&self) -> String {
        self.probe.uri()
    }

    /// The URL handed to the client library (`redis://` scheme).
    #[getter]
    fn connection_url(&self) -> String {
        self.probe.connection_url()
    }

    // -----------------------------------------------------------------------------------------
    // Keys

    #[staticmethod]
    fn quote_table(table: &str) -> String {
        keys::quote_table(table)
    }

    #[staticmethod]
    fn get_counter_key(table: &str) -> String {
        keys::get_counter_key(table)
    }

    #[staticmethod]
    fn get_datetime_column_key(table: &str) -> String {
        keys::get_datetime_column_key(table)
    }

    #[staticmethod]
    #[pyo3(signature = (*parts))]
    fn get_entity_key(parts: &Bound<'_, PyTuple>) -> PyResult<String> {
        let mut texts = Vec::new();
        for p in parts.iter() {
            texts.push(p.str()?.to_str()?.to_string());
        }
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        keys::get_entity_key(&refs).map_err(key_err)
    }

    #[staticmethod]
    #[pyo3(signature = (user_id_or_username, sub_key, by_username=false))]
    fn get_user_key(
        user_id_or_username: &str,
        sub_key: &str,
        by_username: bool,
    ) -> PyResult<String> {
        keys::get_user_key(user_id_or_username, sub_key, by_username).map_err(key_err)
    }

    #[staticmethod]
    fn get_plugin_key(plugin_name: &str, sub_key: &str) -> PyResult<String> {
        keys::get_plugin_key(plugin_name, sub_key).map_err(key_err)
    }

    #[staticmethod]
    fn get_pipe_key(connector_keys: &str, metric_key: &str, location_key: Option<&str>) -> String {
        keys::get_pipe_key(connector_keys, metric_key, location_key)
    }

    #[staticmethod]
    #[pyo3(signature = (doc, indices, table_name=None))]
    fn get_document_key(
        py: Python<'_>,
        doc: &Bound<'_, PyDict>,
        indices: Vec<String>,
        table_name: Option<String>,
    ) -> PyResult<String> {
        let ctx = PyCtx::scalars(py)?;
        let mut d = Doc::new();
        for (k, v) in doc.iter() {
            d.insert(k.str()?.to_str()?.to_string(), py_to_cell(&ctx, &v)?);
        }
        Ok(keys::get_document_key(&d, &indices, table_name.as_deref()))
    }

    /// `get_user_keys_vals(user, password_hash, mutable_only)`: `None` values are unset.
    #[staticmethod]
    #[pyo3(signature = (user, password_hash=None, mutable_only=false))]
    fn get_user_keys_vals<'py>(
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        password_hash: Option<String>,
        mutable_only: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let u = core_user(user)?;
        let vals = RsValkeyConnector::get_user_keys_vals(&u, password_hash, mutable_only)
            .map_err(key_err)?;
        let out = PyDict::new(py);
        for (k, v) in vals {
            out.set_item(k, v)?;
        }
        Ok(out)
    }

    #[staticmethod]
    #[pyo3(signature = (plugin, mutable_only=false))]
    fn get_plugin_keys_vals<'py>(
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        mutable_only: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let rec = plugin_record(plugin, true)?;
        let vals = RsValkeyConnector::get_plugin_keys_vals(&rec, mutable_only).map_err(key_err)?;
        let out = PyDict::new(py);
        for (k, v) in vals {
            out.set_item(k, v)?;
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------------------------
    // Commands

    /// `set(key, value, ex=None)`.
    #[pyo3(signature = (key, value, ex=None, **kw))]
    fn set(
        &self,
        py: Python<'_>,
        key: &str,
        value: &Bound<'_, PyAny>,
        ex: Option<i64>,
        kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        if let Some(kw) = kw.filter(|k| !k.is_empty()) {
            let names: Vec<String> = kw
                .keys()
                .iter()
                .filter_map(|k| k.str().ok().map(|s| s.to_string()))
                .collect();
            return Err(PyTypeError::new_err(format!(
                "set() does not take {}; use `ex`.",
                names.join(", ")
            )));
        }
        let text = if let Ok(b) = value.cast::<PyBytes>() {
            String::from_utf8_lossy(b.as_bytes()).into_owned()
        } else {
            value.str()?.to_str()?.to_string()
        };
        let c = self.conn(py)?;
        let key = key.to_string();
        block_on(py, async move {
            match ex {
                Some(seconds) => c.set_ex(&key, &text, seconds).await,
                None => c.set(&key, &text).await,
            }
        })
        .map_err(conn_err)?;
        Ok(true)
    }

    /// `get(key, decode=True)`.
    #[pyo3(signature = (key, decode=true))]
    fn get<'py>(&self, py: Python<'py>, key: &str, decode: bool) -> PyResult<Bound<'py, PyAny>> {
        let c = self.conn(py)?;
        let key = key.to_string();
        let val = block_on(py, async move { c.get(&key).await }).map_err(conn_err)?;
        Ok(match val {
            None => py.None().into_bound(py),
            Some(s) if decode => pyo3::types::PyString::new(py, &s).into_any(),
            Some(s) => PyBytes::new(py, s.as_bytes()).into_any(),
        })
    }

    /// `execute(command, *args)`: one of the raw commands plugins use through
    /// `ValkeyConnector.client` (`GET`, `SET [EX|PX n] [NX]`, `DEL`, `UNLINK`, `EXISTS`, `INCR`,
    /// `TYPE`, `KEYS`, `PING`, `ZADD`, `ZREM`, `ZCARD`, `ZRANGE [WITHSCORES]`, `ZRANGEBYSCORE`,
    /// `SADD`, `SREM`, `SCARD`, `SMEMBERS`, `LLEN`, `RPUSH`, `LRANGE`, `HSET`, `HGETALL`,
    /// `FLUSHDB`). Strings come back as `bytes`, as `valkey-py` returns them.
    #[pyo3(signature = (command, *args))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        command: &str,
        args: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut texts: Vec<String> = Vec::with_capacity(args.len());
        for a in args.iter() {
            texts.push(if let Ok(b) = a.cast::<PyBytes>() {
                String::from_utf8_lossy(b.as_bytes()).into_owned()
            } else {
                a.str()?.to_str()?.to_string()
            });
        }
        let name = command.to_uppercase();
        let c = self.conn(py)?;
        let backend = c.backend().clone();
        let need = |n: usize| -> PyResult<()> {
            if texts.len() < n {
                Err(PyTypeError::new_err(format!(
                    "{name} takes at least {n} argument(s)."
                )))
            } else {
                Ok(())
            }
        };
        let bounds = |text: &str| -> PyResult<meerschaum_connector_valkey::ScoreBound> {
            Ok(match text {
                "-inf" => meerschaum_connector_valkey::ScoreBound::NegInf,
                "+inf" | "inf" => meerschaum_connector_valkey::ScoreBound::PosInf,
                other => meerschaum_connector_valkey::ScoreBound::Value(
                    other
                        .trim_start_matches('(')
                        .parse::<f64>()
                        .map_err(|_| PyValueError::new_err(format!("Invalid score '{other}'.")))?,
                ),
            })
        };
        let int = |text: &str| -> PyResult<i64> {
            text.parse::<i64>()
                .map_err(|_| PyValueError::new_err(format!("Invalid integer '{text}'.")))
        };
        let be = |e: meerschaum_connector_valkey::BackendError| PyConnectionError::new_err(e.0);
        let bytes_list = |py: Python<'py>, items: Vec<String>| -> PyResult<Bound<'py, PyAny>> {
            let out = PyList::empty(py);
            for item in items {
                out.append(PyBytes::new(py, item.as_bytes()))?;
            }
            Ok(out.into_any())
        };
        match name.as_str() {
            "PING" => {
                let ok = block_on(py, async move { backend.ping().await }).map_err(be)?;
                Ok(ok.into_pyobject(py)?.to_owned().into_any())
            }
            "GET" => {
                need(1)?;
                let key = texts[0].clone();
                let v = block_on(py, async move { backend.get(&key).await }).map_err(be)?;
                Ok(match v {
                    Some(s) => PyBytes::new(py, s.as_bytes()).into_any(),
                    None => py.None().into_bound(py),
                })
            }
            "SET" => {
                need(2)?;
                let (key, value) = (texts[0].clone(), texts[1].clone());
                let mut ex: Option<i64> = None;
                let mut px: Option<i64> = None;
                let mut nx = false;
                let mut i = 2;
                while i < texts.len() {
                    match texts[i].to_uppercase().as_str() {
                        "EX" => {
                            ex = Some(int(texts.get(i + 1).map(String::as_str).unwrap_or(""))?);
                            i += 1;
                        }
                        "PX" => {
                            px = Some(int(texts.get(i + 1).map(String::as_str).unwrap_or(""))?);
                            i += 1;
                        }
                        "NX" => nx = true,
                        other => {
                            return Err(PyTypeError::new_err(format!(
                                "SET does not support '{other}'."
                            )));
                        }
                    }
                    i += 1;
                }
                if nx && px.is_none() && ex.is_none() {
                    return Err(PyValueError::new_err(
                        "SET NX needs EX or PX in the built-in client (an atomic NX without an \
                         expiration is not available); install the `valkey` package for the \
                         full client.",
                    ));
                }
                let set = block_on(py, async move {
                    if nx {
                        backend
                            .set_nx_px(&key, &value, px.unwrap_or(ex.unwrap_or(0) * 1000) as u64)
                            .await
                    } else if let Some(seconds) = ex {
                        backend.set_ex(&key, &value, seconds).await.map(|_| true)
                    } else {
                        backend.set(&key, &value).await.map(|_| true)
                    }
                })
                .map_err(be)?;
                Ok(if set {
                    true.into_pyobject(py)?.to_owned().into_any()
                } else {
                    py.None().into_bound(py)
                })
            }
            "DEL" | "UNLINK" => {
                need(1)?;
                let keys = texts.clone();
                let n = block_on(py, async move { backend.del(&keys).await }).map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "EXISTS" => {
                need(1)?;
                let keys = texts.clone();
                let n = block_on(py, async move {
                    let mut total = 0;
                    for k in &keys {
                        total += backend.exists(k).await?;
                    }
                    Ok::<_, meerschaum_connector_valkey::BackendError>(total)
                })
                .map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "INCR" => {
                need(1)?;
                let key = texts[0].clone();
                let n = block_on(py, async move { backend.incr(&key).await }).map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "TYPE" => {
                need(1)?;
                let key = texts[0].clone();
                let t = block_on(py, async move { backend.key_type(&key).await }).map_err(be)?;
                Ok(PyBytes::new(py, t.as_bytes()).into_any())
            }
            "KEYS" => {
                need(1)?;
                let pattern = texts[0].clone();
                let keys = block_on(py, async move { backend.keys(&pattern).await }).map_err(be)?;
                bytes_list(py, keys)
            }
            "ZADD" => {
                need(3)?;
                let key = texts[0].clone();
                let rest: Vec<String> = texts[1..].to_vec();
                if !rest.len().is_multiple_of(2) {
                    return Err(PyTypeError::new_err("ZADD takes score and member pairs."));
                }
                let mut pairs = Vec::new();
                for pair in rest.chunks(2) {
                    let score = pair[0].parse::<f64>().map_err(|_| {
                        PyValueError::new_err(format!("Invalid score '{}'.", pair[0]))
                    })?;
                    pairs.push((pair[1].clone(), score));
                }
                let n = pairs.len() as i64;
                block_on(py, async move {
                    for (member, score) in &pairs {
                        backend.zadd(&key, member, *score).await?;
                    }
                    Ok::<_, meerschaum_connector_valkey::BackendError>(())
                })
                .map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "ZREM" | "SREM" => {
                need(2)?;
                let key = texts[0].clone();
                let members: Vec<String> = texts[1..].to_vec();
                let is_z = name == "ZREM";
                let n = block_on(py, async move {
                    let mut total = 0;
                    for m in &members {
                        total += if is_z {
                            backend.zrem(&key, m).await?
                        } else {
                            backend.srem(&key, m).await?
                        };
                    }
                    Ok::<_, meerschaum_connector_valkey::BackendError>(total)
                })
                .map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "SADD" => {
                need(2)?;
                let key = texts[0].clone();
                let members: Vec<String> = texts[1..].to_vec();
                let n = members.len() as i64;
                block_on(py, async move {
                    for m in &members {
                        backend.sadd(&key, m).await?;
                    }
                    Ok::<_, meerschaum_connector_valkey::BackendError>(())
                })
                .map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "ZCARD" | "SCARD" | "LLEN" => {
                need(1)?;
                let key = texts[0].clone();
                let which = name.clone();
                let n = block_on(py, async move {
                    match which.as_str() {
                        "ZCARD" => backend.zcard(&key).await,
                        "SCARD" => backend.scard(&key).await,
                        _ => backend.llen(&key).await,
                    }
                })
                .map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "ZRANGE" => {
                need(3)?;
                let key = texts[0].clone();
                let (start, stop) = (int(&texts[1])?, int(&texts[2])?);
                let with_scores = texts[3..]
                    .iter()
                    .any(|t| t.eq_ignore_ascii_case("WITHSCORES"));
                let rev = texts[3..].iter().any(|t| t.eq_ignore_ascii_case("REV"));
                let members = block_on(
                    py,
                    async move { backend.zrange(&key, start, stop, rev).await },
                )
                .map_err(be)?;
                let out = PyList::empty(py);
                for (member, score) in members {
                    let m = PyBytes::new(py, member.as_bytes());
                    if with_scores {
                        out.append(PyTuple::new(
                            py,
                            [m.into_any(), score.into_pyobject(py)?.into_any()],
                        )?)?;
                    } else {
                        out.append(m)?;
                    }
                }
                Ok(out.into_any())
            }
            "ZRANGEBYSCORE" => {
                need(3)?;
                let key = texts[0].clone();
                let (min, max) = (bounds(&texts[1])?, bounds(&texts[2])?);
                let members = block_on(
                    py,
                    async move { backend.zrangebyscore(&key, min, max).await },
                )
                .map_err(be)?;
                bytes_list(py, members)
            }
            "SMEMBERS" => {
                need(1)?;
                let key = texts[0].clone();
                let members =
                    block_on(py, async move { backend.smembers(&key).await }).map_err(be)?;
                bytes_list(py, members)
            }
            "RPUSH" => {
                need(2)?;
                let key = texts[0].clone();
                let values: Vec<String> = texts[1..].to_vec();
                let n = block_on(py, async move {
                    for v in &values {
                        backend.rpush(&key, v).await?;
                    }
                    backend.llen(&key).await
                })
                .map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "LRANGE" => {
                need(3)?;
                let key = texts[0].clone();
                let (start, stop) = (int(&texts[1])?, int(&texts[2])?);
                let items = block_on(py, async move { backend.lrange(&key, start, stop).await })
                    .map_err(be)?;
                bytes_list(py, items)
            }
            "HSET" => {
                need(3)?;
                let key = texts[0].clone();
                if !texts[1..].len().is_multiple_of(2) {
                    return Err(PyTypeError::new_err("HSET takes field and value pairs."));
                }
                let fields: Vec<(String, String)> = texts[1..]
                    .chunks(2)
                    .map(|p| (p[0].clone(), p[1].clone()))
                    .collect();
                let n = fields.len() as i64;
                block_on(py, async move { backend.hset(&key, &fields).await }).map_err(be)?;
                Ok(n.into_pyobject(py)?.into_any())
            }
            "HGETALL" => {
                need(1)?;
                let key = texts[0].clone();
                let fields =
                    block_on(py, async move { backend.hgetall(&key).await }).map_err(be)?;
                let out = PyDict::new(py);
                for (f, v) in fields {
                    out.set_item(
                        PyBytes::new(py, f.as_bytes()),
                        PyBytes::new(py, v.as_bytes()),
                    )?;
                }
                Ok(out.into_any())
            }
            "FLUSHDB" => {
                block_on(py, async move { backend.flushdb().await }).map_err(be)?;
                Ok(true.into_pyobject(py)?.to_owned().into_any())
            }
            other => Err(PyValueError::new_err(format!(
                "The command {other} is not supported by the built-in client; \
                 install the `valkey` package for the full client."
            ))),
        }
    }

    /// `test_connection()`: `PING`.
    #[pyo3(signature = (**_kw))]
    fn test_connection(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> PyResult<bool> {
        let c = self.conn(py)?;
        block_on(py, async move { c.ping().await }).map_err(conn_err)
    }

    /// `push_df(df, table, datetime_column=None)`: the number of new documents.
    #[pyo3(signature = (df, table, datetime_column=None, **_kw))]
    fn push_df(
        &self,
        py: Python<'_>,
        df: &Bound<'_, PyAny>,
        table: &str,
        datetime_column: Option<String>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<i64> {
        let frame = sync_frame(py, df)?;
        let c = self.conn(py)?;
        let table = table.to_string();
        block_on(py, async move {
            c.push_df(&frame, &table, datetime_column.as_deref()).await
        })
        .map_err(conn_err)
    }

    /// `push_docs(docs, table, datetime_column=None)`: the number of new documents.
    #[pyo3(signature = (docs, table, datetime_column=None, **_kw))]
    fn push_docs(
        &self,
        py: Python<'_>,
        docs: &Bound<'_, PyAny>,
        table: &str,
        datetime_column: Option<String>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<i64> {
        let ctx = PyCtx::scalars(py)?;
        let docs = docs_in(&ctx, docs)?;
        let c = self.conn(py)?;
        let table = table.to_string();
        block_on(py, async move {
            c.push_docs(&docs, &table, datetime_column.as_deref()).await
        })
        .map_err(conn_err)
    }

    /// `read(table, begin, end, params, datetime_column, select_columns, omit_columns)`.
    #[pyo3(signature = (
        table, begin=None, end=None, params=None, datetime_column=None, select_columns=None,
        omit_columns=None, **_kw
    ))]
    #[allow(clippy::too_many_arguments)]
    fn read<'py>(
        &self,
        py: Python<'py>,
        table: &str,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
        datetime_column: Option<String>,
        select_columns: Option<&Bound<'py, PyAny>>,
        omit_columns: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = PyCtx::scalars(py)?;
        let begin = date_bound(&ctx, begin)?.map(|b| b.to_value());
        let end = date_bound(&ctx, end)?.map(|b| b.to_value());
        let params = params_map(params)?;
        let select = str_list(select_columns)?;
        let omit = str_list(omit_columns)?;
        let c = self.conn(py)?;
        let table = table.to_string();
        let frame = block_on(py, async move {
            c.read(
                &table,
                begin.as_ref(),
                end.as_ref(),
                params.as_ref(),
                datetime_column.as_deref(),
                select,
                omit,
            )
            .await
        })
        .map_err(conn_err)?;
        match frame {
            Some(f) => frame_out(py, &f),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// `read_docs(table, begin, end)`.
    #[pyo3(signature = (table, begin=None, end=None, **_kw))]
    fn read_docs<'py>(
        &self,
        py: Python<'py>,
        table: &str,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let ctx = PyCtx::scalars(py)?;
        let begin = date_bound(&ctx, begin)?.map(|b| b.to_value());
        let end = date_bound(&ctx, end)?.map(|b| b.to_value());
        let c = self.conn(py)?;
        let table = table.to_string();
        let docs = block_on(py, async move {
            c.read_docs(&table, begin.as_ref(), end.as_ref()).await
        })
        .map_err(conn_err)?;
        docs_out(py, &docs)
    }

    /// `drop_table(table)`.
    #[pyo3(signature = (table, **_kw))]
    fn drop_table(
        &self,
        py: Python<'_>,
        table: &str,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let c = self.conn(py)?;
        let table = table.to_string();
        block_on(py, async move { c.drop_table(&table).await }).map_err(conn_err)
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
        let c = self.conn(py)?;
        let res = block_on(py, async move { c.register_pipe(&p).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_id<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = id_pipe(pipe)?;
        let c = self.conn(py)?;
        let id = block_on(
            py,
            async move { InstanceConnector::get_pipe_id(&*c, &p).await },
        )
        .map_err(conn_err)?;
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
        let c = self.conn(py)?;
        let attrs = block_on(py, async move {
            InstanceConnector::get_pipe_attributes(&*c, &p).await
        })
        .map_err(conn_err)?;
        core_to_py(py, &Value::Map(attrs))
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn edit_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Raw)?;
        let c = self.conn(py)?;
        let res = block_on(py, async move { c.edit_pipe(&p).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn pipe_exists(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<bool> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn(py)?;
        block_on(py, async move { c.pipe_exists(&p).await }).map_err(conn_err)
    }

    #[pyo3(signature = (pipe, **_kw))]
    fn drop_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn(py)?;
        let res = block_on(
            py,
            async move { InstanceConnector::drop_pipe(&*c, &p).await },
        );
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
        let c = self.conn(py)?;
        let res = block_on(
            py,
            async move { InstanceConnector::delete_pipe(&*c, &p).await },
        );
        success_to_py(py, &res)
    }

    #[pyo3(signature = (
        pipe, select_columns=None, omit_columns=None, begin=None, end=None, params=None, **_kw
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
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let q = data_query(
            &ctx,
            select_columns,
            omit_columns,
            begin,
            end,
            params,
            None,
            None,
        )?;
        let c = self.conn(py)?;
        let frame = block_on(py, async move { c.get_pipe_data(&p, &q).await }).map_err(conn_err)?;
        match frame {
            Some(f) => frame_out(py, &f),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// `sync_pipe(pipe, df=None, check_existing=True)`.
    #[pyo3(signature = (pipe, df=None, check_existing=true, **_kw))]
    fn sync_pipe<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        df: Option<&Bound<'py, PyAny>>,
        check_existing: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let Some(df) = df.filter(|d| !d.is_none()) else {
            let msg = format!("DataFrame is None. Cannot sync {}.", pipe.str()?);
            return success_to_py(py, &SuccessTuple::fail(msg));
        };
        let ctx = PyCtx::new(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let frame = if df.hasattr("columns")? && df.hasattr("dtypes")? {
            frame_from_py(&ctx, df, ColumnMode::Full)?.frame
        } else {
            sync_frame(py, df)?
        };
        let opts = SyncOptions {
            check_existing,
            ..Default::default()
        };
        let c = self.conn(py)?;
        let before = p.parameters().get("valkey").cloned();
        let (res, after) = block_on(py, async move {
            let res = c.sync_pipe(&p, frame, &opts).await;
            // A sync records the columns' dtypes under `parameters['valkey']` (v4 calls
            // `pipe.update_parameters(...)`, which also updates the Python pipe).
            let after = if res.success {
                c.get_pipe_attributes(&p)
                    .await
                    .ok()
                    .and_then(|attrs| attrs.get("parameters").cloned())
                    .and_then(|params| params.get("valkey").cloned())
            } else {
                None
            };
            (res, after)
        });
        if let Some(after) = after.filter(|a| Some(a) != before.as_ref()) {
            let mut patch = Map::new();
            patch.insert("valkey".into(), after);
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

    #[pyo3(signature = (pipe, **_kw))]
    fn get_pipe_columns_types<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn(py)?;
        let types =
            block_on(py, async move { c.get_pipe_columns_types(&p).await }).map_err(conn_err)?;
        let out = PyDict::new(py);
        for (k, v) in types {
            out.set_item(k, v)?;
        }
        Ok(out)
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
        let c = self.conn(py)?;
        let res = block_on(py, async move {
            InstanceConnector::clear_pipe(&*c, &p, &q).await
        });
        success_to_py(py, &res)
    }

    /// `get_sync_time(pipe, newest=True)`: `params` is ignored, as in v4.
    #[pyo3(signature = (pipe, newest=true, **_kw))]
    fn get_sync_time<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        newest: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let c = self.conn(py)?;
        let st =
            block_on(py, async move { c.get_sync_time(&p, newest).await }).map_err(conn_err)?;
        bound_out(py, st.as_ref())
    }

    #[pyo3(signature = (pipe, begin=None, end=None, params=None, **_kw))]
    fn get_pipe_rowcount(
        &self,
        py: Python<'_>,
        pipe: &Bound<'_, PyAny>,
        begin: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        params: Option<&Bound<'_, PyAny>>,
        _kw: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Option<i64>> {
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let q = data_query(&ctx, None, None, begin, end, params, None, None)?;
        let c = self.conn(py)?;
        block_on(py, async move { c.get_pipe_rowcount(&p, &q).await }).map_err(conn_err)
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
        let c = self.conn(py)?;
        let keys = block_on(py, async move { c.fetch_pipes_keys(&query).await })
            .map_err(|e: ConnectorError| e)
            .map_err(conn_err)?;
        pipes_keys_out(py, &meerschaum_connector::PipesKeys::List(keys))
    }

    /// `fetch(pipe, begin, end)`: the documents of the pipe's source key.
    #[pyo3(signature = (pipe, begin=None, end=None, params=None, **_kw))]
    fn fetch<'py>(
        &self,
        py: Python<'py>,
        pipe: &Bound<'py, PyAny>,
        begin: Option<&Bound<'py, PyAny>>,
        end: Option<&Bound<'py, PyAny>>,
        params: Option<&Bound<'py, PyAny>>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let ctx = PyCtx::scalars(py)?;
        let p = core_pipe(pipe, ParamsView::Resolved)?;
        let q = data_query(&ctx, None, None, begin, end, params, None, None)?;
        let c = self.conn(py)?;
        let docs = block_on(py, async move { c.fetch_docs(&p, &q).await }).map_err(conn_err)?;
        docs_out(py, &docs)
    }

    // -----------------------------------------------------------------------------------------
    // Users

    #[pyo3(signature = (user, **_kw))]
    fn register_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let mut u = core_user(user)?;
        let c = self.conn(py)?;
        let (res, u) = block_on(py, async move {
            let res = c.valkey_register_user(&mut u).await;
            (res, u)
        });
        // v4 sets the new `user_id` on the user it registers.
        if let Some(id) = &u.user_id {
            user.setattr("user_id", core_to_py(py, id)?)?;
        }
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
        let c = self.conn(py)?;
        let id = block_on(py, async move { c.valkey_get_user_id(&u).await });
        opt_string_out(py, id)
    }

    #[pyo3(signature = (user, **_kw))]
    fn edit_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let u = core_user(user)?;
        let c = self.conn(py)?;
        let res = block_on(py, async move { c.valkey_edit_user(&u).await });
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
        let c = self.conn(py)?;
        let attrs = block_on(py, async move { c.valkey_get_user_attributes(&u).await });
        match attrs {
            Some(m) => core_to_py(py, &Value::Map(m)),
            None => Ok(py.None().into_bound(py)),
        }
    }

    #[pyo3(signature = (user, **_kw))]
    fn delete_user<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let u = core_user(user)?;
        let c = self.conn(py)?;
        let res = block_on(py, async move { c.valkey_delete_user(&u).await });
        success_to_py(py, &res)
    }

    #[pyo3(signature = (**_kw))]
    fn get_users(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<String>> {
        let c = self.conn(py)?;
        block_on(py, async move { c.valkey_get_users().await }).map_err(conn_err)
    }

    #[pyo3(signature = (user, **_kw))]
    fn get_user_password_hash<'py>(
        &self,
        py: Python<'py>,
        user: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let u = core_user(user)?;
        let c = self.conn(py)?;
        let h = block_on(py, async move { c.valkey_get_user_password_hash(&u).await });
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
        let c = self.conn(py)?;
        let t = block_on(py, async move { c.valkey_get_user_type(&u).await });
        opt_string_out(py, t)
    }

    // -----------------------------------------------------------------------------------------
    // Plugins

    #[pyo3(signature = (plugin, force=false, **_kw))]
    fn register_plugin<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        force: bool,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let _ = force;
        let rec = plugin_record(plugin, true)?;
        let c = self.conn(py)?;
        let res = block_on(py, async move { c.valkey_register_plugin(&rec).await });
        success_to_py(py, &res)
    }

    /// `get_plugin_id(plugin)`: the plugin's ID is its name when it is registered.
    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_id<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rec = plugin_record(plugin, false)?;
        let name = rec.name.clone();
        let c = self.conn(py)?;
        let user_id = block_on(py, async move { c.valkey_get_plugin_user_id(&rec).await });
        opt_string_out(py, user_id.map(|_| name))
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_version<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn(py)?;
        let v = block_on(py, async move { c.valkey_get_plugin_version(&rec).await });
        opt_string_out(py, v)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_user_id<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn(py)?;
        let v = block_on(py, async move { c.valkey_get_plugin_user_id(&rec).await });
        opt_string_out(py, v)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_username<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn(py)?;
        let v = block_on(py, async move { c.valkey_get_plugin_username(&rec).await });
        opt_string_out(py, v)
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn get_plugin_attributes<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn(py)?;
        let attrs = block_on(
            py,
            async move { c.valkey_get_plugin_attributes(&rec).await },
        );
        core_to_py(py, &Value::Map(attrs))
    }

    #[pyo3(signature = (plugin, **_kw))]
    fn delete_plugin<'py>(
        &self,
        py: Python<'py>,
        plugin: &Bound<'py, PyAny>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let rec = plugin_record(plugin, false)?;
        let c = self.conn(py)?;
        let res = block_on(py, async move { c.valkey_delete_plugin(&rec).await });
        success_to_py(py, &res)
    }
}

/// Register the Valkey bindings.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyValkeyConnector>()?;
    Ok(())
}
