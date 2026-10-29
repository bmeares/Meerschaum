//! Native (Rust) plugins from the Python façade: `_core.native_plugin_names`,
//! `_core.native_connector_register`, `_core.native_connector_types` and
//! `_core.NativeConnector`.
//!
//! A native plugin (a single `.rs` file or a native directory in a plugins directory) is not a
//! Python module, so `PluginConnector` cannot import it. The façade asks these bindings which
//! plugins are native and runs their `register(pipe)` hook in-process, exactly as the CLI's
//! `register pipes` does (`meerschaum_cli::native::manage::native_connector_register`).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use meerschaum_connector::{Connector, ConnectorError, DataQuery, InstanceConnector, SyncOptions};
use meerschaum_core::{Map, SuccessTuple};
use meerschaum_plugin::NativePlugins;
use pyo3::exceptions::{PyAttributeError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::convert::{PyCtx, json_to_py};
use crate::pipe_bridge::{
    ParamsView, core_pipe, data_query, params_map, success_to_py, value_to_py,
};
use crate::runtime::block_on;

/// `native_plugin_names()`: the names of the native plugins in the configured plugin
/// directories, in priority order. A scan only: nothing is built or loaded.
#[pyfunction]
pub fn native_plugin_names(py: Python<'_>) -> PyResult<Vec<String>> {
    let config = crate::facade::merged_config(py)?;
    let dirs = config.paths().plugins_dirs.clone();
    Ok(py.detach(move || {
        meerschaum_plugin::discovery::discover_report(&dirs)
            .candidates
            .into_iter()
            .map(|candidate| candidate.name)
            .collect()
    }))
}

/// `native_connector_register(pipe)`: run the `register(pipe)` hook of the native plugin that
/// owns the pipe's connector (`plugin:<name>` or a native `#[make_connector]` type).
///
/// Returns `None` when no native plugin owns the connector or it defines no `register`, else
/// `(valid, parameters, repr)`: `valid` is whether the hook returned a dict (or nothing),
/// `repr` the returned value as text for the façade's warning. A failing hook raises
/// `RuntimeError`.
#[pyfunction]
pub fn native_connector_register<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    let core = core_pipe(pipe, ParamsView::Raw)?;
    let config = crate::facade::publish_live_config(py)?;
    let allow_writes = config_writes_allowed(py);
    let outcome = py.detach(move || {
        let paths = config.paths();
        let natives = Arc::new(
            NativePlugins::open(
                &paths.plugins_dirs,
                &meerschaum_cli::native_plugins::cache_path(&paths.root_dir),
            )
            .map_err(|e| e.to_string())?,
        );
        // The hook's host calls (`host::plugin_config()`, ...), with this process's trust.
        let host = facade_host_with(
            &config,
            natives.clone(),
            allow_writes,
            Some(core.instance_keys().to_string()),
        );
        meerschaum_cli::native::manage::native_connector_register_with_host(
            &natives,
            &config,
            &core,
            Some(host),
        )
        .transpose()
    });
    match outcome.map_err(PyRuntimeError::new_err)? {
        None => Ok(None),
        Some(outcome) => {
            let parameters =
                value_to_py(py, &meerschaum_core::Value::Map(outcome.parameters.clone()))?;
            Ok(Some(PyTuple::new(
                py,
                [
                    outcome.valid.into_pyobject(py)?.to_owned().into_any(),
                    parameters,
                    outcome.repr.into_pyobject(py)?.into_any(),
                ],
            )?))
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Connector types declared by native plugins (`#[make_connector] mod readings { ... }` or a
// native directory's manifest): `_core.native_connector_types` and `_core.NativeConnector`.
// ---------------------------------------------------------------------------------------------

/// The native plugins of the configured plugin directories, opened once per process (and per
/// root and plugin directories) and shared by the connector bindings, so a library is loaded
/// once. `refresh` reopens them (`unload_plugin_connectors()` on the Python side).
fn cached_natives(
    config: &meerschaum_core::config::Config,
    refresh: bool,
) -> Result<Arc<NativePlugins>, String> {
    type Key = (u32, Vec<PathBuf>, PathBuf);
    static CACHE: Mutex<Option<(Key, Arc<NativePlugins>)>> = Mutex::new(None);
    let paths = config.paths();
    let key: Key = (
        std::process::id(),
        paths.plugins_dirs.clone(),
        paths.root_dir.clone(),
    );
    let mut slot = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if !refresh
        && let Some((cached_key, natives)) = slot.as_ref()
        && *cached_key == key
    {
        return Ok(natives.clone());
    }
    let natives = Arc::new(
        NativePlugins::open(
            &paths.plugins_dirs,
            &meerschaum_cli::native_plugins::cache_path(&paths.root_dir),
        )
        .map_err(|e| e.to_string())?,
    );
    *slot = Some((key, natives.clone()));
    Ok(natives)
}

/// `native_connector_types(refresh=False)`: the connector types the native plugins declare, as
/// `{type: {'plugin', 'is_instance', 'is_streaming', 'is_thread_safe', 'methods'}}`.
///
/// A plugins directory without native plugins answers `{}` after a directory scan. Otherwise the
/// manifest cache is read (a hit loads no code); a single-file or source plugin whose build is
/// missing or stale is built first, as the CLI does.
#[pyfunction]
#[pyo3(signature = (refresh=false))]
pub fn native_connector_types(py: Python<'_>, refresh: bool) -> PyResult<Bound<'_, PyDict>> {
    let config = crate::facade::merged_config(py)?;
    let natives = py
        .detach(move || cached_natives(&config, refresh))
        .map_err(PyRuntimeError::new_err)?;
    let out = PyDict::new(py);
    let Some(types) = natives.manifest()["connector_types"].as_object() else {
        return Ok(out);
    };
    for (typ, desc) in types {
        let Some(plugin) = natives.connector_plugin(typ) else {
            continue;
        };
        let entry = PyDict::new(py);
        entry.set_item("plugin", plugin)?;
        for flag in ["is_instance", "is_streaming", "is_thread_safe"] {
            entry.set_item(flag, desc[flag].as_bool().unwrap_or(false))?;
        }
        entry.set_item("methods", json_to_py(py, &desc["methods"])?)?;
        out.set_item(typ, entry)?;
    }
    Ok(out)
}

/// `NativeConnector(type, label, attributes)`: a connector of a type a native plugin declares.
///
/// The Python façade (`meerschaum.connectors.native`) keeps one per connector and forwards its
/// methods here: `call()` runs any method of the connector module with the pipe as context (the
/// `register` hook, custom methods), and the instance methods (`register_pipe`, `get_pipe_id`,
/// ...) run through the plugin's `InstanceConnector`, as the CLI's `-i <type>:<label>` does.
/// An instance method the plugin does not define answers the v4 default of an
/// `InstanceConnector` without it (`None`, `False`, `{}`, `[]`, or a failed `SuccessTuple`).
#[pyclass(frozen, name = "NativeConnector", module = "meerschaum._core")]
pub struct PyNativeConnector {
    inner: Arc<meerschaum_plugin::NativeConnector>,
    keys: String,
}

impl PyNativeConnector {
    /// The connector with host services for this call (`host::pipe(...)`, `host::read_sql(...)`,
    /// `host::plugin_config()`), over the worker session's connectors and the process's live
    /// config as of now.
    fn hosted(&self, py: Python<'_>) -> PyResult<meerschaum_plugin::NativeConnector> {
        let config = crate::facade::publish_live_config(py)?;
        let natives = py
            .detach(|| cached_natives(&config, false))
            .map_err(PyRuntimeError::new_err)?;
        let host = facade_host(py, &config, natives);
        Ok(self.inner.as_ref().clone().with_host_services(host))
    }
}

/// Whether host calls may write config files as Python's `write_plugin_config()` does: not
/// inside `replace_config()` (`meerschaum.config._allow_write_missing`).
pub(crate) fn config_writes_allowed(py: Python<'_>) -> bool {
    py.import("meerschaum.config")
        .and_then(|m| m.getattr("_allow_write_missing"))
        .and_then(|v| v.extract::<bool>())
        .unwrap_or(true)
}

/// Host services with the trust of this Python process, over the worker session and a copy of
/// `config` (the live config's snapshot, which is read-only) that writes as Python would.
pub(crate) fn facade_host(
    py: Python<'_>,
    config: &meerschaum_core::config::Config,
    natives: Arc<NativePlugins>,
) -> Arc<dyn meerschaum_plugin::HostServices> {
    facade_host_with(config, natives, config_writes_allowed(py), None)
}

/// [`facade_host`] without the interpreter (`allow_writes` read beforehand); `instance` is the
/// default instance of `host::pipe(...)`.
fn facade_host_with(
    config: &meerschaum_core::config::Config,
    natives: Arc<NativePlugins>,
    allow_writes: bool,
    instance: Option<String>,
) -> Arc<dyn meerschaum_plugin::HostServices> {
    let config = meerschaum_cli::native_plugins::host_config(config);
    config.with_state(|s| s.set_allow_writes(allow_writes));
    meerschaum_cli::native_plugins::local_host(meerschaum_cli::native_plugins::HostEnv::new(
        crate::facade::worker_session(),
        config,
        natives,
        instance,
    ))
}

/// Take the plugin config sections native plugins wrote through host calls into the live
/// config dictionary (`meerschaum.config.config['plugins'][<name>]`) and into `config`, a
/// snapshot of it: Python's `get_plugin_config()` and the next native call see them, and
/// Python's own `write_plugin_config()` does not write the old sections back.
pub(crate) fn absorb_plugin_config_writes(
    py: Python<'_>,
    config: &meerschaum_core::config::Config,
) -> PyResult<()> {
    let written = meerschaum_cli::native_plugins::take_written_plugin_configs(config);
    if written.is_empty() {
        return Ok(());
    }
    let store = crate::config_api::store(py);
    // A `plugins` section Python never loaded is read from its file when it is first used.
    let plugins = match store.get_item("plugins")? {
        Some(plugins) if plugins.is_instance_of::<PyDict>() => plugins.cast_into::<PyDict>()?,
        _ => return Ok(()),
    };
    config.with_state(|s| {
        use meerschaum_core::config::ConfigValue;
        let mut in_memory = s.take_config_lossless();
        if let Some(section) = in_memory
            .get_mut("plugins")
            .and_then(ConfigValue::as_map_mut)
        {
            for (plugin, value) in &written {
                if let Ok(value) = serde_json::from_value::<meerschaum_core::Value>(value.clone()) {
                    section.insert(plugin.clone(), ConfigValue::from_value(&value));
                }
            }
        }
        s.set_config_lossless(in_memory);
    });
    for (plugin, value) in written {
        plugins.set_item(plugin, json_to_py(py, &value)?)?;
    }
    Ok(())
}

fn not_implemented_or<'py>(
    py: Python<'py>,
    error: ConnectorError,
    default: impl FnOnce(Python<'py>) -> PyResult<Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    match error {
        ConnectorError::NotImplemented(_) => default(py),
        other => Err(crate::api_client_api::conn_err(other)),
    }
}

fn none(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    Ok(py.None().into_bound(py))
}

fn empty_dict(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    Ok(PyDict::new(py).into_any())
}

fn serde_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<Bound<'py, PyAny>> {
    let json = serde_json::to_value(value).map_err(|e| PyValueError::new_err(e.to_string()))?;
    json_to_py(py, &json)
}

/// A keyword argument (`None` when missing or `None`).
fn kwarg<'py>(kw: Option<&Bound<'py, PyDict>>, name: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(match kw {
        Some(kw) => kw.get_item(name)?.filter(|v| !v.is_none()),
        None => None,
    })
}

fn kwarg_bool(kw: Option<&Bound<'_, PyDict>>, name: &str, default: bool) -> PyResult<bool> {
    Ok(match kwarg(kw, name)? {
        Some(v) => v.is_truthy()?,
        None => default,
    })
}

#[pymethods]
impl PyNativeConnector {
    #[new]
    #[pyo3(signature = (connector_type, label, attributes=None))]
    fn new(
        py: Python<'_>,
        connector_type: &str,
        label: &str,
        attributes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let config = crate::facade::merged_config(py)?;
        let attributes = match attributes {
            Some(a) => crate::convert::py_to_json(a)?,
            None => serde_json::Value::Object(Default::default()),
        };
        let (typ, lbl) = (connector_type.to_string(), label.to_string());
        let connector = py.detach(move || {
            let natives = cached_natives(&config, false)?;
            natives
                .connector(&typ, &lbl, attributes)
                .ok_or_else(|| format!("No native plugin declares the connector type '{typ}'."))?
                .map_err(|e| e.to_string())
        });
        let connector = connector.map_err(PyRuntimeError::new_err)?;
        Ok(Self {
            inner: Arc::new(connector),
            keys: format!("{connector_type}:{label}"),
        })
    }

    fn __repr__(&self) -> String {
        format!("NativeConnector('{}')", self.keys)
    }

    #[getter]
    fn is_instance(&self) -> bool {
        self.inner.is_instance()
    }

    #[getter]
    fn is_streaming(&self) -> bool {
        self.inner.is_streaming()
    }

    /// `call(method, pipe=None, kwargs=None)`: run `method` of the connector module with the
    /// pipe (keys, raw parameters, `temporary`) as `context.pipe` and `kwargs` as
    /// `context.kwargs`. Returns the JSON value as Python (a pandas DataFrame when the method
    /// returned rows), or `NotImplemented` when the module does not define the method.
    #[pyo3(signature = (method, pipe=None, kwargs=None))]
    fn call<'py>(
        &self,
        py: Python<'py>,
        method: &str,
        pipe: Option<&Bound<'py, PyAny>>,
        kwargs: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut context = serde_json::json!({});
        if let Some(pipe) = pipe.filter(|p| !p.is_none()) {
            let core = core_pipe(pipe, ParamsView::Raw)?;
            context["pipe"] = serde_json::json!({
                "keys": core.keys(),
                "parameters": core.raw_parameters(),
                "temporary": core.temporary,
            });
        }
        if let Some(kwargs) = kwargs.filter(|k| !k.is_none()) {
            context["kwargs"] = crate::convert::py_to_json(kwargs)?;
        }
        let connector = self.hosted(py)?;
        let method_name = method.to_string();
        let reply = py.detach(move || connector.call(&method_name, context, None));
        match reply {
            Ok(reply) => match reply.data {
                Some(frame) if reply.value.is_null() => {
                    crate::api_client_api::frame_out(py, &frame)
                }
                _ => json_to_py(py, &reply.value),
            },
            Err(ConnectorError::NotImplemented(_)) => Ok(py.NotImplemented().into_bound(py)),
            Err(e) => Err(crate::api_client_api::conn_err(e)),
        }
    }

    /// `test_connection()`: the module's `test_connection`, `True` when it has none.
    #[pyo3(signature = (**_kw))]
    fn test_connection(&self, py: Python<'_>, _kw: Option<&Bound<'_, PyDict>>) -> PyResult<bool> {
        let connector = self.hosted(py)?;
        match block_on(py, async move { connector.test_connection().await }) {
            Ok(ok) => Ok(ok),
            Err(ConnectorError::NotImplemented(_)) => Ok(true),
            Err(e) => Err(crate::api_client_api::conn_err(e)),
        }
    }

    /// `instance_call(method, pipe=None, **kw)`: an `InstanceConnector` method of a native
    /// instance type, with the keyword arguments of the v4 method of the same name.
    #[pyo3(signature = (method, pipe=None, **kw))]
    fn instance_call<'py>(
        &self,
        py: Python<'py>,
        method: &str,
        pipe: Option<&Bound<'py, PyAny>>,
        kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let c = Arc::new(self.hosted(py)?);
        if method == "fetch_pipes_keys" {
            let query = crate::api_client_api::keys_query(
                kwarg(kw, "connector_keys")?.as_ref(),
                kwarg(kw, "metric_keys")?.as_ref(),
                kwarg(kw, "location_keys")?.as_ref(),
                kwarg(kw, "tags")?.as_ref(),
                kwarg(kw, "params")?.as_ref(),
            )?;
            return match block_on(py, async move { c.fetch_pipes_keys(&query).await }) {
                Ok(keys) => crate::api_client_api::pipes_keys_out(py, &keys),
                Err(e) => not_implemented_or(py, e, |py| Ok(PyList::empty(py).into_any())),
            };
        }
        let Some(pipe) = pipe.filter(|p| !p.is_none()) else {
            return Err(PyTypeError::new_err(format!("{method}() needs a pipe")));
        };
        let p = match method {
            // The calls which load the pipe's attributes must not read `pipe.parameters`.
            "get_pipe_id" | "get_pipe_attributes" => crate::api_client_api::id_pipe(pipe)?,
            "register_pipe" => core_pipe(pipe, ParamsView::Unresolved)?,
            _ => core_pipe(pipe, ParamsView::Raw)?,
        };
        let ctx = PyCtx::scalars(py)?;
        let query = || -> PyResult<DataQuery> {
            data_query(
                &ctx,
                kwarg(kw, "select_columns")?.as_ref(),
                kwarg(kw, "omit_columns")?.as_ref(),
                kwarg(kw, "begin")?.as_ref(),
                kwarg(kw, "end")?.as_ref(),
                kwarg(kw, "params")?.as_ref(),
                kwarg(kw, "limit")?.map(|l| l.extract()).transpose()?,
                kwarg(kw, "order")?.map(|o| o.extract()).transpose()?,
            )
        };
        let tuple = |t: SuccessTuple| success_to_py(py, &t).map(Bound::into_any);
        match method {
            "register_pipe" => tuple(block_on(py, async move { c.register_pipe(&p).await })),
            "delete_pipe" => tuple(block_on(py, async move { c.delete_pipe(&p).await })),
            "drop_pipe" => tuple(block_on(py, async move { c.drop_pipe(&p).await })),
            "drop_pipe_indices" => {
                tuple(block_on(py, async move { c.drop_pipe_indices(&p).await }))
            }
            "create_pipe_indices" => {
                tuple(block_on(py, async move { c.create_pipe_indices(&p).await }))
            }
            "compress_pipe" => tuple(block_on(py, async move { c.compress_pipe(&p).await })),
            "analyze_pipe" => tuple(block_on(py, async move { c.analyze_pipe(&p).await })),
            "edit_pipe" => {
                let patch = kwarg_bool(kw, "patch", false)?;
                tuple(block_on(py, async move { c.edit_pipe(&p, patch).await }))
            }
            "decompress_pipe" => {
                let no_policy = kwarg_bool(kw, "no_policy", false)?;
                tuple(block_on(py, async move {
                    c.decompress_pipe(&p, no_policy).await
                }))
            }
            "vacuum_pipe" => {
                let full = kwarg_bool(kw, "full", false)?;
                tuple(block_on(py, async move { c.vacuum_pipe(&p, full).await }))
            }
            "partition_pipe" => {
                let minutes: Option<i64> = kwarg(kw, "chunk_minutes")?
                    .map(|m| m.extract())
                    .transpose()?;
                tuple(block_on(
                    py,
                    async move { c.partition_pipe(&p, minutes).await },
                ))
            }
            "clear_pipe" => {
                let q = query()?;
                tuple(block_on(py, async move { c.clear_pipe(&p, &q).await }))
            }
            "deduplicate_pipe" => {
                let q = query()?;
                tuple(block_on(
                    py,
                    async move { c.deduplicate_pipe(&p, &q).await },
                ))
            }
            "sync_pipe" => {
                let Some(df) = kwarg(kw, "df")? else {
                    let msg = format!("DataFrame is `None`. Cannot sync {}.", pipe.str()?);
                    return tuple(SuccessTuple::fail(msg));
                };
                let frame = crate::api_client_api::sync_frame(py, &df)?;
                let opts = SyncOptions {
                    check_existing: kwarg_bool(kw, "check_existing", true)?,
                    chunksize: kwarg(kw, "chunksize")?.map(|c| c.extract()).transpose()?,
                    begin: None,
                    end: None,
                    extra: Map::new(),
                    ..Default::default()
                };
                tuple(block_on(
                    py,
                    async move { c.sync_pipe(&p, frame, &opts).await },
                ))
            }
            "get_pipe_id" => match block_on(py, async move { c.get_pipe_id(&p).await }) {
                Ok(id) => crate::api_client_api::pipe_id_out(py, id),
                Err(e) => not_implemented_or(py, e, none),
            },
            "pipe_exists" => match block_on(py, async move { c.pipe_exists(&p).await }) {
                Ok(exists) => Ok(exists.into_pyobject(py)?.to_owned().into_any()),
                Err(e) => not_implemented_or(py, e, |py| {
                    Ok(false.into_pyobject(py)?.to_owned().into_any())
                }),
            },
            "get_pipe_attributes" => {
                match block_on(py, async move { c.get_pipe_attributes(&p).await }) {
                    Ok(attrs) => value_to_py(py, &meerschaum_core::Value::Map(attrs)),
                    Err(e) => not_implemented_or(py, e, empty_dict),
                }
            }
            "get_pipe_index_names" => {
                match block_on(py, async move { c.get_pipe_index_names(&p).await }) {
                    Ok(names) => value_to_py(py, &meerschaum_core::Value::Map(names)),
                    Err(e) => not_implemented_or(py, e, empty_dict),
                }
            }
            "get_pipe_columns_types" => {
                match block_on(py, async move { c.get_pipe_columns_types(&p).await }) {
                    Ok(types) => serde_to_py(py, &types),
                    Err(e) => not_implemented_or(py, e, empty_dict),
                }
            }
            "get_pipe_columns_indices" => {
                match block_on(py, async move { c.get_pipe_columns_indices(&p).await }) {
                    Ok(indices) => serde_to_py(py, &indices),
                    Err(e) => not_implemented_or(py, e, empty_dict),
                }
            }
            "get_pipe_size" => match block_on(py, async move { c.get_pipe_size(&p).await }) {
                Ok(size) => serde_to_py(py, &size),
                Err(e) => not_implemented_or(py, e, none),
            },
            "get_pipe_rowcount" => {
                let q = query()?;
                match block_on(py, async move { c.get_pipe_rowcount(&p, &q).await }) {
                    Ok(count) => serde_to_py(py, &count),
                    Err(e) => not_implemented_or(py, e, none),
                }
            }
            "get_sync_time" => {
                let params = params_map(kwarg(kw, "params")?.as_ref())?;
                let newest = kwarg_bool(kw, "newest", true)?;
                match block_on(py, async move {
                    c.get_sync_time(&p, params.as_ref(), newest).await
                }) {
                    Ok(bound) => crate::api_client_api::bound_out(py, bound.as_ref()),
                    Err(e) => not_implemented_or(py, e, none),
                }
            }
            "get_pipe_data" => {
                let q = query()?;
                match block_on(py, async move { c.get_pipe_data(&p, &q).await }) {
                    Ok(Some(frame)) => crate::api_client_api::frame_out(py, &frame),
                    Ok(None) => none(py),
                    Err(e) => not_implemented_or(py, e, none),
                }
            }
            other => Err(PyAttributeError::new_err(format!(
                "'{other}' is not an instance method of native connector '{}'.",
                self.keys
            ))),
        }
    }
}
