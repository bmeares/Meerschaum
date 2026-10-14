//! `meerschaum-core` bound into `meerschaum._core`: the config registry (`get_config`),
//! `{{ Pipe() }}` templating (`replace_pipes_syntax`) and password hashing.
//!
//! **`get_config` and the Python `config` dictionary.** During the parity phase the v4 Python
//! package still mutates its module-level `meerschaum.config.config` dictionary directly
//! (`set_config`, `replace_config`, the environment patches, plugins editing the returned dicts).
//! The binding therefore treats that dictionary as the shared in-memory store: each call loads it
//! into the Rust registry, runs the Rust lookup (file reads, default fallback, `MRSM{}` symlink
//! substitution, writing missing files), writes back only the top-level keys the lookup changed,
//! and returns the dictionary's own object when the keys were already loaded (so callers that
//! mutate the result keep working). A read-only lookup of keys that are already loaded (and hold no
//! `MRSM{` symlink to substitute) skips the registry and walks the dictionary ([`fast_lookup`],
//! PARITY row 12). See `PARITY.md`.

use std::sync::OnceLock;

use meerschaum_core::config::{Config, GetOptions};
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::template::TemplateResolver;
use meerschaum_core::{Map, Pipe, Value};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyDict, PyFloat, PyInt, PyList, PyModule, PyString, PyTuple};

use crate::convert::{core_to_py, py_to_core};

static REGISTRY: OnceLock<Result<Config, String>> = OnceLock::new();

/// The process-wide Rust config registry (built from the environment on first use).
fn registry() -> PyResult<&'static Config> {
    REGISTRY
        .get_or_init(|| Config::from_env().map_err(|e| e.to_string()))
        .as_ref()
        .map_err(|e| PyRuntimeError::new_err(e.clone()))
}

pub(crate) fn emit_warnings(
    py: Python<'_>,
    warnings: Vec<String>,
    stacklevel: i64,
) -> PyResult<()> {
    if warnings.is_empty() {
        return Ok(());
    }
    let warn = py.import("meerschaum.utils.warnings")?.getattr("warn")?;
    for msg in warnings {
        let kwargs = PyDict::new(py);
        kwargs.set_item("stacklevel", stacklevel)?;
        kwargs.set_item("color", false)?;
        warn.call((msg,), Some(&kwargs))?;
    }
    Ok(())
}

fn walk<'py>(root: &Bound<'py, PyAny>, keys: &[String]) -> Option<Bound<'py, PyAny>> {
    let mut current = root.clone();
    for k in keys {
        current = current.get_item(k).ok()?;
    }
    Some(current)
}

fn map_has_path(map: &Map, keys: &[String]) -> bool {
    let mut current: Option<&Value> = None;
    for (i, k) in keys.iter().enumerate() {
        current = if i == 0 {
            map.get(k)
        } else {
            current.and_then(|v| v.get(k))
        };
        if current.is_none() {
            return false;
        }
    }
    true
}

/// The `meerschaum.config` module (imported once; `importlib.reload` mutates it in place).
fn config_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    static MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();
    MODULE
        .get_or_try_init(py, || Ok(py.import("meerschaum.config")?.unbind()))
        .map(|m| m.bind(py))
}

/// Whether `obj` (a config subtree) holds an `MRSM{` symlink anywhere, as v4's
/// `'MRSM{' in json.dumps(config[keys[0]])` decides. `None` when the subtree holds an object
/// that is not plain config data (the full lookup decides what to do with it).
fn holds_symlink(obj: &Bound<'_, PyAny>, depth: usize) -> Option<bool> {
    const MARKER: &str = "MRSM{";
    if depth > 64 {
        return None;
    }
    if let Ok(s) = obj.downcast::<PyString>() {
        return Some(s.to_str().ok()?.contains(MARKER));
    }
    if let Ok(d) = obj.downcast_exact::<PyDict>() {
        for (k, v) in d.iter() {
            match k.downcast::<PyString>() {
                Ok(ks) => {
                    if ks.to_str().ok()?.contains(MARKER) {
                        return Some(true);
                    }
                }
                // `json.dumps` writes int / float / bool / None keys without the marker.
                Err(_) if k.is_none() || k.is_instance_of::<PyInt>() => {}
                Err(_) if k.is_instance_of::<PyFloat>() => {}
                Err(_) => return None,
            }
            if holds_symlink(&v, depth + 1)? {
                return Some(true);
            }
        }
        return Some(false);
    }
    if let Ok(l) = obj.downcast_exact::<PyList>() {
        for v in l.iter() {
            if holds_symlink(&v, depth + 1)? {
                return Some(true);
            }
        }
        return Some(false);
    }
    if let Ok(t) = obj.downcast_exact::<PyTuple>() {
        for v in t.iter() {
            if holds_symlink(&v, depth + 1)? {
                return Some(true);
            }
        }
        return Some(false);
    }
    if obj.is_none() || obj.is_instance_of::<PyInt>() || obj.is_instance_of::<PyFloat>() {
        // `bool` is an `int` subclass.
        return Some(false);
    }
    None
}

/// The read-only fast path (PARITY row 12): when every key is already loaded in the Python
/// `config` dictionary and (with `substitute`) the first key's subtree holds no `MRSM{`
/// symlink, the full lookup would only walk the dictionary and return its own object (no
/// substitution, no file reads, no defaults, no writes, no warnings, no `sync_files`), so the
/// walk is done here directly. Nothing is cached: every call inspects the live dictionary,
/// so `set_config`, `replace_config`, `write_config`, environment patches and callers that
/// mutate the returned dicts are seen immediately. `None` falls through to the full lookup.
fn fast_lookup<'py>(
    py_config: &Bound<'py, PyAny>,
    keys: &Bound<'py, PyTuple>,
    substitute: bool,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let Ok(dict) = py_config.downcast_exact::<PyDict>() else {
        return Ok(None);
    };
    let n = keys.len();
    if n == 0 {
        return Ok(None);
    }
    let first = keys.get_item(0)?;
    let Ok(first_str) = first.downcast_exact::<PyString>() else {
        return Ok(None);
    };
    if first_str.to_str()? == meerschaum_core::static_config().config.symlinks_key {
        return Ok(None);
    }
    let Some(top) = dict.get_item(first_str)? else {
        return Ok(None);
    };
    if substitute && holds_symlink(&top, 0) != Some(false) {
        return Ok(None);
    }
    let mut current = top;
    for i in 1..n {
        let key = keys.get_item(i)?;
        if !key.is_exact_instance_of::<PyString>() {
            return Ok(None);
        }
        let Ok(d) = current.downcast_exact::<PyDict>() else {
            return Ok(None);
        };
        match d.get_item(&key)? {
            Some(next) => current = next,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

/// `get_config(*keys, patch=True, substitute=True, sync_files=True, write_missing=True,
/// as_tuple=False, warn=True, debug=False)`.
#[pyfunction]
#[pyo3(signature = (
    *keys, patch=true, substitute=true, sync_files=true, write_missing=true,
    as_tuple=false, warn=true, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn get_config<'py>(
    py: Python<'py>,
    keys: &Bound<'py, PyTuple>,
    patch: bool,
    substitute: bool,
    sync_files: bool,
    write_missing: bool,
    as_tuple: bool,
    warn: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _ = debug;
    let module = config_module(py)?;
    let py_config = module.getattr(intern!(py, "config"))?;
    if let Some(found) = fast_lookup(&py_config, keys, substitute)? {
        return if as_tuple {
            Ok(
                PyTuple::new(py, [true.into_pyobject(py)?.to_owned().into_any(), found])?
                    .into_any(),
            )
        } else {
            Ok(found)
        };
    }
    if keys.is_empty() {
        // `_config()` returns the dictionary itself.
        return if as_tuple {
            Ok(PyTuple::new(
                py,
                [true.into_pyobject(py)?.to_owned().into_any(), py_config],
            )?
            .into_any())
        } else {
            Ok(py_config)
        };
    }
    let keys: Vec<String> = keys
        .iter()
        .map(|k| match k.downcast::<PyString>() {
            Ok(s) => Ok(s.to_str()?.to_string()),
            Err(_) => Ok(k.str()?.to_str()?.to_string()),
        })
        .collect::<PyResult<_>>()?;
    let allow_writes: bool = module
        .getattr("_allow_write_missing")
        .and_then(|v| v.extract())
        .unwrap_or(true);

    // Fast path: when the key path is already loaded, the lookup only reads (and may
    // re-substitute) the first key and the symlinks, so only those are converted.
    let symlinks_key = &meerschaum_core::static_config().config.symlinks_key;
    let loaded = walk(&py_config, &keys).is_some();
    let before = match (loaded, py_config.downcast::<PyDict>()) {
        (true, Ok(dict)) => {
            let mut partial = Map::new();
            for k in [keys[0].as_str(), symlinks_key.as_str()] {
                if let Some(v) = dict.get_item(k)? {
                    partial.insert(k.to_string(), py_to_core(&v)?);
                }
            }
            partial
        }
        _ => match py_to_core(&py_config)? {
            Value::Map(m) => m,
            _ => Map::new(),
        },
    };
    let registry = registry()?;
    registry.set_config(before.clone());
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let opts = GetOptions {
        patch: patch && allow_writes,
        substitute,
        write_missing: write_missing && allow_writes,
        warn,
    };
    let (valid, value) = registry.get_with(&key_refs, opts);
    let after = registry.snapshot();
    let warnings = registry.take_warnings();

    // Write back the top-level keys the lookup loaded or changed.
    let dict = py_config.downcast::<PyDict>().ok();
    let k0 = &keys[0];
    let newly_loaded = !before.contains_key(k0) && after.contains_key(k0);
    if let Some(dict) = dict {
        for (k, v) in &after {
            if before.get(k) != Some(v) {
                dict.set_item(k, core_to_py(py, v)?)?;
            }
        }
    }
    emit_warnings(py, warnings, 3)?;
    if newly_loaded && sync_files && allow_writes {
        let sync = py
            .import("meerschaum.config._sync")?
            .getattr("sync_files")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("keys", vec![k0.clone()])?;
        sync.call((), Some(&kwargs))?;
    }

    let result = if !valid {
        py.None().into_bound(py)
    } else if map_has_path(&before, &keys) || newly_loaded {
        // Keys already loaded (or loaded now from disk): return the dictionary's own object.
        match walk(&py_config, &keys) {
            Some(obj) => obj,
            None => core_to_py(py, &value)?,
        }
    } else {
        // Served from the defaults: a fresh object, as Python returns.
        core_to_py(py, &value)?
    };
    if as_tuple {
        return Ok(
            PyTuple::new(py, [valid.into_pyobject(py)?.to_owned().into_any(), result])?.into_any(),
        );
    }
    Ok(result)
}

// ---------------------------------------------------------------------------------------------
// {{ Pipe() }} templating

/// A template resolver that answers from live Python `Pipe` objects.
pub(crate) struct PyResolver<'py> {
    pub(crate) py: Python<'py>,
    pub(crate) defaults: PipeDefaults,
    pub(crate) self_pipe: Option<(Pipe, Bound<'py, PyAny>)>,
    /// The Python pipes built for referenced pipes during this resolution. A template asks for
    /// a referenced pipe's table name and then for its raw parameters (and attribute chains for
    /// more); reusing one Python `Pipe` per reference loads its attributes from the instance
    /// once, as v4's single `Pipe(...)` per template does. Shared with the resolutions nested
    /// in this one (see [`BuiltScope`]).
    pub(crate) built: BuiltScope,
}

type BuiltPipes = std::rc::Rc<std::cell::RefCell<Vec<(Pipe, Py<PyAny>)>>>;

thread_local! {
    static ACTIVE_BUILT: std::cell::RefCell<Option<BuiltPipes>> =
        const { std::cell::RefCell::new(None) };
}

/// The Python pipes a top-level template resolution built, shared with every resolution nested
/// in it on this thread.
///
/// Resolving `{{ Pipe('a', 'b') }}` asks the referenced Python pipe for its `.target`, which
/// resolves that pipe's own templates through another `_core` call while the outer one is still
/// running. Without sharing, every level of a derived chain built (and loaded from the instance)
/// the pipes below it again: v4 loads each referenced pipe once per top-level resolution.
pub(crate) struct BuiltScope {
    built: BuiltPipes,
    outermost: bool,
}

impl Default for BuiltScope {
    fn default() -> Self {
        ACTIVE_BUILT.with(|active| {
            let mut active = active.borrow_mut();
            match active.as_ref() {
                Some(built) => BuiltScope {
                    built: built.clone(),
                    outermost: false,
                },
                None => {
                    let built = BuiltPipes::default();
                    *active = Some(built.clone());
                    BuiltScope {
                        built,
                        outermost: true,
                    }
                }
            }
        })
    }
}

impl Drop for BuiltScope {
    fn drop(&mut self) {
        if self.outermost {
            ACTIVE_BUILT.with(|active| *active.borrow_mut() = None);
        }
    }
}

impl<'py> PyResolver<'py> {
    /// The Python `Pipe` for a core pipe (the `_pipe` object itself for `self`).
    fn py_pipe(&self, pipe: &Pipe) -> PyResult<Bound<'py, PyAny>> {
        self.py_pipe_as(pipe, false)
    }

    /// [`py_pipe`](Self::py_pipe); with `by_keys`, a pipe already built for the same keys is
    /// reused whatever parameters `pipe` carries.
    ///
    /// The template core hands `table_reference` / `attribute` the referenced pipe with its
    /// parameters already resolved, but the Python `Pipe` answers `.target` and attribute
    /// chains from its own (memoized) resolution. A `Pipe(..., parameters=resolved)` would load
    /// its metadata from the instance a second time. PARITY: v4 reads the attribute from the
    /// one `Pipe(...)` the template built.
    fn py_pipe_as(&self, pipe: &Pipe, by_keys: bool) -> PyResult<Bound<'py, PyAny>> {
        if let Some((core, obj)) = &self.self_pipe {
            if core == pipe {
                return Ok(obj.clone());
            }
        }
        {
            let built = self.built.built.borrow();
            let exact = built
                .iter()
                .find(|(core, _)| core == pipe && core.parameters() == pipe.parameters());
            let found = exact.or_else(|| {
                by_keys
                    .then(|| built.iter().find(|(core, _)| core == pipe))
                    .flatten()
            });
            if let Some((_, obj)) = found {
                return Ok(obj.bind(self.py).clone());
            }
        }
        let mrsm = self.py.import("meerschaum")?;
        let kwargs = PyDict::new(self.py);
        kwargs.set_item("instance", pipe.instance_keys())?;
        if !pipe.parameters().is_empty() {
            kwargs.set_item(
                "parameters",
                core_to_py(self.py, &Value::Map(pipe.parameters().clone()))?,
            )?;
        }
        let obj = mrsm.getattr("Pipe")?.call(
            (
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
            ),
            Some(&kwargs),
        )?;
        self.built
            .built
            .borrow_mut()
            .push((pipe.clone(), obj.clone().unbind()));
        Ok(obj)
    }

    fn table_reference_py(&self, pipe: &Pipe) -> PyResult<String> {
        let py = self.py;
        let obj = self.py_pipe_as(pipe, true)?;
        let target: String = obj.getattr("target")?.extract()?;
        let conn = obj.getattr("instance_connector")?;
        let conn_type: String = conn.getattr("type")?.extract()?;
        if conn_type != "sql" {
            return Ok(target);
        }
        let schema = if conn.hasattr("get_pipe_schema")? {
            conn.call_method1("get_pipe_schema", (&obj,))?
        } else {
            py.None().into_bound(py)
        };
        let flavor = conn.getattr("flavor")?;
        py.import("meerschaum.utils.sql")?
            .getattr("sql_item_name")?
            .call1((target, flavor, schema))?
            .extract()
    }
}

fn py_err_text(err: PyErr) -> String {
    err.to_string()
}

impl TemplateResolver for PyResolver<'_> {
    fn defaults(&self) -> PipeDefaults {
        self.defaults.clone()
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        self.table_reference_py(pipe).map_err(py_err_text)
    }

    fn attribute(&self, pipe: &Pipe, name: &str) -> Result<Value, String> {
        let obj = self.py_pipe_as(pipe, true).map_err(py_err_text)?;
        let value = obj.getattr(name).map_err(py_err_text)?;
        py_to_core(&value).map_err(py_err_text)
    }

    fn raw_parameters(&self, pipe: &Pipe) -> Map {
        let Ok(obj) = self.py_pipe(pipe) else {
            return pipe.parameters().clone();
        };
        obj.getattr("attributes")
            .and_then(|a| a.get_item("parameters"))
            .and_then(|p| py_to_core(&p))
            .ok()
            .and_then(|v| v.as_map().cloned())
            .unwrap_or_default()
    }
}

/// The pipe-constructor defaults, with the instance from the Python config.
pub(crate) fn py_defaults(py: Python<'_>) -> PyResult<PipeDefaults> {
    let mut defaults = PipeDefaults::default();
    if let Ok(instance) = py
        .import("meerschaum.config")?
        .getattr("get_config")?
        .call1(("meerschaum", "instance"))
        .and_then(|v| v.str())
    {
        defaults.instance_keys = instance.to_str()?.to_string();
    }
    Ok(defaults)
}

/// `replace_pipes_syntax(text, _pipe=None)`.
#[pyfunction]
#[pyo3(signature = (text, _pipe=None))]
pub fn replace_pipes_syntax<'py>(
    py: Python<'py>,
    text: &str,
    _pipe: Option<Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut defaults = PipeDefaults::default();
    if let Ok(instance) = py
        .import("meerschaum.config")?
        .getattr("get_config")?
        .call1(("meerschaum", "instance"))
        .and_then(|v| v.str())
    {
        defaults.instance_keys = instance.to_str()?.to_string();
    }
    let self_pipe = match _pipe.filter(|p| !p.is_none()) {
        None => None,
        Some(obj) => {
            let kwargs = meerschaum_core::Map::from_iter([
                (
                    "connector".to_string(),
                    Value::Str(obj.getattr("connector_keys")?.str()?.extract()?),
                ),
                (
                    "metric".to_string(),
                    Value::Str(obj.getattr("metric_key")?.str()?.extract()?),
                ),
                (
                    "location".to_string(),
                    match obj.getattr("location_key")? {
                        l if l.is_none() => Value::Null,
                        l => Value::Str(l.str()?.extract()?),
                    },
                ),
                (
                    "instance".to_string(),
                    Value::Str(obj.getattr("instance_keys")?.str()?.extract()?),
                ),
            ]);
            let core = Pipe::from_call(&[], &kwargs, &defaults)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Some((core, obj))
        }
    };
    let resolver = PyResolver {
        py,
        defaults,
        self_pipe,
        built: Default::default(),
    };
    let self_core = resolver.self_pipe.as_ref().map(|(p, _)| p.clone());
    let replaced =
        meerschaum_core::template::replace_pipes_syntax(text, self_core.as_ref(), &resolver);
    emit_warnings(py, replaced.warnings, 2)?;
    core_to_py(py, &replaced.value)
}

// ---------------------------------------------------------------------------------------------
// Passwords

/// `hash_password(password, salt=None, rounds=None)` (passlib `pbkdf2-sha256` format).
#[pyfunction]
#[pyo3(signature = (password, salt=None, rounds=None))]
pub fn hash_password(
    py: Python<'_>,
    password: Option<String>,
    salt: Option<Vec<u8>>,
    rounds: Option<u32>,
) -> PyResult<String> {
    let password = password.unwrap_or_default();
    py.allow_threads(|| meerschaum_core::users::hash_password(&password, salt.as_deref(), rounds))
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// `verify_password(password, password_hash)`.
#[pyfunction]
pub fn verify_password(
    py: Python<'_>,
    password: Option<String>,
    password_hash: Option<String>,
) -> PyResult<bool> {
    let (Some(password), Some(password_hash)) = (password, password_hash) else {
        return Ok(false);
    };
    let ok = py
        .allow_threads(|| meerschaum_core::users::verify_password(&password, &password_hash))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    if !ok && password_hash.split('$').count() != 5 {
        emit_warnings(
            py,
            vec![format!(
                "Failed to extract context from password hash '{password_hash}'. Is it corrupted?"
            )],
            2,
        )?;
    }
    Ok(ok)
}
