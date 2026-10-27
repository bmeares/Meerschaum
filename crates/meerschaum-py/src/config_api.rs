//! The config registry at the Python boundary: `get_config`, `read_config`, `write_config`,
//! `search_and_substitute_config`, `get_keyfile_path`, the plugin config helpers, the config file
//! backups and the environment layers, all running on `meerschaum_core::config` (wave 15, task O).
//!
//! **Who owns what.** `meerschaum_core::config::ConfigState` owns every rule: file selection,
//! YAML/JSON reading and writing (lossless: dates, `bytes`, big integers, tuples, sets and typed
//! keys survive), defaults, `MRSM{}` substitution and restoration, `MRSM_CONFIG` / `MRSM_PATCH`,
//! `MRSM_<TYPE>_<LABEL>` connectors, backups, and the messages (replayed here on stdout/stderr
//! in the order v4 produced them).
//!
//! **Where the in-process tree lives.** Python code holds the dictionaries `get_config` returns
//! and mutates them (`get_config('system', 'connectors', 'sql')['chunksize'] = 5`, plugins
//! editing their own section, `meerschaum.config.config[...]`), and v4 callers rely on a mutation
//! being visible on the next call. Only Python objects can give that, so the process-wide tree is
//! one `dict` that this module owns ([`config_dict`] / [`config_set`] / [`config_reset`]; the
//! `meerschaum.config.config` attribute is that dict). A call that needs the registry's rules
//! (anything but a read of a loaded, symlink-free subtree) converts what the rule can touch into
//! a [`ConfigValue`] tree, runs the core method on a registry that holds no state between calls,
//! and writes back only the top-level keys that changed, so everything else keeps its identity.
//! Python objects with no config form (a function, a connector) travel as opaque placeholder
//! strings and come back as the same object. Nothing is cached: `set_config`, `replace_config`,
//! environment patches and callers editing returned dicts are seen on the next call.
//!
//! The read-only fast path ([`fast_lookup`], PARITY row 12) walks that dict directly.

use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use meerschaum_core::config::lossless::map_contains_text;
use meerschaum_core::config::{
    Config, ConfigError, ConfigMap, ConfigState, ConfigValue, GetOptions, LoadError, Notice, Paths,
    SubstituteOptions,
};
use meerschaum_core::{Map, Value};
use num_bigint::BigInt;
use pyo3::exceptions::{PyKeyError, PyOSError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PyModule, PySet,
    PyString, PyTuple,
};

use crate::yaml_api::{ToPy, representer_error, to_py_err};

// ---------------------------------------------------------------------------------------------
// The live dictionary
// ---------------------------------------------------------------------------------------------

static STORE: Mutex<Option<Py<PyDict>>> = Mutex::new(None);

/// The process-wide config dictionary. The Python side creates it (`meerschaum.config._store`,
/// so that importing `meerschaum` does not load this extension); the first use here adopts that
/// very object, and `config_set` / `config_reset` replace it afterwards.
pub(crate) fn store(py: Python<'_>) -> Bound<'_, PyDict> {
    {
        let guard = STORE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(d) = guard.as_ref() {
            return d.bind(py).clone();
        }
    }
    // Import outside the lock: it may release the GIL.
    let adopted = py
        .import("meerschaum.config._store")
        .and_then(|m| m.getattr("_dict"))
        .ok()
        .and_then(|d| d.cast_into::<PyDict>().ok());
    let mut guard = STORE.lock().unwrap_or_else(|e| e.into_inner());
    match guard.as_ref() {
        Some(d) => d.bind(py).clone(),
        None => {
            let d = adopted.unwrap_or_else(|| PyDict::new(py));
            *guard = Some(d.clone().unbind());
            d
        }
    }
}

/// `meerschaum.config.config`: the live config dictionary.
#[pyfunction]
pub fn config_dict(py: Python<'_>) -> Bound<'_, PyDict> {
    store(py)
}

/// `set_config(cf)`: make `cf` the live config dictionary (the object itself, as v4 rebinds).
#[pyfunction]
pub fn config_set<'py>(py: Python<'py>, cf: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyDict>> {
    let d = cf
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("Invalid value for config: {cf}")))?
        .clone();
    *STORE.lock().unwrap_or_else(|e| e.into_inner()) = Some(d.clone().unbind());
    let _ = py;
    Ok(d)
}

/// `importlib.reload(meerschaum.config)`: start again from an empty dictionary.
#[pyfunction]
pub fn config_reset(py: Python<'_>) -> Bound<'_, PyDict> {
    let d = PyDict::new(py);
    *STORE.lock().unwrap_or_else(|e| e.into_inner()) = Some(d.clone().unbind());
    d
}

// ---------------------------------------------------------------------------------------------
// The registry and the host's state
// ---------------------------------------------------------------------------------------------

static REGISTRY: OnceLock<Config> = OnceLock::new();

/// What the Python process says about itself on each call: where the config lives and whether it
/// may write (`meerschaum.config.paths`, `meerschaum.config._allow_write_missing`).
struct Host {
    root: PathBuf,
    config_dir: PathBuf,
    /// `VIRTENV_RESOURCES_PATH` when it is not the default `<root>/venvs` (`MRSM_VENVS_DIR`,
    /// `set_venvs_dir`): what `Paths::venvs_dir` carries.
    venvs_dir: Option<PathBuf>,
    /// `DOT_CONFIG_DIR_PATH` (`$XDG_CONFIG_HOME`, else `~/.config`): the systemd user unit
    /// directory derives from it.
    dot_config_dir: PathBuf,
    allow_writes: bool,
}

fn config_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    static MODULE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    if let Some(module) = MODULE.get(py) {
        return Ok(module.bind(py));
    }
    // Import outside the cell: importing `meerschaum.config` can call back into `_core` (and so
    // into this function), and a `PyOnceLock` initializer must not re-enter its own cell. Two
    // racing threads import the same `sys.modules` entry; the first `set` wins.
    let module = py.import("meerschaum.config")?.unbind();
    let _ = MODULE.set(py, module);
    MODULE
        .get(py)
        .map(|module| module.bind(py))
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("meerschaum.config is not cached"))
}

fn path_text(obj: &Bound<'_, PyAny>) -> PyResult<PathBuf> {
    Ok(PathBuf::from(obj.str()?.to_str()?))
}

fn host(py: Python<'_>) -> PyResult<Host> {
    let paths = py.import("meerschaum.config.paths")?;
    let allow_writes = config_module(py)?
        .getattr("_allow_write_missing")
        .and_then(|v| v.extract::<bool>())
        .unwrap_or(true);
    let root = path_text(&paths.getattr("ROOT_DIR_PATH")?)?;
    let venvs_dir = paths
        .getattr("VIRTENV_RESOURCES_PATH")
        .ok()
        .and_then(|p| path_text(&p).ok())
        .filter(|p| *p != root.join("venvs"));
    Ok(Host {
        root,
        config_dir: path_text(&paths.getattr("CONFIG_DIR_PATH")?)?,
        venvs_dir,
        dot_config_dir: paths
            .getattr("DOT_CONFIG_DIR_PATH")
            .ok()
            .and_then(|p| path_text(&p).ok())
            .map(|p| meerschaum_core::config::paths::resolve_path(&p))
            .unwrap_or_else(Paths::process_dot_config_dir),
        allow_writes,
    })
}

/// The registry (built on first use from the host's paths; it applies nothing at construction and
/// keeps no config between calls).
fn registry(host: &Host) -> &'static Config {
    REGISTRY.get_or_init(|| {
        let mut paths = Paths::for_root(host.root.clone());
        paths.config_dir = host.config_dir.clone();
        Config::bare(
            paths,
            meerschaum_core::static_config::os_cpu_count().unwrap_or(1),
            cfg!(windows),
            host.allow_writes,
        )
    })
}

/// The top-level keys of the default config (the files `get_config` may load), computed once.
fn default_top_level_keys(host: &Host) -> &'static [String] {
    static KEYS: OnceLock<Vec<String>> = OnceLock::new();
    KEYS.get_or_init(|| {
        meerschaum_core::config::default_config(
            &meerschaum_core::config::paths::posix(&host.root),
            meerschaum_core::static_config::os_cpu_count().unwrap_or(1),
            cfg!(windows),
        )
        .keys()
        .cloned()
        .collect()
    })
}

/// A snapshot of the live config (the process-wide dictionary, as it is now) as a [`Config`] for
/// the native flows (`MRSM{}` symlinks, defaults, connector sections). Built from the registry's
/// own conversion of the dictionary: no file is parsed except the file of a top-level key that
/// was never loaded (read-only: nothing is written, and the key is stored in the dictionary, so
/// it is read once). Call it with the interpreter lock held.
pub(crate) fn live_config(py: Python<'_>) -> PyResult<Config> {
    let host = host(py)?;
    let dict = store(py);
    let mut conv = ToCfg::new(py, false)?;
    let before = load_keys(&mut conv, &dict, None)?;
    let mut paths = Paths::for_root(host.root.clone());
    paths.config_dir = host.config_dir.clone();
    paths.venvs_dir = host.venvs_dir.clone();
    paths.dot_config_dir = host.dot_config_dir.clone();
    // The plugins directories the Python process uses (`MRSM_PLUGINS_DIR` may name several): the
    // worker manifest is built from them.
    if let Ok(dirs) = py
        .import("meerschaum.config.paths")
        .and_then(|m| m.getattr("PLUGINS_DIR_PATHS"))
        .and_then(|d| d.try_iter())
    {
        let listed: Vec<PathBuf> = dirs
            .filter_map(|d| d.ok().and_then(|d| path_text(&d).ok()))
            .collect();
        if !listed.is_empty() {
            paths.plugins_dirs = listed;
        }
    }
    let config = Config::bare(
        paths,
        meerschaum_core::static_config::os_cpu_count().unwrap_or(1),
        cfg!(windows),
        false,
    );
    let missing: Vec<&String> = default_top_level_keys(&host)
        .iter()
        .filter(|k| !before.contains_key(k.as_str()))
        .collect();
    let (loaded, notices) = config.with_state(|s| {
        s.set_config_lossless(before.clone());
        let _ = s.take_notices();
        let opts = GetOptions {
            patch: false,
            substitute: false,
            write_missing: false,
            warn: false,
        };
        for key in missing {
            let _ = s.get_config_lossless(std::slice::from_ref(key), opts);
        }
        (s.config_lossless().clone(), s.take_notices())
    });
    let _ = notices;
    if loaded.len() != before.len() {
        write_back(py, &dict, &before, &loaded, &conv.opaque)?;
    }
    Ok(config)
}

/// The paths of the native view of the live config (what the sync flow and the worker manifest
/// see): `{'root', 'config_dir', 'plugins_dirs', 'venvs_dir'}`. Exposed for tests.
#[pyfunction]
pub fn live_config_paths(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let config = live_config(py)?;
    let paths = config.paths();
    let out = PyDict::new(py);
    out.set_item("root", paths.root_dir.to_string_lossy().to_string())?;
    out.set_item("config_dir", paths.config_dir.to_string_lossy().to_string())?;
    out.set_item(
        "plugins_dirs",
        paths
            .plugins_dirs
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect::<Vec<_>>(),
    )?;
    out.set_item(
        "dot_config_dir",
        paths.dot_config_dir.to_string_lossy().to_string(),
    )?;
    out.set_item(
        "systemd_user_dir",
        paths
            .path("SYSTEMD_USER_RESOURCES_PATH")
            .to_string_lossy()
            .to_string(),
    )?;
    out.set_item(
        "venvs_dir",
        paths.venvs_dir.map(|p| p.to_string_lossy().to_string()),
    )?;
    Ok(out)
}

/// Run `f` on the registry holding `before` as its config; returns `f`'s result, the config it
/// left behind and the messages it recorded. The registry keeps nothing afterwards.
fn run<R>(
    host: &Host,
    before: ConfigMap,
    f: impl FnOnce(&mut ConfigState) -> R,
) -> (R, ConfigMap, Vec<Notice>) {
    registry(host).with_state(|s| {
        {
            let paths = s.paths_mut();
            paths.root_dir = host.root.clone();
            paths.config_dir = host.config_dir.clone();
            paths.venvs_dir = host.venvs_dir.clone();
            paths.dot_config_dir = host.dot_config_dir.clone();
        }
        s.set_allow_writes(host.allow_writes);
        s.set_config_lossless(before);
        let _ = s.take_notices();
        let result = f(s);
        let after = s.take_config_lossless();
        (result, after, s.take_notices())
    })
}

/// Replay the registry's messages the way v4 produced them: `print()` to stdout, `warn()` to
/// stderr (`stacklevel` applies to the `get_config` invalid-keys warning).
pub(crate) fn emit_notices(py: Python<'_>, notices: Vec<Notice>, stacklevel: i64) -> PyResult<()> {
    emit_notices_debug(py, notices, stacklevel, false)
}

/// [`emit_notices`] that also prints the `if debug: dprint(...)` lines when `debug` is set.
pub(crate) fn emit_notices_debug(
    py: Python<'_>,
    notices: Vec<Notice>,
    stacklevel: i64,
    debug: bool,
) -> PyResult<()> {
    if notices.is_empty() {
        return Ok(());
    }
    let builtins = py.import("builtins")?;
    let warn = py.import("meerschaum.utils.warnings")?.getattr("warn")?;
    for notice in notices {
        match notice {
            Notice::Debug(text) => {
                if debug {
                    py.import("meerschaum.utils.debug")?
                        .getattr("dprint")?
                        .call1((text,))?;
                }
            }
            Notice::Print(text) | Notice::PrintOnly(text) => {
                builtins.getattr("print")?.call1((text,))?;
            }
            Notice::Warn(text) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("stack", false)?;
                warn.call((text,), Some(&kwargs))?;
            }
            Notice::WarnPlain(text) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("stacklevel", stacklevel)?;
                kwargs.set_item("color", false)?;
                warn.call((text,), Some(&kwargs))?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Python <-> ConfigValue
// ---------------------------------------------------------------------------------------------

const OPAQUE_PREFIX: &str = "\u{2}mrsm-py-object:";
const OPAQUE_SUFFIX: char = '\u{2}';
const MAX_DEPTH: usize = 100;

/// Python -> [`ConfigValue`]. In lenient mode an object with no config form (a function, a
/// connector, a cycle's inner side) becomes an opaque placeholder string that [`FromCfg`] turns
/// back into the same object; in strict mode it is a `RepresenterError` (what `yaml.dump` raises).
struct ToCfg<'py> {
    py: Python<'py>,
    date_cls: Bound<'py, PyAny>,
    datetime_cls: Bound<'py, PyAny>,
    opaque: Vec<Py<PyAny>>,
    strict: bool,
}

impl<'py> ToCfg<'py> {
    fn new(py: Python<'py>, strict: bool) -> PyResult<Self> {
        let datetime = py.import("datetime")?;
        Ok(ToCfg {
            py,
            date_cls: datetime.getattr("date")?,
            datetime_cls: datetime.getattr("datetime")?,
            opaque: Vec::new(),
            strict,
        })
    }

    fn opaque(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<ConfigValue> {
        if self.strict {
            return Err(representer_error(self.py, obj));
        }
        self.opaque.push(obj.clone().unbind());
        Ok(ConfigValue::Str(format!(
            "{OPAQUE_PREFIX}{}{OPAQUE_SUFFIX}",
            self.opaque.len() - 1
        )))
    }

    fn map(&mut self, dict: &Bound<'py, PyDict>, depth: usize) -> PyResult<ConfigMap> {
        let mut out = ConfigMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key = match k.cast::<PyString>() {
                Ok(s) => s.to_str()?.to_string(),
                Err(_) => k.str()?.to_str()?.to_string(),
            };
            out.insert(key, self.value(&v, depth + 1)?);
        }
        Ok(out)
    }

    fn value(&mut self, obj: &Bound<'py, PyAny>, depth: usize) -> PyResult<ConfigValue> {
        if obj.is_none() {
            return Ok(ConfigValue::Null);
        }
        if let Ok(b) = obj.cast::<PyBool>() {
            return Ok(ConfigValue::Bool(b.is_true()));
        }
        if obj.is_instance_of::<PyInt>() {
            return Ok(match obj.extract::<i64>() {
                Ok(i) => ConfigValue::Int(i),
                Err(_) => ConfigValue::BigInt(
                    obj.str()?
                        .to_str()?
                        .parse::<BigInt>()
                        .map_err(|e| PyValueError::new_err(e.to_string()))?,
                ),
            });
        }
        if obj.is_instance_of::<PyFloat>() {
            return Ok(ConfigValue::Float(obj.extract::<f64>()?));
        }
        if let Ok(s) = obj.cast::<PyString>() {
            return Ok(ConfigValue::Str(s.to_str()?.to_string()));
        }
        if let Ok(b) = obj.cast::<PyBytes>() {
            return Ok(ConfigValue::Bytes(b.as_bytes().to_vec()));
        }
        if obj.is_instance_of::<PyByteArray>() || obj.is_instance_of::<PyFrozenSet>() {
            return self.opaque(obj);
        }
        if depth > MAX_DEPTH {
            return self.opaque(obj);
        }
        if let Ok(d) = obj.cast::<PyDict>() {
            let mut entries: Vec<(ConfigValue, ConfigValue)> = Vec::with_capacity(d.len());
            let mut all_str = true;
            for (k, v) in d.iter() {
                if !k.is_instance_of::<PyString>() {
                    all_str = false;
                }
                entries.push((self.value(&k, depth + 1)?, self.value(&v, depth + 1)?));
            }
            return Ok(if all_str {
                let mut map = ConfigMap::with_capacity(entries.len());
                for (k, v) in entries {
                    if let ConfigValue::Str(k) = k {
                        map.insert(k, v);
                    }
                }
                ConfigValue::Map(map)
            } else {
                ConfigValue::from_pairs(entries)
            });
        }
        if let Ok(l) = obj.cast::<PyList>() {
            let mut items = Vec::with_capacity(l.len());
            for item in l.iter() {
                items.push(self.value(&item, depth + 1)?);
            }
            return Ok(ConfigValue::List(items));
        }
        if let Ok(t) = obj.cast::<PyTuple>() {
            let mut items = Vec::with_capacity(t.len());
            for item in t.iter() {
                items.push(self.value(&item, depth + 1)?);
            }
            return Ok(ConfigValue::Tuple(items));
        }
        if let Ok(s) = obj.cast::<PySet>() {
            let mut items = Vec::with_capacity(s.len());
            for item in s.iter() {
                items.push(self.value(&item, depth + 1)?);
            }
            return Ok(ConfigValue::Set(items));
        }
        if obj.is_instance(&self.datetime_cls)? {
            let attr = |name: &str| -> PyResult<u32> { obj.getattr(name)?.extract::<u32>() };
            let offset = obj.call_method0("utcoffset")?;
            let utc_offset_us = if offset.is_none() {
                None
            } else {
                let days: i64 = offset.getattr("days")?.extract()?;
                let seconds: i64 = offset.getattr("seconds")?.extract()?;
                let micro: i64 = offset.getattr("microseconds")?.extract()?;
                Some((days * 86_400 + seconds) * 1_000_000 + micro)
            };
            return Ok(ConfigValue::DateTime(
                meerschaum_core::yaml::node::DateTime {
                    date: meerschaum_core::yaml::node::Date {
                        year: obj.getattr("year")?.extract::<i32>()?,
                        month: attr("month")?,
                        day: attr("day")?,
                    },
                    hour: attr("hour")?,
                    minute: attr("minute")?,
                    second: attr("second")?,
                    microsecond: attr("microsecond")?,
                    utc_offset_us,
                },
            ));
        }
        if obj.is_instance(&self.date_cls)? {
            return Ok(ConfigValue::Date(meerschaum_core::yaml::node::Date {
                year: obj.getattr("year")?.extract::<i32>()?,
                month: obj.getattr("month")?.extract::<u32>()?,
                day: obj.getattr("day")?.extract::<u32>()?,
            }));
        }
        self.opaque(obj)
    }
}

/// [`ConfigValue`] -> Python, restoring the opaque objects of a [`ToCfg`].
struct FromCfg<'py, 'a> {
    py: Python<'py>,
    to_py: ToPy<'py>,
    opaque: &'a [Py<PyAny>],
}

impl<'py, 'a> FromCfg<'py, 'a> {
    fn new(py: Python<'py>, opaque: &'a [Py<PyAny>]) -> PyResult<Self> {
        Ok(FromCfg {
            py,
            to_py: ToPy::new(py)?,
            opaque,
        })
    }

    fn opaque_object(&self, text: &str) -> Option<Bound<'py, PyAny>> {
        let n = text
            .strip_prefix(OPAQUE_PREFIX)?
            .strip_suffix(OPAQUE_SUFFIX)?
            .parse::<usize>()
            .ok()?;
        self.opaque.get(n).map(|o| o.bind(self.py).clone())
    }

    fn map(&self, map: &ConfigMap) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(self.py);
        for (k, v) in map {
            let key = match self.opaque_object(k) {
                Some(o) => o,
                None => PyString::new(self.py, k).into_any(),
            };
            dict.set_item(key, self.value(v)?)?;
        }
        Ok(dict)
    }

    fn value(&self, value: &ConfigValue) -> PyResult<Bound<'py, PyAny>> {
        let py = self.py;
        Ok(match value {
            ConfigValue::Null => py.None().into_bound(py),
            ConfigValue::Bool(b) => PyBool::new(py, *b).to_owned().into_any(),
            ConfigValue::Int(i) => i.into_pyobject(py)?.into_any(),
            ConfigValue::BigInt(i) => self.to_py.big_int(i)?,
            ConfigValue::Float(f) => PyFloat::new(py, *f).into_any(),
            ConfigValue::Str(s) => match self.opaque_object(s) {
                Some(o) => o,
                None => PyString::new(py, s).into_any(),
            },
            ConfigValue::Bytes(b) => PyBytes::new(py, b).into_any(),
            ConfigValue::Date(d) => self.to_py.date(d)?,
            ConfigValue::DateTime(dt) => self.to_py.datetime(dt)?,
            ConfigValue::List(items) => {
                let list = PyList::empty(py);
                for item in items {
                    list.append(self.value(item)?)?;
                }
                list.into_any()
            }
            ConfigValue::Tuple(items) => {
                let mut converted = Vec::with_capacity(items.len());
                for item in items {
                    converted.push(self.value(item)?);
                }
                PyTuple::new(py, converted)?.into_any()
            }
            ConfigValue::Set(items) => {
                let set = PySet::empty(py)?;
                for item in items {
                    set.add(self.value(item)?)?;
                }
                set.into_any()
            }
            ConfigValue::Map(m) => self.map(m)?.into_any(),
            ConfigValue::Pairs(pairs) => {
                let dict = PyDict::new(py);
                for (k, v) in pairs {
                    dict.set_item(self.value(k)?, self.value(v)?)?;
                }
                dict.into_any()
            }
        })
    }
}

/// Write the top-level keys that changed between `before` and `after` into `dict`, and drop the
/// ones the call removed; every other key keeps its Python object.
fn write_back(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    before: &ConfigMap,
    after: &ConfigMap,
    opaque: &[Py<PyAny>],
) -> PyResult<()> {
    let from = FromCfg::new(py, opaque)?;
    for (k, v) in after {
        if before.get(k) != Some(v) {
            dict.set_item(k, from.value(v)?)?;
        }
    }
    for k in before.keys() {
        if !after.contains_key(k) {
            let _ = dict.del_item(k);
        }
    }
    Ok(())
}

/// The top-level keys of the live dictionary named by `names` (all of them when `None`), as a
/// config map.
fn load_keys<'a>(
    conv: &mut ToCfg<'a>,
    dict: &Bound<'a, PyDict>,
    names: Option<&[&str]>,
) -> PyResult<ConfigMap> {
    let mut out = ConfigMap::new();
    for (k, v) in dict.iter() {
        let key = match k.cast::<PyString>() {
            Ok(s) => s.to_str()?.to_string(),
            Err(_) => k.str()?.to_str()?.to_string(),
        };
        if names.is_none_or(|n| n.contains(&key.as_str())) {
            out.insert(key, conv.value(&v, 1)?);
        }
    }
    Ok(out)
}

fn py_config_error(py: Python<'_>, error: ConfigError) -> PyErr {
    match error {
        ConfigError::Load { error, .. } => match error {
            LoadError::Yaml(e) => to_py_err(e),
            LoadError::Json(e) => py
                .import("json")
                .and_then(|m| m.getattr("JSONDecodeError"))
                .and_then(|c| c.call1((e.msg.clone(), e.doc.clone(), e.pos)))
                .map(|exc| PyErr::from_value(exc))
                .unwrap_or_else(|_| PyValueError::new_err(e.message.clone())),
            LoadError::Key(key) => PyKeyError::new_err(key),
            LoadError::Decode(message) => PyValueError::new_err(message),
        },
        ConfigError::Io(e) => PyOSError::new_err(e.to_string()),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

// ---------------------------------------------------------------------------------------------
// get_config
// ---------------------------------------------------------------------------------------------

fn walk<'py>(root: &Bound<'py, PyAny>, keys: &[String]) -> Option<Bound<'py, PyAny>> {
    let mut current = root.clone();
    for k in keys {
        current = current.get_item(k).ok()?;
    }
    Some(current)
}

fn map_has_path(map: &ConfigMap, keys: &[String]) -> bool {
    let mut current: Option<&ConfigValue> = None;
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

/// Whether `obj` (a config subtree) holds an `MRSM{` symlink anywhere, as v4's
/// `'MRSM{' in json.dumps(config[keys[0]])` decides. `None` when the subtree holds an object
/// that is not plain config data (the full lookup decides what to do with it).
fn holds_symlink(obj: &Bound<'_, PyAny>, depth: usize) -> Option<bool> {
    const MARKER: &str = "MRSM{";
    if depth > 64 {
        return None;
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Some(s.to_str().ok()?.contains(MARKER));
    }
    if let Ok(d) = obj.cast_exact::<PyDict>() {
        for (k, v) in d.iter() {
            match k.cast::<PyString>() {
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
    if let Ok(l) = obj.cast_exact::<PyList>() {
        for v in l.iter() {
            if holds_symlink(&v, depth + 1)? {
                return Some(true);
            }
        }
        return Some(false);
    }
    if let Ok(t) = obj.cast_exact::<PyTuple>() {
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

/// The read-only fast path (PARITY row 12): when every key is already loaded in the live
/// dictionary and (with `substitute`) the first key's subtree holds no `MRSM{` symlink, the full
/// lookup would only walk the dictionary and return its own object (no substitution, no file
/// reads, no defaults, no writes, no warnings, no `sync_files`), so the walk is done here
/// directly. Nothing is cached: every call inspects the live dictionary, so `set_config`,
/// `replace_config`, `write_config`, environment patches and callers that mutate the returned
/// dicts are seen immediately. `None` falls through to the full lookup.
fn fast_lookup<'py>(
    dict: &Bound<'py, PyDict>,
    keys: &Bound<'py, PyTuple>,
    substitute: bool,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let n = keys.len();
    if n == 0 {
        return Ok(None);
    }
    let first = keys.get_item(0)?;
    let Ok(first_str) = first.cast_exact::<PyString>() else {
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
        let Ok(d) = current.cast_exact::<PyDict>() else {
            return Ok(None);
        };
        match d.get_item(&key)? {
            Some(next) => current = next,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

fn tuple_result<'py>(
    py: Python<'py>,
    valid: bool,
    value: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    Ok(PyTuple::new(py, [PyBool::new(py, valid).to_owned().into_any(), value])?.into_any())
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
    if debug {
        // `dprint(f"Indexing keys: {keys}", color=False)`
        let kwargs = PyDict::new(py);
        kwargs.set_item("color", false)?;
        py.import("meerschaum.utils.debug")?
            .getattr("dprint")?
            .call((format!("Indexing keys: {}", keys.repr()?),), Some(&kwargs))?;
    }
    let dict = store(py);
    if let Some(found) = fast_lookup(&dict, keys, substitute)? {
        return if as_tuple {
            tuple_result(py, true, found)
        } else {
            Ok(found)
        };
    }
    if keys.is_empty() {
        // `_config()` returns the dictionary itself.
        let whole = dict.into_any();
        return if as_tuple {
            tuple_result(py, true, whole)
        } else {
            Ok(whole)
        };
    }
    let keys: Vec<String> = keys
        .iter()
        .map(|k| match k.cast::<PyString>() {
            Ok(s) => Ok(s.to_str()?.to_string()),
            Err(_) => Ok(k.str()?.to_str()?.to_string()),
        })
        .collect::<PyResult<_>>()?;
    let host = host(py)?;

    // When the key path is already loaded, the lookup only reads (and may re-substitute) the
    // first key and the symlinks, so only those are converted.
    let symlinks_key = &meerschaum_core::static_config().config.symlinks_key;
    let loaded = walk(dict.as_any(), &keys).is_some();
    let mut conv = ToCfg::new(py, false)?;
    let names: Option<Vec<&str>> = loaded.then(|| vec![keys[0].as_str(), symlinks_key.as_str()]);
    let before = load_keys(&mut conv, &dict, names.as_deref())?;
    let opts = GetOptions {
        patch: patch && host.allow_writes,
        substitute,
        write_missing: write_missing && host.allow_writes,
        warn,
    };
    let ((valid, value), after, notices) = run(&host, before.clone(), |s| {
        s.get_config_lossless(&keys, opts)
    });

    // Write back the top-level keys the lookup loaded or changed.
    let k0 = &keys[0];
    let newly_loaded = !before.contains_key(k0) && after.contains_key(k0);
    write_back(py, &dict, &before, &after, &conv.opaque)?;
    emit_notices(py, notices, 3)?;
    if newly_loaded && sync_files && host.allow_writes {
        sync_files_in(py, &host, &dict, std::slice::from_ref(k0))?;
    }

    let result = if !valid {
        py.None().into_bound(py)
    } else if map_has_path(&before, &keys) || newly_loaded {
        // Keys already loaded (or loaded now from disk): return the dictionary's own object.
        match walk(dict.as_any(), &keys) {
            Some(obj) => obj,
            None => FromCfg::new(py, &conv.opaque)?.value(&value)?,
        }
    } else {
        // Served from the defaults: a fresh object, as Python returns.
        FromCfg::new(py, &conv.opaque)?.value(&value)?
    };
    if as_tuple {
        return tuple_result(py, valid, result);
    }
    Ok(result)
}

/// `sync_files(keys)` against the live dictionary: the `stack` key's derivative files (Compose,
/// Grafana, the extensions script) are rewritten from the config as it is now (`_sync.py`).
fn sync_files_in(
    py: Python<'_>,
    host: &Host,
    dict: &Bound<'_, PyDict>,
    keys: &[String],
) -> PyResult<()> {
    if !keys.iter().any(|k| k == "stack") {
        return Ok(());
    }
    // The files read other top-level keys through `MRSM{}`, so the whole live tree goes in.
    let mut conv = ToCfg::new(py, false)?;
    let before = load_keys(&mut conv, dict, None)?;
    let wanted: Vec<&str> = keys.iter().map(String::as_str).collect();
    let (result, after, notices) = run(host, before.clone(), |s| s.sync_files(&wanted));
    write_back(py, dict, &before, &after, &conv.opaque)?;
    emit_notices(py, notices, 3)?;
    result.map_err(PyOSError::new_err)
}

/// `sync_files(keys=None)`.
#[pyfunction]
#[pyo3(signature = (keys=None))]
pub fn sync_files(py: Python<'_>, keys: Option<Vec<String>>) -> PyResult<()> {
    let host = host(py)?;
    let dict = store(py);
    sync_files_in(py, &host, &dict, &keys.unwrap_or_default())
}

/// `get_plugin_config(*keys)` for a named plugin: `get_config('plugins', plugin, *keys)`.
#[pyfunction]
#[pyo3(signature = (plugin_name, *keys, warn=false, **kw))]
pub fn get_plugin_config<'py>(
    py: Python<'py>,
    plugin_name: &str,
    keys: &Bound<'py, PyTuple>,
    warn: bool,
    kw: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut all: Vec<Bound<'py, PyAny>> = vec![
        PyString::new(py, "plugins").into_any(),
        PyString::new(py, plugin_name).into_any(),
    ];
    all.extend(keys.iter());
    let flag = |name: &str, default: bool| -> PyResult<bool> {
        match kw.and_then(|k| k.get_item(name).ok().flatten()) {
            Some(v) => v.extract(),
            None => Ok(default),
        }
    };
    get_config(
        py,
        &PyTuple::new(py, all)?,
        flag("patch", true)?,
        flag("substitute", true)?,
        flag("sync_files", true)?,
        flag("write_missing", true)?,
        flag("as_tuple", false)?,
        warn,
        flag("debug", false)?,
    )
}

/// `write_plugin_config(config_dict)` for a named plugin.
#[pyfunction]
#[pyo3(signature = (plugin_name, config_dict, **kw))]
pub fn write_plugin_config<'py>(
    py: Python<'py>,
    plugin_name: &str,
    config_dict: &Bound<'py, PyAny>,
    kw: Option<&Bound<'py, PyDict>>,
) -> PyResult<bool> {
    // Native plugins' own writes first, so that this write keeps their sections.
    if meerschaum_cli::native_plugins::has_written_plugin_configs() {
        crate::native_plugin_api::absorb_plugin_config_writes(py, &live_config(py)?)?;
    }
    let keys = PyTuple::new(py, ["plugins"])?;
    let plugins = get_config(py, &keys, true, true, true, true, false, false, false)?;
    let plugins: Bound<'py, PyDict> = match plugins.cast_into::<PyDict>() {
        Ok(d) => d,
        Err(_) => PyDict::new(py),
    };
    plugins.set_item(plugin_name, config_dict)?;
    let cf = PyDict::new(py);
    cf.set_item("plugins", plugins)?;
    let directory = match kw.and_then(|k| k.get_item("directory").ok().flatten()) {
        Some(d) if !d.is_none() => Some(path_text(&d)?),
        _ => None,
    };
    write_config(py, Some(cf.as_any()), directory, false, None)
}

// ---------------------------------------------------------------------------------------------
// read_config / write_config / search_and_substitute_config
// ---------------------------------------------------------------------------------------------

/// `read_config(directory=None, keys=None, write_missing=True, substitute=True,
/// with_filenames=False, raise_parsing_errors=False)`.
#[pyfunction]
#[pyo3(signature = (
    directory=None, keys=None, write_missing=true, substitute=true, with_filenames=false,
    raise_parsing_errors=false
))]
pub fn read_config<'py>(
    py: Python<'py>,
    directory: Option<PathBuf>,
    keys: Option<Vec<String>>,
    write_missing: bool,
    substitute: bool,
    with_filenames: bool,
    raise_parsing_errors: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let host = host(py)?;
    let dict = store(py);
    let mut conv = ToCfg::new(py, false)?;
    // Substitution looks keys up in the live config (and may load them).
    let before = if substitute {
        load_keys(&mut conv, &dict, None)?
    } else {
        ConfigMap::new()
    };
    let (result, after, notices) = run(&host, before.clone(), |s| {
        s.read_config_lossless(
            directory.as_deref(),
            keys.as_deref(),
            write_missing,
            substitute,
            raise_parsing_errors,
        )
    });
    if substitute {
        write_back(py, &dict, &before, &after, &conv.opaque)?;
    }
    emit_notices(py, notices, 3)?;
    let (config, filenames) = result.map_err(|e| py_config_error(py, e))?;
    let from = FromCfg::new(py, &conv.opaque)?;
    let config = from.map(&config)?;
    if with_filenames {
        return Ok(PyTuple::new(
            py,
            [config.into_any(), PyList::new(py, filenames)?.into_any()],
        )?
        .into_any());
    }
    Ok(config.into_any())
}

/// `write_config(config_dict=None, directory=None, debug=False, **kw)`.
#[pyfunction]
#[pyo3(signature = (config_dict=None, directory=None, debug=false, **kw))]
pub fn write_config<'py>(
    py: Python<'py>,
    config_dict: Option<&Bound<'py, PyAny>>,
    directory: Option<PathBuf>,
    debug: bool,
    kw: Option<&Bound<'py, PyDict>>,
) -> PyResult<bool> {
    // v4's `write_config` never reads `debug` (nor any keyword argument).
    let _ = (debug, kw);
    let host = host(py)?;
    if !host.allow_writes {
        return Ok(false);
    }
    let source: Bound<'py, PyAny> = match config_dict {
        Some(c) if !c.is_none() => c.clone(),
        _ => {
            // `_config(allow_replaced=False)`: the config as it was before `replace_config`.
            let backup = config_module(py)?.getattr("_backup_config")?;
            if backup.is_none() {
                store(py).into_any()
            } else {
                backup
            }
        }
    };
    let source = source
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("config_dict must be a dictionary"))?
        .clone();

    // Convert key by key, as v4 writes key by key: an object `yaml.dump` cannot represent fails
    // that key after the earlier keys were written.
    let mut conv = ToCfg::new(py, true)?;
    let mut writable = ConfigMap::new();
    let mut failure: Option<(String, PyErr)> = None;
    for (k, v) in source.iter() {
        let key = match k.cast::<PyString>() {
            Ok(s) => s.to_str()?.to_string(),
            Err(_) => k.str()?.to_str()?.to_string(),
        };
        match conv.value(&v, 0) {
            Ok(value) => {
                writable.insert(key, value);
            }
            Err(e) => {
                failure = Some((key, e));
                break;
            }
        }
    }
    let ((ok, io_error), _, notices) = run(&host, ConfigMap::new(), |s| {
        let ok = s.write_config_lossless(Some(&writable), directory.as_deref());
        (ok, s.take_write_error())
    });
    emit_notices(py, notices, 3)?;
    if !ok {
        // v4 raises what `open(path, 'w+')` raises (`PermissionError` for a read-only file);
        // the file itself is never touched when a write fails.
        if let Some((kind, message)) = io_error {
            return Err(PyErr::from(std::io::Error::new(kind, message)));
        }
        return Ok(false);
    }
    if let Some((_, err)) = failure {
        let text = err.value(py).str()?.to_string();
        let builtins = py.import("builtins")?;
        builtins.getattr("print")?.call1((format!(
            "FAILED TO WRITE!\n{text}\n{{'sort_keys': False, 'indent': 2}}"
        ),))?;
        return Ok(false);
    }
    Ok(true)
}

/// The first keys of every `MRSM{...}` symlink in `config` (`None` when a symlink has no keys:
/// it reads the whole config).
fn symlink_targets(config: &ConfigMap, options: &SubstituteOptions) -> Option<Vec<String>> {
    fn strings<'a>(v: &'a ConfigValue, out: &mut Vec<&'a str>) {
        match v {
            ConfigValue::Str(s) => out.push(s),
            ConfigValue::List(items) | ConfigValue::Tuple(items) | ConfigValue::Set(items) => {
                items.iter().for_each(|i| strings(i, out))
            }
            ConfigValue::Map(m) => {
                for (k, v) in m {
                    out.push(k);
                    strings(v, out);
                }
            }
            ConfigValue::Pairs(p) => {
                for (k, v) in p {
                    strings(k, out);
                    strings(v, out);
                }
            }
            _ => {}
        }
    }
    let needle = options.needle();
    let mut texts = Vec::new();
    for v in config.values() {
        strings(v, &mut texts);
    }
    for k in config.keys() {
        texts.push(k);
    }
    let mut names: Vec<String> = Vec::new();
    for text in texts {
        // The pattern `MRSM{(.*?)}`: the shortest run without a line break up to the end key.
        let mut from = 0;
        while let Some(found) = text[from..].find(&needle) {
            let start = from + found;
            let keys_start = start + needle.len();
            let Some(end) = text[keys_start..].find(options.end_key.as_str()) else {
                break;
            };
            let raw = &text[keys_start..keys_start + end];
            if raw.contains('\n') {
                from = start + 1;
                continue;
            }
            let first = raw.split(options.delimiter.as_str()).next().unwrap_or("");
            let first = first
                .strip_prefix(options.literal_key.as_str())
                .unwrap_or(first);
            if first.is_empty() {
                return None;
            }
            if !names.iter().any(|n| n == first) {
                names.push(first.to_string());
            }
            from = keys_start + end + options.end_key.len();
        }
    }
    Some(names)
}

/// Substitute the `MRSM{}` symlinks of `config` against the live config, loading what they name
/// and writing the loaded keys back. `Ok(None)` when `config` holds none (the caller keeps the
/// object it has).
fn substitute_in_live_config<'a>(
    py: Python<'a>,
    conv: &mut ToCfg<'a>,
    config: &ConfigMap,
    options: &SubstituteOptions,
) -> PyResult<Option<ConfigMap>> {
    if !map_contains_text(config, &options.needle()) {
        return Ok(None);
    }
    let host = host(py)?;
    let dict = store(py);
    let targets = symlink_targets(config, options);
    let names: Option<Vec<&str>> = targets
        .as_ref()
        .map(|t| t.iter().map(String::as_str).collect());
    let before = load_keys(conv, &dict, names.as_deref())?;
    let (result, after, notices) = run(&host, before.clone(), |s| {
        s.search_and_substitute_with(config, options)
    });
    write_back(py, &dict, &before, &after, &conv.opaque)?;
    emit_notices(py, notices, 3)?;
    result.map(Some).map_err(PyValueError::new_err)
}

/// `search_and_substitute_config(config, leading_key='MRSM', delimiter=':', begin_key='{',
/// end_key='}', literal_key='!', keep_symlinks=True)`.
#[pyfunction]
#[pyo3(signature = (
    config, leading_key="MRSM", delimiter=":", begin_key="{", end_key="}", literal_key="!",
    keep_symlinks=true
))]
#[allow(clippy::too_many_arguments)]
pub fn search_and_substitute_config<'py>(
    py: Python<'py>,
    config: &Bound<'py, PyAny>,
    leading_key: &str,
    delimiter: &str,
    begin_key: &str,
    end_key: &str,
    literal_key: &str,
    keep_symlinks: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = config
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("config must be a dictionary"))?;
    let options = SubstituteOptions {
        leading_key: leading_key.to_string(),
        delimiter: delimiter.to_string(),
        begin_key: begin_key.to_string(),
        end_key: end_key.to_string(),
        literal_key: literal_key.to_string(),
        keep_symlinks,
    };
    let mut conv = ToCfg::new(py, false)?;
    let cfg = conv.map(dict, 0)?;
    match substitute_in_live_config(py, &mut conv, &cfg, &options)? {
        // No symlinks: v4 returns the dictionary it was given.
        None => Ok(config.clone()),
        Some(substituted) => Ok(FromCfg::new(py, &conv.opaque)?
            .map(&substituted)?
            .into_any()),
    }
}

/// Substitute the `MRSM{}` symlinks of a pipe's resolved parameters (`Pipe.get_parameters`):
/// `search_and_substitute_config(parameters)` over the live config. Parameters without a symlink
/// skip the registry.
pub(crate) fn substitute_parameters<'py>(
    py: Python<'py>,
    parameters: &Map,
) -> PyResult<Bound<'py, PyAny>> {
    let cfg = ConfigValue::map_from_value(parameters);
    let options = SubstituteOptions::default();
    if !map_contains_text(&cfg, &options.needle()) {
        return crate::convert::core_to_py(py, &Value::Map(parameters.clone()));
    }
    let mut conv = ToCfg::new(py, false)?;
    match substitute_in_live_config(py, &mut conv, &cfg, &options)? {
        None => crate::convert::core_to_py(py, &Value::Map(parameters.clone())),
        Some(substituted) => Ok(FromCfg::new(py, &conv.opaque)?
            .map(&substituted)?
            .into_any()),
    }
}

/// `revert_symlinks_config(config)`.
#[pyfunction]
pub fn revert_symlinks_config<'py>(
    py: Python<'py>,
    config: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let dict = config
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("config must be a dictionary"))?;
    let symlinks_key = &meerschaum_core::static_config().config.symlinks_key;
    if !dict.contains(symlinks_key)? {
        return Ok(config.clone());
    }
    let mut conv = ToCfg::new(py, false)?;
    let cfg = conv.map(dict, 0)?;
    let reverted = meerschaum_core::config::revert_symlinks_lossless(&cfg);
    Ok(FromCfg::new(py, &conv.opaque)?.map(&reverted)?.into_any())
}

fn py_path<'py>(py: Python<'py>, path: &std::path::Path) -> PyResult<Bound<'py, PyAny>> {
    py.import("pathlib")?
        .getattr("Path")?
        .call1((path.to_string_lossy().to_string(),))
}

/// `get_keyfile_path(key, create_new=False, directory=None)`.
#[pyfunction]
#[pyo3(signature = (key, create_new=false, directory=None))]
pub fn get_keyfile_path<'py>(
    py: Python<'py>,
    key: &str,
    create_new: bool,
    directory: Option<PathBuf>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let host = host(py)?;
    let (path, _, _) = run(&host, ConfigMap::new(), |s| {
        s.get_keyfile_path(key, create_new, directory.as_deref())
    });
    path.map(|p| py_path(py, &p)).transpose()
}

// ---------------------------------------------------------------------------------------------
// Backups
// ---------------------------------------------------------------------------------------------

/// `get_key_backups_dir(key, create=False)`.
#[pyfunction]
#[pyo3(signature = (key, create=false))]
pub fn get_key_backups_dir<'py>(
    py: Python<'py>,
    key: &str,
    create: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let host = host(py)?;
    let (dir, _, _) = run(&host, ConfigMap::new(), |s| s.key_backups_dir(key, create));
    py_path(py, &dir)
}

/// `list_config_backups(key)`: the backups of a key, newest first.
#[pyfunction]
pub fn list_config_backups<'py>(py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyList>> {
    let host = host(py)?;
    let (backups, _, _) = run(&host, ConfigMap::new(), |s| s.list_config_backups(key));
    let list = PyList::empty(py);
    for backup in backups {
        list.append(py_path(py, &backup)?)?;
    }
    Ok(list)
}

/// `backup_config_key(key, debug=False)`.
#[pyfunction]
#[pyo3(signature = (key, debug=false))]
pub fn backup_config_key<'py>(
    py: Python<'py>,
    key: &str,
    debug: bool,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let host = host(py)?;
    let (backup, _, notices) = run(&host, ConfigMap::new(), |s| s.backup_config_key(key));
    emit_notices_debug(py, notices, 3, debug)?;
    backup.map(|p| py_path(py, &p)).transpose()
}

/// `restore_config_backup(key, backup_path=None, index=0, debug=False)` without the reload (the
/// façade reloads Meerschaum when it succeeds).
#[pyfunction]
#[pyo3(signature = (key, backup_path=None, index=0, debug=false))]
pub fn restore_config_backup(
    py: Python<'_>,
    key: &str,
    backup_path: Option<PathBuf>,
    index: i64,
    debug: bool,
) -> PyResult<(bool, String)> {
    let host = host(py)?;
    let (result, _, notices) = run(&host, ConfigMap::new(), |s| {
        s.restore_config_backup(key, backup_path.as_deref(), index)
    });
    emit_notices_debug(py, notices, 3, debug)?;
    Ok(result)
}

// ---------------------------------------------------------------------------------------------
// Environment layers
// ---------------------------------------------------------------------------------------------

/// The `(name, value)` pairs of `env` (`os.environ` when `None`).
fn env_pairs(py: Python<'_>, env: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<(String, String)>> {
    let source: Bound<'_, PyAny> = match env {
        Some(e) if !e.is_none() => e.clone(),
        _ => py.import("os")?.getattr(intern!(py, "environ"))?,
    };
    let mut out = Vec::new();
    for pair in source.call_method0("items")?.try_iter()? {
        let pair = pair?;
        // A variable that is not valid UTF-8 reaches Python as surrogate escapes, which cannot
        // become a `String`; it cannot name or hold anything Meerschaum reads, so it is skipped.
        if let (Ok(name), Ok(value)) = (
            pair.get_item(0)?.extract::<String>(),
            pair.get_item(1)?.extract::<String>(),
        ) {
            out.push((name, value));
        }
    }
    Ok(out)
}

/// Run an environment layer against the live config and make the result the live dictionary
/// (v4 rebinds with `set_config`; unchanged top-level keys keep their objects).
fn apply_layer(py: Python<'_>, f: impl FnOnce(&mut ConfigState)) -> PyResult<()> {
    let host = host(py)?;
    let dict = store(py);
    let mut conv = ToCfg::new(py, false)?;
    let before = load_keys(&mut conv, &dict, None)?;
    let (_, after, notices) = run(&host, before.clone(), f);
    let from = FromCfg::new(py, &conv.opaque)?;
    let replacement = PyDict::new(py);
    for (k, v) in &after {
        match (before.get(k) == Some(v), dict.get_item(k)?) {
            (true, Some(existing)) => replacement.set_item(k, existing)?,
            _ => replacement.set_item(k, from.value(v)?)?,
        }
    }
    // Replace the Python-side dict too (`_store.set_store` hands it back to `config_set`), so
    // `meerschaum.config.config` is the object this registry uses.
    py.import("meerschaum.config._store")?
        .getattr("set_store")?
        .call1((replacement,))?;
    emit_notices(py, notices, 3)
}

/// `apply_environment_patches(env=None)`: `MRSM_CONFIG`, then `MRSM_PATCH`.
#[pyfunction]
#[pyo3(signature = (env=None))]
pub fn apply_environment_patches(py: Python<'_>, env: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let pairs = env_pairs(py, env)?;
    apply_layer(py, move |s| s.apply_environment_patches(&pairs))
}

/// `apply_environment_uris(env=None)`: the `MRSM_<TYPE>_<LABEL>` connectors.
#[pyfunction]
#[pyo3(signature = (env=None))]
pub fn apply_environment_uris(py: Python<'_>, env: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let pairs = env_pairs(py, env)?;
    apply_layer(py, move |s| {
        // PARITY: v4 replaces the whole config with `meerschaum` plus the connector
        // (`config/environment.py:166`).
        s.apply_environment_uris(&pairs, false);
    })
}

/// Register the config functions on the `_core` module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(config_dict, m)?)?;
    m.add_function(wrap_pyfunction!(config_set, m)?)?;
    m.add_function(wrap_pyfunction!(config_reset, m)?)?;
    m.add_function(wrap_pyfunction!(get_config, m)?)?;
    m.add_function(wrap_pyfunction!(get_plugin_config, m)?)?;
    m.add_function(wrap_pyfunction!(write_plugin_config, m)?)?;
    m.add_function(wrap_pyfunction!(read_config, m)?)?;
    m.add_function(wrap_pyfunction!(write_config, m)?)?;
    m.add_function(wrap_pyfunction!(search_and_substitute_config, m)?)?;
    m.add_function(wrap_pyfunction!(revert_symlinks_config, m)?)?;
    m.add_function(wrap_pyfunction!(get_keyfile_path, m)?)?;
    m.add_function(wrap_pyfunction!(get_key_backups_dir, m)?)?;
    m.add_function(wrap_pyfunction!(list_config_backups, m)?)?;
    m.add_function(wrap_pyfunction!(backup_config_key, m)?)?;
    m.add_function(wrap_pyfunction!(restore_config_backup, m)?)?;
    m.add_function(wrap_pyfunction!(apply_environment_patches, m)?)?;
    m.add_function(wrap_pyfunction!(apply_environment_uris, m)?)?;
    m.add_function(wrap_pyfunction!(sync_files, m)?)?;
    m.add_function(wrap_pyfunction!(live_config_paths, m)?)?;
    Ok(())
}
