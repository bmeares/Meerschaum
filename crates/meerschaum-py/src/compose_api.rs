//! `meerschaum.compose.utils` over `meerschaum-compose` (wave 16, D14): the configuration
//! helpers the Python names `read_compose_config`, `get_env_dict`, `get_dir_paths` and `init_env`
//! delegate to. Dictionaries cross as `dict`s of plain values (paths as strings); the messages a
//! call would have printed come back as `[('warn' | 'info', text)]` for the façade to print.
//!
//! The process environment is the process's own (`os.environ` writes reach it through `putenv`);
//! nothing here modifies it: `compose_init_env` and `compose_read_config` return the environment
//! they computed and the façade applies it.

use meerschaum_compose::env::ProcessEnv;
use meerschaum_compose::error::{ComposeError, ErrorKind, Message, Messages};
use meerschaum_compose::project::{self, Host};
use meerschaum_core::{Map, Value};
use pyo3::exceptions::{
    PyAttributeError, PyException, PyFileExistsError, PyFileNotFoundError, PyOSError, PyTypeError,
    PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::path::Path;

use crate::convert::{core_to_py, py_to_core};

fn to_py_err(e: ComposeError) -> PyErr {
    match e.kind {
        ErrorKind::FileNotFound => PyFileNotFoundError::new_err(e.message),
        ErrorKind::FileExists => PyFileExistsError::new_err(e.message),
        ErrorKind::Value => PyValueError::new_err(e.message),
        ErrorKind::Type => PyTypeError::new_err(e.message),
        ErrorKind::Attribute => PyAttributeError::new_err(e.message),
        ErrorKind::Os => PyOSError::new_err(e.message),
        ErrorKind::Yaml | ErrorKind::Exception => PyException::new_err(e.message),
    }
}

fn messages_to_py<'py>(py: Python<'py>, msgs: &Messages) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for m in &msgs.items {
        let (kind, text) = match m {
            Message::Warn(t) => ("warn", t),
            Message::Info(t) => ("info", t),
        };
        out.append((kind, text))?;
    }
    Ok(out)
}

fn env_to_py<'py>(py: Python<'py>, env: &ProcessEnv) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    for (k, v) in env {
        out.set_item(k, v)?;
    }
    Ok(out)
}

/// The process environment, in order; a variable that is not valid UTF-8 is skipped
/// (`std::env::vars()` would panic on it).
fn process_env() -> ProcessEnv {
    std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect()
}

fn host() -> PyResult<Host> {
    let cwd = std::env::current_dir().map_err(|e| PyOSError::new_err(e.to_string()))?;
    Host::new(cwd, process_env()).map_err(to_py_err)
}

fn config_arg(config: &Bound<'_, PyAny>) -> PyResult<Map> {
    match py_to_core(config)? {
        Value::Map(m) => Ok(m),
        _ => Err(PyTypeError::new_err(
            "The compose configuration must be a dictionary.",
        )),
    }
}

/// `_core.compose_init_env(compose_file, env_file=None)`: the environment after loading the
/// project's `.env` (or `env_file`; existing variables win), with the messages.
#[pyfunction]
#[pyo3(signature = (compose_file, env_file=None))]
pub fn compose_init_env<'py>(
    py: Python<'py>,
    compose_file: &str,
    env_file: Option<&str>,
) -> PyResult<Bound<'py, PyTuple>> {
    let mut env: ProcessEnv = process_env();
    let mut msgs = Messages::default();
    project::init_env(
        Path::new(compose_file),
        env_file.map(Path::new),
        &mut env,
        &mut msgs,
    );
    PyTuple::new(
        py,
        [
            env_to_py(py, &env)?.into_any(),
            messages_to_py(py, &msgs)?.into_any(),
        ],
    )
}

/// `_core.compose_read_config(compose_file, env_file=None, isolated=False)`: the resolved
/// compose configuration (`read_compose_config`), the environment after reading it (undefined
/// variables blanked) and the messages.
#[pyfunction]
#[pyo3(signature = (compose_file, env_file=None, isolated=false))]
pub fn compose_read_config<'py>(
    py: Python<'py>,
    compose_file: &str,
    env_file: Option<&str>,
    isolated: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let host = host()?;
    let mut env: ProcessEnv = process_env();
    let mut msgs = Messages::default();
    let (config, _document) = project::read_compose_config(
        &host,
        Path::new(compose_file),
        env_file.map(Path::new),
        isolated,
        &mut env,
        &mut msgs,
    )
    .map_err(to_py_err)?;
    PyTuple::new(
        py,
        [
            core_to_py(py, &Value::Map(config))?,
            env_to_py(py, &env)?.into_any(),
            messages_to_py(py, &msgs)?.into_any(),
        ],
    )
}

/// `_core.compose_env_dict(config)`: `get_env_dict(compose_config)`, the environment a project's
/// commands run under.
#[pyfunction]
pub fn compose_env_dict<'py>(
    py: Python<'py>,
    config: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let config = config_arg(config)?;
    let env: ProcessEnv = process_env();
    let dict = project::get_env_dict(&config, &env).map_err(to_py_err)?;
    let map: Map = dict.into_iter().collect();
    core_to_py(py, &Value::Map(map))
}

/// `_core.compose_dir_paths(config, dir_name)`: `get_dir_paths(compose_config, 'root' |
/// 'plugins')` as strings, with the messages.
#[pyfunction]
pub fn compose_dir_paths<'py>(
    py: Python<'py>,
    config: &Bound<'py, PyAny>,
    dir_name: &str,
) -> PyResult<Bound<'py, PyTuple>> {
    let config = config_arg(config)?;
    let host = host()?;
    let mut msgs = Messages::default();
    let paths = project::get_dir_paths(&config, dir_name, &host, &mut msgs).map_err(to_py_err)?;
    let list = PyList::empty(py);
    for p in paths {
        list.append(project::posix(&p))?;
    }
    PyTuple::new(py, [list.into_any(), messages_to_py(py, &msgs)?.into_any()])
}

/// `_core.compose_installed_plugins(config)`: `get_installed_plugins(compose_config)`, the names
/// of the plugins in the project's plugins directories.
#[pyfunction]
pub fn compose_installed_plugins(config: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let config = config_arg(config)?;
    let env: ProcessEnv = process_env();
    let dict = project::get_env_dict(&config, &env).map_err(to_py_err)?;
    let dirs = meerschaum_compose::plugins::plugins_dirs_from_env(&dict);
    Ok(meerschaum_compose::plugins::plugin_names_in_dirs(&dirs))
}
