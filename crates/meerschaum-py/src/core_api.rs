//! `meerschaum-core` bound into `meerschaum._core`: `{{ Pipe() }}` templating
//! (`replace_pipes_syntax`) and password hashing. The config registry (`get_config`, `read_config`,
//! ...) is in `config_api.rs`.

use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::template::TemplateResolver;
use meerschaum_core::{Map, Pipe, Value};
use pyo3::exceptions::{PyOverflowError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt};

use crate::convert::{core_to_py, py_to_core};

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
        if let Some((core, obj)) = &self.self_pipe
            && core == pipe
        {
            return Ok(obj.clone());
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

/// Match hashlib's conversion order and platform C-long overflow exceptions exactly.
fn password_rounds_long(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<std::os::raw::c_long> {
    // SAFETY: `value` is a live Python object and the GIL is held. The stable-ABI function
    // performs the same __index__ conversion as hashlib and sets the Python error on failure.
    let rounds = unsafe { pyo3::ffi::PyLong_AsLong(value.as_ptr()) };
    if rounds == -1 && PyErr::occurred(py) {
        return Err(PyErr::fetch(py));
    }
    Ok(rounds)
}

fn checked_password_rounds(rounds: std::os::raw::c_long) -> PyResult<u32> {
    if rounds < 1 {
        return Err(PyValueError::new_err(
            "iteration value must be greater than 0.",
        ));
    }
    if i128::from(rounds) > i128::from(i32::MAX) {
        return Err(PyOverflowError::new_err("iteration value is too great."));
    }
    Ok(rounds as u32)
}

fn password_rounds(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<u32> {
    checked_password_rounds(password_rounds_long(py, value)?)
}

/// `hash_password(password, salt=None, rounds=None)` (passlib `pbkdf2-sha256` format).
#[pyfunction]
#[pyo3(signature = (password, salt=None, rounds=None))]
pub fn hash_password(
    py: Python<'_>,
    password: Option<String>,
    salt: Option<Vec<u8>>,
    rounds: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let password = password.unwrap_or_default();
    let rounds = rounds.map(|value| password_rounds(py, value)).transpose()?;
    py.detach(|| meerschaum_core::users::hash_password(&password, salt.as_deref(), rounds))
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
    let parts: Vec<&str> = password_hash.split('$').collect();
    let malformed = || -> PyResult<bool> {
        emit_warnings(
            py,
            vec![format!(
                "Failed to extract context from password hash '{password_hash}'. Is it corrupted?"
            )],
            2,
        )?;
        Ok(false)
    };
    if parts.len() != 5
        || meerschaum_core::users::ab64_decode(parts[3]).is_err()
        || meerschaum_core::users::ab64_decode(parts[4]).is_err()
    {
        return malformed();
    }
    // v4 parses an arbitrary-size Python int, then hashlib checks its iteration bound.
    // Preserve that order: a huge valid decimal is an OverflowError, not a malformed hash.
    let Ok(rounds) = py.get_type::<PyInt>().call1((parts[2],)) else {
        return malformed();
    };
    let algorithm = parts[1].rsplit('-').next().unwrap_or(parts[1]);
    // Argument Clinic rejects an embedded NUL in the digest name before converting rounds.
    if algorithm.contains('\0') {
        py.import("_hashlib")?.call_method1("new", (algorithm,))?;
    }
    let rounds = password_rounds_long(py, &rounds)?;
    // CPython looks up a digest after C-long conversion but before its iteration bounds.
    // Only native-unsupported names consult stdlib lookup metadata: no password, salt, or
    // Python PBKDF2 implementation is involved. Its error type/message depend on the runtime.
    let digest = match meerschaum_core::users::pbkdf2_hmac(algorithm, b"", b"", 1) {
        Err(meerschaum_core::users::PasswordError::UnsupportedDigest(_)) => {
            Some(py.import("_hashlib")?.call_method1("new", (algorithm,))?)
        }
        _ => None,
    };
    let rounds = checked_password_rounds(rounds)?;
    if let Some(digest) = digest
        && digest.getattr("digest_size")?.extract::<usize>()? == 0
    {
        // PARITY: CPython 3.11-3.15 validates the derived key length after iterations.
        // SHAKE has no fixed digest size and cannot be used for PBKDF2.
        return Err(PyValueError::new_err("key length must be greater than 0."));
    }
    let normalized = format!("${}${rounds}${}${}", parts[1], parts[3], parts[4]);
    py.detach(|| meerschaum_core::users::verify_password(&password, &normalized))
        .map_err(|e| PyValueError::new_err(e.to_string()))
}
