//! `meerschaum._core.Pipe`: a Pipe's identity (keys) and parameter storage in Rust
//! (design §4.4). The Python `Pipe` keeps one of these under `MRSM_CORE=rust` as its source of
//! truth for the keys and the **raw** (unresolved) parameters; resolution of `{{ Pipe() }}`
//! templates against the instance goes through a Python-side resolver (`PyResolver`).
//!
//! Constructing a `Pipe` never touches an instance: it only maps arguments onto the core
//! constructor. Nothing here reads or writes a database.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use meerschaum_core::{Map, Pipe, Value};
use pyo3::exceptions::{PyIndexError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::convert::{core_to_py, py_to_core};
use crate::core_api::{PyResolver, emit_warnings, py_defaults};

/// A path element: a dict key or a list index.
enum Step {
    Key(String),
    Index(i64),
}

fn steps(path: &Bound<'_, PyAny>) -> PyResult<Vec<Step>> {
    let mut out = Vec::new();
    for item in path.try_iter()? {
        let item = item?;
        if let Ok(i) = item.extract::<i64>() {
            if !item.is_instance_of::<pyo3::types::PyBool>() {
                out.push(Step::Index(i));
                continue;
            }
        }
        out.push(Step::Key(item.str()?.to_str()?.to_string()));
    }
    Ok(out)
}

fn index_of(len: usize, i: i64) -> Option<usize> {
    let idx = if i < 0 { len as i64 + i } else { i };
    (0..len as i64).contains(&idx).then_some(idx as usize)
}

fn get_in<'a>(root: &'a Map, path: &[Step]) -> Option<&'a Value> {
    let mut cur: Option<&Value> = None;
    for (n, step) in path.iter().enumerate() {
        cur = match (step, cur) {
            (Step::Key(k), None) if n == 0 => root.get(k),
            (Step::Key(k), Some(Value::Map(m))) => m.get(k),
            (Step::Index(i), Some(Value::List(l))) => index_of(l.len(), *i).map(|i| &l[i]),
            _ => None,
        };
        cur?;
    }
    cur
}

fn child_mut<'a>(parent: &'a mut Value, step: &Step, create: bool) -> PyResult<&'a mut Value> {
    match (parent, step) {
        (Value::Map(m), Step::Key(k)) => {
            if !m.contains_key(k) {
                if !create {
                    return Err(PyKeyError::new_err(k.clone()));
                }
                m.insert(k.clone(), Value::Map(Map::new()));
            }
            Ok(m.get_mut(k).expect("inserted above"))
        }
        (Value::List(l), Step::Index(i)) => {
            let idx = index_of(l.len(), *i)
                .ok_or_else(|| PyIndexError::new_err("list index out of range"))?;
            Ok(&mut l[idx])
        }
        _ => Err(PyKeyError::new_err(
            "the path does not lead through a container",
        )),
    }
}

/// The Rust identity and raw parameters of a pipe.
#[pyclass(name = "Pipe", module = "meerschaum._core")]
pub struct PyPipe {
    inner: Mutex<Pipe>,
}

impl PyPipe {
    fn lock(&self) -> std::sync::MutexGuard<'_, Pipe> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn map_arg(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Map> {
    match obj.filter(|o| !o.is_none()) {
        None => Ok(Map::new()),
        Some(o) => match py_to_core(o)? {
            Value::Map(m) => Ok(m),
            _ => Err(PyValueError::new_err(
                "The parameters must be a dictionary.",
            )),
        },
    }
}

#[pymethods]
impl PyPipe {
    #[new]
    #[pyo3(signature = (connector_keys, metric_key, location_key=None, instance_keys=None, parameters=None))]
    fn new(
        py: Python<'_>,
        connector_keys: &str,
        metric_key: &str,
        location_key: Option<&str>,
        instance_keys: Option<&str>,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let defaults = py_defaults(py)?;
        let mut builder = Pipe::builder(connector_keys, metric_key)
            .location(location_key)
            .parameters(map_arg(parameters)?)
            .defaults(defaults);
        if let Some(ik) = instance_keys {
            builder = builder.instance(ik);
        }
        let pipe = builder
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self {
            inner: Mutex::new(pipe),
        })
    }

    #[getter]
    fn connector_keys(&self) -> String {
        self.lock().connector_keys().to_string()
    }

    #[getter]
    fn metric_key(&self) -> String {
        self.lock().metric_key().to_string()
    }

    #[getter]
    fn location_key(&self) -> Option<String> {
        self.lock().location_key().map(str::to_string)
    }

    #[getter]
    fn instance_keys(&self) -> String {
        self.lock().instance_keys().to_string()
    }

    /// The raw (unresolved) parameters as a new dict.
    #[getter]
    fn raw_parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let map = self.lock().raw_parameters().clone();
        core_to_py(py, &Value::Map(map))
    }

    /// Replace the raw parameters (`pipe.parameters = {...}`).
    fn set_raw_parameters(&self, parameters: &Bound<'_, PyAny>) -> PyResult<()> {
        let map = map_arg(Some(parameters))?;
        self.lock().set_parameters(map);
        Ok(())
    }

    /// Deep-merge a patch into the raw parameters (`update_parameters(persist=False)`).
    fn update_raw_parameters(&self, patch: &Bound<'_, PyAny>) -> PyResult<()> {
        let map = map_arg(Some(patch))?;
        self.lock().update_parameters(map);
        Ok(())
    }

    /// The raw value at `path` (a sequence of keys and list indices); `KeyError` if absent.
    fn get_path<'py>(
        &self,
        py: Python<'py>,
        path: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let path = steps(path)?;
        let pipe = self.lock();
        match get_in(pipe.raw_parameters(), &path) {
            Some(v) => core_to_py(py, v),
            None => Err(PyKeyError::new_err("path not found")),
        }
    }

    /// Set the raw value at `path`, creating missing dictionaries on the way.
    fn set_path(&self, path: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let path = steps(path)?;
        let value = py_to_core(value)?;
        let Some((last, init)) = path.split_last() else {
            return Err(PyValueError::new_err("The path is empty."));
        };
        let mut pipe = self.lock();
        let mut root = Value::Map(pipe.raw_parameters().clone());
        {
            let mut cur = &mut root;
            for step in init {
                cur = child_mut(cur, step, true)?;
            }
            match (cur, last) {
                (Value::Map(m), Step::Key(k)) => {
                    m.insert(k.clone(), value);
                }
                (Value::List(l), Step::Index(i)) => {
                    let idx = index_of(l.len(), *i)
                        .ok_or_else(|| PyIndexError::new_err("list index out of range"))?;
                    l[idx] = value;
                }
                _ => {
                    return Err(PyKeyError::new_err(
                        "the path does not lead through a container",
                    ));
                }
            }
        }
        if let Value::Map(m) = root {
            pipe.set_parameters(m);
        }
        Ok(())
    }

    /// Delete the raw value at `path`; `KeyError` if absent.
    fn del_path(&self, path: &Bound<'_, PyAny>) -> PyResult<()> {
        let path = steps(path)?;
        let Some((last, init)) = path.split_last() else {
            return Err(PyValueError::new_err("The path is empty."));
        };
        let mut pipe = self.lock();
        let mut root = Value::Map(pipe.raw_parameters().clone());
        {
            let mut cur = &mut root;
            for step in init {
                cur = child_mut(cur, step, false)?;
            }
            match (cur, last) {
                (Value::Map(m), Step::Key(k)) => {
                    m.shift_remove(k)
                        .ok_or_else(|| PyKeyError::new_err(k.clone()))?;
                }
                (Value::List(l), Step::Index(i)) => {
                    let idx = index_of(l.len(), *i)
                        .ok_or_else(|| PyIndexError::new_err("list index out of range"))?;
                    l.remove(idx);
                }
                _ => {
                    return Err(PyKeyError::new_err(
                        "the path does not lead through a container",
                    ));
                }
            }
        }
        if let Value::Map(m) = root {
            pipe.set_parameters(m);
        }
        Ok(())
    }

    /// The resolved parameters (references, `{{ Pipe() }}`, `MRSM{}`). With `pipe` (the Python
    /// `Pipe` this object backs) cross-pipe templates are answered from live Python pipes;
    /// without it only self templates and config symlinks resolve.
    #[pyo3(signature = (pipe=None))]
    fn resolved_parameters<'py>(
        &self,
        py: Python<'py>,
        pipe: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut core = self.lock().clone();
        let warnings = match pipe.filter(|p| !p.is_none()) {
            Some(obj) => {
                let resolver = PyResolver {
                    py,
                    defaults: py_defaults(py)?,
                    self_pipe: Some((core.clone(), obj)),
                };
                core.resolve_parameters_with(&resolver, None)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
            }
            None => {
                let resolver = meerschaum_core::template::InMemoryResolver {
                    defaults: py_defaults(py)?,
                    pipes: Vec::new(),
                };
                core.resolve_parameters_with(&resolver, None)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
            }
        };
        emit_warnings(py, warnings, 2)?;
        core_to_py(py, &Value::Map(core.parameters().clone()))
    }

    /// `pipe.target` for the given SQL flavor (no side effects).
    #[pyo3(signature = (sql_flavor=None))]
    fn target(&self, sql_flavor: Option<&str>) -> PyResult<String> {
        self.lock()
            .target(sql_flavor)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// `str(pipe)` without ANSI styling.
    fn __str__(&self) -> String {
        self.lock().to_string()
    }

    fn __repr__(&self) -> String {
        self.lock().to_string()
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.downcast::<PyPipe>() {
            Ok(o) => *self.lock() == *o.borrow().lock(),
            Err(_) => false,
        }
    }

    fn __hash__(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.lock().hash(&mut h);
        h.finish()
    }

    /// `Pipe.__getstate__`: the keys plus the raw parameters.
    fn __getstate__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        core_to_py(py, &Value::Map(self.lock().to_state()))
    }

    fn __setstate__(&self, py: Python<'_>, state: &Bound<'_, PyAny>) -> PyResult<()> {
        let Value::Map(map) = py_to_core(state)? else {
            return Err(PyValueError::new_err("The state must be a dictionary."));
        };
        let pipe = Pipe::from_state(&map, &py_defaults(py)?)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        *self.lock() = pipe;
        Ok(())
    }

    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let (ck, mk, lk, ik, params) = {
            let p = self.lock();
            (
                p.connector_keys().to_string(),
                p.metric_key().to_string(),
                p.location_key().map(str::to_string),
                p.instance_keys().to_string(),
                p.raw_parameters().clone(),
            )
        };
        let cls = py.get_type::<PyPipe>();
        let args = PyTuple::new(
            py,
            [
                ck.into_pyobject(py)?.into_any(),
                mk.into_pyobject(py)?.into_any(),
                lk.into_pyobject(py)?.into_any(),
                ik.into_pyobject(py)?.into_any(),
                core_to_py(py, &Value::Map(params))?,
            ],
        )?;
        PyTuple::new(py, [cls.into_any(), args.into_any()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_indices_wrap() {
        assert_eq!(index_of(3, -1), Some(2));
        assert_eq!(index_of(3, 3), None);
        assert_eq!(index_of(0, 0), None);
    }

    #[test]
    fn paths_walk_maps_and_lists() {
        let mut inner = Map::new();
        inner.insert("k".into(), Value::List(vec![Value::Int(1), Value::Int(2)]));
        let mut root = Map::new();
        root.insert("a".into(), Value::Map(inner));
        let path = [
            Step::Key("a".into()),
            Step::Key("k".into()),
            Step::Index(-1),
        ];
        assert!(matches!(get_in(&root, &path), Some(Value::Int(2))));
        assert!(get_in(&root, &[Step::Key("zz".into())]).is_none());
    }
}
