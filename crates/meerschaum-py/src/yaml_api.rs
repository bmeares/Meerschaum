//! `meerschaum._core.yaml`: PyYAML-compatible `load` / `safe_load` / `dump` with a lossless
//! Python boundary.
//!
//! The codec lives in `meerschaum_core::yaml` (a port of PyYAML's reader, scanner, parser,
//! composer, constructor, representer, serializer and emitter). This module only converts
//! between its [`Node`] tree and real Python objects: `datetime.date` / `datetime.datetime`
//! (naive and with offsets, microseconds), `bytes`, arbitrary-size `int`, `bool`, `None`,
//! `tuple`, `set`, and `dict` keys of any hashable type (so `{1: a, True: b}` collapses the way
//! a Python dict does). Aliases yield the same Python object, and shared containers dump as
//! anchors, as PyYAML's `id()`-based representer does.
//!
//! This is never a JSON projection: nothing here goes through `meerschaum_core::Value`.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use meerschaum_core::yaml::dumper::{DumpOptions, dump_node};
use meerschaum_core::yaml::loader::{LoadOptions, load_text};
use meerschaum_core::yaml::node::{Date, DateTime, Node, NodeRef};
use meerschaum_core::yaml::scanner::{ErrorKind, PyYamlError};
use num_bigint::BigInt;
use pyo3::exceptions::{
    PyAttributeError, PyException, PyIndexError, PyKeyError, PyRecursionError, PyTypeError,
    PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PyNone, PySet,
    PyString, PyTuple, PyType,
};

pyo3::create_exception!(yaml, YAMLError, PyException);
pyo3::create_exception!(yaml, MarkedYAMLError, YAMLError);
pyo3::create_exception!(yaml, ReaderError, YAMLError);
pyo3::create_exception!(yaml, ScannerError, MarkedYAMLError);
pyo3::create_exception!(yaml, ParserError, MarkedYAMLError);
pyo3::create_exception!(yaml, ComposerError, MarkedYAMLError);
pyo3::create_exception!(yaml, ConstructorError, MarkedYAMLError);
pyo3::create_exception!(yaml, RepresenterError, YAMLError);
pyo3::create_exception!(yaml, EmitterError, YAMLError);

const MAX_DEPTH: usize = 400;

fn to_py_err(e: PyYamlError) -> PyErr {
    match e.kind {
        ErrorKind::Reader => ReaderError::new_err(e.message),
        ErrorKind::Scanner => ScannerError::new_err(e.message),
        ErrorKind::Parser => ParserError::new_err(e.message),
        ErrorKind::Composer => ComposerError::new_err(e.message),
        ErrorKind::Constructor => ConstructorError::new_err(e.message),
        ErrorKind::Representer => RepresenterError::new_err(e.message),
        ErrorKind::Emitter => EmitterError::new_err(e.message),
        ErrorKind::Value => PyValueError::new_err(e.message),
        ErrorKind::Key => PyKeyError::new_err(e.message),
        ErrorKind::Index => PyIndexError::new_err(e.message),
        ErrorKind::Recursion => PyRecursionError::new_err(e.message),
        ErrorKind::Attribute => PyAttributeError::new_err(e.message),
    }
}

// ---------------------------------------------------------------------------------------------
// Node -> Python
// ---------------------------------------------------------------------------------------------

struct ToPy<'py> {
    py: Python<'py>,
    date_cls: Bound<'py, PyAny>,
    datetime_cls: Bound<'py, PyAny>,
    timezone_cls: Bound<'py, PyAny>,
    timedelta_cls: Bound<'py, PyAny>,
    memo: HashMap<*const Node, Py<PyAny>>,
}

impl<'py> ToPy<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let datetime = py.import("datetime")?;
        Ok(ToPy {
            py,
            date_cls: datetime.getattr("date")?,
            datetime_cls: datetime.getattr("datetime")?,
            timezone_cls: datetime.getattr("timezone")?,
            timedelta_cls: datetime.getattr("timedelta")?,
            memo: HashMap::new(),
        })
    }

    fn big_int(&self, i: &BigInt) -> PyResult<Bound<'py, PyAny>> {
        if let Ok(small) = i64::try_from(i) {
            return Ok(small.into_pyobject(self.py)?.into_any());
        }
        self.py.get_type::<PyInt>().call1((i.to_string(),))
    }

    fn date(&self, d: &Date) -> PyResult<Bound<'py, PyAny>> {
        self.date_cls.call1((d.year, d.month, d.day))
    }

    fn datetime(&self, dt: &DateTime) -> PyResult<Bound<'py, PyAny>> {
        let tz: Bound<'py, PyAny> = match dt.utc_offset_us {
            None => self.py.None().into_bound(self.py),
            Some(0) => {
                // PyYAML uses `timezone.utc` for `Z`.
                self.timezone_cls.getattr("utc")?
            }
            Some(us) => {
                let kwargs = PyDict::new(self.py);
                kwargs.set_item("microseconds", us)?;
                let delta = self.timedelta_cls.call((), Some(&kwargs))?;
                self.timezone_cls.call1((delta,))?
            }
        };
        let kwargs = PyDict::new(self.py);
        kwargs.set_item("tzinfo", tz)?;
        self.datetime_cls.call(
            (
                dt.date.year,
                dt.date.month,
                dt.date.day,
                dt.hour,
                dt.minute,
                dt.second,
                dt.microsecond,
            ),
            Some(&kwargs),
        )
    }

    fn convert(&mut self, node: &NodeRef) -> PyResult<Bound<'py, PyAny>> {
        let key = Rc::as_ptr(node);
        if let Some(found) = self.memo.get(&key) {
            return Ok(found.bind(self.py).clone());
        }
        let py = self.py;
        let out: Bound<'py, PyAny> = match &**node {
            Node::Null => py.None().into_bound(py),
            Node::Bool(b) => PyBool::new(py, *b).to_owned().into_any(),
            Node::Int(i) => self.big_int(i)?,
            Node::Float(f) => PyFloat::new(py, *f).into_any(),
            Node::Str(s) => PyString::new(py, s).into_any(),
            Node::Bytes(b) => PyBytes::new(py, b).into_any(),
            Node::Date(d) => self.date(d)?,
            Node::DateTime(dt) => self.datetime(dt)?,
            Node::Seq(items) => {
                let list = PyList::empty(py);
                // Register first so an alias sees the same list.
                self.memo.insert(key, list.clone().into_any().unbind());
                for item in items {
                    list.append(self.convert(item)?)?;
                }
                list.into_any()
            }
            Node::Tuple(items) => {
                let mut converted = Vec::with_capacity(items.len());
                for item in items {
                    converted.push(self.convert(item)?);
                }
                PyTuple::new(py, converted)?.into_any()
            }
            Node::Map(entries) => {
                let dict = PyDict::new(py);
                self.memo.insert(key, dict.clone().into_any().unbind());
                for (k, v) in entries {
                    let pk = self.convert(k)?;
                    let pv = self.convert(v)?;
                    dict.set_item(pk, pv)?;
                }
                dict.into_any()
            }
            Node::Set(items) => {
                let set = PySet::empty(py)?;
                self.memo.insert(key, set.clone().into_any().unbind());
                for item in items {
                    set.add(self.convert(item)?)?;
                }
                set.into_any()
            }
        };
        if matches!(
            &**node,
            Node::Seq(_)
                | Node::Tuple(_)
                | Node::Map(_)
                | Node::Set(_)
                | Node::Date(_)
                | Node::DateTime(_)
        ) {
            self.memo.insert(key, out.clone().unbind());
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------------------------
// Python -> Node
// ---------------------------------------------------------------------------------------------

struct FromPy<'py> {
    py: Python<'py>,
    date_cls: Bound<'py, PyAny>,
    datetime_cls: Bound<'py, PyAny>,
    mapping_cls: Bound<'py, PyAny>,
    memo: HashMap<usize, NodeRef>,
    in_progress: HashSet<usize>,
}

fn representer_error(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyErr {
    let repr = obj
        .repr()
        .map(|r| r.to_string())
        .unwrap_or_else(|_| "<object>".to_string());
    let _ = py;
    RepresenterError::new_err(format!("('cannot represent an object', {repr})"))
}

impl<'py> FromPy<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        let datetime = py.import("datetime")?;
        let abc = py.import("collections.abc")?;
        Ok(FromPy {
            py,
            date_cls: datetime.getattr("date")?,
            datetime_cls: datetime.getattr("datetime")?,
            mapping_cls: abc.getattr("Mapping")?,
            memo: HashMap::new(),
            in_progress: HashSet::new(),
        })
    }

    fn convert(&mut self, obj: &Bound<'py, PyAny>, depth: usize) -> PyResult<NodeRef> {
        if depth > MAX_DEPTH {
            return Err(PyRecursionError::new_err(
                "maximum recursion depth exceeded",
            ));
        }
        let py = self.py;
        if obj.is_none() || obj.is_instance_of::<PyNone>() {
            return Ok(Rc::new(Node::Null));
        }
        if obj.is_instance_of::<PyBool>() {
            return Ok(Rc::new(Node::Bool(obj.is_truthy()?)));
        }
        if obj.is_instance_of::<PyInt>() {
            return Ok(Rc::new(Node::Int(match obj.extract::<i64>() {
                Ok(i) => BigInt::from(i),
                Err(_) => obj
                    .str()?
                    .to_str()?
                    .parse::<BigInt>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
            })));
        }
        if obj.is_instance_of::<PyFloat>() {
            return Ok(Rc::new(Node::Float(obj.extract::<f64>()?)));
        }
        if let Ok(s) = obj.downcast::<PyString>() {
            return Ok(Rc::new(Node::Str(s.to_str()?.to_string())));
        }
        if let Ok(b) = obj.downcast::<PyBytes>() {
            return Ok(Rc::new(Node::Bytes(b.as_bytes().to_vec())));
        }
        if obj.is_instance_of::<PyByteArray>() || obj.is_instance_of::<PyFrozenSet>() {
            return Err(representer_error(py, obj));
        }
        let id = obj.as_ptr() as usize;
        let is_tuple = obj.is_instance_of::<PyTuple>();
        let empty_tuple = is_tuple && obj.len()? == 0;
        let trackable = !empty_tuple;
        if trackable {
            if let Some(found) = self.memo.get(&id) {
                return Ok(found.clone());
            }
            if self.in_progress.contains(&id) {
                // PARITY: PyYAML writes anchors for self-referential structures; an `Rc`
                // tree cannot hold a cycle.
                return Err(RepresenterError::new_err(
                    "recursive objects are not supported by the native YAML dumper",
                ));
            }
            self.in_progress.insert(id);
        }
        let result = self.convert_container(obj, depth);
        if trackable {
            self.in_progress.remove(&id);
        }
        let node = result?;
        if trackable {
            self.memo.insert(id, node.clone());
        }
        Ok(node)
    }

    fn convert_container(&mut self, obj: &Bound<'py, PyAny>, depth: usize) -> PyResult<NodeRef> {
        let py = self.py;
        if let Ok(list) = obj.downcast::<PyList>() {
            let mut items = Vec::with_capacity(list.len());
            for item in list.iter() {
                items.push(self.convert(&item, depth + 1)?);
            }
            return Ok(Rc::new(Node::Seq(items)));
        }
        if let Ok(t) = obj.downcast::<PyTuple>() {
            let mut items = Vec::with_capacity(t.len());
            for item in t.iter() {
                items.push(self.convert(&item, depth + 1)?);
            }
            return Ok(Rc::new(Node::Tuple(items)));
        }
        if let Ok(d) = obj.downcast::<PyDict>() {
            let mut entries = Vec::with_capacity(d.len());
            for (k, v) in d.iter() {
                let kn = self.convert(&k, depth + 1)?;
                let vn = self.convert(&v, depth + 1)?;
                entries.push((kn, vn));
            }
            return Ok(Rc::new(Node::Map(entries)));
        }
        if let Ok(s) = obj.downcast::<PySet>() {
            let mut items = Vec::with_capacity(s.len());
            for item in s.iter() {
                items.push(self.convert(&item, depth + 1)?);
            }
            return Ok(Rc::new(Node::Set(items)));
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
            return Ok(Rc::new(Node::DateTime(DateTime {
                date: Date {
                    year: obj.getattr("year")?.extract::<i32>()?,
                    month: attr("month")?,
                    day: attr("day")?,
                },
                hour: attr("hour")?,
                minute: attr("minute")?,
                second: attr("second")?,
                microsecond: attr("microsecond")?,
                utc_offset_us,
            })));
        }
        if obj.is_instance(&self.date_cls)? {
            return Ok(Rc::new(Node::Date(Date {
                year: obj.getattr("year")?.extract::<i32>()?,
                month: obj.getattr("month")?.extract::<u32>()?,
                day: obj.getattr("day")?.extract::<u32>()?,
            })));
        }
        if obj.is_instance(&self.mapping_cls)? {
            let items = obj.call_method0("items")?;
            let mut entries = Vec::new();
            for pair in items.try_iter()? {
                let pair = pair?;
                let k = pair.get_item(0)?;
                let v = pair.get_item(1)?;
                entries.push((self.convert(&k, depth + 1)?, self.convert(&v, depth + 1)?));
            }
            return Ok(Rc::new(Node::Map(entries)));
        }
        Err(representer_error(py, obj))
    }
}

// ---------------------------------------------------------------------------------------------
// Input decoding
// ---------------------------------------------------------------------------------------------

fn decode_utf16(bytes: &[u8], little: bool) -> Result<String, PyYamlError> {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| {
            if little {
                u16::from_le_bytes([c[0], c[1]])
            } else {
                u16::from_be_bytes([c[0], c[1]])
            }
        })
        .collect();
    String::from_utf16(&units).map_err(|e| PyYamlError {
        kind: ErrorKind::Reader,
        message: format!(
            "'utf-16-{}' codec can't decode: {e}",
            if little { "le" } else { "be" }
        ),
    })
}

/// `Reader.determine_encoding` + decoding of a `bytes` input.
fn decode_bytes(bytes: &[u8], name: &str) -> Result<String, PyYamlError> {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        return decode_utf16(bytes, true);
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        return decode_utf16(bytes, false);
    }
    match std::str::from_utf8(bytes) {
        Ok(s) => Ok(s.to_string()),
        Err(e) => {
            let pos = e.valid_up_to();
            let byte = bytes[pos];
            let reason = if e.error_len().is_none() {
                "unexpected end of data"
            } else if (0x80..=0xBF).contains(&byte) || byte >= 0xF5 || byte == 0xC0 || byte == 0xC1
            {
                "invalid start byte"
            } else {
                "invalid continuation byte"
            };
            Err(PyYamlError {
                kind: ErrorKind::Reader,
                message: format!(
                    "'utf-8' codec can't decode byte #x{byte:02x}: {reason}\n  in \"{name}\", position {pos}"
                ),
            })
        }
    }
}

struct Input {
    text: String,
    name: String,
    snippets: bool,
}

fn read_stream(stream: &Bound<'_, PyAny>) -> PyResult<Input> {
    if let Ok(s) = stream.downcast::<PyString>() {
        return Ok(Input {
            text: s.to_str()?.to_string(),
            name: "<unicode string>".to_string(),
            snippets: true,
        });
    }
    if let Ok(b) = stream.downcast::<PyBytes>() {
        let name = "<byte string>";
        let text = decode_bytes(b.as_bytes(), name).map_err(to_py_err)?;
        return Ok(Input {
            text,
            name: name.to_string(),
            snippets: true,
        });
    }
    let name = match stream.getattr("name") {
        Ok(n) => n.str()?.to_string(),
        Err(_) => "<file>".to_string(),
    };
    let data = stream.call_method0("read")?;
    if let Ok(s) = data.downcast::<PyString>() {
        return Ok(Input {
            text: s.to_str()?.to_string(),
            name,
            snippets: false,
        });
    }
    if let Ok(b) = data.downcast::<PyBytes>() {
        let text = decode_bytes(b.as_bytes(), &name).map_err(to_py_err)?;
        return Ok(Input {
            text,
            name,
            snippets: false,
        });
    }
    Err(PyTypeError::new_err(
        "a YAML stream must be a str, bytes or a file-like object with read()",
    ))
}

fn run_load(py: Python<'_>, stream: &Bound<'_, PyAny>, safe: bool) -> PyResult<Py<PyAny>> {
    let input = read_stream(stream)?;
    let node = load_text(
        &input.text,
        &input.name,
        input.snippets,
        LoadOptions {
            safe,
            project: false,
        },
    )
    .map_err(to_py_err)?;
    let mut conv = ToPy::new(py)?;
    Ok(conv.convert(&node)?.unbind())
}

/// `yaml.load(stream, Loader=...)`: the full loader semantics (YAML 1.1 implicit types, merge
/// keys). `Loader=yaml.SafeLoader` (by class name) selects `safe_load`.
#[pyfunction]
#[pyo3(signature = (stream, *, **kwargs))]
fn load(
    py: Python<'_>,
    stream: &Bound<'_, PyAny>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<PyAny>> {
    let mut safe = false;
    if let Some(loader) = kwargs.and_then(|k| k.get_item("Loader").ok().flatten()) {
        if let Ok(t) = loader.downcast::<PyType>() {
            safe = matches!(t.name()?.to_str()?, "SafeLoader" | "BaseLoader");
        }
    }
    run_load(py, stream, safe)
}

/// `yaml.safe_load(stream)`.
#[pyfunction]
fn safe_load(py: Python<'_>, stream: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    run_load(py, stream, true)
}

/// `yaml.dump(data, stream=None, **kwargs)`: Meerschaum's dumper (see `meerschaum.utils.yaml`).
///
/// Returns the text when `stream` is `None` (bytes when `encoding` is given), otherwise
/// writes to `stream` and returns `None`. Honored keywords: `default_style`,
/// `default_flow_style` (default `False`), `sort_keys` (default `True`), `indent`, `width`,
/// `allow_unicode`, `line_break`, `explicit_start`, `explicit_end`, `encoding`. `Dumper`
/// (any value) switches off the doubled top-level line breaks of Meerschaum's `CustomDumper`.
#[pyfunction]
#[pyo3(signature = (data, stream=None, **kwargs))]
fn dump(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    stream: Option<&Bound<'_, PyAny>>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Py<PyAny>> {
    let mut opts = DumpOptions::default();
    let mut encoding: Option<String> = None;
    if let Some(kw) = kwargs {
        for (key, value) in kw.iter() {
            let name = key.str()?.to_string();
            match name.as_str() {
                "sort_keys" => opts.sort_keys = value.is_truthy()?,
                "default_flow_style" => {
                    opts.default_flow_style = if value.is_none() {
                        None
                    } else {
                        Some(value.is_truthy()?)
                    }
                }
                "default_style" => {
                    opts.default_style = if value.is_none() || !value.is_truthy()? {
                        None
                    } else {
                        value.extract::<String>()?.chars().next()
                    }
                }
                "indent" => opts.indent = value.extract::<Option<usize>>()?,
                "width" => opts.width = value.extract::<Option<usize>>()?,
                "allow_unicode" => opts.allow_unicode = value.is_truthy()?,
                "line_break" => opts.line_break = value.extract::<Option<String>>()?,
                "explicit_start" => opts.explicit_start = value.is_truthy()?,
                "explicit_end" => opts.explicit_end = value.is_truthy()?,
                "encoding" => {
                    if !value.is_none() {
                        encoding = Some(value.extract::<String>()?);
                    }
                }
                "Dumper" => opts.double_top_level_breaks = false,
                "canonical" | "version" | "tags" if value.is_truthy()? => {
                    return Err(pyo3::exceptions::PyNotImplementedError::new_err(format!(
                        "the native YAML dumper does not support '{name}'"
                    )));
                }
                _ => {}
            }
        }
    }
    let mut conv = FromPy::new(py)?;
    let node = conv.convert(data, 0)?;
    let text = dump_node(&node, &opts).map_err(to_py_err)?;
    let payload: Bound<'_, PyAny> = match &encoding {
        Some(enc) => PyString::new(py, &text).call_method1("encode", (enc.as_str(),))?,
        None => PyString::new(py, &text).into_any(),
    };
    match stream {
        Some(s) if !s.is_none() => {
            s.call_method1("write", (payload,))?;
            Ok(py.None())
        }
        _ => Ok(payload.unbind()),
    }
}

/// Register `meerschaum._core.yaml` (`load`, `safe_load`, `dump` and the exception classes).
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let yaml = PyModule::new(py, "yaml")?;
    yaml.add_function(wrap_pyfunction!(load, &yaml)?)?;
    yaml.add_function(wrap_pyfunction!(safe_load, &yaml)?)?;
    yaml.add_function(wrap_pyfunction!(dump, &yaml)?)?;
    yaml.add("YAMLError", py.get_type::<YAMLError>())?;
    yaml.add("MarkedYAMLError", py.get_type::<MarkedYAMLError>())?;
    yaml.add("ReaderError", py.get_type::<ReaderError>())?;
    yaml.add("ScannerError", py.get_type::<ScannerError>())?;
    yaml.add("ParserError", py.get_type::<ParserError>())?;
    yaml.add("ComposerError", py.get_type::<ComposerError>())?;
    yaml.add("ConstructorError", py.get_type::<ConstructorError>())?;
    yaml.add("RepresenterError", py.get_type::<RepresenterError>())?;
    yaml.add("EmitterError", py.get_type::<EmitterError>())?;
    yaml.add("__version__", "native-pyyaml-6.0.3")?;
    m.add_submodule(&yaml)?;
    Ok(())
}
