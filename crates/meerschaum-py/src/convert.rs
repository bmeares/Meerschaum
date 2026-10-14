//! Python object ↔ Rust value conversion at the boundary.
//!
//! Two value types cross the boundary:
//!
//! - [`meerschaum_core::Value`] (parameters, config, `params` and dtype maps): JSON-like
//!   (`None`, `bool`, `int`, `float`, `str`, `list`, `dict`). Python objects without a JSON form
//!   (`Decimal`, `UUID`, `datetime`, ...) are converted with `str()`, as `json.dumps(default=str)`
//!   would.
//! - [`meerschaum_kernel::Value`] (DataFrame cells of `object` columns): every Python object the
//!   v4 dataframe code inspects, with `None`, `pd.NA` and `pd.NaT` kept distinct.
//!
//! The ABI3 build has no access to the CPython `datetime` C API, so datetimes, dates, decimals,
//! UUIDs and geometries are recognized by their (cached) classes and read through attributes.

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use meerschaum_core::Value as CoreValue;
use meerschaum_kernel::Value as Cell;
use meerschaum_kernel::decimal::Decimal;
use meerschaum_kernel::geometry::Geometry;
use meerschaum_kernel::value::{DateTimeValue, TzInfo};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PySet, PyString,
    PyTuple, PyType,
};

/// Import `name`, falling back to `meerschaum.utils.packages.attempt_import(name, lazy=False)`
/// for packages that live in a Meerschaum virtual environment (`shapely`, `geopandas`, ...).
pub fn import_optional<'py>(py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
    match py.import(name) {
        Ok(m) => Ok(m.into_any()),
        Err(first) => {
            let packages = py.import("meerschaum.utils.packages")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("lazy", false)?;
            kwargs.set_item("warn", false)?;
            let module = packages
                .getattr("attempt_import")?
                .call((name,), Some(&kwargs))?;
            if module.is_none() {
                return Err(first);
            }
            Ok(module)
        }
    }
}

/// Classes and singletons resolved once per boundary call.
pub struct PyCtx<'py> {
    pub py: Python<'py>,
    pub decimal_cls: Bound<'py, PyAny>,
    pub uuid_cls: Bound<'py, PyAny>,
    pub datetime_cls: Bound<'py, PyAny>,
    pub date_cls: Bound<'py, PyAny>,
    pub timezone_cls: Bound<'py, PyAny>,
    pub timedelta_cls: Bound<'py, PyAny>,
    pub utc: Bound<'py, PyAny>,
    pub zoneinfo_cls: Bound<'py, PyAny>,
    /// `pandas` (only when importable; frames need it, scalar helpers do not).
    pub pandas: Option<Bound<'py, PyAny>>,
    pub na: Option<Bound<'py, PyAny>>,
    pub nat: Option<Bound<'py, PyAny>>,
    pub timestamp_cls: Option<Bound<'py, PyAny>>,
    shapely: std::cell::OnceCell<Option<Bound<'py, PyAny>>>,
}

impl<'py> PyCtx<'py> {
    /// Resolve the standard-library classes, and pandas when it is importable.
    pub fn new(py: Python<'py>) -> PyResult<Self> {
        Self::new_with(py, true)
    }

    fn new_with(py: Python<'py>, with_pandas: bool) -> PyResult<Self> {
        let datetime = py.import("datetime")?;
        let pandas = if with_pandas {
            import_optional(py, "pandas").ok()
        } else {
            None
        };
        let (na, nat, timestamp_cls) = match &pandas {
            Some(pd) => (
                Some(pd.getattr("NA")?),
                Some(pd.getattr("NaT")?),
                Some(pd.getattr("Timestamp")?),
            ),
            None => (None, None, None),
        };
        let timezone_cls = datetime.getattr("timezone")?;
        Ok(Self {
            py,
            decimal_cls: py.import("decimal")?.getattr("Decimal")?,
            uuid_cls: py.import("uuid")?.getattr("UUID")?,
            datetime_cls: datetime.getattr("datetime")?,
            date_cls: datetime.getattr("date")?,
            utc: timezone_cls.getattr("utc")?,
            timezone_cls,
            timedelta_cls: datetime.getattr("timedelta")?,
            zoneinfo_cls: py.import("zoneinfo")?.getattr("ZoneInfo")?,
            pandas,
            na,
            nat,
            timestamp_cls,
            shapely: std::cell::OnceCell::new(),
        })
    }

    /// [`PyCtx::new`] for calls that convert scalars only (bounds in, a bound or a count out):
    /// pandas is taken from `sys.modules` and never imported. When pandas is not imported, no
    /// argument can be a pandas object, so the conversions are the same; this keeps the first
    /// `get_rowcount()` / `get_sync_time()` of a process from paying for `import pandas`, which
    /// v4's SQLAlchemy path never imports either.
    pub fn scalars(py: Python<'py>) -> PyResult<Self> {
        let mut ctx = Self::new_with(py, false)?;
        if let Ok(Some(pd)) = py
            .import("sys")
            .and_then(|sys| sys.getattr("modules"))
            .and_then(|m| m.call_method1("get", ("pandas",)))
            .map(|pd| (!pd.is_none()).then_some(pd))
        {
            if let (Ok(na), Ok(nat), Ok(ts)) =
                (pd.getattr("NA"), pd.getattr("NaT"), pd.getattr("Timestamp"))
            {
                ctx.na = Some(na);
                ctx.nat = Some(nat);
                ctx.timestamp_cls = Some(ts);
                ctx.pandas = Some(pd);
            }
        }
        Ok(ctx)
    }

    /// `pandas`, or an `ImportError`.
    pub fn pandas(&self) -> PyResult<&Bound<'py, PyAny>> {
        self.pandas.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyImportError::new_err("pandas is required for DataFrames")
        })
    }

    /// `shapely`, imported on first use.
    pub fn shapely(&self) -> PyResult<&Bound<'py, PyAny>> {
        self.shapely
            .get_or_init(|| import_optional(self.py, "shapely").ok())
            .as_ref()
            .ok_or_else(|| {
                pyo3::exceptions::PyImportError::new_err("shapely is required for geometries")
            })
    }

    fn is(&self, obj: &Bound<'_, PyAny>, other: &Option<Bound<'py, PyAny>>) -> bool {
        other.as_ref().is_some_and(|o| obj.as_ptr() == o.as_ptr())
    }
}

// ---------------------------------------------------------------------------------------------
// Core values (JSON-like)

/// Convert a Python object into a [`CoreValue`]. Mappings become maps (keys through `str()`),
/// lists, tuples and sets become lists, and other objects become `str(obj)`.
pub fn py_to_core(obj: &Bound<'_, PyAny>) -> PyResult<CoreValue> {
    if obj.is_none() {
        return Ok(CoreValue::Null);
    }
    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(CoreValue::Bool(b.is_true()));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(match obj.extract::<i64>() {
            Ok(i) => CoreValue::Int(i),
            Err(_) => CoreValue::Float(obj.extract::<f64>()?),
        });
    }
    if let Ok(f) = obj.downcast::<PyFloat>() {
        return Ok(CoreValue::Float(f.value()));
    }
    if let Ok(s) = obj.downcast::<PyString>() {
        return Ok(CoreValue::Str(s.to_str()?.to_string()));
    }
    if let Ok(d) = obj.downcast::<PyDict>() {
        let mut m = meerschaum_core::Map::new();
        for (k, v) in d.iter() {
            let key = match k.downcast::<PyString>() {
                Ok(s) => s.to_str()?.to_string(),
                Err(_) => k.str()?.to_str()?.to_string(),
            };
            m.insert(key, py_to_core(&v)?);
        }
        return Ok(CoreValue::Map(m));
    }
    if obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>()
        || obj.is_instance_of::<PySet>()
        || obj.is_instance_of::<PyFrozenSet>()
    {
        let mut items = Vec::new();
        for item in obj.try_iter()? {
            items.push(py_to_core(&item?)?);
        }
        return Ok(CoreValue::List(items));
    }
    // numpy scalars and other numbers.
    if let Some(v) = numpy_scalar(obj)? {
        return Ok(match v {
            Cell::Bool(b) => CoreValue::Bool(b),
            Cell::Int(i) => CoreValue::Int(i),
            Cell::Float(f) => CoreValue::Float(f),
            other => CoreValue::Str(other.py_str()),
        });
    }
    if obj.hasattr(intern!(obj.py(), "keys"))? && obj.hasattr(intern!(obj.py(), "items"))? {
        let dict = PyDict::new(obj.py());
        for item in obj.call_method0("items")?.try_iter()? {
            let (k, v): (Bound<'_, PyAny>, Bound<'_, PyAny>) = item?.extract()?;
            dict.set_item(k, v)?;
        }
        return py_to_core(dict.as_any());
    }
    Ok(CoreValue::Str(obj.str()?.to_str()?.to_string()))
}

/// Convert a [`CoreValue`] into a Python object.
pub fn core_to_py<'py>(py: Python<'py>, value: &CoreValue) -> PyResult<Bound<'py, PyAny>> {
    crate::static_config::value_to_py(py, value)
}

/// `numpy` scalars (`np.int64`, `np.float64`, `np.bool_`, ...) as cells, or `None`.
fn numpy_scalar(obj: &Bound<'_, PyAny>) -> PyResult<Option<Cell>> {
    let py = obj.py();
    let ty = obj.get_type();
    let module: String = ty.getattr(intern!(py, "__module__"))?.extract()?;
    if module != "numpy" {
        return Ok(None);
    }
    let Ok(dtype) = obj.getattr(intern!(py, "dtype")) else {
        return Ok(None);
    };
    let kind: String = dtype.getattr(intern!(py, "kind"))?.extract()?;
    Ok(match kind.as_str() {
        "b" => Some(Cell::Bool(obj.is_truthy()?)),
        "i" | "u" => Some(match obj.call_method0("__index__")?.extract::<i64>() {
            Ok(i) => Cell::Int(i),
            Err(_) => Cell::Float(obj.extract::<f64>()?),
        }),
        "f" => Some(Cell::Float(obj.extract::<f64>()?)),
        _ => None,
    })
}

// ---------------------------------------------------------------------------------------------
// JSON containers

/// Convert a Python `dict` / `list` (recursively) into JSON. Non-JSON leaves become `str()`
/// (`json.dumps(default=str)`); `NaN` floats become `null`.
pub fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    use serde_json::Value as J;
    if obj.is_none() {
        return Ok(J::Null);
    }
    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(J::Bool(b.is_true()));
    }
    if obj.is_instance_of::<PyInt>() {
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(J::from(i));
        }
        if let Ok(u) = obj.extract::<u64>() {
            return Ok(J::from(u));
        }
        return Ok(J::String(obj.str()?.to_str()?.to_string()));
    }
    if let Ok(f) = obj.downcast::<PyFloat>() {
        return Ok(serde_json::Number::from_f64(f.value()).map_or(J::Null, J::Number));
    }
    if let Ok(s) = obj.downcast::<PyString>() {
        return Ok(J::String(s.to_str()?.to_string()));
    }
    if let Ok(d) = obj.downcast::<PyDict>() {
        let mut m = serde_json::Map::new();
        for (k, v) in d.iter() {
            let key = match k.downcast::<PyString>() {
                Ok(s) => s.to_str()?.to_string(),
                Err(_) => k.str()?.to_str()?.to_string(),
            };
            m.insert(key, py_to_json(&v)?);
        }
        return Ok(J::Object(m));
    }
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        let mut items = Vec::new();
        for item in obj.try_iter()? {
            items.push(py_to_json(&item?)?);
        }
        return Ok(J::Array(items));
    }
    if let Some(v) = numpy_scalar(obj)? {
        return Ok(match v {
            Cell::Bool(b) => J::Bool(b),
            Cell::Int(i) => J::from(i),
            Cell::Float(f) => serde_json::Number::from_f64(f).map_or(J::Null, J::Number),
            other => J::String(other.py_str()),
        });
    }
    Ok(J::String(obj.str()?.to_str()?.to_string()))
}

/// Convert JSON into Python `dict` / `list` / scalars (`json.loads`).
pub fn json_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    use serde_json::Value as J;
    Ok(match value {
        J::Null => py.None().into_bound(py),
        J::Bool(b) => b.into_pyobject(py)?.to_owned().into_any(),
        J::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => i.into_pyobject(py)?.into_any(),
            (None, Some(u)) => u.into_pyobject(py)?.into_any(),
            _ => n.as_f64().unwrap_or(f64::NAN).into_pyobject(py)?.into_any(),
        },
        J::String(s) => PyString::new(py, s).into_any(),
        J::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            list.into_any()
        }
        J::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            dict.into_any()
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Cells (kernel values)

/// Read a `tzinfo` into a [`TzInfo`] (zoneinfo keys, pytz zones, `datetime.timezone`, dateutil),
/// falling back to the fixed offset at `dt`.
fn tzinfo_from_py(
    tz: &Bound<'_, PyAny>,
    dt: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<TzInfo>> {
    let py = tz.py();
    if tz.is_none() {
        return Ok(None);
    }
    for attr in ["key", "zone"] {
        if let Ok(name) = tz.getattr(attr) {
            if let Ok(name) = name.extract::<String>() {
                if let Some(parsed) = TzInfo::parse(&name) {
                    return Ok(Some(parsed));
                }
            }
        }
    }
    if let Ok(name) = tz.str().and_then(|s| s.extract::<String>()) {
        if let Some(parsed) = TzInfo::parse(&name) {
            return Ok(Some(parsed));
        }
    }
    let offset = tz.call_method1(intern!(py, "utcoffset"), (dt.cloned(),))?;
    if offset.is_none() {
        return Ok(None);
    }
    let seconds: f64 = offset.call_method0("total_seconds")?.extract()?;
    let fixed = chrono::FixedOffset::east_opt(seconds as i32)
        .ok_or_else(|| PyValueError::new_err(format!("Invalid UTC offset: {seconds}")))?;
    Ok(Some(if seconds == 0.0 {
        TzInfo::Utc
    } else {
        TzInfo::Fixed(fixed)
    }))
}

fn datetime_from_py(ctx: &PyCtx<'_>, obj: &Bound<'_, PyAny>) -> PyResult<DateTimeValue> {
    let py = ctx.py;
    let tzinfo = obj.getattr(intern!(py, "tzinfo"))?;
    let tz = tzinfo_from_py(&tzinfo, Some(obj))?;
    // pandas Timestamps carry nanoseconds: `value` is the UTC (or naive wall) epoch in ns.
    if ctx
        .timestamp_cls
        .as_ref()
        .is_some_and(|cls| obj.is_instance(cls).unwrap_or(false))
    {
        if let Ok(ns) = obj
            .getattr(intern!(py, "value"))
            .and_then(|v| v.extract::<i64>())
        {
            // `value` is in the timestamp's unit-independent nanoseconds.
            let utc = chrono::DateTime::from_timestamp_nanos(ns).naive_utc();
            return Ok(DateTimeValue { utc, tz });
        }
    }
    let get = |name: &str| -> PyResult<u32> { obj.getattr(name)?.extract::<u32>() };
    let year: i32 = obj.getattr(intern!(py, "year"))?.extract()?;
    let date = NaiveDate::from_ymd_opt(year, get("month")?, get("day")?)
        .ok_or_else(|| PyValueError::new_err("invalid date"))?;
    let local = date
        .and_hms_micro_opt(
            get("hour")?,
            get("minute")?,
            get("second")?,
            get("microsecond")?,
        )
        .ok_or_else(|| PyValueError::new_err("invalid time"))?;
    let utc = match &tz {
        None => local,
        Some(_) => {
            let offset = obj.call_method0(intern!(py, "utcoffset"))?;
            let seconds: f64 = if offset.is_none() {
                0.0
            } else {
                offset.call_method0("total_seconds")?.extract()?
            };
            local - chrono::Duration::microseconds((seconds * 1_000_000.0).round() as i64)
        }
    };
    Ok(DateTimeValue { utc, tz })
}

/// Convert a Python object (a DataFrame cell) into a kernel [`Cell`].
pub fn py_to_cell(ctx: &PyCtx<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Cell> {
    let py = ctx.py;
    if obj.is_none() {
        return Ok(Cell::Null);
    }
    // Exact builtins first (the common case).
    if let Ok(s) = obj.downcast_exact::<PyString>() {
        return Ok(Cell::Str(s.to_str()?.to_string()));
    }
    if let Ok(f) = obj.downcast_exact::<PyFloat>() {
        return Ok(Cell::Float(f.value()));
    }
    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(Cell::Bool(b.is_true()));
    }
    if obj.is_exact_instance_of::<PyInt>() {
        return Ok(match obj.extract::<i64>() {
            Ok(i) => Cell::Int(i),
            Err(_) => Cell::Decimal(
                Decimal::parse(obj.str()?.to_str()?)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
            ),
        });
    }
    if ctx.is(obj, &ctx.na) {
        return Ok(Cell::NA);
    }
    if ctx.is(obj, &ctx.nat) {
        return Ok(Cell::NaT);
    }
    if obj.is_instance_of::<PyDict>() || obj.is_instance_of::<PyList>() {
        return Ok(Cell::Json(py_to_json(obj)?));
    }
    if let Ok(b) = obj.downcast::<PyBytes>() {
        return Ok(Cell::Bytes(b.as_bytes().to_vec()));
    }
    if let Ok(b) = obj.downcast::<PyByteArray>() {
        return Ok(Cell::Bytes(b.to_vec()));
    }
    if obj.is_instance(&ctx.decimal_cls)? {
        let text = obj.str()?;
        return Decimal::parse(text.to_str()?)
            .map(Cell::Decimal)
            .map_err(|e| PyValueError::new_err(e.to_string()));
    }
    if obj.is_instance(&ctx.uuid_cls)? {
        let bytes: Vec<u8> = obj.getattr(intern!(py, "bytes"))?.extract()?;
        return uuid::Uuid::from_slice(&bytes)
            .map(Cell::Uuid)
            .map_err(|e| PyValueError::new_err(e.to_string()));
    }
    if obj.is_instance(&ctx.datetime_cls)? {
        return Ok(Cell::Datetime(datetime_from_py(ctx, obj)?));
    }
    if obj.is_instance(&ctx.date_cls)? {
        let year: i32 = obj.getattr(intern!(py, "year"))?.extract()?;
        let month: u32 = obj.getattr(intern!(py, "month"))?.extract()?;
        let day: u32 = obj.getattr(intern!(py, "day"))?.extract()?;
        return NaiveDate::from_ymd_opt(year, month, day)
            .map(Cell::Date)
            .ok_or_else(|| PyValueError::new_err("invalid date"));
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(Cell::Str(obj.str()?.to_str()?.to_string()));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Cell::Float(obj.extract::<f64>()?));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(Cell::Int(obj.extract::<i64>()?));
    }
    if obj.is_instance_of::<PyTuple>() {
        return Ok(Cell::Json(py_to_json(obj)?));
    }
    if let Some(v) = numpy_scalar(obj)? {
        return Ok(v);
    }
    let ty = obj.get_type();
    let module: String = ty.getattr(intern!(py, "__module__"))?.extract()?;
    if module.starts_with("shapely") {
        let shapely = ctx.shapely()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("include_srid", true)?;
        let wkb = shapely.getattr("to_wkb")?.call((obj,), Some(&kwargs))?;
        let bytes: Vec<u8> = wkb.extract()?;
        return Geometry::from_wkb(&bytes)
            .map(Cell::Geometry)
            .map_err(|e| PyValueError::new_err(e.to_string()));
    }
    if module == "numpy" {
        // `np.datetime64` and friends: go through pandas.
        if let Some(cls) = &ctx.timestamp_cls {
            if let Ok(ts) = cls.call1((obj,)) {
                if ctx.is(&ts, &ctx.nat) {
                    return Ok(Cell::NaT);
                }
                return Ok(Cell::Datetime(datetime_from_py(ctx, &ts)?));
            }
        }
    }
    Ok(Cell::Str(obj.str()?.to_str()?.to_string()))
}

fn tzinfo_to_py<'py>(ctx: &PyCtx<'py>, tz: &TzInfo) -> PyResult<Bound<'py, PyAny>> {
    Ok(match tz {
        TzInfo::Utc => ctx.utc.clone(),
        TzInfo::Fixed(off) => {
            let kwargs = PyDict::new(ctx.py);
            kwargs.set_item("seconds", off.local_minus_utc())?;
            let delta = ctx.timedelta_cls.call((), Some(&kwargs))?;
            ctx.timezone_cls.call1((delta,))?
        }
        TzInfo::Named(named) => ctx.zoneinfo_cls.call1((named.name(),))?,
    })
}

/// A kernel datetime as a Python `datetime` (or a pandas `Timestamp` when it has nanoseconds).
pub fn datetime_to_py<'py>(ctx: &PyCtx<'py>, dt: &DateTimeValue) -> PyResult<Bound<'py, PyAny>> {
    let py = ctx.py;
    let nanos = dt.utc.nanosecond() % 1_000_000_000;
    if nanos % 1000 != 0 {
        if let (Some(cls), Some(ns)) = (&ctx.timestamp_cls, dt.timestamp_nanos()) {
            let ts = cls.call1((ns,))?;
            return match &dt.tz {
                None => Ok(ts),
                Some(tz) => ts
                    .call_method1("tz_localize", ("UTC",))?
                    .call_method1("tz_convert", (tzinfo_to_py(ctx, tz)?,)),
            };
        }
    }
    let build = |n: &NaiveDateTime, tz: Option<Bound<'py, PyAny>>| -> PyResult<Bound<'py, PyAny>> {
        let args = (
            n.year(),
            n.month(),
            n.day(),
            n.hour(),
            n.minute(),
            n.second(),
            n.nanosecond() / 1000 % 1_000_000,
        );
        let kwargs = PyDict::new(py);
        if let Some(tz) = tz {
            kwargs.set_item("tzinfo", tz)?;
        }
        ctx.datetime_cls.call(args, Some(&kwargs))
    };
    match &dt.tz {
        None => build(&dt.utc, None),
        Some(TzInfo::Utc) => build(&dt.utc, Some(ctx.utc.clone())),
        Some(tz) => build(&dt.utc, Some(ctx.utc.clone()))?
            .call_method1(intern!(py, "astimezone"), (tzinfo_to_py(ctx, tz)?,)),
    }
}

/// Convert a kernel [`Cell`] into the Python object v4 would hold in an `object` column.
pub fn cell_to_py<'py>(ctx: &PyCtx<'py>, value: &Cell) -> PyResult<Bound<'py, PyAny>> {
    let py = ctx.py;
    Ok(match value {
        Cell::Null => py.None().into_bound(py),
        Cell::NA => match &ctx.na {
            Some(na) => na.clone(),
            None => py.None().into_bound(py),
        },
        Cell::NaT => match &ctx.nat {
            Some(nat) => nat.clone(),
            None => py.None().into_bound(py),
        },
        Cell::Bool(b) => b.into_pyobject(py)?.to_owned().into_any(),
        Cell::Int(i) => i.into_pyobject(py)?.into_any(),
        Cell::Float(f) => f.into_pyobject(py)?.into_any(),
        Cell::Str(s) => PyString::new(py, s).into_any(),
        Cell::Bytes(b) => PyBytes::new(py, b).into_any(),
        Cell::Decimal(d) => ctx.decimal_cls.call1((d.to_sci_string(),))?,
        // `UUID(int=...)` (positionally) skips parsing the hyphenated text.
        Cell::Uuid(u) => {
            ctx.uuid_cls
                .call1((py.None(), py.None(), py.None(), py.None(), u.as_u128()))?
        }
        Cell::Datetime(dt) => datetime_to_py(ctx, dt)?,
        Cell::Date(d) => ctx.date_cls.call1((d.year(), d.month(), d.day()))?,
        Cell::Json(j) => json_to_py(py, j)?,
        Cell::Geometry(g) => {
            let shapely = ctx.shapely()?;
            shapely
                .getattr("from_wkb")?
                .call1((PyBytes::new(py, &g.to_wkb(true)),))?
        }
    })
}

/// Convert a column of cells into a Python list, as [`cell_to_py`] would value by value, with the
/// geometries decoded by a single vectorized `shapely.from_wkb` call.
pub fn cells_to_pylist<'py>(ctx: &PyCtx<'py>, values: &[Cell]) -> PyResult<Bound<'py, PyList>> {
    let py = ctx.py;
    let mut items: Vec<Bound<'py, PyAny>> = Vec::with_capacity(values.len());
    let mut geometry_ixs: Vec<usize> = Vec::new();
    let mut wkbs: Vec<Bound<'py, PyBytes>> = Vec::new();
    for (i, v) in values.iter().enumerate() {
        match v {
            Cell::Geometry(g) => {
                geometry_ixs.push(i);
                wkbs.push(PyBytes::new(py, &g.to_wkb(true)));
                items.push(py.None().into_bound(py));
            }
            other => items.push(cell_to_py(ctx, other)?),
        }
    }
    if !geometry_ixs.is_empty() {
        let geometries = ctx
            .shapely()?
            .getattr("from_wkb")?
            .call1((PyList::new(py, &wkbs)?,))?
            .call_method0(intern!(py, "tolist"))?;
        for (ix, geometry) in geometry_ixs.into_iter().zip(geometries.try_iter()?) {
            items[ix] = geometry?;
        }
    }
    PyList::new(py, items)
}

/// Convert a list of Python objects (an `object` column) into cells, as [`py_to_cell`] would
/// value by value, with the shapely geometries encoded by a single vectorized `shapely.to_wkb`
/// call.
pub fn pylist_to_cells(ctx: &PyCtx<'_>, list: &Bound<'_, PyList>) -> PyResult<Vec<Cell>> {
    let py = ctx.py;
    let mut cells = Vec::with_capacity(list.len());
    let mut geometry_ixs: Vec<usize> = Vec::new();
    let mut geometries: Vec<Bound<'_, PyAny>> = Vec::new();
    // Whether each class seen is a shapely geometry class (by type pointer).
    let mut shapely_types: Vec<(usize, bool)> = Vec::new();
    for (i, item) in list.iter().enumerate() {
        let ty = item.get_type();
        let ptr = ty.as_ptr() as usize;
        let is_shapely = match shapely_types.iter().find(|(p, _)| *p == ptr) {
            Some((_, known)) => *known,
            None => {
                // Only classes `py_to_cell` would reach its shapely branch for: none of the
                // builtin, pandas or standard-library classes it checks first is from shapely.
                let module: String = ty.getattr(intern!(py, "__module__"))?.extract()?;
                let known = module.starts_with("shapely");
                shapely_types.push((ptr, known));
                known
            }
        };
        if is_shapely {
            geometry_ixs.push(i);
            geometries.push(item);
            cells.push(Cell::Null);
        } else {
            cells.push(py_to_cell(ctx, &item)?);
        }
    }
    if !geometry_ixs.is_empty() {
        let kwargs = PyDict::new(py);
        kwargs.set_item("include_srid", true)?;
        let wkbs = ctx
            .shapely()?
            .getattr("to_wkb")?
            .call((PyList::new(py, &geometries)?,), Some(&kwargs))?
            .call_method0(intern!(py, "tolist"))?;
        for (ix, wkb) in geometry_ixs.into_iter().zip(wkbs.try_iter()?) {
            let wkb = wkb?;
            let bytes = wkb.downcast::<PyBytes>()?.as_bytes();
            cells[ix] = Geometry::from_wkb(bytes)
                .map(Cell::Geometry)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
        }
    }
    Ok(cells)
}

/// The fully-qualified class name of an object (`pandas.core.frame.DataFrame`).
pub fn qualname(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    let ty: Bound<'_, PyType> = obj.get_type();
    let module: String = ty.getattr("__module__")?.extract()?;
    let name: String = ty.getattr("__name__")?.extract()?;
    Ok(format!("{module}.{name}"))
}
