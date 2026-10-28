//! DataFrames at the boundary: pandas / GeoPandas / Polars / Dask frames ↔ the kernel [`Frame`].
//!
//! **Into the kernel.** Typed pandas columns (NumPy, nullable, Arrow-backed, tz-aware datetimes,
//! categoricals) cross as Arrow through `__arrow_c_stream__` (one `pyarrow` table built with
//! `pa.array(series, from_pandas=True)`, so `NaN` in float columns is null, as the kernel's
//! fixtures assume). `object` and `geometry` columns cross value by value
//! ([`crate::convert::py_to_cell`]), because the kernel keeps them as object columns of Python-like
//! values. Polars frames cross as Arrow directly. Dask frames are computed first.
//!
//! **Out of the kernel.** The kernel's [`Frame`] erases pandas backends (`int64`, `Int64` and
//! `int64[pyarrow]` are one Polars `Int64`), so the boundary restores the exact pandas dtype v4
//! would return from *hints*: per-column pandas dtype strings computed by the caller from what v4
//! does (the input frame's dtypes, `to_pandas_dtype()` of the requested dtypes). A hint is used when
//! its logical class (int, float, bool, string, datetime+tz, date, bytes, numeric, ...) matches
//! the kernel column's; otherwise a default mapping is used (see [`default_target`]). When v4
//! rebuilds the frame through Polars (`to_pandas(use_pyarrow_extension_array=True)`), every native
//! column comes back Arrow-backed ([`OutSpec::arrow_native`]).

use indexmap::IndexMap;
use meerschaum_kernel::dtypes::{Srid, pandas_dtype_of};
use meerschaum_kernel::frame::{Column, ColumnData, Frame};
use meerschaum_kernel::value::Value as Cell;
use polars::prelude::DataType;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};

use crate::arrow_bridge::{ArrowStream, import_from_python};
use crate::convert::{PyCtx, cells_to_pylist, import_optional, pylist_to_cells, qualname};

/// What kind of frame came in (and should go back out).
#[derive(Clone, Debug, PartialEq)]
pub enum FrameKind {
    Pandas,
    /// A GeoDataFrame: its active geometry column name.
    Geo {
        geometry: Option<String>,
    },
    Polars,
    /// A Dask DataFrame (computed at the boundary) with its partition count.
    Dask {
        npartitions: usize,
    },
}

/// A Python frame converted for the kernel.
pub struct PyFrame<'py> {
    pub frame: Frame,
    pub kind: FrameKind,
    /// The pandas index (`None` for Polars).
    pub index: Option<Bound<'py, PyAny>>,
    /// `str(df[col].dtype)` for each column, in order.
    pub dtypes: IndexMap<String, String>,
    /// The original column label objects (pandas allows non-string labels).
    pub labels: IndexMap<String, Bound<'py, PyAny>>,
    /// The input pandas Series of each column ([`ColumnMode::Full`] only), which the output
    /// reuses for columns the kernel left unchanged (see [`OutSpec::reuse`]).
    pub series: IndexMap<String, Bound<'py, PyAny>>,
    /// Columns that crossed as all-null placeholders ([`frame_from_py_with`]).
    pub placeholders: Vec<String>,
    /// Original Polars Object columns; Arrow-native Null columns also have an object hint.
    pub polars_objects: Vec<String>,
}

/// Restore the pandas merge dtype when a nullable-string cast changes the old dtype's
/// storage or null sentinel. The kernel represents all these pandas backends as String.
pub fn string_merge_hint<'py>(
    ctx: &PyCtx<'py>,
    old_series: Option<&Bound<'py, PyAny>>,
    target: &str,
) -> PyResult<String> {
    let Some(old_series) = old_series else {
        return Ok(target.to_string());
    };
    if target != "string" && !target.starts_with("string[") {
        return Ok(target.to_string());
    }
    let pd = ctx.pandas()?;
    let old_dtype = old_series.getattr("dtype")?;
    if old_dtype.is_instance(&pd.getattr("StringDtype")?)? {
        let target_dtype = pd
            .getattr("api")?
            .getattr("types")?
            .getattr("pandas_dtype")?
            .call1((target,));
        if let Ok(target_dtype) = target_dtype {
            // PARITY: str (NaN) against string (pd.NA), or different string storage
            // backends, merges to object. This also holds for empty and all-null deltas.
            if !old_dtype.eq(target_dtype)? {
                return Ok("object".to_string());
            }
        }
    }
    Ok(target.to_string())
}

/// Restore `normalize_nulls`' pd.NA in ordinary object columns containing strings.
/// Special object values undergo separate serialization/deserialization and keep their
/// sentinels. Native string outputs under pandas' string inference are left to dtype restoration.
pub fn object_string_nulls(
    old: &ColumnData,
    new: &ColumnData,
    delta: &ColumnData,
    infer_string: bool,
) -> Option<ColumnData> {
    if !matches!(old, ColumnData::Object(_)) {
        return None;
    }
    match delta {
        ColumnData::Object(values) if values.iter().any(Cell::is_na) => {}
        ColumnData::Native(s)
            if s.dtype() == &DataType::String && !infer_string && s.null_count() > 0 => {}
        _ => return None,
    }
    let mut has_string = false;
    for data in [old, new, delta] {
        match data {
            ColumnData::Object(values) => {
                for value in values {
                    match value {
                        Cell::Str(_) => has_string = true,
                        Cell::Null | Cell::NA | Cell::Bool(_) | Cell::Int(_) | Cell::Float(_) => {}
                        _ => return None,
                    }
                }
            }
            ColumnData::Native(s) if s.dtype() == &DataType::String => {
                has_string |= s.null_count() < s.len();
            }
            ColumnData::Native(s)
                if s.dtype().is_integer()
                    || s.dtype().is_float()
                    || matches!(s.dtype(), DataType::Boolean | DataType::Null) => {}
            _ => return None,
        }
    }
    has_string.then(|| {
        ColumnData::Object(
            delta
                .values()
                .into_iter()
                .map(|v| if v.is_na() { Cell::NA } else { v })
                .collect(),
        )
    })
}

/// Whether pandas' `infer_objects()` makes both normalized merge inputs strings.
/// An all-null object input stays object, including when the resulting delta is empty.
pub fn object_string_inference(old: &ColumnData, new: &ColumnData) -> bool {
    let strings = |data: &ColumnData| match data {
        ColumnData::Object(values) => object_values_class(values).as_deref() == Some("string"),
        ColumnData::Native(s) => s.dtype() == &DataType::String && s.null_count() < s.len(),
    };
    strings(old) && strings(new)
}

/// UUID and bytes are not serialized before pandas' `normalize_nulls().fillna(pd.NA)`.
/// Their delta retains pd.NA while it still contains a non-null special value; otherwise
/// v4's final value parser converts missing entries back to None.
pub fn object_special_nulls(
    old: &ColumnData,
    new: &ColumnData,
    delta: &ColumnData,
) -> Option<ColumnData> {
    if !matches!(old, ColumnData::Object(_)) {
        return None;
    }
    let special = [old, new].iter().any(|data| match data {
        ColumnData::Object(values) => matches!(
            values.iter().find(|value| !value.is_na()),
            Some(Cell::Uuid(_) | Cell::Bytes(_))
        ),
        ColumnData::Native(series) => {
            series.dtype() == &DataType::Binary && series.null_count() < series.len()
        }
    });
    let has_nulls = match delta {
        ColumnData::Object(values) => values.iter().any(Cell::is_na),
        ColumnData::Native(series) => series.null_count() > 0,
    };
    // v4's detector skips the final parser only if the delta still contains a special
    // value. An all-null delta goes through attempt_cast_to_uuid/bytes, which returns None.
    let keep_na = match delta {
        ColumnData::Object(values) => matches!(
            values.iter().find(|value| !value.is_na()),
            Some(Cell::Uuid(_) | Cell::Bytes(_))
        ),
        ColumnData::Native(series) => {
            series.dtype() == &DataType::Binary && series.null_count() < series.len()
        }
    };
    // Native bytes (including empty deltas) must also regain the object merge dtype.
    (special && (has_nulls || matches!(delta, ColumnData::Native(_)))).then(|| {
        ColumnData::Object(
            delta
                .values()
                .into_iter()
                .map(|v| {
                    if v.is_na() {
                        if keep_na { Cell::NA } else { Cell::Null }
                    } else {
                        v
                    }
                })
                .collect(),
        )
    })
}

/// When only the old frame contains JSON, v4 serializes only that frame. New nulls
/// are normalized to pd.NA, and JSON deserialization leaves that non-string sentinel intact.
pub fn object_json_nulls(
    old: &ColumnData,
    new: &ColumnData,
    delta: &ColumnData,
) -> Option<ColumnData> {
    let json = |data: &ColumnData| {
        data.first_valid_index()
            .is_some_and(|index| matches!(data.get(index), Cell::Json(_)))
    };
    if !json(old) || json(new) {
        return None;
    }
    let has_nulls = match delta {
        ColumnData::Object(values) => values.iter().any(Cell::is_na),
        ColumnData::Native(series) => series.null_count() > 0,
    };
    has_nulls.then(|| {
        ColumnData::Object(
            delta
                .values()
                .into_iter()
                .map(|v| if v.is_na() { Cell::NA } else { v })
                .collect(),
        )
    })
}

/// How to convert a single column into the kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnMode {
    /// All rows.
    Full,
    /// One row holding the first valid value (or a null), for the first-valid-value detectors;
    /// zero rows when the frame is empty.
    Probe,
}

fn srid_from_crs(crs: &Bound<'_, PyAny>) -> PyResult<Option<Srid>> {
    if crs.is_none() {
        return Ok(None);
    }
    if let Ok(Some(epsg)) = crs
        .call_method0("to_epsg")
        .and_then(|e| e.extract::<Option<i64>>())
    {
        return Ok(Some(Srid::Code(epsg)));
    }
    let text: String = crs.call_method0("to_string")?.extract()?;
    Ok(Some(
        match text.strip_prefix("EPSG:").and_then(|s| s.parse().ok()) {
            Some(code) => Srid::Code(code),
            None => Srid::Authority(text),
        },
    ))
}

fn is_dask(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(qualname(obj)?.starts_with("dask"))
}

/// Convert a Python frame (pandas, GeoPandas, Polars or Dask) into a kernel [`Frame`].
pub fn frame_from_py<'py>(
    ctx: &PyCtx<'py>,
    df: &Bound<'py, PyAny>,
    mode: ColumnMode,
) -> PyResult<PyFrame<'py>> {
    frame_from_py_with(ctx, df, mode, None)
}

/// [`frame_from_py`], where the pandas columns missing from `typed` (other than geometry
/// columns) cross as all-null placeholders instead of being converted
/// ([`PyFrame::placeholders`]). For callers whose kernel function never reads untyped columns
/// and returns them unchanged, so the output can copy the input Series; the caller must check
/// that every placeholder came back unchanged and convert the frame fully otherwise.
pub fn frame_from_py_with<'py>(
    ctx: &PyCtx<'py>,
    df: &Bound<'py, PyAny>,
    mode: ColumnMode,
    typed: Option<&IndexMap<String, String>>,
) -> PyResult<PyFrame<'py>> {
    let py = ctx.py;
    let name = qualname(df)?;
    if name.starts_with("polars.") {
        let mut collected = if name.ends_with("LazyFrame") {
            df.call_method0("collect")?
        } else {
            df.clone()
        };
        // Polars exports Object values as opaque pointer bytes. Import supported special
        // scalars value by value instead; arbitrary Python object transport is unsupported.
        let pl = import_optional(py, "polars")?;
        let schema = collected.getattr("schema")?;
        let mut objects = Vec::new();
        for item in schema.call_method0("items")?.try_iter()? {
            let (column, dtype): (String, Bound<'py, PyAny>) = item?.extract()?;
            if dtype.eq(pl.getattr("Object")?)? {
                let series = collected.get_item(column.as_str())?;
                let values = series.call_method0("to_list")?;
                let list = values.cast::<PyList>()?;
                if list.iter().any(|original| {
                    let missing = original.is_none()
                        || ctx
                            .na
                            .as_ref()
                            .is_some_and(|value| value.as_ptr() == original.as_ptr())
                        || ctx
                            .nat
                            .as_ref()
                            .is_some_and(|value| value.as_ptr() == original.as_ptr());
                    !(missing
                        || original.is_exact_instance_of::<PyString>()
                        || original.is_exact_instance_of::<PyBytes>()
                        || original.get_type().as_ptr() == ctx.uuid_cls.as_ptr()
                        || original.get_type().as_ptr() == ctx.decimal_cls.as_ptr())
                }) {
                    return Err(PyTypeError::new_err(format!(
                        "Polars Object column '{column}' contains unsupported Python objects. Supported Object values are str, bytes, Decimal, UUID, or missing values; convert other values to a native Polars dtype."
                    )));
                }
                let values = pylist_to_cells(ctx, list)?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("dtype", pl.getattr("Null")?)?;
                let nulls = PyList::new(py, (0..values.len()).map(|_| py.None()))?;
                let placeholder = pl
                    .getattr("Series")?
                    .call((column.as_str(), nulls), Some(&kwargs))?;
                collected = collected.call_method1("with_columns", (placeholder,))?;
                objects.push(Column::object(column, values));
            }
        }
        // Polars (1.36) exports `Extension` string columns as views whose buffer layout arrow-rs
        // 56 rejects; `to_arrow()` exports the non-view layout and keeps the extension metadata.
        let exported = match collected.call_method0(intern!(py, "to_arrow")) {
            Ok(table) => table,
            Err(_) => collected.clone(),
        };
        let mut frame = frame_from_arrow(&exported)?;
        let polars_objects = objects.iter().map(|column| column.name.clone()).collect();
        for column in objects {
            frame.set(&column.name, column.data).map_err(frame_err)?;
        }
        let frame = match mode {
            ColumnMode::Full => frame,
            ColumnMode::Probe => probe_kernel_frame(&frame),
        };
        let dtypes = frame.dtypes();
        let labels = frame
            .names()
            .into_iter()
            .map(|n| {
                let label = PyString::new(py, &n).into_any();
                (n, label)
            })
            .collect();
        return Ok(PyFrame {
            frame,
            kind: FrameKind::Polars,
            index: None,
            dtypes,
            labels,
            series: IndexMap::new(),
            placeholders: vec![],
            polars_objects,
        });
    }
    if is_dask(df)? {
        let npartitions: usize = df.getattr(intern!(py, "npartitions"))?.extract()?;
        let computed = df.call_method0("compute")?;
        let mut out = frame_from_py_with(ctx, &computed, mode, typed)?;
        out.kind = FrameKind::Dask { npartitions };
        return Ok(out);
    }
    if !df.hasattr(intern!(py, "columns"))? || !df.hasattr(intern!(py, "iloc"))? {
        return Err(PyTypeError::new_err(format!(
            "Expected a pandas, Polars or Dask DataFrame, got {name}."
        )));
    }

    let kind = if name.starts_with("geopandas.") {
        let geometry = df
            .getattr(intern!(py, "_geometry_column_name"))
            .ok()
            .and_then(|g| g.extract::<String>().ok());
        FrameKind::Geo { geometry }
    } else {
        FrameKind::Pandas
    };
    let pa = import_optional(py, "pyarrow")?;
    let height: usize = df.len()?;
    let iloc = df.getattr(intern!(py, "iloc"))?;
    let columns: Vec<Bound<'py, PyAny>> = df
        .getattr(intern!(py, "columns"))?
        .try_iter()?
        .collect::<PyResult<_>>()?;

    let mut dtypes = IndexMap::new();
    let mut labels = IndexMap::new();
    let mut sources = IndexMap::new();
    let mut placeholders: Vec<String> = Vec::new();
    // (name, Some(object column)) or (name, None) for native columns resolved from Arrow.
    let mut slots: Vec<(String, Option<Column>)> = Vec::new();
    let mut native_names: Vec<String> = Vec::new();
    let mut native_arrays: Vec<Bound<'py, PyAny>> = Vec::new();

    for (i, label) in columns.iter().enumerate() {
        let col_name: String = match label.cast::<PyString>() {
            Ok(s) => s.to_str()?.to_string(),
            Err(_) => label.str()?.to_str()?.to_string(),
        };
        let series = iloc.get_item((pyo3::types::PySlice::full(py), i))?;
        let dtype: String = series.getattr(intern!(py, "dtype"))?.str()?.extract()?;
        let series = match mode {
            ColumnMode::Full => series,
            ColumnMode::Probe => probe_series(py, &series, height)?,
        };
        dtypes.insert(col_name.clone(), dtype.clone());
        labels.insert(col_name.clone(), label.clone());
        if mode == ColumnMode::Full {
            sources.insert(col_name.clone(), series.clone());
        }

        if mode == ColumnMode::Full
            && dtype != "geometry"
            && typed.is_some_and(|t| !t.contains_key(&col_name))
        {
            slots.push((
                col_name.clone(),
                Some(Column::native(polars::prelude::Series::full_null(
                    col_name.as_str().into(),
                    height,
                    &DataType::Null,
                ))),
            ));
            placeholders.push(col_name);
            continue;
        }
        if dtype == "object" || dtype == "geometry" {
            let _t = crate::trace::Stage::new(format!("in: object column {col_name}"));
            slots.push((
                col_name.clone(),
                Some(object_column(ctx, &col_name, &series, &dtype)?),
            ));
            continue;
        }
        let kwargs = PyDict::new(py);
        kwargs.set_item("from_pandas", true)?;
        let _t = crate::trace::Stage::new(format!("in: pa.array({col_name}) [{dtype}]"));
        match pa.getattr("array")?.call((&series,), Some(&kwargs)) {
            Ok(array) => {
                native_names.push(col_name.clone());
                native_arrays.push(array);
                slots.push((col_name, None));
            }
            Err(_) => {
                slots.push((
                    col_name.clone(),
                    Some(object_column(ctx, &col_name, &series, "object")?),
                ));
            }
        }
    }

    let mut native_frame = Frame::empty();
    if !native_names.is_empty() {
        let _t = crate::trace::Stage::new("in: Table.from_arrays + import + Frame");
        let kwargs = PyDict::new(py);
        kwargs.set_item("names", PyList::new(py, &native_names)?)?;
        let table = pa
            .getattr("Table")?
            .getattr("from_arrays")?
            .call((PyList::new(py, &native_arrays)?,), Some(&kwargs))?;
        native_frame = frame_from_arrow(&table)?;
    }
    let mut out_columns = Vec::with_capacity(slots.len());
    for (col_name, slot) in slots {
        match slot {
            Some(col) => out_columns.push(col),
            None => out_columns.push(native_frame.column(&col_name).cloned().ok_or_else(|| {
                PyTypeError::new_err(format!(
                    "Column '{col_name}' was lost converting to Arrow (got {:?}).",
                    native_frame.names()
                ))
            })?),
        }
    }
    let frame_height = match mode {
        ColumnMode::Full => height,
        ColumnMode::Probe => height.min(1),
    };
    let frame = if out_columns.is_empty() {
        Frame::with_height(frame_height)
    } else {
        Frame::new(out_columns).map_err(frame_err)?
    };
    Ok(PyFrame {
        frame,
        kind,
        index: Some(df.getattr(intern!(py, "index"))?),
        dtypes,
        labels,
        series: sources,
        placeholders,
        polars_objects: vec![],
    })
}

/// Import an Arrow PyCapsule producer into a kernel [`Frame`]. A zero-row stream may carry no
/// batches at all; an empty batch with the stream's schema keeps the columns.
pub(crate) fn frame_from_arrow(obj: &Bound<'_, PyAny>) -> PyResult<Frame> {
    let (schema, mut batches) = import_from_python(obj)?;
    if batches.is_empty() {
        batches.push(arrow::array::RecordBatch::new_empty(schema));
    }
    Frame::from_record_batches(&batches).map_err(frame_err)
}

/// The one-row slice of `series` at its first valid value (or its first row when it has none).
fn probe_series<'py>(
    py: Python<'py>,
    series: &Bound<'py, PyAny>,
    height: usize,
) -> PyResult<Bound<'py, PyAny>> {
    if height == 0 {
        return Ok(series.clone());
    }
    let mask = series
        .call_method0(intern!(py, "notna"))?
        .call_method0(intern!(py, "to_numpy"))?;
    let pos: usize = mask.call_method0("argmax")?.extract()?;
    let valid: bool = mask.get_item(pos)?.is_truthy()?;
    let pos = if valid { pos } else { 0 };
    let slice = pyo3::types::PySlice::new(py, pos as isize, pos as isize + 1, 1);
    series.getattr(intern!(py, "iloc"))?.get_item(slice)
}

/// [`ColumnMode::Probe`] on a kernel frame (Polars inputs).
fn probe_kernel_frame(frame: &Frame) -> Frame {
    if frame.height() == 0 {
        return frame.clone();
    }
    let columns = frame
        .columns()
        .iter()
        .map(|c| {
            let ix = c.data.first_valid_index().unwrap_or(0);
            Column {
                name: c.name.clone(),
                data: c.data.take(&[ix]),
                crs: c.crs.clone(),
                tags: c.tags.clone(),
            }
        })
        .collect();
    Frame::new(columns).unwrap_or_else(|_| Frame::with_height(1))
}

fn object_column<'py>(
    ctx: &PyCtx<'py>,
    name: &str,
    series: &Bound<'py, PyAny>,
    dtype: &str,
) -> PyResult<Column> {
    let py = ctx.py;
    let values = series.call_method0(intern!(py, "tolist"))?;
    let list = values.cast::<PyList>()?;
    let cells = pylist_to_cells(ctx, list)?;
    let mut col = Column::object(name, cells);
    if dtype == "geometry"
        && let Ok(crs) = series.getattr(intern!(py, "crs"))
    {
        col.crs = srid_from_crs(&crs)?;
    }
    Ok(col)
}

pub fn frame_err(e: impl std::fmt::Display) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------------------------
// Out of the kernel

/// The row labels of the result.
pub enum IndexSpec<'py> {
    /// A fresh `RangeIndex`.
    Range,
    /// The input index, unchanged.
    Keep(Bound<'py, PyAny>),
    /// `index.take(positions)`.
    Take(Bound<'py, PyAny>, Vec<usize>),
}

/// How to build the output frame.
pub struct OutSpec<'py> {
    pub index: IndexSpec<'py>,
    /// Pandas dtype hints per column (see the module docs).
    pub hints: IndexMap<String, String>,
    /// v4 rebuilt the frame through Polars: native columns are Arrow-backed.
    pub arrow_native: bool,
    /// Columns v4 enforced through Polars (the native subset of a mixed frame): Arrow-backed.
    pub arrow_columns: Vec<String>,
    /// Return a GeoDataFrame with this active geometry column (when present in the output).
    pub geometry: Option<String>,
    /// Original column labels.
    pub labels: IndexMap<String, Bound<'py, PyAny>>,
    /// Input Series of columns the kernel returned unchanged, in the same row order
    /// ([`unchanged_columns`]). When the output dtype is the input dtype, the column is a copy of
    /// the input Series (`df.copy()`, as v4 returns columns it does not cast) instead of a
    /// conversion of the kernel column.
    pub reuse: IndexMap<String, Bound<'py, PyAny>>,
    /// Placeholder columns ([`PyFrame::placeholders`]), always copied from [`OutSpec::reuse`].
    pub placeholders: Vec<String>,
    /// Original Polars Object columns; Arrow-native Null columns also have an object hint.
    pub polars_objects: Vec<String>,
}

/// Whether two cells are the same value of the same kind (unlike [`Cell`]'s `PartialEq`, which
/// compares as a join key: `1 == 1.0`, `None == NaN`).
fn cells_identical(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Null, Cell::Null) | (Cell::NA, Cell::NA) | (Cell::NaT, Cell::NaT) => true,
        (Cell::Bool(x), Cell::Bool(y)) => x == y,
        (Cell::Int(x), Cell::Int(y)) => x == y,
        (Cell::Float(x), Cell::Float(y)) => x.to_bits() == y.to_bits(),
        (Cell::Str(x), Cell::Str(y)) => x == y,
        (Cell::Bytes(x), Cell::Bytes(y)) => x == y,
        (Cell::Decimal(x), Cell::Decimal(y)) => x == y,
        (Cell::Uuid(x), Cell::Uuid(y)) => x == y,
        (Cell::Datetime(x), Cell::Datetime(y)) => x == y,
        (Cell::Date(x), Cell::Date(y)) => x == y,
        (Cell::Json(x), Cell::Json(y)) => x == y,
        (Cell::Geometry(x), Cell::Geometry(y)) => x == y,
        _ => false,
    }
}

/// Whether the kernel returned `output` exactly as `input` came in (same storage, dtype and
/// values, row for row).
fn column_identical(input: &Column, output: &Column) -> bool {
    if input.crs != output.crs {
        return false;
    }
    match (&input.data, &output.data) {
        (ColumnData::Native(a), ColumnData::Native(b)) => {
            if a.dtype() != b.dtype() || a.len() != b.len() || a.null_count() != b.null_count() {
                return false;
            }
            if a.dtype().is_float() {
                // Bit for bit: `equals_missing` has `NaN == NaN` and `-0.0 == 0.0`.
                let (Ok(x), Ok(y)) = (a.cast(&DataType::Float64), b.cast(&DataType::Float64))
                else {
                    return false;
                };
                let (Ok(x), Ok(y)) = (x.f64(), y.f64()) else {
                    return false;
                };
                return x
                    .iter()
                    .zip(y.iter())
                    .all(|(p, q)| p.map(f64::to_bits) == q.map(f64::to_bits));
            }
            a.equals_missing(b)
        }
        (ColumnData::Object(a), ColumnData::Object(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| cells_identical(x, y))
        }
        _ => false,
    }
}

/// The input Series of the columns in `output` that the kernel left unchanged (same rows in the
/// same order, so only for calls that keep every row).
pub fn unchanged_columns<'py>(
    input: &PyFrame<'py>,
    output: &Frame,
) -> IndexMap<String, Bound<'py, PyAny>> {
    if input.frame.height() != output.height() {
        return IndexMap::new();
    }
    output
        .columns()
        .iter()
        .filter_map(|out| {
            let series = input.series.get(&out.name)?;
            let inp = input.frame.column(&out.name)?;
            column_identical(inp, out).then(|| (out.name.clone(), series.clone()))
        })
        .collect()
}

/// The logical class of a pandas dtype string (backend-independent).
pub fn logical_class(dtype: &str) -> String {
    let d = dtype.trim();
    let base = d.strip_suffix("[pyarrow]").unwrap_or(d);
    let lower = base.to_ascii_lowercase();
    if lower.starts_with("datetime64") || lower.starts_with("timestamp") {
        let inner = base
            .split_once('[')
            .map(|(_, r)| r.trim_end_matches(']'))
            .unwrap_or("");
        let tz = inner
            .split_once(',')
            .map(|(_, tz)| tz.trim().trim_start_matches("tz=").to_string());
        return match tz {
            Some(tz) if tz.eq_ignore_ascii_case("utc") || tz == "+00:00" => "datetime[UTC]".into(),
            Some(tz) => format!("datetime[{tz}]"),
            None => "datetime".into(),
        };
    }
    if lower.starts_with("timedelta64") || lower.starts_with("duration") {
        return "timedelta".into();
    }
    if lower.starts_with("decimal") {
        return "numeric".into();
    }
    if lower.starts_with("date32") || lower.starts_with("date64") || lower == "date" {
        return "date".into();
    }
    if lower.starts_with("int") || lower.starts_with("uint") {
        return "int".into();
    }
    if lower.starts_with("float") || lower == "double" || lower == "halffloat" {
        return "float".into();
    }
    match lower.as_str() {
        "bool" | "boolean" => "bool".into(),
        "str" | "string" | "large_string" | "string_view" | "utf8" => "string".into(),
        "binary" | "large_binary" | "binary_view" | "bytes" => "bytes".into(),
        "category" => "category".into(),
        "geometry" => "geometry".into(),
        "object" | "o" => "object".into(),
        other => other.to_string(),
    }
}

/// The logical class of the values of an object column (`None` when every value is null).
fn object_values_class(values: &[Cell]) -> Option<String> {
    let mut class: Option<String> = None;
    for v in values {
        let c = match v {
            v if v.is_na() => continue,
            Cell::Bool(_) => "bool".to_string(),
            Cell::Int(_) => "int".to_string(),
            Cell::Float(_) => "float".to_string(),
            Cell::Str(_) => "string".to_string(),
            Cell::Bytes(_) => "bytes".to_string(),
            Cell::Decimal(_) => "numeric".to_string(),
            Cell::Uuid(_) => "uuid".to_string(),
            Cell::Date(_) => "date".to_string(),
            Cell::Json(_) => "json".to_string(),
            Cell::Geometry(_) => "geometry".to_string(),
            Cell::Datetime(d) => match &d.tz {
                None => "datetime".to_string(),
                Some(tz) => logical_class(&format!("datetime64[us, {}]", tz.name())),
            },
            _ => "object".to_string(),
        };
        match &class {
            None => class = Some(c),
            Some(prev) if *prev == c => {}
            Some(_) => return Some("mixed".into()),
        }
    }
    class
}

/// The pandas dtype a column takes when no hint applies.
fn default_target(data: &ColumnData) -> Option<String> {
    let ColumnData::Native(s) = data else {
        return Some("object".into());
    };
    let nulls = s.null_count() > 0;
    Some(match s.dtype() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let name = pandas_dtype_of(s.dtype());
            if nulls {
                let mut chars = name.chars();
                match chars.next() {
                    Some('u') => format!("UInt{}", &name[4..]),
                    _ => format!("Int{}", &name[3..]),
                }
            } else {
                name
            }
        }
        DataType::Float32 | DataType::Float64 => pandas_dtype_of(s.dtype()),
        DataType::Boolean => {
            if nulls {
                "boolean".into()
            } else {
                "bool".into()
            }
        }
        DataType::Datetime(..) | DataType::Duration(_) => pandas_dtype_of(s.dtype()),
        DataType::Categorical(..) | DataType::Enum(..) => "category".into(),
        DataType::String | DataType::Decimal(..) | DataType::Null => "object".into(),
        // Dates, binary, times, nested types: keep the Arrow-backed column.
        _ => return None,
    })
}

/// Choose the pandas dtype for an output column.
fn choose_target(data: &ColumnData, hint: Option<&String>) -> Option<String> {
    let Some(hint) = hint else {
        return default_target(data);
    };
    let hint_class = logical_class(hint);
    let matches = match data {
        ColumnData::Native(s) => {
            let col_class = logical_class(&pandas_dtype_of(s.dtype()));
            col_class == hint_class
                || (s.null_count() == s.len() && hint_class != "object")
                || (hint_class == "string" && col_class == "category")
        }
        ColumnData::Object(values) => match object_values_class(values) {
            None => true,
            Some(c) => {
                hint_class == "object"
                    || c == hint_class
                    || (hint_class == "geometry" && c == "geometry")
            }
        },
    };
    if matches {
        Some(hint.clone())
    } else {
        default_target(data)
    }
}

/// The Arrow timestamp units, coarsest first.
const TIMESTAMP_UNITS: [&str; 4] = ["s", "ms", "us", "ns"];

/// A NumPy-backed `datetime64[unit(, tz)]` Series straight from an Arrow timestamp column, or
/// `None` when the fast path does not apply.
///
/// `ArrowExtensionArray(...).astype('datetime64[...]')` converts through one `Timestamp` object
/// per value (about 3 s per million rows); `ChunkedArray.to_pandas()` converts the buffer
/// directly. The result is the same when the timezones are equal and the target unit is at
/// least as fine as the source unit (a lossless Arrow cast; an overflow raises and falls back).
/// Coarser targets and timezone changes keep the pandas path, whose truncation and conversion
/// rules differ from Arrow's.
fn datetime_series<'py>(
    ctx: &PyCtx<'py>,
    chunked: &Bound<'py, PyAny>,
    target: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let py = ctx.py;
    let Some(inner) = target
        .strip_prefix("datetime64[")
        .and_then(|t| t.strip_suffix(']'))
    else {
        return Ok(None);
    };
    let (unit, tz) = match inner.split_once(',') {
        Some((unit, tz)) => (unit.trim(), Some(tz.trim())),
        None => (inner.trim(), None),
    };
    let Some(target_rank) = TIMESTAMP_UNITS.iter().position(|u| *u == unit) else {
        return Ok(None);
    };
    let arrow_type = chunked.getattr(intern!(py, "type"))?;
    let Ok(source_unit) = arrow_type
        .getattr(intern!(py, "unit"))
        .and_then(|u| u.extract::<String>())
    else {
        return Ok(None);
    };
    let source_tz: Option<String> = arrow_type.getattr(intern!(py, "tz"))?.extract()?;
    let Some(source_rank) = TIMESTAMP_UNITS.iter().position(|u| *u == source_unit) else {
        return Ok(None);
    };
    if source_tz.as_deref() != tz || source_rank > target_rank {
        return Ok(None);
    }
    let pa = import_optional(py, "pyarrow")?;
    let casted = if source_rank == target_rank {
        chunked.clone()
    } else {
        let kwargs = PyDict::new(py);
        kwargs.set_item("tz", tz)?;
        let typ = pa.getattr("timestamp")?.call((unit,), Some(&kwargs))?;
        match chunked.call_method1(intern!(py, "cast"), (typ,)) {
            Ok(c) => c,
            Err(_) => return Ok(None),
        }
    };
    let series = casted.call_method0(intern!(py, "to_pandas"))?;
    let dtype: String = series.getattr(intern!(py, "dtype"))?.str()?.extract()?;
    Ok((dtype == target).then_some(series))
}

/// Build a pandas Series (default RangeIndex) for one kernel column.
#[allow(clippy::too_many_arguments)]
fn column_to_series<'py>(
    ctx: &PyCtx<'py>,
    col: &Column,
    arrow_col: Option<&Bound<'py, PyAny>>,
    target: Option<&str>,
    geo: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let py = ctx.py;
    let pd = ctx.pandas()?;
    let object_series = |values: &[Cell]| -> PyResult<Bound<'py, PyAny>> {
        let list = cells_to_pylist(ctx, values)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("dtype", "object")?;
        pd.getattr("Series")?.call((list,), Some(&kwargs))
    };
    let is_geometry = matches!(&col.data, ColumnData::Object(v) if object_values_class(v).as_deref() == Some("geometry"));
    if is_geometry && (geo || target == Some("geometry")) {
        let ColumnData::Object(values) = &col.data else {
            unreachable!()
        };
        let gpd = import_optional(py, "geopandas")?;
        let list = cells_to_pylist(ctx, values)?;
        let kwargs = PyDict::new(py);
        if let Some(crs) = col.crs.as_ref().and_then(Srid::to_crs) {
            kwargs.set_item("crs", crs)?;
        }
        return gpd.getattr("GeoSeries")?.call((list,), Some(&kwargs));
    }

    let base = match (&col.data, arrow_col) {
        (ColumnData::Native(_), Some(chunked)) => {
            if target == Some("object") {
                return object_series(&col.data.values());
            }
            if let Some(series) = target.and_then(|t| datetime_series(ctx, chunked, t).transpose())
            {
                return series;
            }
            let arr = pd
                .getattr("arrays")?
                .getattr("ArrowExtensionArray")?
                .call1((chunked,))?;
            pd.getattr("Series")?.call1((arr,))?
        }
        _ => object_series(&col.data.values())?,
    };
    let Some(target) = target else {
        return Ok(base);
    };
    let current: String = base.getattr(intern!(py, "dtype"))?.str()?.extract()?;
    if current == target {
        return Ok(base);
    }
    match base.call_method1(intern!(py, "astype"), (target,)) {
        Ok(s) => Ok(s),
        Err(_) => {
            // The hint does not apply (e.g. nulls in a NumPy integer column): use the default.
            let fallback = default_target(&col.data);
            match fallback {
                Some(t) if t != current && t != target => {
                    if t == "object" {
                        return object_series(&col.data.values());
                    }
                    Ok(base
                        .call_method1(intern!(py, "astype"), (t,))
                        .unwrap_or(base))
                }
                _ => Ok(base),
            }
        }
    }
}

/// Convert a kernel [`Frame`] into a pandas (or GeoPandas) DataFrame.
pub fn frame_to_pandas<'py>(
    ctx: &PyCtx<'py>,
    frame: &Frame,
    spec: &OutSpec<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = ctx.py;
    let pd = ctx.pandas()?;
    let geo = spec.geometry.is_some();
    // The output dtype of each column, and the input Series to copy when it can be reused.
    let mut plans: Vec<(Option<String>, Option<&Bound<'py, PyAny>>)> = Vec::new();
    for col in frame.columns() {
        let target = if spec.arrow_native || spec.arrow_columns.contains(&col.name) {
            match &col.data {
                ColumnData::Native(_) => None,
                ColumnData::Object(_) => Some("object".to_string()),
            }
        } else {
            choose_target(&col.data, spec.hints.get(&col.name))
        };
        let holds_geometry = matches!(&col.data, ColumnData::Object(v) if object_values_class(v).as_deref() == Some("geometry"));
        let reuse = match (spec.reuse.get(&col.name), target.as_deref()) {
            // A placeholder has no data of its own: the input Series is the column.
            (Some(src), _) if spec.placeholders.contains(&col.name) => Some(src),
            (Some(src), Some(t)) if !holds_geometry && t != "geometry" => {
                let current: String = src.getattr(intern!(py, "dtype"))?.str()?.extract()?;
                (current == t).then_some(src)
            }
            _ => None,
        };
        plans.push((target, reuse));
    }

    // Export only the native columns that are not reused.
    let native: Vec<Column> = frame
        .columns()
        .iter()
        .zip(&plans)
        .filter(|(c, (_, reuse))| matches!(c.data, ColumnData::Native(_)) && reuse.is_none())
        .map(|(c, _)| c.clone())
        .collect();
    let _t0 = crate::trace::Stage::new("out: Frame -> RecordBatch -> pa.table");
    let table = if native.is_empty() {
        None
    } else {
        let sub = Frame::new(native).map_err(frame_err)?;
        let batch = sub.to_record_batch().map_err(frame_err)?;
        let stream = ArrowStream::new(batch.schema(), vec![batch]);
        let pa = import_optional(py, "pyarrow")?;
        Some(pa.getattr("table")?.call1((Py::new(py, stream)?,))?)
    };

    drop(_t0);
    let data = PyDict::new(py);
    let mut geometry_present: Option<String> = None;
    let mut ordered_labels: Vec<Bound<'py, PyAny>> = Vec::new();
    let range = pd.getattr("RangeIndex")?.call1((frame.height(),))?;
    for (col, (target, reuse)) in frame.columns().iter().zip(&plans) {
        let _t = crate::trace::Stage::new(format!(
            "out: series {} -> {:?}{}",
            col.name,
            target,
            if reuse.is_some() { " (reused)" } else { "" }
        ));
        let series = match reuse {
            Some(src) => {
                // `df.copy()`: a deep copy (object columns keep the same objects), re-indexed
                // like the other columns before the frame index is set.
                let copy = src.call_method0(intern!(py, "copy"))?;
                copy.setattr(intern!(py, "index"), &range)?;
                copy
            }
            None => {
                let arrow_col = match (&col.data, &table) {
                    (ColumnData::Native(_), Some(t)) => {
                        Some(t.call_method1("column", (col.name.as_str(),))?)
                    }
                    _ => None,
                };
                column_to_series(ctx, col, arrow_col.as_ref(), target.as_deref(), geo)?
            }
        };
        let label = spec
            .labels
            .get(&col.name)
            .cloned()
            .unwrap_or_else(|| PyString::new(py, &col.name).into_any());
        if spec.geometry.as_deref() == Some(col.name.as_str())
            && qualname(&series)?.starts_with("geopandas")
        {
            geometry_present = Some(col.name.clone());
        }
        data.set_item(&label, series)?;
        ordered_labels.push(label);
    }
    let _t1 = crate::trace::Stage::new("out: DataFrame + index");
    let kwargs = PyDict::new(py);
    kwargs.set_item("copy", false)?;
    let df = if ordered_labels.is_empty() {
        let kw = PyDict::new(py);
        kw.set_item("index", pd.getattr("RangeIndex")?.call1((frame.height(),))?)?;
        pd.getattr("DataFrame")?.call((), Some(&kw))?
    } else {
        let df = pd.getattr("DataFrame")?.call((data,), Some(&kwargs))?;
        // Keep the column order (and duplicate-free labels) of the kernel frame.
        df.get_item(PyList::new(py, &ordered_labels)?)?
    };
    match &spec.index {
        IndexSpec::Range => {}
        IndexSpec::Keep(index) => {
            if index.len()? == frame.height() {
                df.setattr("index", index)?;
            }
        }
        IndexSpec::Take(index, positions) => {
            let taken = index.call_method1("take", (PyList::new(py, positions)?,))?;
            if taken.len()? == frame.height() {
                df.setattr("index", taken)?;
            }
        }
    }
    if let Some(geometry) = geometry_present {
        let gpd = import_optional(py, "geopandas")?;
        let kw = PyDict::new(py);
        kw.set_item("geometry", geometry.as_str())?;
        let crs = frame
            .column(&geometry)
            .and_then(|c| c.crs.as_ref())
            .and_then(Srid::to_crs);
        if let Some(crs) = crs {
            kw.set_item("crs", crs)?;
        }
        return gpd.getattr("GeoDataFrame")?.call((df,), Some(&kw));
    }
    Ok(df)
}

/// Convert a kernel [`Frame`] into a Polars DataFrame.
///
/// GeoArrow WKB and JSON columns become Polars `Extension` columns (`geoarrow.wkb` with the CRS
/// metadata, `arrow.json`), as v4's `to_polars()` builds them: the extension metadata is stripped
/// for the Arrow export (Polars warns on unregistered extensions) and re-applied with
/// `Series.ext.to(pl.Extension(...))`.
///
/// `declared` (the enforced dtypes) makes declared `json` / `geometry[...]` columns extension
/// columns even when every value is null, as `to_polars(json_cols=..., geometry_cols_types_srids=...)`
/// does.
pub fn frame_to_polars<'py>(
    py: Python<'py>,
    frame: &Frame,
    declared: &IndexMap<String, String>,
) -> PyResult<Bound<'py, PyAny>> {
    use std::sync::Arc;
    const EXT_NAME: &str = "ARROW:extension:name";
    const EXT_META: &str = "ARROW:extension:metadata";
    let batch = frame.to_record_batch().map_err(frame_err)?;
    let mut extensions: Vec<(String, String, String)> = Vec::new();
    let fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let mut md = f.metadata().clone();
            if let Some(name) = md.remove(EXT_NAME) {
                let meta = md.remove(EXT_META).unwrap_or_default();
                if name == "geoarrow.wkb" || name == "arrow.json" || name == "arrow.uuid" {
                    extensions.push((f.name().clone(), name, meta));
                } else {
                    md.insert(EXT_NAME, name);
                    md.insert(EXT_META, meta);
                }
            }
            f.as_ref().clone().with_metadata(md)
        })
        .collect();
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let batch = arrow::array::RecordBatch::try_new_with_options(
        schema.clone(),
        batch.columns().to_vec(),
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(frame_err)?;
    let stream = ArrowStream::new(schema, vec![batch]);
    let pl = import_optional(py, "polars")?;
    let mut df = pl.getattr("DataFrame")?.call1((Py::new(py, stream)?,))?;
    if extensions.iter().any(|(_, name, _)| name == "geoarrow.wkb") {
        // PARITY: v4's `to_polars()` registers the GeoArrow pyarrow extension types.
        if let Ok(module) = import_optional(py, "geoarrow.types.type_pyarrow") {
            let _ = module.call_method0("register_extension_types");
        }
    }
    for (col, typ) in declared {
        if !frame.contains(col) || extensions.iter().any(|(c, _, _)| c == col) {
            continue;
        }
        let norm = meerschaum_kernel::dtypes::resolve_alias(typ).to_lowercase();
        if norm == "json" {
            extensions.push((col.clone(), "arrow.json".into(), String::new()));
        } else if norm.starts_with("geometry") || norm.starts_with("geography") {
            let srid =
                meerschaum_kernel::dtypes::get_geometry_type_srid(typ, "geometry", Srid::Code(0))
                    .map(|(_, srid)| srid)
                    .unwrap_or(Srid::Code(0));
            let meta = match srid.to_crs() {
                Some(crs) => {
                    serde_json::json!({"crs": crs, "crs_type": "authority_code"}).to_string()
                }
                None => "{}".to_string(),
            };
            extensions.push((col.clone(), "geoarrow.wkb".into(), meta));
        }
    }
    for (col, name, meta) in extensions {
        if name == "arrow.uuid" {
            // PARITY: v4's `to_polars()` keeps UUIDs as Python objects (`strict=False`).
            // `scalars`: a Polars result never imports (or installs) pandas.
            let ctx = crate::convert::PyCtx::scalars(py)?;
            let values = frame.data(&col).map_err(frame_err)?.values();
            let list = cells_to_pylist(&ctx, &values)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", pl.getattr("Object")?)?;
            let series = pl
                .getattr("Series")?
                .call((col.as_str(), list), Some(&kwargs))?;
            df = df.call_method1("with_columns", (series,))?;
            continue;
        }
        let mut series = df.get_item(col.as_str())?;
        if series.getattr("dtype")?.eq(pl.getattr("Null")?)? {
            let storage = if name == "arrow.json" {
                "String"
            } else {
                "Binary"
            };
            series = series.call_method1("cast", (pl.getattr(storage)?,))?;
        }
        let storage = series.getattr("dtype")?;
        let dtype = pl.getattr("Extension")?.call1((name, storage, meta))?;
        let converted = series.getattr("ext")?.call_method1("to", (dtype,))?;
        df = df.call_method1("with_columns", (converted,))?;
    }
    Ok(df)
}

/// Convert the result back into the kind of frame that came in.
pub fn frame_to_py<'py>(
    ctx: &PyCtx<'py>,
    frame: &Frame,
    kind: &FrameKind,
    spec: &OutSpec<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    match kind {
        FrameKind::Polars => {
            let mut df = frame_to_polars(ctx.py, frame, &IndexMap::new())?;
            let pl = import_optional(ctx.py, "polars")?;
            for column in frame.columns() {
                if !spec.polars_objects.contains(&column.name) {
                    continue;
                }
                // Keep recognized Python values as objects instead of coercing mixed values
                // through Arrow's homogeneous storage. Polars uses None for missing objects.
                let values: Vec<_> = column
                    .data
                    .values()
                    .into_iter()
                    .map(|v| if v.is_na() { Cell::Null } else { v })
                    .collect();
                let kwargs = PyDict::new(ctx.py);
                kwargs.set_item("dtype", pl.getattr("Object")?)?;
                let series = pl.getattr("Series")?.call(
                    (column.name.as_str(), cells_to_pylist(ctx, &values)?),
                    Some(&kwargs),
                )?;
                df = df.call_method1("with_columns", (series,))?;
            }
            Ok(df)
        }
        FrameKind::Dask { npartitions } => {
            let pdf = frame_to_pandas(ctx, frame, spec)?;
            let dd = import_optional(ctx.py, "dask.dataframe")?;
            let kw = PyDict::new(ctx.py);
            kw.set_item("npartitions", (*npartitions).max(1))?;
            kw.set_item("sort", false)?;
            dd.getattr("from_pandas")?.call((pdf,), Some(&kw))
        }
        _ => frame_to_pandas(ctx, frame, spec),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{NamedFrom, Series};

    #[test]
    fn logical_classes_erase_backends() {
        for d in ["int64", "Int64", "int64[pyarrow]", "uint8", "UInt32"] {
            assert_eq!(logical_class(d), "int", "{d}");
        }
        for d in ["float64", "Float64", "double[pyarrow]", "float32"] {
            assert_eq!(logical_class(d), "float", "{d}");
        }
        for d in ["string", "str", "large_string[pyarrow]", "string[pyarrow]"] {
            assert_eq!(logical_class(d), "string", "{d}");
        }
        assert_eq!(logical_class("bool[pyarrow]"), "bool");
        assert_eq!(logical_class("boolean"), "bool");
        assert_eq!(logical_class("datetime64[ns, UTC]"), "datetime[UTC]");
        assert_eq!(
            logical_class("timestamp[us, tz=UTC][pyarrow]"),
            "datetime[UTC]"
        );
        assert_eq!(logical_class("datetime64[us]"), "datetime");
        assert_eq!(
            logical_class("datetime64[ns, America/New_York]"),
            "datetime[America/New_York]"
        );
        assert_eq!(logical_class("date32[day][pyarrow]"), "date");
        assert_eq!(logical_class("binary[pyarrow]"), "bytes");
        assert_eq!(logical_class("decimal128(10, 2)[pyarrow]"), "numeric");
        assert_eq!(logical_class("object"), "object");
    }

    #[test]
    fn ordinary_object_string_nulls_keep_special_values_and_inference_separate() {
        let old = ColumnData::Object(vec![Cell::Str("a".into()), Cell::Int(1)]);
        let new = ColumnData::Object(vec![Cell::Null, Cell::Str("b".into()), Cell::Int(2)]);
        let normalized = object_string_nulls(&old, &new, &new, true).unwrap();
        let ColumnData::Object(values) = normalized else {
            panic!("ordinary object merge must stay object");
        };
        assert!(matches!(values[0], Cell::NA));
        assert!(matches!(&values[1], Cell::Str(v) if v == "b"));
        assert!(matches!(values[2], Cell::Int(2)));

        let strings = ColumnData::Native(Series::new("s".into(), &[None, Some("b")]));
        assert!(object_string_nulls(&old, &new, &strings, true).is_none());
        assert!(object_string_nulls(&old, &new, &strings, false).is_some());
        let no_nulls = ColumnData::Object(vec![Cell::Str("b".into()), Cell::Int(2)]);
        assert!(object_string_nulls(&old, &new, &no_nulls, false).is_none());
        let no_null_strings = ColumnData::Native(Series::new("s".into(), &["b"]));
        assert!(object_string_nulls(&old, &new, &no_null_strings, false).is_none());

        for special in [
            Cell::Json(serde_json::json!({"x": 1})),
            Cell::Decimal(meerschaum_kernel::Decimal::parse("1.1").unwrap()),
            Cell::Bytes(vec![1]),
            Cell::Uuid(uuid::Uuid::nil()),
        ] {
            let mixed = ColumnData::Object(vec![Cell::Str("a".into()), special, Cell::Null]);
            assert!(object_string_nulls(&mixed, &new, &mixed, false).is_none());
        }
        let encoded = ColumnData::Object(vec![Cell::Str("{\"x\":1}".into())]);
        let decoded = ColumnData::Object(vec![Cell::Null, Cell::Json(serde_json::json!({"x": 2}))]);
        assert!(object_string_nulls(&encoded, &encoded, &decoded, false).is_none());
    }

    #[test]
    fn reuse_requires_identical_columns() {
        let native = |values: &[Option<f64>]| Column::native(Series::new("a".into(), values));
        assert!(column_identical(
            &native(&[Some(1.0), None]),
            &native(&[Some(1.0), None])
        ));
        // Floats compare bit for bit.
        assert!(column_identical(
            &native(&[Some(f64::NAN)]),
            &native(&[Some(f64::NAN)])
        ));
        assert!(!column_identical(
            &native(&[Some(-0.0)]),
            &native(&[Some(0.0)])
        ));
        let ints = Column::native(Series::new("a".into(), &[1i64]));
        assert!(!column_identical(&ints, &native(&[Some(1.0)])));

        let object = |values: Vec<Cell>| Column::object("a", values);
        assert!(column_identical(
            &object(vec![Cell::Str("x".into()), Cell::Null]),
            &object(vec![Cell::Str("x".into()), Cell::Null])
        ));
        // Join-key equality is not enough: `None` vs `pd.NA`, `1` vs `1.0`.
        assert!(!column_identical(
            &object(vec![Cell::Null]),
            &object(vec![Cell::NA])
        ));
        assert!(!column_identical(
            &object(vec![Cell::Int(1)]),
            &object(vec![Cell::Float(1.0)])
        ));
        assert!(!column_identical(&object(vec![Cell::Int(1)]), &ints));
    }

    #[test]
    fn hints_apply_only_within_a_class() {
        let ints = ColumnData::Native(Series::new("a".into(), &[1i64, 2]));
        assert_eq!(
            choose_target(&ints, Some(&"int64[pyarrow]".to_string())).as_deref(),
            Some("int64[pyarrow]")
        );
        // A float hint does not apply to an integer column: the default is used.
        assert_eq!(
            choose_target(&ints, Some(&"float64".to_string())).as_deref(),
            Some("int64")
        );
        let nullable = ColumnData::Native(Series::new("a".into(), &[Some(1i64), None]));
        assert_eq!(choose_target(&nullable, None).as_deref(), Some("Int64"));
        let decimals = ColumnData::Object(vec![Cell::Decimal(
            meerschaum_kernel::Decimal::parse("1.5").unwrap(),
        )]);
        assert_eq!(
            choose_target(&decimals, Some(&"int64".to_string())).as_deref(),
            Some("object")
        );
        let nulls = ColumnData::Object(vec![Cell::Null, Cell::NA]);
        assert_eq!(
            choose_target(&nulls, Some(&"Int64".to_string())).as_deref(),
            Some("Int64")
        );
    }
}
