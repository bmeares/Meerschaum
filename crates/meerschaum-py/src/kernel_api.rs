//! `meerschaum-kernel` bound into `meerschaum._core`.
//!
//! Each function takes exactly the arguments the dispatch guards in
//! `meerschaum/utils/dataframe.py` and `meerschaum/utils/dtypes/__init__.py` pass, converts the
//! frames with [`crate::pandas_bridge`], runs the kernel, and restores the pandas dtypes v4 would
//! return (see `crates/meerschaum-py/PARITY.md`).

use indexmap::IndexMap;
use meerschaum_kernel::detect;
use meerschaum_kernel::diff::{FilterUnseenOptions, filter_unseen_df as k_filter_unseen_df};
use meerschaum_kernel::dtypes::{resolve_alias, to_pandas_dtype as k_to_pandas_dtype};
use meerschaum_kernel::enforce::{
    EnforceOptions, EnforcePath, enforce_dtypes_with_path, enforce_dtypes_with_report,
};
use meerschaum_kernel::query::{ParamValue, QueryOptions, query_df as k_query_df};
use meerschaum_kernel::value::Value as Cell;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFrozenSet, PyList, PySet, PyString, PyTuple};

use crate::convert::{PyCtx, py_to_cell};
use crate::pandas_bridge::{
    ColumnMode, FrameKind, IndexSpec, OutSpec, PyFrame, frame_from_py, frame_from_py_with,
    frame_to_py, object_json_nulls, object_special_nulls, object_string_inference,
    object_string_nulls, string_merge_hint, unchanged_columns,
};

/// A `{col: dtype}` mapping with `str()` of each key and value.
fn str_map(obj: Option<&Bound<'_, PyAny>>) -> PyResult<IndexMap<String, String>> {
    let mut out = IndexMap::new();
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(out);
    };
    let dict = obj.downcast::<PyDict>()?;
    for (k, v) in dict.iter() {
        out.insert(k.str()?.extract()?, v.str()?.extract()?);
    }
    Ok(out)
}

fn geometry_from_kind(kind: &FrameKind) -> Option<String> {
    match kind {
        FrameKind::Geo { geometry } => geometry.clone(),
        _ => None,
    }
}

fn is_fallback_dtype(dtype: &str) -> bool {
    let norm = resolve_alias(dtype).to_lowercase();
    norm == "uuid"
        || norm == "object"
        || norm.starts_with("geometry")
        || norm.starts_with("geography")
        || (norm.starts_with("numeric") && {
            let (p, s) = meerschaum_kernel::dtypes::get_numeric_precision_scale(Some(&norm));
            p.is_none() || s.is_none()
        })
}

// ---------------------------------------------------------------------------------------------
// enforce_dtypes

/// `enforce_dtypes(df, dtypes, explicit_dtypes=None, safe_copy=True, coerce_numeric=False,
/// coerce_timezone=True, strip_timezone=False, as_polars=False, debug=False)`.
#[pyfunction]
#[pyo3(signature = (
    df, dtypes, explicit_dtypes=None, safe_copy=true, coerce_numeric=false,
    coerce_timezone=true, strip_timezone=false, as_polars=false, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn enforce_dtypes<'py>(
    py: Python<'py>,
    df: Bound<'py, PyAny>,
    dtypes: Bound<'py, PyAny>,
    explicit_dtypes: Option<Bound<'py, PyAny>>,
    safe_copy: bool,
    coerce_numeric: bool,
    coerce_timezone: bool,
    strip_timezone: bool,
    as_polars: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _ = (safe_copy, debug);
    if df.is_none() {
        return Ok(df);
    }
    let ctx = PyCtx::new(py)?;
    let dtypes = str_map(Some(&dtypes))?;
    let opts = EnforceOptions {
        explicit_dtypes: str_map(explicit_dtypes.as_ref())?,
        coerce_numeric,
        coerce_timezone,
        strip_timezone,
    };
    let run = |input: &PyFrame<'py>| {
        let frame = &input.frame;
        let _t = crate::trace::Stage::new("kernel: enforce_dtypes");
        py.allow_threads(|| enforce_dtypes_with_report(frame, &dtypes, &opts))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    };
    // v4 never reads or casts untyped columns of a pandas frame (it returns them as copies), so
    // they cross as placeholders and the output copies the input Series. Should the kernel ever
    // change a placeholder, the call is redone with every column converted.
    let mut input = if as_polars {
        frame_from_py(&ctx, &df, ColumnMode::Full)?
    } else {
        frame_from_py_with(&ctx, &df, ColumnMode::Full, Some(&dtypes))?
    };
    let (mut out, mut path) = run(&input)?;
    if !input.placeholders.is_empty() && !placeholders_unchanged(&input, &out) {
        input = frame_from_py(&ctx, &df, ColumnMode::Full)?;
        (out, path) = run(&input)?;
    }
    if as_polars {
        return crate::pandas_bridge::frame_to_polars(py, &out, &dtypes);
    }
    let native = path == EnforcePath::Native;
    // The native subset of a mixed frame comes back Arrow-backed (v4 assigns the Polars
    // result's arrays back).
    let mut arrow_columns = match path {
        EnforcePath::Subset(cols) => cols,
        _ => vec![],
    };
    let mut native = native;
    if input.kind == FrameKind::Polars && native {
        // PARITY: `_enforce_dtypes_with_polars` gives up when a `json` column of a Polars frame is
        // not an `arrow.json` extension (a plain `String`), so v4 takes the object path. The
        // kernel cannot tell a Polars `String` from a pandas `string` column (which v4 does
        // enforce natively), so the decision is made here.
        native = !dtypes.iter().any(|(col, typ)| {
            resolve_alias(typ).eq_ignore_ascii_case("json")
                && matches!(
                    input.frame.column(col).map(|c| &c.data),
                    Some(meerschaum_kernel::ColumnData::Native(_))
                )
        });
    }
    if input.kind == FrameKind::Polars {
        // PARITY: v4 returns pandas unless `as_polars` (`to_pandas(df)` first, so untyped columns
        // are Arrow-backed).
        input.kind = FrameKind::Pandas;
        for col in input.dtypes.keys() {
            if !dtypes.contains_key(col) {
                arrow_columns.push(col.clone());
            }
        }
    }
    enforce_output(&ctx, &input, &out, &dtypes, native, arrow_columns)
}

/// Whether every placeholder column of `input` is in `out`, unchanged, with the same row count
/// (so the input Series can stand in for it).
fn placeholders_unchanged(input: &PyFrame<'_>, out: &meerschaum_kernel::Frame) -> bool {
    if out.height() != input.frame.height() {
        return false;
    }
    let unchanged = unchanged_columns(input, out);
    input.placeholders.iter().all(|name| {
        unchanged.contains_key(name)
            && matches!(
                out.column(name).map(|c| &c.data),
                Some(meerschaum_kernel::ColumnData::Native(s))
                    if s.dtype() == &polars::prelude::DataType::Null
            )
    })
}

/// The pandas frame `enforce_dtypes` returns for `out` (also used by `query_df`).
fn enforce_output<'py>(
    ctx: &PyCtx<'py>,
    input: &PyFrame<'py>,
    out: &meerschaum_kernel::Frame,
    dtypes: &IndexMap<String, String>,
    native: bool,
    arrow_columns: Vec<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut hints = input.dtypes.clone();
    for (col, typ) in dtypes {
        let target = k_to_pandas_dtype(typ);
        let norm = resolve_alias(typ).to_lowercase();
        let hint = if native {
            target
        } else if matches!(norm.as_str(), "json" | "uuid" | "bytes") || norm.starts_with("numeric")
        {
            // PARITY: the object path casts these value by value (`.apply(...)`), and the final
            // `astype` skips them because `are_dtypes_equal('object', target)` holds.
            "object".to_string()
        } else {
            // PARITY: the final loop only casts columns whose current dtype is not
            // `are_dtypes_equal` to the target, so e.g. `int32[pyarrow]` stays as it is for `int`.
            // Datetime columns are coerced explicitly (timezone, precision), so they take the
            // target.
            let is_datetime = meerschaum_kernel::dtypes::are_dtypes_equal(&target, "datetime");
            match input.dtypes.get(col) {
                Some(current)
                    if !is_datetime
                        && meerschaum_kernel::dtypes::are_dtypes_equal(current, &target) =>
                {
                    current.clone()
                }
                _ => target,
            }
        };
        hints.insert(col.clone(), hint);
    }
    // v4 converts the declared geometry columns into a GeoDataFrame.
    let mut geometry = geometry_from_kind(&input.kind);
    if !native {
        for (col, typ) in dtypes {
            let norm = typ.to_lowercase();
            if (norm.starts_with("geometry") || norm.starts_with("geography"))
                && detect::get_geometry_cols(out).contains(col)
            {
                if geometry.is_none() {
                    geometry = Some(col.clone());
                }
                hints.insert(col.clone(), "geometry".into());
            }
        }
    }
    let index = match (&input.index, native) {
        (Some(index), false) => IndexSpec::Keep(index.clone()),
        _ => IndexSpec::Range,
    };
    let spec = OutSpec {
        index,
        hints,
        arrow_native: native,
        arrow_columns,
        geometry,
        labels: input.labels.clone(),
        reuse: unchanged_columns(input, out),
        placeholders: input.placeholders.clone(),
        polars_objects: input.polars_objects.clone(),
    };
    frame_to_py(ctx, out, &input.kind, &spec)
}

/// The columns of a mixed frame that v4 enforces through Polars first (`native_cols` in
/// `enforce_dtypes`): every non-fallback typed column, when the frame also has fallback-typed or
/// untyped columns and the Polars enforcement of that subset succeeds.
fn native_subset_columns(
    frame: &meerschaum_kernel::Frame,
    dtypes: &IndexMap<String, String>,
    opts: &EnforceOptions,
) -> Vec<String> {
    let native: IndexMap<String, String> = dtypes
        .iter()
        .filter(|(_, t)| !is_fallback_dtype(t))
        .map(|(c, t)| (c.clone(), t.clone()))
        .collect();
    let has_fallback = native.len() != dtypes.len();
    let untyped = frame.names().iter().any(|c| !dtypes.contains_key(c));
    if native.is_empty() || !(has_fallback || untyped) {
        return vec![];
    }
    let cols: Vec<String> = native
        .keys()
        .filter(|c| frame.contains(c))
        .cloned()
        .collect();
    if cols.is_empty() {
        return vec![];
    }
    let Ok(subset) = frame.select(&cols) else {
        return vec![];
    };
    match enforce_dtypes_with_path(&subset, &native, opts) {
        Ok((_, true)) => cols,
        _ => vec![],
    }
}

// ---------------------------------------------------------------------------------------------
// filter_unseen_df

/// `filter_unseen_df(old_df, new_df, safe_copy=True, dtypes=None,
/// include_unchanged_columns=False, coerce_mixed_numerics=True, debug=False)`.
#[pyfunction]
#[pyo3(signature = (
    old_df, new_df, safe_copy=true, dtypes=None, include_unchanged_columns=false,
    coerce_mixed_numerics=true, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn filter_unseen_df<'py>(
    py: Python<'py>,
    old_df: Bound<'py, PyAny>,
    new_df: Bound<'py, PyAny>,
    safe_copy: bool,
    dtypes: Option<Bound<'py, PyAny>>,
    include_unchanged_columns: bool,
    coerce_mixed_numerics: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _ = (safe_copy, include_unchanged_columns, debug);
    // PARITY: `if old_df is None: return new_df` returns the very same object.
    if old_df.is_none() {
        return Ok(new_df);
    }
    let ctx = PyCtx::new(py)?;
    let old = frame_from_py(&ctx, &old_df, ColumnMode::Full)?;
    let new = frame_from_py(&ctx, &new_df, ColumnMode::Full)?;
    // PARITY: "Edge case: two empty lists cast to DFs" returns `new_df` itself.
    let same_cols = {
        let mut a: Vec<&String> = old.dtypes.keys().collect();
        let mut b: Vec<&String> = new.dtypes.keys().collect();
        a.sort();
        b.sort();
        a == b
    };
    if same_cols && new.dtypes.is_empty() {
        return Ok(new_df);
    }
    let dtypes_map = match dtypes.as_ref().filter(|d| !d.is_none()) {
        Some(d) => Some(str_map(Some(d))?),
        None => None,
    };
    let opts = FilterUnseenOptions {
        dtypes: dtypes_map.clone(),
        coerce_mixed_numerics,
    };
    let (old_frame, new_frame) = (&old.frame, &new.frame);
    let mut delta = {
        let _t = crate::trace::Stage::new("kernel: filter_unseen_df");
        py.allow_threads(|| k_filter_unseen_df(Some(old_frame), new_frame, &opts))
    };

    // v4 casts the new frame to `to_pandas_dtype()` of `dtypes` (default: the old frame's dtypes,
    // so `int64` becomes `int64[pyarrow]`) for the columns in both frames before the merge.
    let mut hints = new.dtypes.clone();
    let declared = dtypes_map.clone().unwrap_or_else(|| old.dtypes.clone());
    let infer_string = old.dtypes.values().any(|t| t == "object")
        && ctx
            .pandas()?
            .call_method1("get_option", ("future.infer_string",))
            .and_then(|v| v.extract::<bool>())
            .unwrap_or(false);
    for (col, typ) in &declared {
        if new.dtypes.contains_key(col) && old.dtypes.contains_key(col) {
            let target = k_to_pandas_dtype(typ);
            let mut merge_target = string_merge_hint(&ctx, old.series.get(col), &target)?;
            if merge_target != target {
                if let Ok(data) = delta.data(col) {
                    // The nullable-string cast and normalize_nulls use pd.NA. Keep that
                    // sentinel when restoring the object merge dtype: Arrow transport
                    // otherwise turns string nulls into None / NaN.
                    let values = data
                        .values()
                        .into_iter()
                        .map(|v| if v.is_na() { Cell::NA } else { v })
                        .collect();
                    delta
                        .set(col, meerschaum_kernel::ColumnData::Object(values))
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                }
            } else if merge_target == "object" && old.series.contains_key(col) {
                if let (Ok(old_col), Ok(new_col), Ok(result)) =
                    (old.frame.data(col), new.frame.data(col), delta.data(col))
                {
                    if infer_string && object_string_inference(old_col, new_col) {
                        merge_target = "str".to_string();
                    }
                    if let Some(normalized) =
                        object_string_nulls(old_col, new_col, result, infer_string)
                            .filter(|_| merge_target == "object")
                    {
                        delta
                            .set(col, normalized)
                            .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    }
                }
            }
            hints.insert(col.clone(), merge_target);
        }
    }
    // Special-column serialization is detected from the frames regardless of which columns
    // appear in an explicit dtype mapping. Restore those object sentinels after dtype hints.
    for col in old.series.keys() {
        if let (Ok(old_col), Ok(new_col), Ok(result)) =
            (old.frame.data(col), new.frame.data(col), delta.data(col))
        {
            // An explicit partial dtype map does not suppress normalize_nulls() for
            // untyped object columns in either input frame.
            if !declared.contains_key(col)
                && old.dtypes.get(col).is_some_and(|dtype| dtype == "object")
                && new.dtypes.get(col).is_some_and(|dtype| dtype == "object")
            {
                if infer_string && object_string_inference(old_col, new_col) {
                    hints.insert(col.clone(), "str".to_string());
                } else if let Some(normalized) =
                    object_string_nulls(old_col, new_col, result, infer_string)
                {
                    delta
                        .set(col, normalized)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                }
            }
            let result = delta
                .data(col)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            if let Some(normalized) = object_special_nulls(old_col, new_col, result)
                .or_else(|| object_json_nulls(old_col, new_col, result))
            {
                delta
                    .set(col, normalized)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                hints.insert(col.clone(), "object".to_string());
            }
        }
    }
    // PARITY: a column the kernel keeps as `object` (bytes or dates cast by a hint against the
    // old frame's `object` values) merges to `object` in pandas; the hint's class must not recast
    // it (`binary[pyarrow]`, `date32[day][pyarrow]`).
    for col in delta.columns() {
        if let meerschaum_kernel::ColumnData::Object(values) = &col.data {
            let first = values.iter().find(|v| !v.is_na());
            if matches!(
                first,
                Some(meerschaum_kernel::Value::Bytes(_) | meerschaum_kernel::Value::Date(_))
            ) {
                hints.insert(col.name.clone(), "object".to_string());
            }
        }
    }
    let mut labels = old.labels.clone();
    labels.extend(new.labels.clone());
    let spec = OutSpec {
        index: IndexSpec::Range,
        hints,
        arrow_native: false,
        arrow_columns: vec![],
        geometry: geometry_from_kind(&new.kind),
        labels,
        reuse: IndexMap::new(),
        placeholders: vec![],
        polars_objects: new.polars_objects.clone(),
    };
    frame_to_py(&ctx, &delta, &new.kind, &spec)
}

// ---------------------------------------------------------------------------------------------
// query_df

fn param_value(ctx: &PyCtx<'_>, obj: &Bound<'_, PyAny>) -> PyResult<ParamValue> {
    let py = ctx.py;
    let is_seq = obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>()
        || obj.is_instance_of::<PySet>()
        || obj.is_instance_of::<PyFrozenSet>()
        || (!obj.is_instance_of::<PyString>() && obj.hasattr("astype")?);
    if is_seq {
        let mut values = Vec::new();
        for item in obj.try_iter()? {
            values.push(py_to_cell(ctx, &item?)?);
        }
        return Ok(ParamValue::Many(values));
    }
    let _ = py;
    Ok(ParamValue::One(py_to_cell(ctx, obj)?))
}

fn opt_str_list(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<String>>> {
    match obj.filter(|o| !o.is_none()) {
        None => Ok(None),
        Some(o) => {
            let mut out = Vec::new();
            for item in o.try_iter()? {
                out.push(item?.str()?.extract()?);
            }
            Ok(Some(out))
        }
    }
}

/// `query_df(df, params=None, begin=None, end=None, datetime_column=None, select_columns=None,
/// omit_columns=None, inplace=False, reset_index=False, coerce_types=False, debug=False)`.
#[pyfunction]
#[pyo3(signature = (
    df, params=None, begin=None, end=None, datetime_column=None, select_columns=None,
    omit_columns=None, inplace=false, reset_index=false, coerce_types=false, debug=false
))]
#[allow(clippy::too_many_arguments)]
pub fn query_df<'py>(
    py: Python<'py>,
    df: Bound<'py, PyAny>,
    params: Option<Bound<'py, PyAny>>,
    begin: Option<Bound<'py, PyAny>>,
    end: Option<Bound<'py, PyAny>>,
    datetime_column: Option<String>,
    select_columns: Option<Bound<'py, PyAny>>,
    omit_columns: Option<Bound<'py, PyAny>>,
    inplace: bool,
    reset_index: bool,
    coerce_types: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _ = (inplace, debug);
    let ctx = PyCtx::new(py)?;
    let input = frame_from_py(&ctx, &df, ColumnMode::Full)?;
    let mut param_map = IndexMap::new();
    if let Some(p) = params.as_ref().filter(|p| !p.is_none()) {
        for (k, v) in p.downcast::<PyDict>()?.iter() {
            param_map.insert(k.str()?.extract::<String>()?, param_value(&ctx, &v)?);
        }
    }
    let bound = |b: &Option<Bound<'py, PyAny>>| -> PyResult<Option<Cell>> {
        match b.as_ref().filter(|b| !b.is_none()) {
            None => Ok(None),
            Some(b) => Ok(Some(py_to_cell(&ctx, b)?)),
        }
    };
    let begin_cell = bound(&begin)?;
    let end_cell = bound(&end)?;
    let has_query = !param_map.is_empty()
        || begin_cell.as_ref().is_some_and(cell_truthy)
        || end_cell.as_ref().is_some_and(cell_truthy);
    if (begin_cell.is_some() || end_cell.is_some())
        && datetime_column
            .as_ref()
            .is_none_or(|c| !input.dtypes.contains_key(c))
        && has_query
    {
        let warnings = py.import("meerschaum.utils.warnings")?;
        warnings.getattr("warn")?.call1((format!(
            "The datetime column '{}' is not present in the Dataframe, ignoring begin and end...",
            datetime_column.as_deref().unwrap_or("None")
        ),))?;
    }
    let opts = QueryOptions {
        params: param_map,
        begin: begin_cell,
        end: end_cell,
        datetime_column,
        select_columns: opt_str_list(select_columns.as_ref())?,
        omit_columns: opt_str_list(omit_columns.as_ref())?,
        reset_index,
        coerce_types,
    };
    let frame = &input.frame;
    let result = py
        .allow_threads(|| k_query_df(frame, &opts))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    if !has_query {
        // The early path: a copy with the columns selected / omitted, dtypes and index unchanged.
        let spec = OutSpec {
            index: match &input.index {
                Some(index) => IndexSpec::Keep(index.clone()),
                None => IndexSpec::Range,
            },
            hints: input.dtypes.clone(),
            arrow_native: false,
            arrow_columns: vec![],
            geometry: geometry_from_kind(&input.kind),
            labels: input.labels.clone(),
            reuse: unchanged_columns(&input, &result.frame),
            placeholders: vec![],
            polars_objects: input.polars_objects.clone(),
        };
        return frame_to_py(&ctx, &result.frame, &input.kind, &spec);
    }

    // v4 ends with `enforce_dtypes(result_df, df.dtypes)`: through Polars (Arrow-backed columns,
    // fresh index) when every dtype is Arrow-native, else the native subset through Polars.
    let enforce_opts = EnforceOptions {
        explicit_dtypes: IndexMap::new(),
        coerce_numeric: false,
        coerce_timezone: false,
        strip_timezone: false,
    };
    let native = !input.dtypes.is_empty()
        && !input.dtypes.values().any(|t| is_fallback_dtype(t))
        && enforce_dtypes_with_path(&input.frame.clear(), &input.dtypes, &enforce_opts)
            .map(|(_, n)| n)
            .unwrap_or(false);
    let arrow_columns = if native {
        vec![]
    } else {
        native_subset_columns(&result.frame, &input.dtypes, &enforce_opts)
    };
    let index = if native || reset_index {
        IndexSpec::Range
    } else {
        match &input.index {
            Some(index) => IndexSpec::Take(index.clone(), result.index.clone()),
            None => IndexSpec::Range,
        }
    };
    let spec = OutSpec {
        index,
        hints: input.dtypes.clone(),
        arrow_native: native,
        arrow_columns,
        geometry: geometry_from_kind(&input.kind),
        labels: input.labels.clone(),
        reuse: unchanged_columns(&input, &result.frame),
        placeholders: vec![],
        polars_objects: input.polars_objects.clone(),
    };
    frame_to_py(&ctx, &result.frame, &input.kind, &spec)
}

fn cell_truthy(v: &Cell) -> bool {
    match v {
        Cell::Null | Cell::NA | Cell::NaT => false,
        Cell::Bool(b) => *b,
        Cell::Int(i) => *i != 0,
        Cell::Float(f) => *f != 0.0,
        Cell::Str(s) => !s.is_empty(),
        _ => true,
    }
}

// ---------------------------------------------------------------------------------------------
// Detectors

fn probe<'py>(ctx: &PyCtx<'py>, df: &Bound<'py, PyAny>) -> PyResult<Option<PyFrame<'py>>> {
    if df.is_none() {
        return Ok(None);
    }
    Ok(Some(frame_from_py(ctx, df, ColumnMode::Probe)?))
}

macro_rules! simple_detector {
    ($name:ident, $kernel:path, $doc:literal) => {
        #[doc = $doc]
        #[pyfunction]
        pub fn $name<'py>(py: Python<'py>, df: Bound<'py, PyAny>) -> PyResult<Vec<String>> {
            let ctx = PyCtx::new(py)?;
            Ok(match probe(&ctx, &df)? {
                Some(input) => $kernel(&input.frame),
                None => vec![],
            })
        }
    };
}

simple_detector!(
    get_json_cols,
    detect::get_json_cols,
    "`get_json_cols(df)`: columns whose first valid value is a `dict` or `list`."
);
simple_detector!(
    get_numeric_cols,
    detect::get_numeric_cols,
    "`get_numeric_cols(df)`: columns whose first valid value is a `Decimal`."
);
simple_detector!(
    get_uuid_cols,
    detect::get_uuid_cols,
    "`get_uuid_cols(df)`: columns whose first valid value is a `UUID`."
);
simple_detector!(
    get_bytes_cols,
    detect::get_bytes_cols,
    "`get_bytes_cols(df)`: binary columns and columns whose first valid value is `bytes`."
);

/// `get_datetime_cols(df, timezone_aware=True, timezone_naive=True, with_tz_precision=False)`.
#[pyfunction]
#[pyo3(signature = (df, timezone_aware=true, timezone_naive=true, with_tz_precision=false))]
pub fn get_datetime_cols<'py>(
    py: Python<'py>,
    df: Bound<'py, PyAny>,
    timezone_aware: bool,
    timezone_naive: bool,
    with_tz_precision: bool,
) -> PyResult<Bound<'py, PyAny>> {
    if !timezone_aware && !timezone_naive {
        return Err(PyValueError::new_err(
            "`timezone_aware` and `timezone_naive` cannot both be `False`.",
        ));
    }
    let ctx = PyCtx::new(py)?;
    let Some(input) = probe(&ctx, &df)? else {
        return Ok(if with_tz_precision {
            PyDict::new(py).into_any()
        } else {
            PyList::empty(py).into_any()
        });
    };
    let cols = detect::get_datetime_cols_tz_precision(&input.frame, timezone_aware, timezone_naive)
        .map_err(PyValueError::new_err)?;
    if !with_tz_precision {
        return Ok(PyList::new(py, cols.keys())?.into_any());
    }
    let out = PyDict::new(py);
    for (col, (tz, precision)) in cols {
        // Known datetime columns: the precision comes from the pandas dtype string, which keeps
        // `s` (Polars has no second unit).
        let (tz, precision) = match input.dtypes.get(&col) {
            Some(dtype) if is_datetime_dtype(dtype) => {
                tz_precision_from_dtype(dtype).unwrap_or((tz, precision.name().to_string()))
            }
            _ => (tz, precision.name().to_string()),
        };
        out.set_item(col, (tz, precision))?;
    }
    Ok(out.into_any())
}

fn is_datetime_dtype(dtype: &str) -> bool {
    let d = dtype.strip_suffix("[pyarrow]").unwrap_or(dtype);
    d.starts_with("datetime64") || d.starts_with("timestamp")
}

/// `get_tz_precision_from_dtype` (`get_datetime_cols`).
fn tz_precision_from_dtype(dtype: &str) -> Option<(Option<String>, String)> {
    let d = dtype.strip_suffix("[pyarrow]").unwrap_or(dtype);
    let meta: String = d
        .split_once('[')
        .map_or(d, |(_, r)| r)
        .trim_end_matches(']')
        .replace(' ', "");
    let (tz, abbrev) = match meta.split_once(',') {
        Some((unit, tz)) => (
            Some(tz.strip_prefix("tz=").unwrap_or(tz).to_string()),
            unit.to_string(),
        ),
        None => (None, meta.clone()),
    };
    let unit = meerschaum_core::dtypes::precision_unit_alias(&abbrev)?;
    Some((tz, unit.to_string()))
}

/// `get_geometry_cols(df, with_types_srids=False)`.
#[pyfunction]
#[pyo3(signature = (df, with_types_srids=false))]
pub fn get_geometry_cols<'py>(
    py: Python<'py>,
    df: Bound<'py, PyAny>,
    with_types_srids: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let ctx = PyCtx::new(py)?;
    let Some(input) = probe(&ctx, &df)? else {
        return Ok(if with_types_srids {
            PyDict::new(py).into_any()
        } else {
            PyList::empty(py).into_any()
        });
    };
    let cols = detect::get_geometry_cols(&input.frame);
    if !with_types_srids {
        return Ok(PyList::new(py, cols)?.into_any());
    }
    // The types and SRIDs need every value of the geometry columns.
    let out = PyDict::new(py);
    if cols.is_empty() {
        return Ok(out.into_any());
    }
    let subset = df.get_item(PyList::new(py, &cols)?)?;
    let full = frame_from_py(&ctx, &subset, ColumnMode::Full)?;
    for (col, (geometry_type, srid)) in detect::get_geometry_cols_types_srids(&full.frame) {
        let srid_obj = match srid {
            meerschaum_kernel::dtypes::Srid::Code(c) => c.into_pyobject(py)?.into_any(),
            meerschaum_kernel::dtypes::Srid::Authority(a) => PyString::new(py, &a).into_any(),
        };
        out.set_item(col, (geometry_type, srid_obj))?;
    }
    Ok(out.into_any())
}

// ---------------------------------------------------------------------------------------------
// Dtype strings

/// `to_pandas_dtype(dtype)`.
#[pyfunction]
pub fn to_pandas_dtype(dtype: &Bound<'_, PyAny>) -> PyResult<String> {
    let text: String = dtype.str()?.extract()?;
    Ok(k_to_pandas_dtype(&text))
}

/// `are_dtypes_equal(ldtype, rdtype)` (strings, dtype objects, or two `{col: dtype}` dicts).
#[pyfunction]
pub fn are_dtypes_equal(ldtype: &Bound<'_, PyAny>, rdtype: &Bound<'_, PyAny>) -> PyResult<bool> {
    if let (Ok(l), Ok(r)) = (ldtype.downcast::<PyDict>(), rdtype.downcast::<PyDict>()) {
        let mut lkeys: Vec<(String, Bound<'_, PyAny>)> = l
            .iter()
            .map(|(k, v)| Ok((k.str()?.extract::<String>()?, v)))
            .collect::<PyResult<_>>()?;
        let mut rkeys: Vec<(String, Bound<'_, PyAny>)> = r
            .iter()
            .map(|(k, v)| Ok((k.str()?.extract::<String>()?, v)))
            .collect::<PyResult<_>>()?;
        lkeys.sort_by(|a, b| a.0.cmp(&b.0));
        rkeys.sort_by(|a, b| a.0.cmp(&b.0));
        for ((lk, lv), (rk, rv)) in lkeys.iter().zip(rkeys.iter()) {
            if lk != rk || !are_dtypes_equal(lv, rv)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    // PARITY: `ldtype == rdtype` is tried first (pandas / NumPy dtype objects compare to strings).
    if let Ok(eq) = ldtype.eq(rdtype) {
        if eq {
            return Ok(true);
        }
    }
    let l: String = ldtype.str()?.extract()?;
    let r: String = rdtype.str()?.extract()?;
    Ok(meerschaum_kernel::dtypes::are_dtypes_equal(&l, &r))
}

/// `is_dtype_numeric(dtype)`.
#[pyfunction]
pub fn is_dtype_numeric(dtype: &Bound<'_, PyAny>) -> PyResult<bool> {
    let text: String = dtype.str()?.extract()?;
    Ok(meerschaum_kernel::dtypes::is_dtype_numeric(&text))
}

/// `get_geometry_type_srid(dtype='geometry', default_type='geometry', default_srid=0)`.
#[pyfunction]
#[pyo3(signature = (dtype="geometry".to_string(), default_type="geometry".to_string(), default_srid=None))]
pub fn get_geometry_type_srid<'py>(
    py: Python<'py>,
    dtype: String,
    default_type: String,
    default_srid: Option<Bound<'py, PyAny>>,
) -> PyResult<(String, Bound<'py, PyAny>)> {
    use meerschaum_kernel::dtypes::Srid;
    let default_obj = match default_srid {
        Some(d) => d,
        None => 0i64.into_pyobject(py)?.into_any(),
    };
    let default = if let Ok(code) = default_obj.extract::<i64>() {
        Srid::Code(code)
    } else if let Ok(text) = default_obj.extract::<String>() {
        Srid::Authority(text)
    } else {
        Srid::Code(0)
    };
    let (geometry_type, srid) =
        meerschaum_kernel::dtypes::get_geometry_type_srid(&dtype, &default_type, default.clone())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let srid_obj = if srid == default {
        default_obj
    } else {
        match srid {
            Srid::Code(c) => c.into_pyobject(py)?.into_any(),
            Srid::Authority(a) => PyString::new(py, &a).into_any(),
        }
    };
    Ok((geometry_type, srid_obj))
}

/// `get_pd_type_from_db_type(db_type, allow_custom_dtypes=True)`.
#[pyfunction]
#[pyo3(signature = (db_type, allow_custom_dtypes=true))]
pub fn get_pd_type_from_db_type(db_type: &str, allow_custom_dtypes: bool) -> String {
    meerschaum_kernel::dtypes::get_pd_type_from_db_type(db_type, allow_custom_dtypes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tz_precision_from_pandas_dtypes() {
        assert_eq!(
            tz_precision_from_dtype("datetime64[ns, UTC]"),
            Some((Some("UTC".into()), "nanosecond".into()))
        );
        assert_eq!(
            tz_precision_from_dtype("timestamp[s, tz=UTC][pyarrow]"),
            Some((Some("UTC".into()), "second".into()))
        );
        assert_eq!(
            tz_precision_from_dtype("datetime64[us]"),
            Some((None, "microsecond".into()))
        );
    }

    #[test]
    fn fallback_dtypes() {
        assert!(is_fallback_dtype("uuid"));
        assert!(is_fallback_dtype("object"));
        assert!(is_fallback_dtype("geometry[Point, 4326]"));
        assert!(is_fallback_dtype("numeric"));
        assert!(!is_fallback_dtype("numeric[10,2]"));
        assert!(!is_fallback_dtype("int"));
        assert!(!is_fallback_dtype("datetime64[ns, UTC]"));
    }
}
