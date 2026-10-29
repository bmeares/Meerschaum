//! The DataFrame library setting at the boundary (rc15, Q54).
//!
//! - `frame_from_arrow(data, as_polars=False, dtypes=None)`: the one conversion from Arrow data
//!   (any PyCapsule producer, or a list of record batches) to a pandas or Polars frame, through the
//!   kernel [`Frame`] and the same output rules as every read of the façade
//!   ([`crate::pandas_bridge::frame_to_pandas`] with dtype hints, or
//!   [`crate::pandas_bridge::frame_to_polars`]). The Python helper
//!   `meerschaum.utils.dataframe.frame_from_arrow` resolves the library and calls it.
//! - `pin_dataframe_library(config_dir)`: give a root created before rc15 `dataframe: pandas`
//!   (`meerschaum_core::config::dataframe`). The Python config boundary calls it once per config
//!   directory before it loads any key; it is exposed for tests.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use indexmap::IndexMap;
use meerschaum_kernel::Frame;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::arrow_bridge::import_from_python;
use crate::convert::PyCtx;
use crate::pandas_bridge::{IndexSpec, OutSpec, frame_err, frame_to_pandas, frame_to_polars};

/// A `{col: dtype}` mapping with `str()` of each key and value.
fn str_map(obj: Option<&Bound<'_, PyAny>>) -> PyResult<IndexMap<String, String>> {
    let mut out = IndexMap::new();
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(out);
    };
    let dict = obj.cast::<PyDict>()?;
    for (k, v) in dict.iter() {
        out.insert(k.str()?.extract()?, v.str()?.extract()?);
    }
    Ok(out)
}

/// Import an Arrow producer, or a list / tuple of producers sharing one schema, into a kernel
/// frame. A stream without batches keeps its columns (an empty batch with the schema).
fn import_frame(data: &Bound<'_, PyAny>) -> PyResult<Frame> {
    let items: Vec<Bound<'_, PyAny>> = if let Ok(list) = data.cast::<PyList>() {
        list.iter().collect()
    } else if let Ok(tuple) = data.cast::<PyTuple>() {
        tuple.iter().collect()
    } else {
        vec![data.clone()]
    };
    let mut schema = None;
    let mut batches = Vec::new();
    for item in &items {
        let (item_schema, item_batches) = import_from_python(item)?;
        schema.get_or_insert(item_schema);
        batches.extend(item_batches);
    }
    if batches.is_empty() {
        let Some(schema) = schema else {
            return Ok(Frame::empty());
        };
        batches.push(arrow::array::RecordBatch::new_empty(schema));
    }
    Frame::from_record_batches(&batches).map_err(frame_err)
}

/// The active geometry column of a pandas result: the first column declared `geometry` /
/// `geography`, else the first column holding geometries.
fn active_geometry(frame: &Frame, dtypes: &IndexMap<String, String>) -> Option<String> {
    let found = meerschaum_kernel::detect::get_geometry_cols(frame);
    if found.is_empty() {
        return None;
    }
    dtypes
        .iter()
        .find(|(col, typ)| {
            let norm = meerschaum_kernel::dtypes::resolve_alias(typ).to_lowercase();
            (norm.starts_with("geometry") || norm.starts_with("geography")) && found.contains(col)
        })
        .map(|(col, _)| col.clone())
        .or_else(|| found.first().cloned())
}

/// `frame_from_arrow(data, as_polars=False, dtypes=None, debug=False)`.
///
/// `dtypes` (Meerschaum or pandas dtype strings): for pandas, the hints that restore the exact
/// pandas dtype within each column's logical class ([`crate::pandas_bridge`]); for Polars, the
/// declared `json` / `geometry` columns that become extension columns.
#[pyfunction]
#[pyo3(signature = (data, as_polars=false, dtypes=None, debug=false))]
pub fn frame_from_arrow<'py>(
    py: Python<'py>,
    data: Bound<'py, PyAny>,
    as_polars: bool,
    dtypes: Option<Bound<'py, PyAny>>,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = crate::sync_api::DebugGuard::new(debug);
    if data.is_none() {
        return Ok(data);
    }
    let frame = import_frame(&data)?;
    let dtypes = str_map(dtypes.as_ref())?;
    if as_polars {
        return frame_to_polars(py, &frame, &dtypes);
    }
    let ctx = PyCtx::new(py)?;
    let mut hints: IndexMap<String, String> = dtypes
        .iter()
        .map(|(col, typ)| (col.clone(), meerschaum_kernel::dtypes::to_pandas_dtype(typ)))
        .collect();
    let geometry = active_geometry(&frame, &dtypes);
    for col in meerschaum_kernel::detect::get_geometry_cols(&frame) {
        hints.insert(col, "geometry".into());
    }
    let spec = OutSpec {
        index: IndexSpec::Range,
        hints,
        arrow_native: false,
        arrow_columns: Vec::new(),
        geometry,
        labels: IndexMap::new(),
        reuse: IndexMap::new(),
        placeholders: Vec::new(),
        polars_objects: Vec::new(),
    };
    frame_to_pandas(&ctx, &frame, &spec)
}

/// The config directories this process already checked (`pin_dataframe_library`).
static CHECKED: Mutex<Option<HashSet<PathBuf>>> = Mutex::new(None);

/// Run the root pin of `config_dir` once per process ([`pin_dataframe_library`]): `None` when it
/// already ran (or found nothing to do), else the outcome to report.
pub(crate) fn pin_once(
    config_dir: &Path,
) -> Option<meerschaum_core::config::dataframe::PinOutcome> {
    {
        let mut checked = CHECKED.lock().unwrap_or_else(|e| e.into_inner());
        let set = checked.get_or_insert_with(HashSet::new);
        if set.contains(config_dir) {
            return None;
        }
        set.insert(config_dir.to_path_buf());
    }
    Some(meerschaum_core::config::dataframe::pin_old_root(config_dir))
}

/// The Python config boundary's root pin ([`pin_once`]): shows the notice on `sys.stderr`, keeps
/// an already loaded `meerschaum` key of the live dictionary in step with the file, and makes a
/// root whose pin could not be written use pandas in this process (`MRSM_DATAFRAME`, which the
/// jobs and workers it starts inherit).
pub(crate) fn pin_root_for_python(py: Python<'_>, config_dir: &Path, live: &Bound<'_, PyDict>) {
    use meerschaum_core::config::dataframe::{DATAFRAME_KEY, PINNED_LIBRARY, PinOutcome};
    let Some(outcome) = pin_once(config_dir) else {
        return;
    };
    let notice = match outcome {
        PinOutcome::Pinned { notice } => notice,
        PinOutcome::Failed { notice } => {
            let _ = py.import("os").and_then(|os| {
                os.getattr("environ")?
                    .call_method1("setdefault", ("MRSM_DATAFRAME", PINNED_LIBRARY))
            });
            notice
        }
        _ => return,
    };
    if let Ok(Some(loaded)) = live.get_item("meerschaum")
        && let Ok(loaded) = loaded.cast::<PyDict>()
        && !loaded.contains(DATAFRAME_KEY).unwrap_or(true)
    {
        let _ = loaded.set_item(DATAFRAME_KEY, PINNED_LIBRARY);
    }
    emit_notice(py, &notice);
}

/// Show a pin notice on Python's `sys.stderr` (so notebooks and captured streams see it).
pub(crate) fn emit_notice(py: Python<'_>, notice: &str) {
    let shown = py.import("sys").and_then(|sys| {
        let stderr = sys.getattr("stderr")?;
        stderr.call_method1("write", (format!("{notice}\n"),))?;
        let _ = stderr.call_method0("flush");
        Ok(())
    });
    if shown.is_err() {
        eprintln!("{notice}");
    }
}

/// `pin_dataframe_library(config_dir) -> Optional[str]`: give a root created before rc15
/// `dataframe: pandas` in its `meerschaum.yaml` (see `meerschaum_core::config::dataframe`). Returns
/// the notice when this call pinned the root (or could not write the pin), else `None`. Not
/// memoized: every call checks the files.
#[pyfunction]
pub fn pin_dataframe_library(config_dir: PathBuf) -> Option<String> {
    match meerschaum_core::config::dataframe::pin_old_root(&config_dir) {
        meerschaum_core::config::dataframe::PinOutcome::Pinned { notice }
        | meerschaum_core::config::dataframe::PinOutcome::Failed { notice } => Some(notice),
        _ => None,
    }
}

/// Register the functions of this module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(frame_from_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(pin_dataframe_library, m)?)?;
    Ok(())
}
