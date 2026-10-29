//! Python `polars.DataFrame` <-> Rust `polars::DataFrame` over the Arrow C Stream interface.
//!
//! Import calls the frame's `__arrow_c_stream__` and reads the capsule with Polars' own Arrow FFI
//! (`polars-arrow`), so every Polars type survives with its field metadata (`Enum`,
//! `Categorical`, extension types) and no Python library besides polars is needed (pyarrow is
//! not). Export goes back the same way: `polars-arrow` builds the C stream, a single-use
//! [`PolarsStream`] hands it out through `__arrow_c_stream__`, and `polars.DataFrame(...)` reads
//! it. Buffers are shared, not copied, in both directions.

use std::sync::Mutex;

use polars::prelude::*;
use polars_arrow::array::{Array, StructArray};
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};
use polars_arrow::ffi::{ArrowArrayStream, ArrowArrayStreamReader, export_iterator};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::ffi_export::STREAM_CAPSULE_NAME;

fn polars_err(err: PolarsError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

/// Move the `struct ArrowArrayStream` out of the producer's `arrow_array_stream` capsule.
fn take_stream(obj: &Bound<'_, PyAny>) -> PyResult<Box<ArrowArrayStream>> {
    let py = obj.py();
    if !obj.hasattr(intern!(py, "__arrow_c_stream__"))? {
        return Err(PyTypeError::new_err(format!(
            "Expected an object implementing __arrow_c_stream__ (a polars.DataFrame), got {}.",
            obj.get_type().name()?
        )));
    }
    let capsule = obj
        .call_method0(intern!(py, "__arrow_c_stream__"))?
        .cast_into::<PyCapsule>()?;
    let pointer = capsule
        .pointer_checked(Some(STREAM_CAPSULE_NAME))?
        .cast::<ArrowArrayStream>();
    let mut stream = Box::new(ArrowArrayStream::empty());
    // SAFETY: `pointer_checked` verified the name "arrow_array_stream", which the PyCapsule
    // Interface reserves for a valid `struct ArrowArrayStream` (polars-arrow's `ArrowArrayStream`
    // is that C struct). Swapping moves it into our box and leaves an empty (released, all
    // callbacks NULL) struct in the capsule, which is how a consumer takes ownership: the
    // capsule's destructor then sees `release == NULL` and frees nothing twice.
    unsafe { std::ptr::swap(pointer.as_ptr(), &mut *stream) };
    Ok(stream)
}

/// Read a Python frame's Arrow C Stream into a `polars::DataFrame`.
pub fn dataframe_from_python(obj: &Bound<'_, PyAny>) -> PyResult<DataFrame> {
    let stream = take_stream(obj)?;
    // SAFETY: `stream` is a valid, owned C stream (see `take_stream`); the reader owns it from here
    // and releases it when dropped.
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(stream) }.map_err(polars_err)?;
    let ArrowDataType::Struct(fields) = reader.field().dtype().clone() else {
        return Err(PyValueError::new_err(format!(
            "Expected a stream of record batches (struct arrays), got {:?}.",
            reader.field().dtype()
        )));
    };
    let mut chunks: Vec<Vec<Box<dyn Array>>> = vec![Vec::new(); fields.len()];
    // SAFETY: `next` reads the stream the reader owns; every returned array was produced by it and
    // owns its buffers through the C release callback.
    while let Some(array) = unsafe { reader.next() } {
        let array = array.map_err(polars_err)?;
        let batch = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| PyValueError::new_err("A stream chunk is not a struct array."))?;
        for (column, values) in chunks.iter_mut().zip(batch.values()) {
            column.push(values.clone());
        }
    }
    let columns = fields
        .iter()
        .zip(chunks)
        .map(|(field, chunks)| {
            let series = if chunks.is_empty() {
                Series::new_empty(field.name.clone(), &DataType::from_arrow_field(field))
            } else {
                Series::try_from((field, chunks)).map_err(polars_err)?
            };
            Ok(series.into_column())
        })
        .collect::<PyResult<Vec<_>>>()?;
    DataFrame::new_infer_height(columns).map_err(polars_err)
}

/// A single-use Arrow C Stream over a Rust `polars::DataFrame`, readable by any consumer of the
/// PyCapsule Interface (`polars.DataFrame(stream)`, `pyarrow.table(stream)`, ...).
#[pyclass(module = "meerschaum._core", name = "PolarsStream", frozen)]
pub struct PolarsStream(Mutex<Option<ArrowArrayStream>>);

impl PolarsStream {
    /// Export `df` (rechunked to aligned chunks) as Polars' newest Arrow layout, so every Polars
    /// type round-trips.
    pub fn new(mut df: DataFrame) -> Self {
        df.rechunk_mut();
        let fields: Vec<ArrowField> = df
            .schema()
            .to_arrow(CompatLevel::newest())
            .iter_values()
            .cloned()
            .collect();
        let dtype = ArrowDataType::Struct(fields);
        let chunk_dtype = dtype.clone();
        let chunks: Vec<_> = df.iter_chunks(CompatLevel::newest(), false).collect();
        let arrays = chunks.into_iter().map(move |chunk| {
            let (_, arrays) = chunk.into_schema_and_arrays();
            let length = arrays.first().map_or(0, |a| a.len());
            Ok(StructArray::new(chunk_dtype.clone(), length, arrays, None).boxed())
        });
        let field = ArrowField::new("".into(), dtype, false);
        Self(Mutex::new(Some(export_iterator(Box::new(arrays), field))))
    }
}

#[pymethods]
impl PolarsStream {
    /// Hand the stream to the consumer (once). `requested_schema` is ignored: the data is already
    /// in Polars' own layout, which is what the consumer (polars) asks for.
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let stream = self
            .0
            .lock()
            .map_err(|_| PyValueError::new_err("The stream lock is poisoned."))?
            .take()
            .ok_or_else(|| PyValueError::new_err("The stream was already consumed."))?;
        // The capsule owns the stream; a consumer moves it out and leaves `release == NULL`, and
        // dropping the value (`Drop for ArrowArrayStream`) releases it only if nobody did.
        PyCapsule::new_with_value(py, stream, STREAM_CAPSULE_NAME)
    }
}

/// `_core.polars_roundtrip(df: polars.DataFrame) -> polars.DataFrame`.
///
/// Imports the frame into a Rust `polars::DataFrame` (validating it on the way) and returns a new
/// polars frame sharing the same buffers.
#[pyfunction]
pub fn polars_roundtrip<'py>(
    py: Python<'py>,
    df: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let df = dataframe_from_python(df)?;
    // Touch the frame on the Rust side so the round trip proves a usable `DataFrame`, not only a
    // pointer hand-off.
    let height = df.height();
    if df.columns().iter().any(|c| c.len() != height) {
        return Err(PyValueError::new_err(
            "Columns of the extracted DataFrame have mismatched lengths.",
        ));
    }
    let stream = Bound::new(py, PolarsStream::new(df))?;
    py.import(intern!(py, "polars"))?
        .getattr(intern!(py, "DataFrame"))?
        .call1((stream,))
}
