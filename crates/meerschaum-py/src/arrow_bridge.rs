//! The Arrow PyCapsule boundary (§4.2): data crosses between Python and Rust as Arrow C Streams.
//!
//! Import calls the producer's `__arrow_c_stream__` (or `__arrow_c_array__`) and reads the capsule
//! with [`crate::ffi_import`] (arrow-rs FFI plus a repair for polars' `Null` arrays). Buffers are
//! not copied: the imported arrays keep the producer's allocations alive through the C Data
//! Interface release callbacks. Export goes the other way through [`ArrowStream`], a minimal
//! pyclass implementing the PyCapsule protocol on top of [`crate::ffi_export`], so no
//! Python library (pyarrow, polars, arro3) is needed to produce or hold the result. Any consumer of
//! the protocol can read it: `pyarrow.table(s)`, `polars.DataFrame(s)`, `duckdb`, ...

use std::ffi::{CStr, c_void};

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::ffi_export;
use crate::ffi_import::{StreamImporter, import_record_batch};

/// Drain a [`RecordBatchReader`] into its schema and batches.
pub fn collect_reader<R>(reader: R) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowError>
where
    R: RecordBatchReader,
{
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

/// An in-memory Arrow table owned by Rust and exported to Python through the Arrow PyCapsule
/// interface (`__arrow_c_stream__` / `__arrow_c_schema__`).
///
/// The stream may be consumed any number of times; each export shares the same buffers.
#[pyclass(module = "meerschaum._core", name = "ArrowStream", frozen)]
#[derive(Debug, Clone)]
pub struct ArrowStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl ArrowStream {
    /// Build a stream from a schema and its batches.
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    /// The Arrow schema of the stream.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The record batches of the stream.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// The total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

#[pymethods]
impl ArrowStream {
    /// Total number of rows.
    #[getter]
    fn num_rows(&self) -> usize {
        self.row_count()
    }

    /// Number of columns.
    #[getter]
    fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// Number of record batches (chunks).
    #[getter]
    fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Column names, in order.
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn __len__(&self) -> usize {
        self.row_count()
    }

    fn __repr__(&self) -> String {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "ArrowStream(rows={}, batches={}, columns=[{}])",
            self.row_count(),
            self.batches.len(),
            columns
        )
    }

    /// Export the schema as an `arrow_schema` PyCapsule.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        ffi_export::schema_capsule(py, self.schema.as_ref())
    }

    /// Export the data as an `arrow_array_stream` PyCapsule. When the consumer passes a
    /// `requested_schema` capsule, columns are cast to it where Arrow can cast them.
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let requested = requested_schema
            .as_ref()
            .map(ffi_export::requested_schema)
            .transpose()?;
        let reader = ffi_export::batch_reader(
            self.schema.clone(),
            self.batches.clone(),
            requested.as_ref(),
        );
        ffi_export::stream_capsule(py, reader)
    }
}

fn arrow_err(err: ArrowError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

/// The pointer an Arrow PyCapsule Interface capsule carries, after checking that the capsule is
/// named `expected` (an error names the capsule's actual name otherwise).
fn capsule_pointer(capsule: &Bound<'_, PyCapsule>, expected: &CStr) -> PyResult<*mut c_void> {
    // SAFETY: the name is only compared and formatted here, before any Python code can run and
    // rename the capsule.
    match capsule.name()?.map(|name| unsafe { name.as_cstr() }) {
        Some(name) if name == expected => {}
        name => {
            return Err(PyValueError::new_err(format!(
                "Expected a PyCapsule named {expected:?}, got {name:?}."
            )));
        }
    }
    Ok(capsule.pointer_checked(Some(expected))?.as_ptr())
}

/// Import any Python object implementing the Arrow PyCapsule Interface into Rust batches.
///
/// `__arrow_c_stream__` is preferred (tables, readers, frames); `__arrow_c_array__` is accepted
/// for single record batches.
pub fn import_from_python(obj: &Bound<'_, PyAny>) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    let py = obj.py();
    if obj.hasattr(intern!(py, "__arrow_c_stream__"))? {
        let capsule = obj
            .call_method0(intern!(py, "__arrow_c_stream__"))?
            .cast_into::<PyCapsule>()?;
        let pointer = capsule_pointer(&capsule, ffi_export::STREAM_CAPSULE_NAME)?;
        // SAFETY: a capsule named "arrow_array_stream" holds a `struct ArrowArrayStream` (Arrow
        // PyCapsule Interface). `from_raw` moves it out and leaves a released stream behind, which
        // the capsule's destructor then skips, as the interface prescribes for consumers.
        let stream = unsafe { FFI_ArrowArrayStream::from_raw(pointer.cast()) };
        let reader = StreamImporter::try_new(stream).map_err(arrow_err)?;
        return collect_reader(reader).map_err(arrow_err);
    }
    if obj.hasattr(intern!(py, "__arrow_c_array__"))? {
        let (schema_capsule, array_capsule): (Bound<'_, PyCapsule>, Bound<'_, PyCapsule>) = obj
            .call_method0(intern!(py, "__arrow_c_array__"))?
            .extract()?;
        let schema_pointer = capsule_pointer(&schema_capsule, ffi_export::SCHEMA_CAPSULE_NAME)?;
        let array_pointer = capsule_pointer(&array_capsule, ffi_export::ARRAY_CAPSULE_NAME)?;
        // SAFETY: a capsule named "arrow_schema" holds a valid `struct ArrowSchema`, owned and
        // released by the capsule, which `schema_capsule` keeps alive for this borrow.
        let ffi_schema = unsafe { &*schema_pointer.cast::<FFI_ArrowSchema>() };
        // SAFETY: a capsule named "arrow_array" holds a `struct ArrowArray`; `from_raw` moves it
        // out and leaves a released array for the capsule's destructor to skip.
        let array = unsafe { FFI_ArrowArray::from_raw(array_pointer.cast()) };
        let (schema, batch) = import_record_batch(array, ffi_schema).map_err(arrow_err)?;
        return Ok((schema, vec![batch]));
    }
    Err(PyTypeError::new_err(format!(
        "Expected an object implementing the Arrow PyCapsule Interface (__arrow_c_stream__ or \
         __arrow_c_array__), got {}.",
        obj.get_type().name()?
    )))
}

/// `_core.roundtrip_arrow(obj) -> ArrowStream`.
///
/// Imports `obj` (anything implementing `__arrow_c_stream__` or `__arrow_c_array__`: a pyarrow
/// `Table`, `RecordBatch` or `RecordBatchReader`, a polars `DataFrame`, a pandas >= 2.2
/// `DataFrame`, ...) into Rust `RecordBatch`es and hands them back as an [`ArrowStream`]. Proves the
/// zero-copy data path of §4.2 before any kernel function is bound.
#[pyfunction]
pub fn roundtrip_arrow(obj: &Bound<'_, PyAny>) -> PyResult<ArrowStream> {
    let (schema, batches) = import_from_python(obj)?;
    Ok(ArrowStream::new(schema, batches))
}
