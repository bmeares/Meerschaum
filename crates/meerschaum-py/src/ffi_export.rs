//! Export to Python through the Arrow PyCapsule Interface
//! (<https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html>).
//!
//! The producer side of [`crate::arrow_bridge::ArrowStream`]: arrow-rs builds the C structs
//! (`FFI_ArrowSchema`, `FFI_ArrowArrayStream`) and PyO3 wraps each one in a capsule named as the
//! interface prescribes (`arrow_schema`, `arrow_array_stream`).
//!
//! Ownership follows the interface: the capsule owns its struct. A consumer that imports the data
//! moves the struct out and marks the original as released (`release = NULL`); when the capsule is
//! garbage collected, its destructor drops the Rust value, and arrow-rs' `Drop` calls `release` only
//! if it is still set. So a capsule that was never consumed frees its data, and a consumed one does
//! not free it twice.
//!
//! `requested_schema` (the optional argument of `__arrow_c_stream__`) is honored like `pyo3-arrow`
//! and pyarrow do: when Arrow can cast the stream's struct type to the requested one, every batch
//! is cast to it; otherwise the data is exported unchanged and the consumer decides.

use std::ffi::CStr;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow::compute::{can_cast_types, cast};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Capsule name of a `struct ArrowSchema`.
pub const SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
/// Capsule name of a `struct ArrowArray`.
pub const ARRAY_CAPSULE_NAME: &CStr = c"arrow_array";
/// Capsule name of a `struct ArrowArrayStream`.
pub const STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

fn arrow_err(err: ArrowError) -> PyErr {
    PyValueError::new_err(err.to_string())
}

/// The schema `requested_schema` asks for, with the stream's own schema-level metadata (the
/// request carries types, not the producer's metadata such as pandas' block), or `None` when
/// Arrow cannot cast the stream to it.
pub fn cast_target(schema: &Schema, requested: &Schema) -> Option<SchemaRef> {
    let from = DataType::Struct(schema.fields().clone());
    let to = DataType::Struct(requested.fields().clone());
    can_cast_types(&from, &to).then(|| {
        Arc::new(Schema::new_with_metadata(
            requested.fields().clone(),
            schema.metadata().clone(),
        ))
    })
}

/// Cast one batch to `target` (a schema [`cast_target`] accepted) as a struct array, which is how
/// the stream carries it.
fn cast_batch(batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let rows = batch.num_rows();
    let as_struct = StructArray::from(batch);
    let cast = cast(&as_struct, &DataType::Struct(target.fields().clone()))?;
    let cast = cast
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| ArrowError::CastError("the cast did not return a struct array".into()))?;
    let (_, columns, _) = cast.clone().into_parts();
    RecordBatch::try_new_with_options(
        target.clone(),
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
}

/// A reader over `batches`, cast to `requested` when Arrow can cast them (see [`cast_target`]).
/// Cast errors surface when the consumer pulls the batch (through `get_last_error`).
pub fn batch_reader(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    requested: Option<&Schema>,
) -> Box<dyn RecordBatchReader + Send> {
    match requested.and_then(|requested| cast_target(&schema, requested)) {
        Some(target) => {
            let iter_target = target.clone();
            let batches = batches
                .into_iter()
                .map(move |batch| cast_batch(batch, &iter_target));
            Box::new(RecordBatchIterator::new(batches, target))
        }
        None => Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        )),
    }
}

/// Read the `requested_schema` capsule a consumer passed to `__arrow_c_stream__`. The capsule
/// stays owned by the consumer; only its schema is copied.
pub fn requested_schema(capsule: &Bound<'_, PyCapsule>) -> PyResult<Schema> {
    let pointer = capsule.pointer_checked(Some(SCHEMA_CAPSULE_NAME))?;
    // SAFETY: `pointer_checked` verified the capsule is named "arrow_schema", which the PyCapsule
    // Interface reserves for a pointer to a valid `struct ArrowSchema`. It is only borrowed, while
    // `capsule` (kept alive by the caller) owns it, and no Python code runs during the borrow.
    let ffi_schema = unsafe { pointer.cast::<FFI_ArrowSchema>().as_ref() };
    Schema::try_from(ffi_schema).map_err(arrow_err)
}

/// An `arrow_schema` capsule holding `schema` (schema-level metadata included).
pub fn schema_capsule<'py>(py: Python<'py>, schema: &Schema) -> PyResult<Bound<'py, PyCapsule>> {
    let ffi_schema = FFI_ArrowSchema::try_from(schema).map_err(arrow_err)?;
    PyCapsule::new_with_value(py, ffi_schema, SCHEMA_CAPSULE_NAME)
}

/// An `arrow_array_stream` capsule over `reader`.
pub fn stream_capsule<'py>(
    py: Python<'py>,
    reader: Box<dyn RecordBatchReader + Send>,
) -> PyResult<Bound<'py, PyCapsule>> {
    PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(reader), STREAM_CAPSULE_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::Field;
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use std::collections::HashMap;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ],
            HashMap::from([("pandas".to_string(), "{}".to_string())]),
        ))
    }

    fn batch(values: &[i32]) -> RecordBatch {
        let strings: Vec<String> = values.iter().map(i32::to_string).collect();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(values.to_vec())),
                Arc::new(StringArray::from(strings)),
            ],
        )
        .unwrap()
    }

    /// Export through the C struct and import back, as a consumer would.
    fn through_ffi(
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> (SchemaRef, Vec<Result<RecordBatch, ArrowError>>) {
        let stream = FFI_ArrowArrayStream::new(reader);
        let reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        let schema = reader.schema();
        (schema, reader.collect())
    }

    #[test]
    fn empty_stream_keeps_schema_and_metadata() {
        let (schema, batches) = through_ffi(batch_reader(self::schema(), vec![], None));
        assert!(batches.is_empty());
        assert_eq!(schema.fields(), self::schema().fields());
        assert_eq!(schema.metadata(), self::schema().metadata());
    }

    #[test]
    fn multi_batch_stream_keeps_batches() {
        let (_, batches) = through_ffi(batch_reader(
            schema(),
            vec![batch(&[1, 2]), batch(&[]), batch(&[3])],
            None,
        ));
        let batches: Vec<RecordBatch> = batches.into_iter().map(Result::unwrap).collect();
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [2, 0, 1]
        );
        assert_eq!(batches[2], batch(&[3]));
    }

    #[test]
    fn requested_schema_casts_and_keeps_stream_metadata() {
        let requested = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::LargeUtf8, true),
        ]);
        let (schema, batches) = through_ffi(batch_reader(
            schema(),
            vec![batch(&[1, 2]), batch(&[3])],
            Some(&requested),
        ));
        assert_eq!(schema.fields(), requested.fields());
        assert_eq!(schema.metadata(), self::schema().metadata());
        let batches: Vec<RecordBatch> = batches.into_iter().map(Result::unwrap).collect();
        let a = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.values(), &[1, 2]);
    }

    #[test]
    fn uncastable_requested_schema_exports_unchanged() {
        let requested = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let (schema, batches) =
            through_ffi(batch_reader(schema(), vec![batch(&[1])], Some(&requested)));
        assert_eq!(schema.fields(), self::schema().fields());
        assert_eq!(batches.into_iter().next().unwrap().unwrap(), batch(&[1]));
    }

    #[test]
    fn reader_errors_reach_the_consumer() {
        let failing = RecordBatchIterator::new(
            vec![
                Ok(batch(&[1])),
                Err(ArrowError::ComputeError("boom".into())),
            ],
            schema(),
        );
        let (_, batches) = through_ffi(Box::new(failing));
        assert!(batches[0].is_ok());
        let err = batches[1].as_ref().unwrap_err().to_string();
        assert!(err.contains("boom"), "{err}");
    }

    #[test]
    fn capsules_carry_interface_names_and_reject_wrong_ones() {
        Python::initialize();
        Python::attach(|py| {
            let schema = schema();
            let capsule = schema_capsule(py, &schema).unwrap();
            assert_eq!(requested_schema(&capsule).unwrap(), *schema);

            let stream = stream_capsule(py, batch_reader(schema.clone(), vec![], None)).unwrap();
            assert!(stream.pointer_checked(Some(STREAM_CAPSULE_NAME)).is_ok());
            // A stream capsule is not a schema request.
            assert!(requested_schema(&stream).is_err());
        });
    }
}
