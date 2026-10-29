//! Importing Arrow data from Python PyCapsules (`__arrow_c_stream__` / `__arrow_c_array__`).
//!
//! This is `arrow::ffi_stream::ArrowArrayStreamReader` plus one repair. Polars (Python 1.36,
//! Rust polars-arrow 0.51) exports `Null`-typed arrays (`format = "n"`, what an all-`None` column
//! becomes) with `n_buffers = 1`, but the C Data Interface specifies **zero** buffers for that
//! layout, and arrow-rs (56 through at least 60) rejects the array ("The datatype Null doesn't expect
//! buffer at index 0"). All-`None` columns are routine in Meerschaum frames, so before handing an
//! imported array to `arrow::ffi::from_ffi`, [`repair_null_buffers`] sets `n_buffers = 0` on every
//! `Null`-typed node. Producers that follow the spec (pyarrow, arrow-rs) are untouched.
//!
//! The repair is sound for any producer whose `release` callback frees buffers through its own
//! `private_data` rather than by re-reading `n_buffers` (the spec's recommended pattern, and what
//! polars-arrow does: `c_release_array` drops a boxed slice of buffer pointers).
//!
//! A side benefit over `ArrowArrayStreamReader`: every imported batch carries the stream's full
//! schema, including schema-level metadata (e.g. pandas' `b"pandas"` block).

use std::ffi::{CStr, c_char, c_void};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, RecordBatchOptions, RecordBatchReader, StructArray};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow::ffi_stream::FFI_ArrowArrayStream;

/// The `struct ArrowArray` layout from the Arrow C Data Interface specification.
///
/// `arrow::ffi::FFI_ArrowArray` has the same `#[repr(C)]` layout but private fields; this mirror
/// exists only so [`repair_null_buffers`] can write `n_buffers`.
#[repr(C)]
struct RawArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *mut *const c_void,
    children: *mut *mut RawArrowArray,
    dictionary: *mut RawArrowArray,
    release: Option<unsafe extern "C" fn(*mut RawArrowArray)>,
    private_data: *mut c_void,
}

const _: () = assert!(size_of::<RawArrowArray>() == size_of::<FFI_ArrowArray>());
const _: () = assert!(align_of::<RawArrowArray>() == align_of::<FFI_ArrowArray>());

/// The `struct ArrowArrayStream` layout from the Arrow C Stream Interface specification.
///
/// `arrow::ffi_stream::FFI_ArrowArrayStream` has the same `#[repr(C)]` layout, but since arrow 60
/// its callbacks are private; this mirror lets [`StreamImporter`] call them directly (it imports
/// each array itself so it can apply [`repair_null_buffers`] first).
#[repr(C)]
struct RawArrowArrayStream {
    get_schema: Option<unsafe extern "C" fn(*mut RawArrowArrayStream, *mut FFI_ArrowSchema) -> i32>,
    get_next: Option<unsafe extern "C" fn(*mut RawArrowArrayStream, *mut FFI_ArrowArray) -> i32>,
    get_last_error: Option<unsafe extern "C" fn(*mut RawArrowArrayStream) -> *const c_char>,
    release: Option<unsafe extern "C" fn(*mut RawArrowArrayStream)>,
    private_data: *mut c_void,
}

const _: () = assert!(size_of::<RawArrowArrayStream>() == size_of::<FFI_ArrowArrayStream>());
const _: () = assert!(align_of::<RawArrowArrayStream>() == align_of::<FFI_ArrowArrayStream>());

/// Set `n_buffers = 0` on every `Null`-typed node of `array` (recursing through children and
/// dictionaries alongside `schema`). Returns the number of nodes repaired.
///
/// # Safety
///
/// `array` must point to a valid, unreleased Arrow C Data Interface array produced for `schema`,
/// that no one else reads or writes for the duration of the call.
unsafe fn repair_null_buffers_raw(array: *mut RawArrowArray, schema: &FFI_ArrowSchema) -> usize {
    if array.is_null() {
        return 0;
    }
    // SAFETY: the caller guarantees `array` is valid and exclusively ours for this call.
    let node = unsafe { &mut *array };
    let mut repaired = 0;
    if schema.format() == "n" && node.n_buffers != 0 {
        node.n_buffers = 0;
        repaired += 1;
    }
    let n_children = usize::try_from(node.n_children).unwrap_or(0);
    if !node.children.is_null() {
        for (index, child_schema) in schema.children().enumerate().take(n_children) {
            // SAFETY: `children` holds `n_children` valid pointers per the C Data Interface, and
            // `index < n_children`; each child is valid and exclusively ours like its parent.
            repaired += unsafe {
                let child = *node.children.add(index);
                repair_null_buffers_raw(child, child_schema)
            };
        }
    }
    if let Some(dictionary_schema) = schema.dictionary() {
        // SAFETY: a dictionary-encoded array's `dictionary` is null or a valid array for the
        // schema's dictionary, owned by this array.
        repaired += unsafe { repair_null_buffers_raw(node.dictionary, dictionary_schema) };
    }
    repaired
}

/// Repair spec-violating `Null` arrays in an imported [`FFI_ArrowArray`] (see the module docs).
/// Returns the number of nodes repaired.
pub fn repair_null_buffers(array: &mut FFI_ArrowArray, schema: &FFI_ArrowSchema) -> usize {
    if array.is_released() {
        return 0;
    }
    let raw = (array as *mut FFI_ArrowArray).cast::<RawArrowArray>();
    // SAFETY: `RawArrowArray` has `FFI_ArrowArray`'s layout (asserted above); `array` is unreleased
    // and exclusively borrowed, and an `FFI_ArrowArray` only exists for data its producer built
    // for the schema it was exported with.
    unsafe { repair_null_buffers_raw(raw, schema) }
}

/// Convert one imported struct-typed array into a [`RecordBatch`] carrying `schema`.
fn import_batch(
    mut array: FFI_ArrowArray,
    ffi_schema: &FFI_ArrowSchema,
    schema: &SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    repair_null_buffers(&mut array, ffi_schema);
    // SAFETY: `array` is a valid, unreleased array produced for `ffi_schema` (the C Stream / Data
    // Interface contract); `from_ffi` takes ownership and calls `release` when the data drops.
    let data = unsafe { from_ffi(array, ffi_schema) }?;
    if !matches!(data.data_type(), DataType::Struct(_)) {
        return Err(ArrowError::CDataInterface(format!(
            "Expected a struct array (a record batch), got {}.",
            data.data_type()
        )));
    }
    let struct_array = StructArray::from(data);
    if struct_array.null_count() > 0 {
        return Err(ArrowError::CDataInterface(
            "A record batch cannot have top-level nulls.".to_string(),
        ));
    }
    let num_rows = struct_array.len();
    let (_, columns, _) = struct_array.into_parts();
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
}

/// A [`RecordBatchReader`] over an imported Arrow C Stream (`arrow_array_stream` capsule).
pub struct StreamImporter {
    /// Owns the stream: its `Drop` calls the producer's `release`.
    stream: FFI_ArrowArrayStream,
    ffi_schema: FFI_ArrowSchema,
    schema: SchemaRef,
}

/// The C view of `stream` (see [`RawArrowArrayStream`]).
fn raw_stream(stream: &mut FFI_ArrowArrayStream) -> *mut RawArrowArrayStream {
    (stream as *mut FFI_ArrowArrayStream).cast::<RawArrowArrayStream>()
}

impl StreamImporter {
    /// Take ownership of `stream` and read its schema.
    pub fn try_new(mut stream: FFI_ArrowArrayStream) -> Result<Self, ArrowError> {
        if stream.release().is_none() {
            return Err(ArrowError::CDataInterface(
                "The Arrow C stream has already been released.".to_string(),
            ));
        }
        let raw = raw_stream(&mut stream);
        // SAFETY: `raw` views `stream` (same layout, asserted above), which we own and is unreleased.
        let get_schema = unsafe { (*raw).get_schema }.ok_or_else(|| {
            ArrowError::CDataInterface("The Arrow C stream has no get_schema.".to_string())
        })?;
        let mut ffi_schema = FFI_ArrowSchema::empty();
        // SAFETY: the stream is unreleased and `get_schema` is its producer's callback; the C
        // Stream Interface lets the consumer call it with a pointer to an empty schema to fill.
        let code = unsafe { get_schema(raw, &mut ffi_schema) };
        if code != 0 {
            return Err(stream_error(&mut stream, code));
        }
        let schema = Arc::new(Schema::try_from(&ffi_schema)?);
        Ok(Self {
            stream,
            ffi_schema,
            schema,
        })
    }
}

/// Build an error from the stream's `get_last_error`.
fn stream_error(stream: &mut FFI_ArrowArrayStream, code: i32) -> ArrowError {
    let raw = raw_stream(stream);
    // SAFETY: `raw` views the unreleased stream the caller owns (same layout, asserted above).
    let get_last_error = unsafe { (*raw).get_last_error };
    let message = get_last_error.and_then(|get_last_error| {
        // SAFETY: `get_last_error` is the producer's callback on an unreleased stream; it returns
        // null or a NUL-terminated string valid until the next call on the stream.
        let ptr = unsafe { get_last_error(raw) };
        // SAFETY: non-null per the check, NUL-terminated per the C Stream Interface.
        (!ptr.is_null()).then(|| {
            unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned()
        })
    });
    ArrowError::CDataInterface(format!(
        "Arrow C stream error (code {code}): {}",
        message.unwrap_or_else(|| "no error message".to_string())
    ))
}

impl Iterator for StreamImporter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = raw_stream(&mut self.stream);
        // SAFETY: `raw` views the stream we own, unreleased until drop (same layout, asserted above).
        let Some(get_next) = (unsafe { (*raw).get_next }) else {
            return Some(Err(ArrowError::CDataInterface(
                "The Arrow C stream has no get_next.".to_string(),
            )));
        };
        let mut array = FFI_ArrowArray::empty();
        // SAFETY: the stream is unreleased (we own it and release it only on drop) and `get_next`
        // is its producer's callback, called with a pointer to an empty array to fill.
        let code = unsafe { get_next(raw, &mut array) };
        if code != 0 {
            return Some(Err(stream_error(&mut self.stream, code)));
        }
        // End of stream: the producer leaves the array released.
        if array.is_released() {
            return None;
        }
        Some(import_batch(array, &self.ffi_schema, &self.schema))
    }
}

impl RecordBatchReader for StreamImporter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Import a single record batch from an `arrow_schema` and an `arrow_array` struct.
pub fn import_record_batch(
    array: FFI_ArrowArray,
    ffi_schema: &FFI_ArrowSchema,
) -> Result<(SchemaRef, RecordBatch), ArrowError> {
    let schema = Arc::new(Schema::try_from(ffi_schema)?);
    let batch = import_batch(array, ffi_schema, &schema)?;
    Ok((schema, batch))
}
