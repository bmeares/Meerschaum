//! Rust-side check of the Arrow C Stream path used by `_core.roundtrip_arrow`.
//!
//! The Python tests (`tests/test_core.py`) exercise the same boundary through real producers
//! (pyarrow, polars, pandas). This test needs no interpreter: it exports a batch covering every
//! dtype family through `FFI_ArrowArrayStream` (the struct a PyCapsule carries), imports it back
//! with `StreamImporter` (the importer `roundtrip_arrow` uses), drains it with `collect_reader`,
//! and checks equality and that the buffers were not copied.

use std::collections::HashMap;
use std::sync::Arc;

use _core::arrow_bridge::{ArrowStream, collect_reader};
use _core::ffi_import::{StreamImporter, import_record_batch, repair_null_buffers};
use arrow::array::NullArray;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, ListArray, RecordBatch, RecordBatchIterator, StringArray, StringViewArray,
    StructArray, TimestampMicrosecondArray, TimestampNanosecondArray, UInt8Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

fn every_dtype_family() -> RecordBatch {
    let list_values = Int64Array::from(vec![1, 2, 3, 4]);
    let list = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Int64, true)),
        OffsetBuffer::from_lengths([2, 0, 2]),
        Arc::new(list_values),
        Some(vec![true, false, true].into()),
    );
    let structs = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
        ),
    ]);
    let dict: DictionaryArray<Int32Type> =
        vec![Some("red"), None, Some("red")].into_iter().collect();

    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "i8",
            Arc::new(Int8Array::from(vec![Some(-1), None, Some(127)])),
        ),
        (
            "i16",
            Arc::new(Int16Array::from(vec![Some(1), None, Some(-3)])),
        ),
        (
            "i64",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
        ),
        (
            "u8",
            Arc::new(UInt8Array::from(vec![Some(0), None, Some(255)])),
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None, Some(0)])),
        ),
        (
            "f32",
            Arc::new(Float32Array::from(vec![Some(1.5), None, Some(f32::NAN)])),
        ),
        (
            "f64",
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                None,
                Some(f64::INFINITY),
            ])),
        ),
        (
            "bool",
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        ),
        (
            "utf8",
            Arc::new(StringArray::from(vec![Some("héllo"), None, Some("")])),
        ),
        (
            "large_utf8",
            Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("c")])),
        ),
        (
            "utf8_view",
            Arc::new(StringViewArray::from(vec![
                Some("a string longer than 12 bytes"),
                None,
                Some("x"),
            ])),
        ),
        (
            "ts_utc_us",
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(0), None, Some(1_700_000_000_000_000)])
                    .with_timezone("UTC"),
            ),
        ),
        (
            "ts_ny_ns",
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(1), None, Some(2)])
                    .with_timezone("America/New_York"),
            ),
        ),
        (
            "ts_naive_us",
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(5),
                None,
                Some(6),
            ])),
        ),
        (
            "date32",
            Arc::new(Date32Array::from(vec![Some(19_000), None, Some(0)])),
        ),
        (
            "decimal",
            Arc::new(
                Decimal128Array::from(vec![Some(12_345_678_901_234), None, Some(-1)])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            ),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![
                Some(&b"\x00\x01"[..]),
                None,
                Some(&b""[..]),
            ])),
        ),
        (
            "large_binary",
            Arc::new(LargeBinaryArray::from(vec![
                Some(&b"wkb"[..]),
                None,
                Some(&b"\xff"[..]),
            ])),
        ),
        ("list_i64", Arc::new(list)),
        ("struct", Arc::new(structs)),
        ("dictionary", Arc::new(dict)),
        ("null", Arc::new(NullArray::new(3))),
    ];

    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, arr)| {
            let field = Field::new(*name, arr.data_type().clone(), true);
            if *name == "large_binary" {
                // GeoArrow-style extension metadata must survive the boundary (D10).
                field.with_metadata(HashMap::from([(
                    "ARROW:extension:name".to_string(),
                    "geoarrow.wkb".to_string(),
                )]))
            } else {
                field
            }
        })
        .collect();
    let schema =
        Schema::new(fields).with_metadata(HashMap::from([("mrsm".to_string(), "v5".to_string())]));
    RecordBatch::try_new(
        Arc::new(schema),
        columns.into_iter().map(|(_, arr)| arr).collect(),
    )
    .unwrap()
}

#[test]
fn ffi_stream_roundtrip_every_dtype_family() {
    let batch = every_dtype_family();
    let schema = batch.schema();
    let batches = vec![batch.clone(), batch.slice(1, 2)];

    let reader = RecordBatchIterator::new(batches.clone().into_iter().map(Ok), schema.clone());
    let ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));
    let imported = StreamImporter::try_new(ffi_stream).unwrap();

    let (out_schema, out_batches) = collect_reader(imported).unwrap();
    // Schema- and field-level metadata survive, on the stream and on every batch.
    assert_eq!(out_schema.as_ref(), schema.as_ref());
    assert_eq!(out_batches, batches);

    // Zero-copy: the imported Int64 values buffer is the exporter's allocation.
    let src = batches[0].column_by_name("i64").unwrap().to_data();
    let dst = out_batches[0].column_by_name("i64").unwrap().to_data();
    assert_eq!(src.buffers()[0].as_ptr(), dst.buffers()[0].as_ptr());

    let stream = ArrowStream::new(out_schema, out_batches);
    assert_eq!(stream.row_count(), 5);
    assert_eq!(stream.batches().len(), 2);
}

#[test]
fn empty_stream_keeps_schema() {
    let schema = every_dtype_family().schema();
    let reader = RecordBatchIterator::new(std::iter::empty(), schema.clone());
    let imported = StreamImporter::try_new(FFI_ArrowArrayStream::new(Box::new(reader))).unwrap();
    let (out_schema, out_batches) = collect_reader(imported).unwrap();
    assert_eq!(out_schema.as_ref(), schema.as_ref());
    assert!(out_batches.is_empty());
}

#[test]
fn single_batch_import_and_spec_compliant_null_is_untouched() {
    let batch = every_dtype_family();
    let struct_array = StructArray::from(batch.clone());
    let data = struct_array.to_data();
    let ffi_schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();
    let mut ffi_array = FFI_ArrowArray::new(&data);

    // arrow-rs exports `Null` with zero buffers, as the spec requires: nothing to repair.
    assert_eq!(repair_null_buffers(&mut ffi_array, &ffi_schema), 0);

    let (schema, out) = import_record_batch(ffi_array, &ffi_schema).unwrap();
    assert_eq!(schema.fields(), batch.schema().fields());
    assert_eq!(out.columns(), batch.columns());
}
