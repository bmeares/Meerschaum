//! A typed Arrow frame for standalone plugin crates. The host and guest exchange
//! Arrow C streams, including their schemas and field metadata.
use crate::{Result, error};
use arrow::{
    array::{ArrayData, make_array},
    buffer::{Buffer, NullBuffer},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
    json::{
        ReaderBuilder,
        reader::infer_json_schema_from_iterator,
        writer::{JsonArray, WriterBuilder},
    },
    record_batch::{RecordBatch, RecordBatchOptions},
};
use indexmap::{IndexMap, IndexSet};
use serde_json::Value;
use std::sync::Arc;

/// An Arrow record batch owned by a plugin built with `default-features = false`.
#[derive(Clone, Debug)]
pub struct Frame(RecordBatch);

impl Default for Frame {
    fn default() -> Self {
        Self(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }
}

impl Frame {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn height(&self) -> usize {
        self.0.num_rows()
    }

    pub fn width(&self) -> usize {
        self.0.num_columns()
    }

    pub fn is_empty(&self) -> bool {
        self.height() == 0
    }

    pub fn names(&self) -> Vec<String> {
        self.0
            .schema()
            .fields
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Column types as Meerschaum dtype strings. Raw Arrow types and extension
    /// metadata are available through the record batch.
    pub fn dtypes(&self) -> IndexMap<String, String> {
        self.0
            .schema()
            .fields
            .iter()
            .map(|f| {
                let name = f
                    .metadata()
                    .get(meerschaum_core::dtypes::field_meta::DTYPE)
                    .cloned()
                    .unwrap_or_else(|| dtype_name(f.data_type()));
                (f.name().clone(), name)
            })
            .collect()
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Self> {
        Ok(Self(batch.clone()))
    }

    /// Join batches with identical schemas, including the declared nullability
    /// and field metadata. An empty slice has no schema.
    pub fn from_record_batches(batches: &[RecordBatch]) -> Result<Self> {
        let Some(first) = batches.first() else {
            return Ok(Self::empty());
        };
        let schema = first.schema();
        if batches.iter().any(|b| b.schema() != schema) {
            return Err(error("record batches have different schemas"));
        }
        if batches.len() == 1 {
            return Self::from_record_batch(first);
        }
        concat_batches(&schema, batches).map(Self).map_err(error)
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        Ok(self.0.clone())
    }

    /// Detach all imported buffers before the producer releases its library or context.
    pub(crate) fn from_ffi_batches(batches: &[RecordBatch]) -> Result<Self> {
        let Self(batch) = Self::from_record_batches(batches)?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| copy_data(column.to_data()).map(make_array))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(error)?;
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(batch.schema(), columns, &options)
            .map(Self)
            .map_err(error)
    }
}

fn copy_data(data: ArrayData) -> std::result::Result<ArrayData, ArrowError> {
    let (dtype, length, nulls, offset, buffers, children) = data.into_parts();
    let buffers = buffers
        .into_iter()
        .map(|buffer| Buffer::from_slice_ref(buffer.as_slice()))
        .collect();
    let children = children
        .into_iter()
        .map(copy_data)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let nulls = nulls.map(|mask| NullBuffer::from_iter(mask.iter()));
    ArrayData::builder(dtype)
        .len(length)
        .offset(offset)
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls)
        .build()
}

fn dtype_name(dtype: &DataType) -> String {
    match dtype {
        DataType::Timestamp(unit, zone) => {
            let unit = match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            };
            match zone {
                Some(zone) => format!("datetime64[{unit}, {zone}]"),
                None => format!("datetime64[{unit}]"),
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Boolean => "bool".into(),
        DataType::Date32 => "date32[day][pyarrow]".into(),
        DataType::Null => "object".into(),
        other => other.to_string().to_lowercase(),
    }
}

fn declared_type(name: &str) -> Result<DataType> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int" | "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float" | "float64" => DataType::Float64,
        "str" | "string" => DataType::Utf8,
        "date" => DataType::Date32,
        "datetime" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        _ => {
            return Err(error(format!(
                "unsupported guest row dtype '{name}'; supply an Arrow RecordBatch or enable the host feature for kernel dtype conversion"
            )));
        }
    })
}

/// JSON row conversions for typed Arrow guests. The declared dtypes are bool,
/// signed/unsigned integers (8 to 64 bits), float32/64, string, date and UTC
/// microsecond datetime. Arrow infers undeclared fields. For other types and
/// arbitrary metadata, use [`Frame::from_record_batch`].
pub trait FrameExt: Sized {
    fn from_rows<R: Into<Value>>(
        rows: impl IntoIterator<Item = R>,
        dtypes: &[(&str, &str)],
    ) -> Result<Self>;
    /// Render Arrow's JSON representation, including explicit nulls; returns
    /// an error for columns the Arrow JSON writer cannot represent.
    fn try_to_rows(&self) -> Result<Vec<Value>>;
}

impl FrameExt for Frame {
    fn from_rows<R: Into<Value>>(
        rows: impl IntoIterator<Item = R>,
        dtypes: &[(&str, &str)],
    ) -> Result<Self> {
        let rows: Vec<Value> = rows.into_iter().map(Into::into).collect();
        let inferred = infer_json_schema_from_iterator(rows.iter().map(Ok)).map_err(error)?;
        let declared = dtypes
            .iter()
            .map(|&(name, dtype)| Ok((name, declared_type(dtype)?)))
            .collect::<Result<IndexMap<_, _>>>()?;
        // Arrow's inference does not preserve column order. Keep first appearance,
        // then append declared columns missing from every row.
        let names = rows
            .iter()
            .filter_map(Value::as_object)
            .flat_map(|row| row.keys().map(String::as_str))
            .chain(declared.keys().copied())
            .collect::<IndexSet<_>>();
        let fields = names
            .into_iter()
            .map(|name| {
                let dtype = match declared.get(name) {
                    Some(dtype) => dtype.clone(),
                    None => inferred
                        .field_with_name(name)
                        .map_err(error)?
                        .data_type()
                        .clone(),
                };
                Ok(Field::new(name, dtype, true))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
        if rows.is_empty() {
            return Ok(Self(RecordBatch::new_empty(schema)));
        }
        if schema.fields.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
            return RecordBatch::try_new_with_options(schema, vec![], &options)
                .map(Self)
                .map_err(error);
        }
        let mut decoder = ReaderBuilder::new(schema).build_decoder().map_err(error)?;
        decoder.serialize(&rows).map_err(error)?;
        decoder
            .flush()
            .map_err(error)?
            .map(Self)
            .ok_or_else(|| error("missing JSON record batch"))
    }

    fn try_to_rows(&self) -> Result<Vec<Value>> {
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(Vec::new());
        writer.write(&self.0).map_err(error)?;
        writer.finish().map_err(error)?;
        serde_json::from_slice(&writer.into_inner()).map_err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array, ffi_stream::FFI_ArrowArrayStream, record_batch::RecordBatchIterator,
    };
    use serde_json::json;

    #[test]
    fn portable_rows_keep_types_nulls_and_utc_microseconds() {
        let frame = Frame::from_rows(
            [
                json!({"at":"2026-09-01T12:02:00.000019+02:00", "reading":3.5, "sensor":17, "ready":true, "name":"a"}),
                json!({"at":null,"name":""}),
            ],
            &[("at","datetime"),("reading","float"),("sensor","int"),("ready","bool"),("name","string"),("missing","int8")],
        ).unwrap();
        assert_eq!(
            frame.names(),
            ["at", "reading", "sensor", "ready", "name", "missing"]
        );
        assert_eq!(frame.dtypes()["at"], "datetime64[us, UTC]");
        let rows = frame.try_to_rows().unwrap();
        assert_eq!(
            rows[0],
            json!({"at":"2026-09-01T10:02:00.000019Z","reading":3.5,"sensor":17,"ready":true,"name":"a","missing":null})
        );
        assert_eq!(
            rows[1],
            json!({"at":null,"reading":null,"sensor":null,"ready":null,"name":"","missing":null})
        );
        let inferred = Frame::from_rows([json!({"id": 1, "v":"2026-01-01"})], &[]).unwrap();
        assert_eq!(inferred.dtypes()["id"], "int64");
        assert_eq!(inferred.dtypes()["v"], "string");
        let empty = Frame::from_rows(Vec::<Value>::new(), &[("at", "datetime")]).unwrap();
        assert_eq!(empty.dtypes()["at"], "datetime64[us, UTC]");
        assert_eq!(empty.try_to_rows().unwrap(), Vec::<Value>::new());
        assert!(Frame::from_rows([json!({})], &[("id", "imaginary")]).is_err());
        let only_index = Frame::from_rows([json!({}), json!({})], &[]).unwrap();
        assert_eq!(only_index.height(), 2);
    }

    #[test]
    fn c_stream_detaches_sliced_nullable_buffers_and_retains_schema() {
        let field = Field::new("i", DataType::Int64, true).with_metadata(
            std::collections::HashMap::from([("custom".to_string(), "kept".to_string())]),
        );
        let schema = Arc::new(Schema::new(vec![field]).with_metadata(
            std::collections::HashMap::from([("origin".to_string(), "source".to_string())]),
        ));
        let array = Int64Array::from(vec![Some(71), None, Some(8), Some(19)]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])
            .unwrap()
            .slice(1, 2);
        let to_stream = |batches: Vec<RecordBatch>| {
            FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
                batches.into_iter().map(Ok),
                schema.clone(),
            )))
        };
        let frame = crate::sdk::frame(to_stream(vec![batch.clone()]))
            .unwrap()
            .unwrap();
        let owned = frame.to_record_batch().unwrap();
        let source = batch.column(0).to_data();
        let copy = owned.column(0).to_data();
        assert_ne!(source.buffers()[0].as_ptr(), copy.buffers()[0].as_ptr());
        assert_ne!(
            source.nulls().unwrap().buffer().as_ptr(),
            copy.nulls().unwrap().buffer().as_ptr()
        );
        assert_eq!(batch, owned);
        let multi = crate::sdk::frame(to_stream(vec![batch.clone(), batch]))
            .unwrap()
            .unwrap();
        assert_eq!(multi.to_record_batch().unwrap().schema(), schema);
        assert_eq!(
            multi.try_to_rows().unwrap(),
            [
                json!({"i":null}),
                json!({"i":8}),
                json!({"i":null}),
                json!({"i":8})
            ]
        );
    }
}
