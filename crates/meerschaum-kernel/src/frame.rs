//! The kernel's DataFrame: ordered columns that are either native Polars `Series` or object
//! columns of [`Value`]s, mirroring pandas' native vs `object` columns.
//!
//! v4 behavior hinges on which columns are `object` (special values are applied per value and
//! detected from the first valid value), so the kernel keeps that distinction explicitly.
//! Conversions to Polars and to Arrow (with GeoArrow `geoarrow.wkb` and `arrow.json` extension
//! metadata, D10) live here.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::NaiveDate;
use indexmap::IndexMap;
use polars::prelude::*;

use meerschaum_core::dtypes::field_meta as meta;

use crate::decimal::Decimal;
use crate::dtypes::{Srid, pandas_dtype_of};
use crate::geometry::Geometry;
use crate::value::{DateTimeValue, TzInfo, Value};

/// Errors raised by frame operations.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("column '{0}' not found")]
    ColumnNotFound(String),
    #[error("column '{name}' has length {len}, expected {expected}")]
    Length {
        name: String,
        len: usize,
        expected: usize,
    },
    #[error(transparent)]
    Polars(#[from] PolarsError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("{0}")]
    Other(String),
}

pub type FrameResult<T> = Result<T, FrameError>;

/// A column's storage.
#[derive(Clone, Debug)]
pub enum ColumnData {
    /// A typed Polars column (pandas: a NumPy / nullable / Arrow-backed column).
    Native(Series),
    /// An object column (pandas `object`).
    Object(Vec<Value>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Native(s) => s.len(),
            ColumnData::Object(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The pandas dtype string (`df.dtypes[col]`).
    pub fn dtype_str(&self) -> String {
        match self {
            ColumnData::Native(s) => pandas_dtype_of(s.dtype()),
            ColumnData::Object(_) => "object".to_string(),
        }
    }

    /// Materialize the values (`series.tolist()`).
    pub fn values(&self) -> Vec<Value> {
        match self {
            ColumnData::Native(s) => series_to_values(s),
            ColumnData::Object(v) => v.clone(),
        }
    }

    pub fn get(&self, ix: usize) -> Value {
        match self {
            ColumnData::Native(s) => s
                .get(ix)
                .map(|av| any_value_to_value(&av, s.dtype()))
                .unwrap_or(Value::Null),
            ColumnData::Object(v) => v.get(ix).cloned().unwrap_or(Value::Null),
        }
    }

    /// Null mask (`series.isna()`).
    pub fn is_null_mask(&self) -> Vec<bool> {
        match self {
            ColumnData::Native(s) => {
                let nulls = s.is_null();
                let nan = s.dtype().is_float().then(|| s.is_nan().ok()).flatten();
                (0..s.len())
                    .map(|i| {
                        nulls.get(i).unwrap_or(true)
                            || nan.as_ref().and_then(|n| n.get(i)).unwrap_or(false)
                    })
                    .collect()
            }
            ColumnData::Object(v) => v.iter().map(Value::is_na).collect(),
        }
    }

    /// `series.first_valid_index()` as a positional index.
    pub fn first_valid_index(&self) -> Option<usize> {
        // Same answer as `is_null_mask().position(|n| !n)`, without building the mask: the
        // detectors call this on every column of every frame they inspect.
        match self {
            ColumnData::Native(s) => {
                if s.is_empty() || s.null_count() == s.len() {
                    return None;
                }
                if !s.dtype().is_float() {
                    if s.null_count() == 0 {
                        return Some(0);
                    }
                    return s.is_not_null().iter().position(|v| v == Some(true));
                }
                let floats = s.cast(&DataType::Float64).ok()?;
                let floats = floats.f64().ok()?;
                floats.iter().position(|v| v.is_some_and(|f| !f.is_nan()))
            }
            ColumnData::Object(v) => v.iter().position(|v| !v.is_na()),
        }
    }

    /// Gather rows by position.
    pub fn take(&self, indices: &[usize]) -> ColumnData {
        match self {
            ColumnData::Native(s) => {
                let idx = IdxCa::from_vec(
                    PlSmallStr::EMPTY,
                    indices.iter().map(|i| *i as IdxSize).collect(),
                );
                ColumnData::Native(s.take(&idx).expect("indices in bounds"))
            }
            ColumnData::Object(v) => {
                ColumnData::Object(indices.iter().map(|i| v[*i].clone()).collect())
            }
        }
    }

    /// Keep rows where `mask` is true.
    pub fn filter(&self, mask: &[bool]) -> ColumnData {
        let indices: Vec<usize> = mask
            .iter()
            .enumerate()
            .filter_map(|(i, m)| m.then_some(i))
            .collect();
        self.take(&indices)
    }

    /// A column of `n` nulls with the same storage kind.
    pub fn nulls_like(&self, n: usize) -> ColumnData {
        match self {
            ColumnData::Native(s) => {
                ColumnData::Native(Series::full_null(s.name().clone(), n, s.dtype()))
            }
            ColumnData::Object(_) => ColumnData::Object(vec![Value::Null; n]),
        }
    }

    /// Native Polars dtype, if any.
    pub fn polars_dtype(&self) -> Option<&DataType> {
        match self {
            ColumnData::Native(s) => Some(s.dtype()),
            ColumnData::Object(_) => None,
        }
    }
}

/// The Arrow field tags a column was read with (see [`meerschaum_core::dtypes::field_meta`]):
/// the Meerschaum dtype declared by the producer and the database type reported by the driver.
///
/// Tags are authoritative: the detectors in [`crate::detect`] answer from an explicit `dtype` tag
/// instead of inspecting values. Replacing a column's data ([`Frame::set`]) drops its tags.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnTags {
    /// `meerschaum:dtype` (`json`, `bytes`, `uuid`, `numeric`, `geometry`, ...).
    pub dtype: Option<String>,
    /// `meerschaum:db_type` (`TIMESTAMPTZ`, `GEOMETRY(POINT, 4326)`, ...).
    pub db_type: Option<String>,
}

impl ColumnTags {
    /// Whether the column carries no tags.
    pub fn is_empty(&self) -> bool {
        self.dtype.is_none() && self.db_type.is_none()
    }

    /// The Meerschaum dtype the tags declare (the explicit tag refined by the database type,
    /// else the dtype mapped from the database type), via
    /// [`meerschaum_core::dtypes::dtype_from_field_tags`].
    pub fn mrsm_dtype(&self) -> Option<String> {
        meerschaum_core::dtypes::dtype_from_field_tags(
            self.dtype.as_deref(),
            self.db_type.as_deref(),
        )
    }

    /// Whether the explicit `dtype` tag is `kind` (`json`, `uuid`, `numeric[10,2]` is `numeric`,
    /// `geometry[POINT, 4326]` is `geometry`). `None` when there is no explicit tag.
    pub fn declares(&self, kind: &str) -> Option<bool> {
        let tag = self.dtype.as_deref()?;
        let base = tag.split('[').next().unwrap_or(tag);
        let base = meerschaum_core::dtypes::resolve_alias(base);
        Some(base == kind || (kind == "geometry" && base == "geography"))
    }
}

/// A named column with an optional CRS (GeoPandas column CRS for geometry columns) and the
/// source tags it was read with.
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data: ColumnData,
    pub crs: Option<Srid>,
    pub tags: ColumnTags,
}

impl Column {
    pub fn new(name: impl Into<String>, data: ColumnData) -> Self {
        Self {
            name: name.into(),
            data,
            crs: None,
            tags: ColumnTags::default(),
        }
    }

    pub fn object(name: impl Into<String>, values: Vec<Value>) -> Self {
        Self::new(name, ColumnData::Object(values))
    }

    pub fn native(series: Series) -> Self {
        Self::new(series.name().to_string(), ColumnData::Native(series))
    }
}

/// An ordered collection of equal-length columns.
#[derive(Clone, Debug, Default)]
pub struct Frame {
    columns: Vec<Column>,
    height: usize,
}

impl Frame {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn new(columns: Vec<Column>) -> FrameResult<Self> {
        let height = columns.first().map(|c| c.data.len()).unwrap_or(0);
        for c in &columns {
            if c.data.len() != height {
                return Err(FrameError::Length {
                    name: c.name.clone(),
                    len: c.data.len(),
                    expected: height,
                });
            }
        }
        Ok(Self { columns, height })
    }

    /// A frame with no columns but `height` rows (pandas allows an index without columns).
    pub fn with_height(height: usize) -> Self {
        Self {
            columns: vec![],
            height,
        }
    }

    pub fn height(&self) -> usize {
        self.height
    }

    pub fn width(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.height == 0
    }

    pub fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut [Column] {
        &mut self.columns
    }

    pub fn contains(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.columns.iter_mut().find(|c| c.name == name)
    }

    pub fn data(&self, name: &str) -> FrameResult<&ColumnData> {
        self.column(name)
            .map(|c| &c.data)
            .ok_or_else(|| FrameError::ColumnNotFound(name.to_string()))
    }

    /// `df.dtypes` as pandas dtype strings.
    pub fn dtypes(&self) -> IndexMap<String, String> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), c.data.dtype_str()))
            .collect()
    }

    /// `df[name] = data` (replace in place or append).
    pub fn set(&mut self, name: &str, data: ColumnData) -> FrameResult<()> {
        if self.columns.is_empty() && self.height == 0 {
            self.height = data.len();
        }
        if data.len() != self.height {
            return Err(FrameError::Length {
                name: name.to_string(),
                len: data.len(),
                expected: self.height,
            });
        }
        let data = match data {
            ColumnData::Native(s) => ColumnData::Native(s.with_name(name.into())),
            other => other,
        };
        match self.column_mut(name) {
            Some(c) => {
                c.data = data;
                // The new data is no longer what the source declared.
                c.tags = ColumnTags::default();
            }
            None => self.columns.push(Column::new(name, data)),
        }
        Ok(())
    }

    /// Append or replace a whole column (keeps the CRS and tags given).
    pub fn set_column(&mut self, column: Column) -> FrameResult<()> {
        let crs = column.crs.clone();
        let tags = column.tags.clone();
        let name = column.name.clone();
        self.set(&name, column.data)?;
        if let Some(c) = self.column_mut(&name) {
            c.crs = crs;
            c.tags = tags;
        }
        Ok(())
    }

    /// The Meerschaum dtype each tagged column declares ([`ColumnTags::mrsm_dtype`]), in column
    /// order. Untagged columns are omitted.
    pub fn source_dtypes(&self) -> IndexMap<String, String> {
        self.columns
            .iter()
            .filter_map(|c| c.tags.mrsm_dtype().map(|d| (c.name.clone(), d)))
            .collect()
    }

    /// `del df[name]`.
    pub fn drop(&mut self, name: &str) {
        self.columns.retain(|c| c.name != name);
    }

    /// `df[cols]` (reorder / subset; unknown columns raise).
    pub fn select(&self, names: &[String]) -> FrameResult<Frame> {
        let mut columns = Vec::with_capacity(names.len());
        for n in names {
            columns.push(
                self.column(n)
                    .cloned()
                    .ok_or_else(|| FrameError::ColumnNotFound(n.clone()))?,
            );
        }
        Ok(Frame {
            columns,
            height: self.height,
        })
    }

    pub fn take(&self, indices: &[usize]) -> Frame {
        Frame {
            columns: self
                .columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    data: c.data.take(indices),
                    crs: c.crs.clone(),
                    tags: c.tags.clone(),
                })
                .collect(),
            height: indices.len(),
        }
    }

    pub fn filter(&self, mask: &[bool]) -> Frame {
        let indices: Vec<usize> = mask
            .iter()
            .enumerate()
            .filter_map(|(i, m)| m.then_some(i))
            .collect();
        self.take(&indices)
    }

    /// `df.head(0)`.
    pub fn clear(&self) -> Frame {
        self.take(&[])
    }

    /// A row as values (`df.iloc[i]`).
    pub fn row(&self, ix: usize) -> Vec<Value> {
        self.columns.iter().map(|c| c.data.get(ix)).collect()
    }

    /// Build from rows of values keyed by column (all object columns).
    pub fn from_object_columns(columns: Vec<(String, Vec<Value>)>) -> FrameResult<Frame> {
        Frame::new(
            columns
                .into_iter()
                .map(|(n, v)| Column::object(n, v))
                .collect(),
        )
    }

    // ------------------------------------------------------------------ Polars interop

    /// The columns, consuming the frame.
    pub fn into_columns(self) -> Vec<Column> {
        self.columns
    }

    /// Wrap a Polars `DataFrame` (all columns native).
    pub fn from_polars(df: &DataFrame) -> Frame {
        let columns = df
            .columns()
            .iter()
            .map(|c| Column::native(c.as_materialized_series().clone()))
            .collect();
        Frame {
            columns,
            height: df.height(),
        }
    }

    /// Convert to a Polars `DataFrame`, materializing object columns with
    /// [`object_values_to_series`].
    pub fn to_polars(&self) -> FrameResult<DataFrame> {
        let cols: Vec<polars::prelude::Column> = self
            .columns
            .iter()
            .map(|c| match &c.data {
                ColumnData::Native(s) => s.clone().with_name(c.name.as_str().into()).into(),
                ColumnData::Object(v) => object_values_to_series(&c.name, v).into(),
            })
            .collect();
        Ok(DataFrame::new_infer_height(cols)?)
    }

    /// Describe the special (extension) type of each column, used for Arrow metadata.
    pub fn special_kinds(&self) -> IndexMap<String, SpecialKind> {
        let mut out = IndexMap::new();
        for c in &self.columns {
            if let ColumnData::Object(values) = &c.data
                && let Some(kind) =
                    special_kind_of_tags(&c.tags).or_else(|| special_kind_of(values))
            {
                let kind = match kind {
                    SpecialKind::Geometry { .. } => SpecialKind::Geometry { crs: c.crs.clone() },
                    k => k,
                };
                out.insert(c.name.clone(), kind);
            }
        }
        out
    }

    /// Convert to an Arrow `RecordBatch`. Geometry columns become `geoarrow.wkb` (CRS in the
    /// extension metadata as `{"crs": ..., "crs_type": "authority_code"}`, matching v4's
    /// `to_polars`), JSON columns `arrow.json`, UUID columns `arrow.uuid` (`FixedSizeBinary(16)`)
    /// and unbounded numerics `Utf8` tagged with `meerschaum:dtype = numeric`.
    pub fn to_record_batch(&self) -> FrameResult<arrow::record_batch::RecordBatch> {
        use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, StringArray};
        use arrow::datatypes::{DataType as ADataType, Field, Schema};

        let mut df = self.to_polars()?;
        let mut buf = Vec::new();
        IpcWriter::new(&mut buf)
            .with_compat_level(CompatLevel::oldest())
            .finish(&mut df)?;
        let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(buf), None)?;
        let schema = reader.schema();
        let batches: Vec<_> = reader.collect::<Result<_, _>>()?;
        let batch = if batches.is_empty() {
            arrow::record_batch::RecordBatch::new_empty(schema.clone())
        } else {
            arrow::compute::concat_batches(&schema, &batches)?
        };
        let kinds = self.special_kinds();
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            let mut array = batch.column(i).clone();
            let mut metadata: HashMap<String, String> = HashMap::new();
            let mut data_type = field.data_type().clone();
            if let Some(col) = self.column(field.name()) {
                if let Some(db_type) = &col.tags.db_type {
                    metadata.insert(meta::DB_TYPE.into(), db_type.clone());
                }
                if let Some(dtype) = &col.tags.dtype {
                    metadata.insert(meta::DTYPE.into(), dtype.clone());
                }
            }
            if let Some(kind) = kinds.get(field.name()) {
                metadata.insert(meta::DTYPE.into(), kind.mrsm_dtype());
                match kind {
                    SpecialKind::Json => {
                        metadata.insert(meta::EXTENSION_NAME.into(), "arrow.json".into());
                        metadata.insert(meta::EXTENSION_METADATA.into(), String::new());
                    }
                    SpecialKind::Geometry { crs } => {
                        metadata.insert(meta::EXTENSION_NAME.into(), "geoarrow.wkb".into());
                        let ext = match crs.as_ref().and_then(Srid::to_crs) {
                            Some(crs) => {
                                serde_json::json!({"crs": crs, "crs_type": "authority_code"})
                                    .to_string()
                            }
                            None => "{}".to_string(),
                        };
                        metadata.insert(meta::EXTENSION_METADATA.into(), ext);
                    }
                    SpecialKind::Uuid => {
                        let strings = array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .cloned()
                            .or_else(|| {
                                arrow::compute::cast(&array, &ADataType::Utf8)
                                    .ok()
                                    .and_then(|a| a.as_any().downcast_ref::<StringArray>().cloned())
                            })
                            .ok_or_else(|| {
                                FrameError::Other("uuid column is not a string".into())
                            })?;
                        let mut builder = FixedSizeBinaryBuilder::with_capacity(strings.len(), 16);
                        for ix in 0..strings.len() {
                            if strings.is_null(ix) {
                                builder.append_null();
                            } else {
                                match uuid::Uuid::parse_str(strings.value(ix)) {
                                    Ok(u) => builder.append_value(u.as_bytes())?,
                                    Err(_) => builder.append_null(),
                                }
                            }
                        }
                        array = Arc::new(builder.finish());
                        data_type = ADataType::FixedSizeBinary(16);
                        metadata.insert(meta::EXTENSION_NAME.into(), "arrow.uuid".into());
                        metadata.insert(meta::EXTENSION_METADATA.into(), String::new());
                    }
                    SpecialKind::Numeric => {}
                }
            }
            fields.push(Field::new(field.name(), data_type, true).with_metadata(metadata));
            arrays.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        Ok(arrow::record_batch::RecordBatch::try_new_with_options(
            schema,
            arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(self.height)),
        )?)
    }

    /// Build from an Arrow `RecordBatch`, restoring object columns from extension metadata
    /// (`geoarrow.wkb`, `arrow.json`, `arrow.uuid`) and the Meerschaum field tags
    /// ([`meerschaum_core::dtypes::field_meta`]) that SQL query results carry:
    ///
    /// - `meerschaum:dtype = json` (UTF-8 text) becomes an object column of parsed JSON;
    /// - `numeric` (UTF-8 text or `Decimal128`) becomes an object column of decimals;
    /// - `uuid` (`FixedSizeBinary(16)` or text) becomes an object column of UUIDs;
    /// - `geometry` (WKB, or EWKB / MySQL-internal per `meerschaum:encoding`) becomes an object
    ///   column of geometries, with the CRS from the extension metadata, the SRIDs or the
    ///   database type;
    /// - `bytes` and every other column stay native.
    ///
    /// Every column keeps its `meerschaum:dtype` / `meerschaum:db_type` tags ([`ColumnTags`]).
    pub fn from_record_batch(batch: &arrow::record_batch::RecordBatch) -> FrameResult<Frame> {
        Self::from_record_batches(std::slice::from_ref(batch))
    }

    /// [`Frame::from_record_batch`] over batches sharing one schema (a query result stream).
    pub fn from_record_batches(batches: &[arrow::record_batch::RecordBatch]) -> FrameResult<Frame> {
        use arrow::array::{
            Array, BinaryArray, Decimal128Array, FixedSizeBinaryArray, LargeBinaryArray,
            LargeStringArray, StringArray,
        };
        use arrow::datatypes::{Field, Schema};

        let Some(first) = batches.first() else {
            return Ok(Frame::empty());
        };
        let batch = if batches.len() == 1 {
            first.clone()
        } else {
            arrow::compute::concat_batches(&first.schema(), batches)?
        };

        // kind, extension metadata, encoding
        let mut special: IndexMap<String, (String, Option<String>, Option<String>)> =
            IndexMap::new();
        let mut tags: IndexMap<String, ColumnTags> = IndexMap::new();
        let mut plain_fields = Vec::new();
        let mut plain_arrays = Vec::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let md = field.metadata();
            let ext = md.get(meta::EXTENSION_NAME).cloned();
            let dtype_tag = md.get(meta::DTYPE).cloned();
            let col_tags = ColumnTags {
                dtype: dtype_tag.clone(),
                db_type: md.get(meta::DB_TYPE).cloned(),
            };
            let tag_base = dtype_tag
                .as_deref()
                .map(|d| meerschaum_core::dtypes::resolve_alias(d.split('[').next().unwrap_or(d)));
            let kind = match (ext.as_deref(), tag_base) {
                (Some("geoarrow.wkb"), _) => Some("geometry"),
                (Some("arrow.json"), _) => Some("json"),
                (Some("arrow.uuid"), _) => Some("uuid"),
                (_, Some("geometry" | "geography")) => Some("geometry"),
                (_, Some(d @ ("json" | "uuid" | "numeric"))) => Some(d),
                _ => None,
            };
            if let Some(kind) = kind {
                special.insert(
                    field.name().clone(),
                    (
                        kind.to_string(),
                        md.get(meta::EXTENSION_METADATA).cloned(),
                        md.get(meta::ENCODING).cloned(),
                    ),
                );
            }
            if !col_tags.is_empty() {
                tags.insert(field.name().clone(), col_tags);
            }
            plain_fields.push(Field::new(field.name(), field.data_type().clone(), true));
            plain_arrays.push(batch.column(i).clone());
        }
        let plain = arrow::record_batch::RecordBatch::try_new_with_options(
            Arc::new(Schema::new(plain_fields)),
            plain_arrays,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?;
        // Object columns are rebuilt from Arrow directly; the rest go through Polars IPC.
        let mut columns = Vec::new();
        let schema = plain.schema();
        let native_ixs: Vec<usize> = (0..schema.fields().len())
            .filter(|i| !special.contains_key(schema.field(*i).name()))
            .collect();
        let native_df = if native_ixs.is_empty() {
            None
        } else {
            let projected = plain.project(&native_ixs)?;
            let mut buf = Vec::new();
            {
                let mut writer =
                    arrow::ipc::writer::FileWriter::try_new(&mut buf, &projected.schema())?;
                writer.write(&projected)?;
                writer.finish()?;
            }
            Some(IpcReader::new(std::io::Cursor::new(buf)).finish()?)
        };
        for (i, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            let col_tags = tags.get(name).cloned().unwrap_or_default();
            if let Some((kind, ext_md, encoding)) = special.get(name) {
                let array = plain.column(i);
                let n = array.len();
                let mut values = Vec::with_capacity(n);
                let get_bytes = |ix: usize| -> Option<Vec<u8>> {
                    if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
                        return (!a.is_null(ix)).then(|| a.value(ix).to_vec());
                    }
                    if let Some(a) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                        return (!a.is_null(ix)).then(|| a.value(ix).to_vec());
                    }
                    if let Some(a) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                        return (!a.is_null(ix)).then(|| a.value(ix).to_vec());
                    }
                    None
                };
                let get_str = |ix: usize| -> Option<String> {
                    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                        return (!a.is_null(ix)).then(|| a.value(ix).to_string());
                    }
                    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
                        return (!a.is_null(ix)).then(|| a.value(ix).to_string());
                    }
                    if let Some(a) = array.as_any().downcast_ref::<Decimal128Array>() {
                        return (!a.is_null(ix)).then(|| a.value_as_string(ix));
                    }
                    None
                };
                let mut srid_seen: Option<i32> = None;
                for ix in 0..n {
                    let v = match kind.as_str() {
                        "geometry" => get_bytes(ix)
                            .and_then(|b| decode_geometry(&b, encoding.as_deref()))
                            .map(|g| {
                                if srid_seen.is_none() {
                                    srid_seen = g.srid.filter(|s| *s != 0);
                                }
                                Value::Geometry(g)
                            })
                            .unwrap_or(Value::Null),
                        "json" => get_str(ix)
                            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                            .map(json_to_value)
                            .unwrap_or(Value::Null),
                        "uuid" => get_bytes(ix)
                            .and_then(|b| uuid::Uuid::from_slice(&b).ok())
                            .or_else(|| get_str(ix).and_then(|s| uuid::Uuid::parse_str(&s).ok()))
                            .map(Value::Uuid)
                            .unwrap_or(Value::Null),
                        _ => get_str(ix)
                            .and_then(|s| Decimal::parse(&s).ok())
                            .map(Value::Decimal)
                            .unwrap_or(Value::Null),
                    };
                    values.push(v);
                }
                let mut crs = None;
                if kind == "geometry" {
                    crs = ext_md
                        .as_deref()
                        .and_then(|md| serde_json::from_str::<serde_json::Value>(md).ok())
                        .and_then(|j| j.get("crs").and_then(|c| c.as_str().map(str::to_string)))
                        .map(
                            |c| match c.strip_prefix("EPSG:").and_then(|s| s.parse().ok()) {
                                Some(i) => Srid::Code(i),
                                None => Srid::Authority(c),
                            },
                        )
                        .or_else(|| srid_seen.map(|s| Srid::Code(i64::from(s))))
                        .or_else(|| {
                            let dtype = col_tags.mrsm_dtype()?;
                            meerschaum_core::dtypes::GeometrySpec::parse(&dtype)
                                .ok()
                                .flatten()
                                .map(|spec| spec.srid)
                                .filter(Srid::truthy)
                        });
                }
                let mut col = Column::object(name.clone(), values);
                col.crs = crs;
                col.tags = col_tags;
                columns.push(col);
            } else if let Some(df) = &native_df {
                let s = df.column(name)?.as_materialized_series().clone();
                let mut col = Column::native(s);
                col.tags = col_tags;
                columns.push(col);
            }
        }
        let mut frame = Frame::new(columns)?;
        frame.height = batch.num_rows();
        Ok(frame)
    }
}

/// Decode geometry bytes per `meerschaum:encoding`: WKB / EWKB (the default and `ewkb`) or
/// MySQL's internal format (`mysql`: a little-endian SRID followed by WKB).
fn decode_geometry(bytes: &[u8], encoding: Option<&str>) -> Option<Geometry> {
    match encoding {
        Some("mysql") => {
            let (srid, wkb) = bytes.split_at_checked(4)?;
            let mut g = Geometry::from_wkb(wkb).ok()?;
            let srid = i32::from_le_bytes(srid.try_into().ok()?);
            g.srid = (srid != 0).then_some(srid);
            Some(g)
        }
        _ => Geometry::from_wkb(bytes).ok(),
    }
}

/// The special (non-native) Meerschaum type of an object column.
#[derive(Clone, Debug, PartialEq)]
pub enum SpecialKind {
    Json,
    Uuid,
    Numeric,
    Geometry { crs: Option<Srid> },
}

impl SpecialKind {
    pub fn mrsm_dtype(&self) -> String {
        match self {
            SpecialKind::Json => "json".into(),
            SpecialKind::Uuid => "uuid".into(),
            SpecialKind::Numeric => "numeric".into(),
            SpecialKind::Geometry { crs } => match crs.as_ref().filter(|c| c.truthy()) {
                Some(c) => format!("geometry[{c}]"),
                None => "geometry".into(),
            },
        }
    }
}

/// The special kind an explicit `meerschaum:dtype` tag declares.
fn special_kind_of_tags(tags: &ColumnTags) -> Option<SpecialKind> {
    if tags.declares("json")? {
        Some(SpecialKind::Json)
    } else if tags.declares("uuid")? {
        Some(SpecialKind::Uuid)
    } else if tags.declares("numeric")? {
        Some(SpecialKind::Numeric)
    } else if tags.declares("geometry")? {
        Some(SpecialKind::Geometry { crs: None })
    } else {
        None
    }
}

fn special_kind_of(values: &[Value]) -> Option<SpecialKind> {
    let first = values.iter().find(|v| !v.is_na())?;
    match first {
        Value::Json(_) => Some(SpecialKind::Json),
        Value::Uuid(_) => Some(SpecialKind::Uuid),
        Value::Decimal(_) => Some(SpecialKind::Numeric),
        Value::Geometry(_) => Some(SpecialKind::Geometry { crs: None }),
        _ => None,
    }
}

/// Convert a JSON value into a [`Value`]: containers stay JSON, scalars become Python scalars
/// (as `json.loads` returns them).
pub fn json_to_value(j: serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Value::Str(s),
        other => Value::Json(other),
    }
}

fn time_zone_of(tz: Option<&TimeZone>) -> Option<TzInfo> {
    tz.and_then(|t| TzInfo::parse(t.as_str()))
}

/// Convert a Polars `AnyValue` into a [`Value`], using the null flavor pandas would show for
/// the column type (`NaN` for floats and strings, `NaT` for datetimes, `<NA>` otherwise).
pub fn any_value_to_value(av: &AnyValue, dtype: &DataType) -> Value {
    match av {
        AnyValue::Null => match dtype {
            DataType::Float32 | DataType::Float64 | DataType::String => Value::Float(f64::NAN),
            DataType::Datetime(..) => Value::NaT,
            DataType::Null => Value::Null,
            _ => Value::NA,
        },
        AnyValue::Boolean(b) => Value::Bool(*b),
        AnyValue::String(s) => Value::Str(s.to_string()),
        AnyValue::StringOwned(s) => Value::Str(s.to_string()),
        AnyValue::UInt8(v) => Value::Int(*v as i64),
        AnyValue::UInt16(v) => Value::Int(*v as i64),
        AnyValue::UInt32(v) => Value::Int(*v as i64),
        AnyValue::UInt64(v) => match i64::try_from(*v) {
            Ok(value) => Value::Int(value),
            Err(_) => Value::Decimal(Decimal::from_i128_scale(i128::from(*v), 0)),
        },
        AnyValue::Int8(v) => Value::Int(*v as i64),
        AnyValue::Int16(v) => Value::Int(*v as i64),
        AnyValue::Int32(v) => Value::Int(*v as i64),
        AnyValue::Int64(v) => Value::Int(*v),
        AnyValue::Int128(v) => match i64::try_from(*v) {
            Ok(value) => Value::Int(value),
            Err(_) => Value::Decimal(Decimal::from_i128_scale(*v, 0)),
        },
        AnyValue::Float32(v) => Value::Float(*v as f64),
        AnyValue::Float64(v) => Value::Float(*v),
        AnyValue::Date(d) => NaiveDate::from_num_days_from_ce_opt(*d + 719_163)
            .map(Value::Date)
            .unwrap_or(Value::NA),
        AnyValue::Datetime(v, tu, tz) => DateTimeValue::from_timestamp(*v, *tu, time_zone_of(*tz))
            .map(Value::Datetime)
            .unwrap_or(Value::NaT),
        AnyValue::DatetimeOwned(v, tu, tz) => {
            DateTimeValue::from_timestamp(*v, *tu, time_zone_of(tz.as_deref()))
                .map(Value::Datetime)
                .unwrap_or(Value::NaT)
        }
        AnyValue::Binary(b) => Value::Bytes(b.to_vec()),
        AnyValue::BinaryOwned(b) => Value::Bytes(b.clone()),
        AnyValue::Decimal(v, _precision, scale) => {
            Value::Decimal(Decimal::from_i128_scale(*v, *scale as i64))
        }
        other => {
            // Nested values (lists / structs) are JSON containers.
            let s = format!("{other}");
            Value::Str(s)
        }
    }
}

/// Materialize a Polars `Series` as values.
pub fn series_to_values(s: &Series) -> Vec<Value> {
    let dtype = s.dtype().clone();
    s.iter().map(|av| any_value_to_value(&av, &dtype)).collect()
}

/// Exact integer reconstruction for native wide columns. Large Python integers already use
/// the kernel's Decimal representation; never route those digits through a float or a cast.
fn exact_integer_cell(value: &Value) -> Option<i128> {
    match value {
        Value::Int(value) => Some(i128::from(*value)),
        Value::Bool(value) => Some(if *value { 1 } else { 0 }),
        Value::Decimal(value @ Decimal::Finite { exp: 0, .. }) => {
            value.to_plain_string().parse().ok()
        }
        _ => None,
    }
}

/// Build a native `Series` of `dtype` from values; `None` if some non-null value does not fit.
pub fn values_to_series(name: &str, values: &[Value], dtype: &DataType) -> Option<Series> {
    let name: PlSmallStr = name.into();
    let nn = |v: &Value| !v.is_na();
    Some(match dtype {
        DataType::Boolean => {
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Bool(b) => Some(*b),
                    _ => return None,
                });
            }
            BooleanChunked::from_iter_options(name, out.into_iter()).into_series()
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => {
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Int(i) => Some(*i),
                    Value::Bool(b) => Some(*b as i64),
                    _ => return None,
                });
            }
            Int64Chunked::from_iter_options(name, out.into_iter())
                .into_series()
                .strict_cast(dtype)
                .ok()?
        }
        DataType::UInt64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(if value.is_na() {
                    None
                } else {
                    Some(u64::try_from(exact_integer_cell(value)?).ok()?)
                });
            }
            UInt64Chunked::from_iter_options(name, out.into_iter()).into_series()
        }
        DataType::Int128 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(if value.is_na() {
                    None
                } else {
                    Some(exact_integer_cell(value)?)
                });
            }
            Int128Chunked::from_iter_options(name, out.into_iter()).into_series()
        }
        DataType::Float32 | DataType::Float64 => {
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    Value::Bool(b) => Some(*b as i64 as f64),
                    _ => return None,
                });
            }
            Float64Chunked::from_iter_options(name, out.into_iter())
                .into_series()
                .cast(dtype)
                .ok()?
        }
        DataType::String => {
            let mut out: Vec<Option<&str>> = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Str(s) => Some(s.as_str()),
                    _ => return None,
                });
            }
            StringChunked::from_iter_options(name, out.into_iter()).into_series()
        }
        DataType::Binary => {
            let mut out: Vec<Option<&[u8]>> = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Bytes(b) => Some(b.as_slice()),
                    _ => return None,
                });
            }
            BinaryChunked::from_iter_options(name, out.into_iter()).into_series()
        }
        DataType::Date => {
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Date(d) => Some(days_since_epoch(d)),
                    _ => return None,
                });
            }
            Int32Chunked::from_iter_options(name, out.into_iter())
                .into_date()
                .into_series()
        }
        DataType::Datetime(tu, tz) => {
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Datetime(d) => Some(d.to_timestamp(*tu)?),
                    _ => return None,
                });
            }
            Int64Chunked::from_iter_options(name, out.into_iter())
                .into_datetime(*tu, tz.clone())
                .into_series()
        }
        DataType::Decimal(p, s) => {
            let scale = *s;
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Decimal(d) => Some(d.to_i128_at_scale(scale as i64)?),
                    _ => return None,
                });
            }
            Int128Chunked::from_iter_options(name, out.into_iter())
                .into_decimal_unchecked(*p, scale)
                .into_series()
        }
        DataType::Null => Series::full_null(name, values.len(), &DataType::Null),
        _ => return None,
    })
}

pub(crate) fn days_since_epoch(d: &NaiveDate) -> i32 {
    (*d - NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch")).num_days() as i32
}

/// Convert an object column into the closest native Polars column for export:
/// JSON → JSON text, UUID → string, numerics → `Decimal(p, s)` when representable (else plain
/// strings), geometry → WKB, homogeneous scalars → their native type, mixed → `str()` of each.
pub fn object_values_to_series(name: &str, values: &[Value]) -> Series {
    if let Some(s) = infer_native_series(name, values) {
        return s;
    }
    let first = values.iter().find(|v| !v.is_na());
    let pname: PlSmallStr = name.into();
    match first {
        None => Series::full_null(pname, values.len(), &DataType::Null),
        Some(Value::Json(_)) => StringChunked::from_iter_options(
            pname,
            values.iter().map(|v| match v {
                Value::Json(j) => Some(j.to_string()),
                v if v.is_na() => None,
                other => Some(crate::serialize::json_dumps_value(other)),
            }),
        )
        .into_series(),
        Some(Value::Geometry(_)) => BinaryChunked::from_iter_options(
            pname,
            values.iter().map(|v| match v {
                Value::Geometry(g) => Some(g.to_wkb(true)),
                _ => None,
            }),
        )
        .into_series(),
        Some(Value::Decimal(_)) => {
            let decimals: Option<Vec<Option<&Decimal>>> = values
                .iter()
                .map(|v| match v {
                    Value::Decimal(d) if !d.is_nan() => Some(Some(d)),
                    v if v.is_na() => Some(None),
                    _ => None,
                })
                .collect();
            if let Some(decimals) = decimals {
                // PARITY: Polars' inference from Python `Decimal`s (v4's `pl.from_pandas` of an
                // object column): precision = the most integer digits + the largest scale, where a
                // value below 1 has no integer digit (`0.000012345` is `Decimal(9, 9)`, `0.5` is
                // `Decimal(1, 1)`); never below 1 (a column of zeros is `Decimal(1, 0)`).
                let mut max_scale = 0i64;
                let mut max_int = 0i64;
                let mut finite = true;
                for d in decimals.iter().flatten() {
                    match (d.precision_scale(), d) {
                        (Some((_, s)), Decimal::Finite { coeff, exp, .. }) => {
                            max_scale = max_scale.max(s);
                            // Integer digits as Polars counts them: none below 1 (`precision_scale`
                            // counts one, as a database column needs).
                            max_int = max_int.max((coeff.len() as i64 + exp).max(0));
                        }
                        _ => finite = false,
                    }
                }
                if finite && max_int + max_scale <= 38 {
                    let ints: Vec<Option<i128>> = decimals
                        .iter()
                        .map(|d| d.and_then(|d| d.to_i128_at_scale(max_scale)))
                        .collect();
                    return Int128Chunked::from_iter_options(pname, ints.into_iter())
                        .into_decimal_unchecked(
                            ((max_int + max_scale) as usize).max(1),
                            max_scale as usize,
                        )
                        .into_series();
                }
            }
            StringChunked::from_iter_options(
                pname,
                values.iter().map(|v| match v {
                    Value::Decimal(d) if !d.is_nan() => Some(d.to_plain_string()),
                    v if v.is_na() => None,
                    other => Some(other.py_str()),
                }),
            )
            .into_series()
        }
        _ => StringChunked::from_iter_options(
            pname,
            values.iter().map(|v| match v {
                Value::Uuid(u) => Some(u.hyphenated().to_string()),
                v if v.is_na() => None,
                other => Some(other.py_str()),
            }),
        )
        .into_series(),
    }
}

/// `infer_objects()` for one object column: the native series if all non-null values share a
/// native scalar kind, else `None`.
///
/// pandas 3 rules: all `int` → `int64` (with nulls → `float64`), `int`/`float` → `float64`,
/// `bool` (no nulls) → `bool`, `str` → `str`, naive or same-tz datetimes → `datetime64[us]`,
/// `bytes` / `date` / special types stay `object`.
pub fn infer_native_series(name: &str, values: &[Value]) -> Option<Series> {
    let non_null: Vec<&Value> = values.iter().filter(|v| !v.is_na()).collect();
    if non_null.is_empty() {
        return None;
    }
    let has_null = non_null.len() != values.len();
    let all = |f: fn(&Value) -> bool| non_null.iter().all(|v| f(v));
    if all(|v| matches!(v, Value::Int(_))) {
        if has_null {
            return values_to_series(name, values, &DataType::Float64);
        }
        return values_to_series(name, values, &DataType::Int64);
    }
    if all(|v| matches!(v, Value::Int(_) | Value::Float(_))) {
        return values_to_series(name, values, &DataType::Float64);
    }
    if all(|v| matches!(v, Value::Bool(_))) && !has_null {
        return values_to_series(name, values, &DataType::Boolean);
    }
    if all(|v| matches!(v, Value::Str(_))) {
        return values_to_series(name, values, &DataType::String);
    }
    if all(|v| matches!(v, Value::Datetime(_))) {
        let tzs: Vec<Option<TzInfo>> = non_null
            .iter()
            .map(|v| match v {
                Value::Datetime(d) => d.tz.clone(),
                _ => None,
            })
            .collect();
        let first = tzs[0].clone();
        if tzs.iter().all(|t| *t == first) {
            let tz = first
                .map(|t| t.name())
                .and_then(|n| TimeZone::opt_try_new(Some(n)).ok().flatten());
            return values_to_series(
                name,
                values,
                &DataType::Datetime(TimeUnit::Microseconds, tz),
            );
        }
    }
    None
}

#[cfg(test)]
mod wide_integer_tests {
    use super::*;

    #[test]
    fn native_wide_integer_values_and_reconstruction_are_exact() {
        let unsigned = UInt64Chunked::from_iter_options(
            "value".into(),
            [Some(0), Some(i64::MAX as u64), Some(u64::MAX), None].into_iter(),
        )
        .into_series();
        let values = series_to_values(&unsigned);
        assert!(matches!(values[1], Value::Int(i64::MAX)));
        assert_eq!(values[2].py_str(), u64::MAX.to_string());
        let rebuilt = values_to_series("value", &values, &DataType::UInt64).unwrap();
        assert_eq!(
            rebuilt.u64().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(0), Some(i64::MAX as u64), Some(u64::MAX), None]
        );
        let signed = Int128Chunked::from_iter_options(
            "value".into(),
            [Some(i128::MIN), Some(i128::MAX), Some(1), None].into_iter(),
        )
        .into_series();
        let values = series_to_values(&signed);
        assert_eq!(values[0].py_str(), i128::MIN.to_string());
        assert_eq!(values[1].py_str(), i128::MAX.to_string());
        let rebuilt = values_to_series("value", &values, &DataType::Int128).unwrap();
        assert_eq!(
            rebuilt.i128().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(i128::MIN), Some(i128::MAX), Some(1), None]
        );
        assert!(values_to_series("bad", &[Value::Int(-1)], &DataType::UInt64).is_none());
        assert!(
            values_to_series(
                "bad",
                &[Value::Decimal(Decimal::parse("1.5").unwrap())],
                &DataType::UInt64
            )
            .is_none()
        );
        assert!(
            values_to_series(
                "bad",
                &[Value::Decimal(
                    Decimal::parse("340282366920938463463374607431768211456").unwrap()
                )],
                &DataType::Int128
            )
            .is_none()
        );
    }
}
