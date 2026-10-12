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
        self.is_null_mask().iter().position(|n| !n)
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

/// A named column with an optional CRS (GeoPandas column CRS for geometry columns).
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data: ColumnData,
    pub crs: Option<Srid>,
}

impl Column {
    pub fn new(name: impl Into<String>, data: ColumnData) -> Self {
        Self {
            name: name.into(),
            data,
            crs: None,
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
            Some(c) => c.data = data,
            None => self.columns.push(Column::new(name, data)),
        }
        Ok(())
    }

    /// Append or replace a whole column (keeps the CRS given).
    pub fn set_column(&mut self, column: Column) -> FrameResult<()> {
        let crs = column.crs.clone();
        let name = column.name.clone();
        self.set(&name, column.data)?;
        if let Some(c) = self.column_mut(&name) {
            c.crs = crs;
        }
        Ok(())
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

    /// Wrap a Polars `DataFrame` (all columns native).
    pub fn from_polars(df: &DataFrame) -> Frame {
        let columns = df
            .get_columns()
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
        Ok(DataFrame::new(cols)?)
    }

    /// Describe the special (extension) type of each column, used for Arrow metadata.
    pub fn special_kinds(&self) -> IndexMap<String, SpecialKind> {
        let mut out = IndexMap::new();
        for c in &self.columns {
            if let ColumnData::Object(values) = &c.data {
                if let Some(kind) = special_kind_of(values) {
                    let kind = match kind {
                        SpecialKind::Geometry { .. } => {
                            SpecialKind::Geometry { crs: c.crs.clone() }
                        }
                        k => k,
                    };
                    out.insert(c.name.clone(), kind);
                }
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
            if let Some(kind) = kinds.get(field.name()) {
                metadata.insert("meerschaum:dtype".into(), kind.mrsm_dtype());
                match kind {
                    SpecialKind::Json => {
                        metadata.insert("ARROW:extension:name".into(), "arrow.json".into());
                        metadata.insert("ARROW:extension:metadata".into(), String::new());
                    }
                    SpecialKind::Geometry { crs } => {
                        metadata.insert("ARROW:extension:name".into(), "geoarrow.wkb".into());
                        let ext = match crs.as_ref().and_then(Srid::to_crs) {
                            Some(crs) => {
                                serde_json::json!({"crs": crs, "crs_type": "authority_code"})
                                    .to_string()
                            }
                            None => "{}".to_string(),
                        };
                        metadata.insert("ARROW:extension:metadata".into(), ext);
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
                        metadata.insert("ARROW:extension:name".into(), "arrow.uuid".into());
                        metadata.insert("ARROW:extension:metadata".into(), String::new());
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
    /// (`geoarrow.wkb`, `arrow.json`, `arrow.uuid`, `meerschaum:dtype`).
    pub fn from_record_batch(batch: &arrow::record_batch::RecordBatch) -> FrameResult<Frame> {
        use arrow::array::{
            Array, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
            StringArray,
        };
        use arrow::datatypes::{Field, Schema};

        let mut special: IndexMap<String, (String, Option<String>)> = IndexMap::new();
        let mut plain_fields = Vec::new();
        let mut plain_arrays = Vec::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let md = field.metadata();
            let ext = md.get("ARROW:extension:name").cloned();
            let mrsm = md.get("meerschaum:dtype").cloned();
            let kind = match (ext.as_deref(), mrsm.as_deref()) {
                (Some("geoarrow.wkb"), _) => Some("geometry".to_string()),
                (Some("arrow.json"), _) => Some("json".to_string()),
                (Some("arrow.uuid"), _) => Some("uuid".to_string()),
                (_, Some(d)) => Some(d.to_string()),
                _ => None,
            };
            if let Some(kind) = kind {
                special.insert(
                    field.name().clone(),
                    (kind, md.get("ARROW:extension:metadata").cloned()),
                );
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
            if let Some((kind, ext_md)) = special.get(name) {
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
                    None
                };
                let mut crs = None;
                for ix in 0..n {
                    let v = match kind.as_str() {
                        "geometry" => get_bytes(ix)
                            .and_then(|b| Geometry::from_wkb(&b).ok())
                            .map(Value::Geometry)
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
                if kind == "geometry" {
                    if let Some(md) = ext_md {
                        crs = serde_json::from_str::<serde_json::Value>(md)
                            .ok()
                            .and_then(|j| j.get("crs").and_then(|c| c.as_str().map(str::to_string)))
                            .map(
                                |c| match c.strip_prefix("EPSG:").and_then(|s| s.parse().ok()) {
                                    Some(i) => Srid::Int(i),
                                    None => Srid::Str(c),
                                },
                            );
                    }
                }
                let mut col = Column::object(name.clone(), values);
                col.crs = crs;
                columns.push(col);
            } else if let Some(df) = &native_df {
                let s = df.column(name)?.as_materialized_series().clone();
                columns.push(Column::native(s));
            }
        }
        let mut frame = Frame::new(columns)?;
        frame.height = batch.num_rows();
        Ok(frame)
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
            SpecialKind::Geometry { crs } => match crs.as_ref().filter(|c| c.is_truthy()) {
                Some(c) => format!("geometry[{c}]"),
                None => "geometry".into(),
            },
        }
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
        AnyValue::UInt64(v) => Value::Int(*v as i64),
        AnyValue::Int8(v) => Value::Int(*v as i64),
        AnyValue::Int16(v) => Value::Int(*v as i64),
        AnyValue::Int32(v) => Value::Int(*v as i64),
        AnyValue::Int64(v) => Value::Int(*v),
        AnyValue::Int128(v) => Value::Int(*v as i64),
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
        AnyValue::Decimal(v, scale) => Value::Decimal(Decimal::from_i128_scale(*v, *scale as i64)),
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
        | DataType::UInt32
        | DataType::UInt64 => {
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
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Str(s) => Some(s.clone()),
                    _ => return None,
                });
            }
            StringChunked::from_iter_options(name, out.into_iter()).into_series()
        }
        DataType::Binary => {
            let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(values.len());
            for v in values {
                out.push(match v {
                    _ if !nn(v) => None,
                    Value::Bytes(b) => Some(b.clone()),
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
            let scale = s.unwrap_or(0);
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
                let mut max_scale = 0i64;
                let mut max_int = 1i64;
                let mut finite = true;
                for d in decimals.iter().flatten() {
                    match d.precision_scale() {
                        Some((p, s)) => {
                            max_scale = max_scale.max(s);
                            max_int = max_int.max(p - s);
                        }
                        None => finite = false,
                    }
                }
                if finite && max_int + max_scale <= 38 {
                    let ints: Vec<Option<i128>> = decimals
                        .iter()
                        .map(|d| d.and_then(|d| d.to_i128_at_scale(max_scale)))
                        .collect();
                    return Int128Chunked::from_iter_options(pname, ints.into_iter())
                        .into_decimal_unchecked(
                            Some((max_int + max_scale) as usize),
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
