//! Pandas dtype strings and their Polars equivalents: `to_pandas_dtype`, the pandas / pyarrow
//! dtype normalization, and the Polars ↔ pandas type mapping.
//!
//! The Meerschaum dtype vocabulary (`MRSM_ALIAS_DTYPES`, `MRSM_PD_DTYPES`, `are_dtypes_equal`,
//! `is_dtype_numeric`, `is_dtype_special`, `get_geometry_type_srid`, precision units) and the
//! database → pandas mapping (`get_pd_type_from_db_type`, `get_numeric_precision_scale`) are
//! owned by [`meerschaum_core::dtypes`] and re-exported here.

use polars::prelude::{DataType, TimeUnit};

pub use meerschaum_core::dtypes::{
    GeometryParseError, MRSM_ALIAS_DTYPES, MRSM_PD_DTYPES, PrecisionUnit, Srid, alias_dtype,
    are_dtype_items_equal as are_dtypes_maps_equal, are_dtypes_equal, get_geometry_type_srid,
    get_next_precision_unit, get_numeric_precision_scale, get_pd_type_from_db_type,
    is_dtype_numeric, is_dtype_special, pd_dtype as mrsm_pd_dtype, resolve_alias,
};
pub use meerschaum_core::pyfmt::{is_int as py_is_int, py_isupper};

/// The Polars time unit of a precision unit (sub-second units only).
pub fn precision_time_unit(unit: PrecisionUnit) -> Option<TimeUnit> {
    match unit {
        PrecisionUnit::Nanosecond => Some(TimeUnit::Nanoseconds),
        PrecisionUnit::Microsecond => Some(TimeUnit::Microseconds),
        PrecisionUnit::Millisecond => Some(TimeUnit::Milliseconds),
        _ => None,
    }
}

/// `to_pandas_dtype()`: cast a Meerschaum dtype to a pandas dtype string.
pub fn to_pandas_dtype(dtype: &str) -> String {
    if let Some(known) = mrsm_pd_dtype(dtype) {
        return known.to_string();
    }
    if let Some(alias) = alias_dtype(dtype) {
        return mrsm_pd_dtype(alias).unwrap_or("object").to_string();
    }
    if dtype.starts_with("numeric")
        || dtype.starts_with("geometry")
        || dtype.starts_with("geography")
    {
        return "object".to_string();
    }
    // PARITY: "if the first word of the given dtype is in all caps, treat it as a SQL db type"
    // (Python `str.isupper()`: at least one cased character and no lowercase ones).
    let first_word = dtype.split(' ').next().unwrap_or("");
    if py_isupper(first_word) {
        return get_pd_type_from_db_type(dtype, true);
    }
    normalize_pandas_dtype(dtype).unwrap_or_else(|| "object".to_string())
}

/// Emulates `str(pandas.api.types.pandas_dtype(dtype))` for the dtype strings pandas accepts;
/// `None` where pandas raises.
pub fn normalize_pandas_dtype(dtype: &str) -> Option<String> {
    let d = dtype;
    let simple = match d {
        "float" | "float64" | "double" | "f8" | "float_" => Some("float64"),
        "float32" | "single" | "f4" => Some("float32"),
        "float16" | "half" | "f2" => Some("float16"),
        "int" | "int64" | "i8" | "long" => Some("int64"),
        "int32" | "i4" | "intc" => Some("int32"),
        "int16" | "i2" | "short" => Some("int16"),
        "int8" | "i1" | "byte" => Some("int8"),
        "uint" | "uint64" | "u8" => Some("uint64"),
        "uint32" | "u4" => Some("uint32"),
        "uint16" | "u2" => Some("uint16"),
        "uint8" | "u1" | "ubyte" => Some("uint8"),
        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64"
        | "Float32" | "Float64" => Some(d),
        "boolean" => Some("boolean"),
        "bool" | "bool_" | "?" => Some("bool"),
        "string" | "string[python]" | "string[pyarrow]" => Some("string"),
        "str" => Some("str"),
        "object" | "O" => Some("object"),
        "category" => Some("category"),
        "datetime64" | "M8" => Some("datetime64"),
        "timedelta64" | "m8" => Some("timedelta64"),
        "bytes" | "S" => Some("bytes"),
        _ => None,
    };
    if let Some(s) = simple {
        return Some(s.to_string());
    }
    if let Some(inner) = d.strip_suffix("[pyarrow]") {
        if inner == "string" || inner == "str" {
            return Some("string".to_string());
        }
        return normalize_pyarrow_type(inner).map(|t| format!("{t}[pyarrow]"));
    }
    for prefix in ["datetime64", "timedelta64"] {
        if let Some(rest) = d.strip_prefix(prefix) {
            let rest = rest.strip_prefix('[')?.strip_suffix(']')?;
            let mut parts = rest.splitn(2, ',');
            let unit = parts.next()?.trim();
            if !matches!(unit, "s" | "ms" | "us" | "ns") {
                return None;
            }
            return match parts.next() {
                None => Some(format!("{prefix}[{unit}]")),
                Some(tz) if prefix == "datetime64" => {
                    let tz = tz.trim();
                    crate::value::TzInfo::parse(tz)?;
                    Some(format!("datetime64[{unit}, {tz}]"))
                }
                Some(_) => None,
            };
        }
    }
    None
}

fn normalize_pyarrow_type(t: &str) -> Option<String> {
    Some(
        match t {
            "double" | "float64" => "double",
            "float" | "float32" => "float",
            "halffloat" | "float16" => "halffloat",
            "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16" | "uint32" | "uint64" => t,
            "bool" | "boolean" => "bool",
            "large_string" | "large_str" | "large_utf8" => "large_string",
            "utf8" => "string",
            "string_view" => "string_view",
            "binary" => "binary",
            "large_binary" => "large_binary",
            "binary_view" => "binary_view",
            "date32" | "date32[day]" => "date32[day]",
            "date64" | "date64[ms]" => "date64[ms]",
            "null" => "null",
            _ => {
                if let Some(rest) = t.strip_prefix("timestamp[") {
                    let rest = rest.strip_suffix(']')?;
                    let mut parts = rest.splitn(2, ',');
                    let unit = parts.next()?.trim();
                    if !matches!(unit, "s" | "ms" | "us" | "ns") {
                        return None;
                    }
                    return Some(match parts.next() {
                        None => format!("timestamp[{unit}]"),
                        Some(tz) => {
                            let tz = tz.trim().strip_prefix("tz=").unwrap_or(tz.trim());
                            format!("timestamp[{unit}, tz={tz}]")
                        }
                    });
                }
                return None;
            }
        }
        .to_string(),
    )
}

fn time_unit_abbrev(tu: TimeUnit) -> &'static str {
    match tu {
        TimeUnit::Nanoseconds => "ns",
        TimeUnit::Microseconds => "us",
        TimeUnit::Milliseconds => "ms",
    }
}

/// The pandas dtype string the kernel reports for a native Polars column.
///
/// Polars has no notion of NumPy- vs Arrow-backed columns, so integers, floats and booleans use
/// the NumPy names (which `are_dtypes_equal` treats as equal to the `[pyarrow]` and nullable
/// names) and the remaining types use the names pandas gives Arrow-backed columns.
pub fn pandas_dtype_of(dtype: &DataType) -> String {
    match dtype {
        DataType::Boolean => "bool".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::Float32 => "float32".into(),
        DataType::Float64 => "float64".into(),
        DataType::String => "string".into(),
        DataType::Binary | DataType::BinaryOffset => "binary[pyarrow]".into(),
        DataType::Date => "date32[day][pyarrow]".into(),
        DataType::Datetime(tu, tz) => match tz {
            Some(tz) => format!("datetime64[{}, {}]", time_unit_abbrev(*tu), tz.as_str()),
            None => format!("datetime64[{}]", time_unit_abbrev(*tu)),
        },
        DataType::Duration(tu) => format!("timedelta64[{}]", time_unit_abbrev(*tu)),
        DataType::Time => "time64[ns][pyarrow]".into(),
        DataType::Decimal(p, s) => format!(
            "decimal128({}, {})[pyarrow]",
            p.unwrap_or(38),
            s.unwrap_or(0)
        ),
        DataType::Categorical(..) | DataType::Enum(..) => "category".into(),
        _ => "object".into(),
    }
}

/// The Polars type for a pandas dtype string, where one exists.
pub fn polars_dtype_for_pandas(pd_dtype: &str) -> Option<DataType> {
    let d = pd_dtype.trim();
    let base = d.strip_suffix("[pyarrow]").unwrap_or(d);
    Some(match base.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" | "int" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" | "float" if base != "float" || d.ends_with("[pyarrow]") => DataType::Float32,
        "float64" | "double" | "float" => DataType::Float64,
        "string" | "str" | "large_string" | "string_view" | "utf8" => DataType::String,
        "binary" | "large_binary" | "binary_view" | "bytes" => DataType::Binary,
        "date32[day]" | "date32" | "date64[ms]" | "date64" | "date" => DataType::Date,
        _ => {
            if let Some((tu, tz)) = parse_datetime_dtype(d) {
                let tz = tz.and_then(|t| {
                    polars::prelude::TimeZone::opt_try_new(Some(t))
                        .ok()
                        .flatten()
                });
                return Some(DataType::Datetime(tu.unwrap_or(TimeUnit::Microseconds), tz));
            }
            return None;
        }
    })
}

/// Parse a pandas/pyarrow datetime dtype string into `(unit, tz)`.
///
/// Units coarser than milliseconds (`s`) are not representable in Polars and map to `ms`.
pub fn parse_datetime_dtype(dtype: &str) -> Option<(Option<TimeUnit>, Option<String>)> {
    let d = dtype.strip_suffix("[pyarrow]").unwrap_or(dtype);
    let rest = d
        .strip_prefix("datetime64")
        .or_else(|| d.strip_prefix("timestamp"))?;
    if rest.is_empty() {
        return Some((None, None));
    }
    let inner = rest.strip_prefix('[')?.strip_suffix(']')?;
    let mut parts = inner.splitn(2, ',');
    let unit = parts.next()?.trim();
    let tu = match unit {
        "ns" => TimeUnit::Nanoseconds,
        "us" => TimeUnit::Microseconds,
        "ms" | "s" => TimeUnit::Milliseconds,
        _ => return None,
    };
    let tz = parts
        .next()
        .map(|t| t.trim().strip_prefix("tz=").unwrap_or(t.trim()).to_string());
    Some((Some(tu), tz))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_type_srid() {
        let g = |d: &str| get_geometry_type_srid(d, "geometry", Srid::Code(0)).unwrap();
        assert_eq!(
            g("geometry[Point, 4326]"),
            ("Point".into(), Srid::Code(4326))
        );
        assert_eq!(g("geometry[0, Point]"), ("Point".into(), Srid::Code(0)));
        assert_eq!(
            g("geometry[POINT, ESRI:102003]"),
            ("POINT".into(), Srid::Authority("ESRI:102003".into()))
        );
        assert!(get_geometry_type_srid("geometry[4326.0]", "geometry", Srid::Code(0)).is_err());
    }

    #[test]
    fn db_types() {
        assert_eq!(
            get_pd_type_from_db_type("GEOGRAPHY(POINT, 4326)", true),
            "int64[pyarrow]"
        );
        assert_eq!(
            get_pd_type_from_db_type("GEOMETRY", true),
            "geometry[geometry]"
        );
        assert_eq!(
            get_pd_type_from_db_type("NUMERIC(10, 2)", true),
            "numeric[10,2]"
        );
    }
}
