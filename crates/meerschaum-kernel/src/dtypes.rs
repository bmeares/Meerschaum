//! Meerschaum dtype strings, pandas dtype strings and their Polars equivalents.
//!
//! Ports the string-level helpers of `meerschaum/utils/dtypes/__init__.py` (`to_pandas_dtype`,
//! `are_dtypes_equal`, `is_dtype_numeric`, `is_dtype_special`, `get_geometry_type_srid`,
//! precision units) and the dtype parts of `meerschaum/utils/dtypes/sql.py`
//! (`get_numeric_precision_scale`, `get_pd_type_from_db_type`).
//!
//! TODO(core-merge): the Meerschaum dtype-string types (aliases, precision units, geometry
//! modifiers) should come from `meerschaum_core` once that crate lands; they are defined locally
//! so the kernel can be built and tested independently.

use polars::prelude::{DataType, TimeUnit};

/// `MRSM_ALIAS_DTYPES`.
pub fn alias_dtype(dtype: &str) -> Option<&'static str> {
    Some(match dtype {
        "decimal" | "Decimal" | "number" => "numeric",
        "jsonl" | "JSON" => "json",
        "binary" | "blob" | "varbinary" | "bytea" => "bytes",
        "guid" | "UUID" => "uuid",
        "geom" => "geometry",
        "geog" => "geography",
        "boolean" => "bool",
        "day" => "date",
        _ => return None,
    })
}

/// `MRSM_ALIAS_DTYPES.get(dtype, dtype)`.
pub fn resolve_alias(dtype: &str) -> &str {
    alias_dtype(dtype).unwrap_or(dtype)
}

/// `MRSM_PD_DTYPES`.
pub fn mrsm_pd_dtype(dtype: &str) -> Option<&'static str> {
    Some(match dtype {
        "json" | "numeric" | "geometry" | "geography" | "uuid" => "object",
        "date" => "date32[day][pyarrow]",
        "datetime" => "datetime64[us, UTC]",
        "bool" => "bool[pyarrow]",
        "int" | "int64" => "int64[pyarrow]",
        "int8" => "int8[pyarrow]",
        "int16" => "int16[pyarrow]",
        "int32" => "int32[pyarrow]",
        "str" => "string",
        "bytes" => "binary[pyarrow]",
        _ => return None,
    })
}

/// A datetime precision unit (`MRSM_PRECISION_UNITS_SCALARS` keys).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PrecisionUnit {
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl PrecisionUnit {
    /// Resolve a unit or alias (`MRSM_PRECISION_UNITS_ALIASES`).
    pub fn parse(unit: &str) -> Option<Self> {
        Some(match unit {
            "ns" | "nanosecond" => Self::Nanosecond,
            "us" | "microsecond" => Self::Microsecond,
            "ms" | "millisecond" => Self::Millisecond,
            "s" | "sec" | "second" => Self::Second,
            "m" | "min" | "minute" => Self::Minute,
            "h" | "hr" | "hour" => Self::Hour,
            "d" | "D" | "day" => Self::Day,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Nanosecond => "nanosecond",
            Self::Microsecond => "microsecond",
            Self::Millisecond => "millisecond",
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
        }
    }

    /// `MRSM_PRECISION_UNITS_ABBREVIATIONS`.
    pub fn abbreviation(self) -> &'static str {
        match self {
            Self::Nanosecond => "ns",
            Self::Microsecond => "us",
            Self::Millisecond => "ms",
            Self::Second => "s",
            Self::Minute => "min",
            Self::Hour => "hr",
            Self::Day => "D",
        }
    }

    /// `MRSM_PRECISION_UNITS_SCALARS` (units per second).
    pub fn scalar(self) -> f64 {
        match self {
            Self::Nanosecond => 1_000_000_000.0,
            Self::Microsecond => 1_000_000.0,
            Self::Millisecond => 1000.0,
            Self::Second => 1.0,
            Self::Minute => 1.0 / 60.0,
            Self::Hour => 1.0 / 3600.0,
            Self::Day => 1.0 / 86400.0,
        }
    }

    /// Microseconds in one unit (for `timedelta(**{unit + 's': 1})`); `None` for nanoseconds.
    pub fn micros(self) -> Option<i64> {
        Some(match self {
            Self::Nanosecond => return None,
            Self::Microsecond => 1,
            Self::Millisecond => 1000,
            Self::Second => 1_000_000,
            Self::Minute => 60_000_000,
            Self::Hour => 3_600_000_000,
            Self::Day => 86_400_000_000,
        })
    }

    pub fn time_unit(self) -> Option<TimeUnit> {
        match self {
            Self::Nanosecond => Some(TimeUnit::Nanoseconds),
            Self::Microsecond => Some(TimeUnit::Microseconds),
            Self::Millisecond => Some(TimeUnit::Milliseconds),
            _ => None,
        }
    }
}

/// `get_next_precision_unit()`.
pub fn get_next_precision_unit(unit: &str, decrease: bool) -> Result<&'static str, String> {
    let resolved =
        PrecisionUnit::parse(unit).ok_or_else(|| format!("Invalid precision unit '{unit}'."))?;
    // Sorted by scalar ascending: day, hour, minute, second, millisecond, microsecond, nanosecond.
    let order = [
        PrecisionUnit::Day,
        PrecisionUnit::Hour,
        PrecisionUnit::Minute,
        PrecisionUnit::Second,
        PrecisionUnit::Millisecond,
        PrecisionUnit::Microsecond,
        PrecisionUnit::Nanosecond,
    ];
    let ix = order
        .iter()
        .position(|u| *u == resolved)
        .expect("unit in order") as i64;
    let new_ix = ix + if decrease { -1 } else { 1 };
    if new_ix < 0 || new_ix >= order.len() as i64 {
        return Err(format!(
            "No precision {} '{unit}'.",
            if decrease { "below" } else { "above" }
        ));
    }
    Ok(order[new_ix as usize].name())
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

/// Python's `str.isupper()`.
pub fn py_isupper(s: &str) -> bool {
    let mut cased = false;
    for c in s.chars() {
        if c.is_lowercase() {
            return false;
        }
        if c.is_uppercase() {
            cased = true;
        }
    }
    cased
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

/// `are_dtypes_equal()` for two dtype strings.
pub fn are_dtypes_equal(ldtype: &str, rdtype: &str) -> bool {
    if ldtype == rdtype {
        return true;
    }
    let l = ldtype.split('[').next().unwrap_or("");
    let r = rdtype.split('[').next().unwrap_or("");
    let l = resolve_alias(l);
    let r = resolve_alias(r);

    let json = ["json", "object"];
    if json.contains(&l) && json.contains(&r) {
        return true;
    }
    let numeric = ["numeric", "decimal", "object"];
    if (numeric.contains(&l) || l.starts_with("decimal"))
        && (numeric.contains(&r) || r.starts_with("decimal"))
    {
        return true;
    }
    let uuid = ["uuid", "object"];
    if uuid.contains(&l) && uuid.contains(&r) {
        return true;
    }
    let bytes = ["bytes", "object", "binary", "large_binary"];
    if bytes.contains(&l) && bytes.contains(&r) {
        return true;
    }
    let geometry = ["geometry", "object", "geography"];
    if geometry.contains(&l) && geometry.contains(&r) {
        return true;
    }
    let ll = l.to_lowercase();
    let rl = r.to_lowercase();
    if ll == rl {
        return true;
    }
    let dt_prefixes = ["datetime", "timestamp"];
    if dt_prefixes.iter().any(|p| ll.contains(p)) && dt_prefixes.iter().any(|p| rl.contains(p)) {
        return true;
    }
    let strings = ["str", "string", "large_string", "string_view", "object"];
    if strings.contains(&l) && strings.contains(&r) {
        return true;
    }
    let ints = [
        "int", "int64", "int32", "int16", "int8", "uint", "uint64", "uint32", "uint16", "uint8",
    ];
    if ints.contains(&ll.as_str()) && ints.contains(&rl.as_str()) {
        return true;
    }
    if ll.contains("int") && rl.contains("int") {
        return true;
    }
    let floats = [
        "float", "float64", "float32", "float16", "float128", "double",
    ];
    if floats.contains(&ll.as_str()) && floats.contains(&rl.as_str()) {
        return true;
    }
    let bools = ["bool", "boolean"];
    if bools.contains(&l) && bools.contains(&r) {
        return true;
    }
    // PARITY: the bracketed entries can never match after the `split('[')` above.
    let dates = [
        "date",
        "date32",
        "date32[pyarrow]",
        "date32[day][pyarrow]",
        "date64",
        "date64[pyarrow]",
        "date64[ms][pyarrow]",
    ];
    if dates.contains(&l) && dates.contains(&r) {
        return true;
    }
    false
}

/// `are_dtypes_equal()` for two dtypes dictionaries.
///
/// PARITY: keys are sorted and `zip`ped, so extra keys on either side are ignored
/// (`{'a': 'int64'}` equals `{'a': 'int64', 'b': 'float64'}`).
pub fn are_dtypes_maps_equal<'a>(
    left: impl IntoIterator<Item = (&'a str, &'a str)>,
    right: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> bool {
    let mut l: Vec<(&str, &str)> = left.into_iter().collect();
    let mut r: Vec<(&str, &str)> = right.into_iter().collect();
    l.sort_by(|a, b| a.0.cmp(b.0));
    r.sort_by(|a, b| a.0.cmp(b.0));
    for ((lk, lt), (rk, rt)) in l.iter().zip(r.iter()) {
        if lk != rk {
            return false;
        }
        if !are_dtypes_equal(lt, rt) {
            return false;
        }
    }
    true
}

/// `is_dtype_numeric()`.
pub fn is_dtype_numeric(dtype: &str) -> bool {
    let lower = dtype.to_lowercase();
    ["numeric", "float", "double", "int"]
        .iter()
        .any(|s| lower.contains(s))
}

/// `is_dtype_special()`.
pub fn is_dtype_special(type_: &str) -> bool {
    let t = resolve_alias(type_);
    if matches!(
        t,
        "uuid"
            | "json"
            | "bytes"
            | "numeric"
            | "datetime"
            | "geometry"
            | "geography"
            | "date"
            | "bool"
    ) {
        return true;
    }
    are_dtypes_equal(t, "datetime")
        || are_dtypes_equal(t, "date")
        || t.starts_with("numeric")
        || t.starts_with("bool")
        || t.starts_with("geometry")
        || t.starts_with("geography")
}

/// An SRID as parsed from a geometry dtype: an integer or an authority string (`ESRI:102003`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Srid {
    Int(i64),
    Str(String),
}

impl Srid {
    /// Python truthiness (`0` and `''` are falsy).
    pub fn is_truthy(&self) -> bool {
        match self {
            Srid::Int(i) => *i != 0,
            Srid::Str(s) => !s.is_empty(),
        }
    }

    /// The CRS string used in GeoArrow metadata (`EPSG:4326`, `ESRI:102003`).
    pub fn to_crs(&self) -> Option<String> {
        if !self.is_truthy() {
            return None;
        }
        let s = match self {
            Srid::Int(i) => i.to_string(),
            Srid::Str(s) => s.clone(),
        };
        Some(if s.contains(':') {
            s
        } else {
            format!("EPSG:{s}")
        })
    }

    /// The integer EPSG code, if any.
    pub fn epsg(&self) -> Option<i64> {
        match self {
            Srid::Int(i) => Some(*i),
            Srid::Str(s) => s
                .strip_prefix("EPSG:")
                .or_else(|| s.strip_prefix("epsg:"))
                .and_then(|c| c.parse().ok()),
        }
    }
}

impl std::fmt::Display for Srid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Srid::Int(i) => write!(f, "{i}"),
            Srid::Str(s) => f.write_str(s),
        }
    }
}

/// `meerschaum.utils.misc.is_int()`: `float(s).is_integer()`.
pub fn py_is_int(s: &str) -> bool {
    let t = s.trim();
    let lower = t.to_ascii_lowercase();
    if lower.contains("inf") || lower.contains("nan") {
        return false;
    }
    t.replace('_', "")
        .parse::<f64>()
        .map(|f| f.fract() == 0.0)
        .unwrap_or(false)
}

/// `get_geometry_type_srid()`.
///
/// PARITY: an integral-but-not-int-literal SRID such as `4326.0` passes `is_int` and then
/// fails `int(part)`, raising `ValueError` in v4; this returns `Err` in that case.
pub fn get_geometry_type_srid(
    dtype: &str,
    default_type: &str,
    default_srid: Srid,
) -> Result<(String, Srid), String> {
    let dtype = dtype.replace('(', "[").replace(')', "]");
    let bare = dtype.split('[').next().unwrap_or("").to_string();
    let modifier = if bare.is_empty() {
        dtype.clone()
    } else {
        dtype
            .splitn(2, bare.as_str())
            .last()
            .unwrap_or("")
            .to_string()
    };
    let modifier = modifier.trim_start_matches('[').trim_end_matches(']');
    if modifier.is_empty() {
        return Ok((default_type.to_string(), default_srid));
    }
    let mut parts: Vec<Srid> = Vec::new();
    for part in modifier.split(',') {
        let p = part.rsplit('=').next().unwrap_or("").trim();
        if py_is_int(p) {
            let i: i64 = p.parse().map_err(|_| {
                format!("ValueError: invalid literal for int() with base 10: '{p}'")
            })?;
            parts.push(Srid::Int(i));
        } else {
            parts.push(Srid::Str(p.to_string()));
        }
    }
    let mut srid = default_srid;
    let mut geometry_type = default_type.to_string();
    for part in &parts {
        match part {
            Srid::Int(_) => {
                srid = part.clone();
                break;
            }
            Srid::Str(s) if s.contains(':') => {
                srid = part.clone();
                break;
            }
            _ => {}
        }
    }
    for part in &parts {
        if let Srid::Str(s) = part {
            if *part != srid {
                geometry_type = s.clone();
                break;
            }
        }
    }
    Ok((geometry_type, srid))
}

/// `get_numeric_precision_scale(None, dtype)`.
pub fn get_numeric_precision_scale(dtype: Option<&str>) -> (Option<i64>, Option<i64>) {
    let Some(dtype) = dtype else {
        return (None, None);
    };
    if dtype.is_empty() {
        return (None, None);
    }
    let (lb, rb) = if dtype.contains('[') {
        ('[', ']')
    } else {
        ('(', ')')
    };
    if dtype.contains(lb) && dtype.matches(',').count() == 1 && dtype.ends_with(rb) {
        let inner = dtype.split_once(lb).map(|x| x.1).unwrap_or("");
        let inner = inner.trim_end_matches(rb);
        let mut parts = inner.splitn(2, ',');
        let p = parts.next().map(|s| s.trim().parse::<i64>());
        let s = parts.next().map(|s| s.trim().parse::<i64>());
        if let (Some(Ok(p)), Some(Ok(s))) = (p, s) {
            return (Some(p), Some(s));
        }
    }
    (None, None)
}

/// `DB_TO_PD_DTYPES` exact matches (keyed by the upper-cased type before `(`).
fn db_to_pd_exact(key: &str) -> Option<&'static str> {
    Some(match key {
        "FLOAT" | "REAL" | "DOUBLE_PRECISION" | "DOUBLE" => "float64[pyarrow]",
        "DECIMAL" | "NUMBER" | "NUMERIC" => "numeric",
        "BIGINT" => "int64[pyarrow]",
        "INT" | "INTEGER" => "int32[pyarrow]",
        "GEOMETRY" | "GEOMETRY(GEOMETRY)" => "geometry",
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIMEZONE" | "DATETIME" => "datetime64[us]",
        "TIMESTAMP WITH TIMEZONE" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
            "datetime64[us, UTC]"
        }
        "DATE" => "date",
        "DATETIME2" | "DATETIME2(6)" => "datetime64[us]",
        "DATETIME2(3)" => "datetime64[ms]",
        "DATETIME2(0)" => "datetime64[s]",
        "DATETIMEOFFSET" | "DATETIMEOFFSET(6)" => "datetime64[us, UTC]",
        "DATETIMEOFFSET(3)" => "datetime64[ms, UTC]",
        "DATETIMEOFFSET(0)" => "datetime64[s, UTC]",
        "TEXT" | "VARCHAR" | "CLOB" => "string[pyarrow]",
        "BOOL" | "BOOLEAN" | "BOOLEAN()" | "TINYINT" | "TINYINT(1)" | "BIT" | "BIT(1)" => {
            "bool[pyarrow]"
        }
        "JSON" | "JSONB" => "json",
        "UUID" | "UNIQUEIDENTIFIER" => "uuid",
        "BYTEA" | "BLOB" | "VARBINARY" | "VARBINARY(MAX)" => "bytes",
        _ => return None,
    })
}

/// `DB_TO_PD_DTYPES['substrings']` in insertion order (order is observable).
const DB_TO_PD_SUBSTRINGS: &[(&str, &str)] = &[
    ("CHAR", "string[pyarrow]"),
    ("TIMESTAMP", "datetime64[us]"),
    ("TIME", "datetime64[us]"),
    ("DATE", "date"),
    ("DOUBLE", "double[pyarrow]"),
    ("DECIMAL", "numeric"),
    ("NUMERIC", "numeric"),
    ("NUMBER", "numeric"),
    ("INT", "int64[pyarrow]"),
    ("BOOL", "bool[pyarrow]"),
    ("JSON", "json"),
    ("BYTE", "bytes"),
    ("LOB", "bytes"),
    ("BINARY", "bytes"),
    ("GEOMETRY", "geometry"),
    ("GEOGRAPHY", "geography"),
];

/// `get_pd_type_from_db_type()`.
///
/// PARITY: substring matching follows dict order, so `GEOGRAPHY(POINT, 4326)` matches `INT`
/// (inside `POINT`) and maps to `int64[pyarrow]`; `GEOMETRY` maps to `geometry[geometry]`.
pub fn get_pd_type_from_db_type(db_type: &str, allow_custom_dtypes: bool) -> String {
    let parse_custom = |pd_type: &str, db_t: &str| -> String {
        if db_t.to_lowercase().contains("json") {
            return "json".to_string();
        }
        if are_dtypes_equal(pd_type, "numeric") && pd_type != "object" {
            if let (Some(p), Some(s)) = get_numeric_precision_scale(Some(&db_t.to_uppercase())) {
                if p != 0 && s != 0 {
                    return format!("numeric[{p},{s}]");
                }
            }
        }
        if are_dtypes_equal(pd_type, "geometry") && pd_type != "object" {
            let (geometry_type, srid) =
                get_geometry_type_srid(&db_t.to_uppercase(), "geometry", Srid::Int(0))
                    .unwrap_or(("geometry".to_string(), Srid::Int(0)));
            let mut modifiers = Vec::new();
            if !geometry_type.is_empty() {
                modifiers.push(geometry_type);
            }
            if srid.is_truthy() {
                modifiers.push(srid.to_string());
            }
            let typ = if pd_type.to_lowercase().contains("geography") {
                "geography"
            } else {
                "geometry"
            };
            if modifiers.is_empty() {
                return typ.to_string();
            }
            return format!("{typ}[{}]", modifiers.join(", "));
        }
        pd_type.to_string()
    };
    let upper = db_type.to_uppercase();
    let key = upper.split('(').next().unwrap_or("").trim();
    if let Some(pd_type) = db_to_pd_exact(key) {
        return if allow_custom_dtypes {
            parse_custom(pd_type, db_type)
        } else {
            pd_type.to_string()
        };
    }
    for (db_t, pd_t) in DB_TO_PD_SUBSTRINGS {
        if upper.contains(db_t) {
            return if allow_custom_dtypes {
                parse_custom(pd_t, db_t)
            } else {
                pd_t.to_string()
            };
        }
    }
    "object".to_string()
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
        let g = |d: &str| get_geometry_type_srid(d, "geometry", Srid::Int(0)).unwrap();
        assert_eq!(
            g("geometry[Point, 4326]"),
            ("Point".into(), Srid::Int(4326))
        );
        assert_eq!(g("geometry[0, Point]"), ("Point".into(), Srid::Int(0)));
        assert_eq!(
            g("geometry[POINT, ESRI:102003]"),
            ("POINT".into(), Srid::Str("ESRI:102003".into()))
        );
        assert!(get_geometry_type_srid("geometry[4326.0]", "geometry", Srid::Int(0)).is_err());
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
