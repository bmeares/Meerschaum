//! The Meerschaum dtype vocabulary: the parsing side of `meerschaum/utils/dtypes/__init__.py`.
//!
//! - [`MRSM_ALIAS_DTYPES`], [`MRSM_PD_DTYPES`] and the precision-unit tables.
//! - [`are_dtypes_equal`] and [`is_dtype_special`] / [`is_dtype_numeric`].
//! - [`GeometrySpec`]: the `geometry[...]` / `geography[...]` parser
//!   (`get_geometry_type_srid`) and its canonical string.
//! - [`MrsmDtype`]: a classification of a dtype string.
//! - [`PrecisionUnit`]: `precision` units and their aliases.
//!
//! Enforcement (casting data) lives in `meerschaum-kernel`.

use std::fmt;

use crate::pyfmt::{is_int, parse_py_float, parse_py_int};

/// `MRSM_ALIAS_DTYPES`: alias -> Meerschaum dtype.
pub const MRSM_ALIAS_DTYPES: &[(&str, &str)] = &[
    ("decimal", "numeric"),
    ("Decimal", "numeric"),
    ("number", "numeric"),
    ("jsonl", "json"),
    ("JSON", "json"),
    ("binary", "bytes"),
    ("blob", "bytes"),
    ("varbinary", "bytes"),
    ("bytea", "bytes"),
    ("guid", "uuid"),
    ("UUID", "uuid"),
    ("geom", "geometry"),
    ("geog", "geography"),
    ("boolean", "bool"),
    ("day", "date"),
];

/// `MRSM_PD_DTYPES`: Meerschaum dtype -> pandas dtype (the `None` key is omitted).
pub const MRSM_PD_DTYPES: &[(&str, &str)] = &[
    ("json", "object"),
    ("numeric", "object"),
    ("geometry", "object"),
    ("geography", "object"),
    ("uuid", "object"),
    ("date", "date32[day][pyarrow]"),
    ("datetime", "datetime64[us, UTC]"),
    ("bool", "bool[pyarrow]"),
    ("int", "int64[pyarrow]"),
    ("int8", "int8[pyarrow]"),
    ("int16", "int16[pyarrow]"),
    ("int32", "int32[pyarrow]"),
    ("int64", "int64[pyarrow]"),
    ("str", "string"),
    ("bytes", "binary[pyarrow]"),
];

/// `MRSM_PRECISION_UNITS_ALIASES`: alias -> precision unit.
pub const MRSM_PRECISION_UNITS_ALIASES: &[(&str, &str)] = &[
    ("ns", "nanosecond"),
    ("us", "microsecond"),
    ("ms", "millisecond"),
    ("s", "second"),
    ("sec", "second"),
    ("m", "minute"),
    ("min", "minute"),
    ("h", "hour"),
    ("hr", "hour"),
    ("d", "day"),
    ("D", "day"),
];

/// Look up an alias in [`MRSM_ALIAS_DTYPES`].
pub fn alias_dtype(dtype: &str) -> Option<&'static str> {
    MRSM_ALIAS_DTYPES
        .iter()
        .find(|(a, _)| *a == dtype)
        .map(|(_, t)| *t)
}

/// `MRSM_ALIAS_DTYPES.get(dtype, dtype)`.
pub fn resolve_alias(dtype: &str) -> &str {
    alias_dtype(dtype).unwrap_or(dtype)
}

/// Look up a Meerschaum dtype in [`MRSM_PD_DTYPES`].
pub fn pd_dtype(dtype: &str) -> Option<&'static str> {
    MRSM_PD_DTYPES
        .iter()
        .find(|(a, _)| *a == dtype)
        .map(|(_, t)| *t)
}

/// Look up an alias in [`MRSM_PRECISION_UNITS_ALIASES`].
pub fn precision_unit_alias(unit: &str) -> Option<&'static str> {
    MRSM_PRECISION_UNITS_ALIASES
        .iter()
        .find(|(a, _)| *a == unit)
        .map(|(_, t)| *t)
}

/// Port of `are_dtypes_equal` for dtype strings.
pub fn are_dtypes_equal(ldtype: &str, rdtype: &str) -> bool {
    if ldtype == rdtype {
        return true;
    }
    let l = ldtype.split('[').next().unwrap_or(ldtype);
    let r = rdtype.split('[').next().unwrap_or(rdtype);
    let l = resolve_alias(l);
    let r = resolve_alias(r);

    let in_ = |x: &str, set: &[&str]| set.contains(&x);

    let json_dtypes = ["json", "object"];
    if in_(l, &json_dtypes) && in_(r, &json_dtypes) {
        return true;
    }
    let numeric_dtypes = ["numeric", "decimal", "object"];
    if (in_(l, &numeric_dtypes) || l.starts_with("decimal"))
        && (in_(r, &numeric_dtypes) || r.starts_with("decimal"))
    {
        return true;
    }
    let uuid_dtypes = ["uuid", "object"];
    if in_(l, &uuid_dtypes) && in_(r, &uuid_dtypes) {
        return true;
    }
    let bytes_dtypes = ["bytes", "object", "binary", "large_binary"];
    if in_(l, &bytes_dtypes) && in_(r, &bytes_dtypes) {
        return true;
    }
    let geometry_dtypes = ["geometry", "object", "geography"];
    if in_(l, &geometry_dtypes) && in_(r, &geometry_dtypes) {
        return true;
    }
    let (ll, rl) = (l.to_lowercase(), r.to_lowercase());
    if ll == rl {
        return true;
    }
    let datetime_prefixes = ["datetime", "timestamp"];
    let l_dt = datetime_prefixes.iter().any(|p| ll.contains(p));
    let r_dt = datetime_prefixes.iter().any(|p| rl.contains(p));
    if l_dt && r_dt {
        return true;
    }
    let string_dtypes = ["str", "string", "large_string", "string_view", "object"];
    if in_(l, &string_dtypes) && in_(r, &string_dtypes) {
        return true;
    }
    let int_dtypes = [
        "int", "int64", "int32", "int16", "int8", "uint", "uint64", "uint32", "uint16", "uint8",
    ];
    if in_(&ll, &int_dtypes) && in_(&rl, &int_dtypes) {
        return true;
    }
    if ll.contains("int") && rl.contains("int") {
        return true;
    }
    let float_dtypes = [
        "float", "float64", "float32", "float16", "float128", "double",
    ];
    if in_(&ll, &float_dtypes) && in_(&rl, &float_dtypes) {
        return true;
    }
    let bool_dtypes = ["bool", "boolean"];
    if in_(l, &bool_dtypes) && in_(r, &bool_dtypes) {
        return true;
    }
    // PARITY: the bracketed entries can never match because brackets were stripped above
    // (`utils/dtypes/__init__.py:240`).
    let date_dtypes = [
        "date",
        "date32",
        "date32[pyarrow]",
        "date32[day][pyarrow]",
        "date64",
        "date64[pyarrow]",
        "date64[ms][pyarrow]",
    ];
    in_(l, &date_dtypes) && in_(r, &date_dtypes)
}

/// Port of `are_dtypes_equal` for two dtypes dictionaries.
///
/// PARITY: keys are compared pairwise after sorting with `zip`, so extra keys on either side
/// are ignored (`utils/dtypes/__init__.py:155`).
pub fn are_dtype_maps_equal(
    ldtypes: &indexmap::IndexMap<String, String>,
    rdtypes: &indexmap::IndexMap<String, String>,
) -> bool {
    let mut lkeys: Vec<&String> = ldtypes.keys().collect();
    let mut rkeys: Vec<&String> = rdtypes.keys().collect();
    lkeys.sort();
    rkeys.sort();
    for (lk, rk) in lkeys.iter().zip(rkeys.iter()) {
        if lk != rk {
            return false;
        }
        if !are_dtypes_equal(&ldtypes[*lk], &rdtypes[*rk]) {
            return false;
        }
    }
    true
}

/// Port of `is_dtype_numeric`.
pub fn is_dtype_numeric(dtype: &str) -> bool {
    let lower = dtype.to_lowercase();
    ["numeric", "float", "double", "int"]
        .iter()
        .any(|s| lower.contains(s))
}

/// Port of `is_dtype_special`.
pub fn is_dtype_special(dtype: &str) -> bool {
    let t = resolve_alias(dtype);
    if [
        "uuid",
        "json",
        "bytes",
        "numeric",
        "datetime",
        "geometry",
        "geography",
        "date",
        "bool",
    ]
    .contains(&t)
    {
        return true;
    }
    are_dtypes_equal(t, "datetime")
        || are_dtypes_equal(t, "date")
        || t.starts_with("numeric")
        || t.starts_with("bool")
        || t.starts_with("geometry")
        || t.starts_with("geography")
}

/// Port of `get_numeric_precision_scale`'s parsing of an explicit dtype
/// (`numeric[10,2]`, `NUMERIC(10, 2)`). Returns `None` when no precision and scale are given.
pub fn parse_numeric_precision_scale(dtype: &str) -> Option<(i64, i64)> {
    if dtype.is_empty() {
        return None;
    }
    let (lb, rb) = if dtype.contains('[') {
        ('[', ']')
    } else {
        ('(', ')')
    };
    if dtype.contains(lb) && dtype.matches(',').count() == 1 && dtype.ends_with(rb) {
        let inner = dtype.split_once(lb)?.1.trim_end_matches(rb);
        let (p, s) = inner.split_once(',')?;
        return Some((parse_py_int(p.trim())?, parse_py_int(s.trim())?));
    }
    None
}

/// Whether a spatial dtype is `geometry` or `geography`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpatialKind {
    /// `geometry[...]`.
    Geometry,
    /// `geography[...]`.
    Geography,
}

/// The SRID part of a `geometry[...]` dtype: an integer or an `AUTHORITY:CODE` string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Srid {
    /// A numeric SRID (`4326`).
    Code(i64),
    /// An authority string (`EPSG:4326`, `ESRI:102003`).
    Authority(String),
}

impl Srid {
    /// The integer SRID (`int(srid.split(':')[-1])` for authority strings), as the SQL type
    /// builders compute it.
    pub fn code(&self) -> Option<i64> {
        match self {
            Srid::Code(c) => Some(*c),
            Srid::Authority(a) => parse_py_int(a.rsplit(':').next().unwrap_or(a)),
        }
    }

    /// Python truthiness (`0` is falsy).
    pub fn truthy(&self) -> bool {
        match self {
            Srid::Code(c) => *c != 0,
            Srid::Authority(a) => !a.is_empty(),
        }
    }
}

impl fmt::Display for Srid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Srid::Code(c) => write!(f, "{c}"),
            Srid::Authority(a) => f.write_str(a),
        }
    }
}

/// A parsed `geometry[...]` / `geography[...]` dtype (`get_geometry_type_srid`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GeometrySpec {
    /// `geometry` or `geography` (from the dtype's prefix).
    pub kind: SpatialKind,
    /// The geometry type exactly as written (`MULTIPOLYGON`, `Point`, `LINESTRINGZ`), or the
    /// default type `geometry`.
    pub geometry_type: String,
    /// The SRID or authority string; `Code(0)` by default.
    pub srid: Srid,
}

/// An error parsing a `geometry[...]` modifier.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid literal for int() with base 10: {}", crate::pyfmt::str_repr(.0))]
pub struct GeometryParseError(pub String);

/// Port of `get_geometry_type_srid(dtype, default_type, default_srid)`: returns
/// `(geometry_type, srid)`.
///
/// Accepts `geometry`, `geometry[]`, `geometry[MULTIPOLYGON, 6570]`, `geometry[3361]`,
/// `geometry[0, Point]`, `geometry[EPSG:4326]`, `geometry[POINT, ESRI:102003]`, PostGIS
/// `GEOMETRY(POINT, 4326)` and `srid=4326`-style parts.
///
/// PARITY: like the Python function, the type keeps its original case (the docstring's
/// `MultiLineString` normalization is not implemented), and a part such as `4326.0` passes
/// `is_int` but fails `int()`, which is an error (`utils/dtypes/__init__.py:1064`).
pub fn get_geometry_type_srid(
    dtype: &str,
    default_type: &str,
    default_srid: Srid,
) -> Result<(String, Srid), GeometryParseError> {
    let dtype = dtype.replace('(', "[").replace(')', "]");
    let bare = dtype.split('[').next().unwrap_or(&dtype).to_string();
    let modifier_src = if bare.is_empty() {
        // `str.split('')` raises in Python; an empty prefix only happens for `[...]` strings.
        dtype.as_str()
    } else {
        dtype.split_once(bare.as_str()).map_or("", |(_, rest)| rest)
    };
    let modifier = modifier_src.trim_start_matches('[').trim_end_matches(']');
    if modifier.is_empty() {
        return Ok((default_type.to_string(), default_srid));
    }

    enum PartVal {
        Int(i64),
        Str(String),
    }
    let mut parts = Vec::new();
    for part in modifier.split(',') {
        let p = part.rsplit('=').next().unwrap_or(part).trim().to_string();
        if is_int(&p) {
            match parse_py_int(&p) {
                Some(i) => parts.push(PartVal::Int(i)),
                None => return Err(GeometryParseError(p)),
            }
        } else {
            parts.push(PartVal::Str(p));
        }
    }

    let mut srid = default_srid;
    for part in &parts {
        match part {
            PartVal::Int(i) => {
                srid = Srid::Code(*i);
                break;
            }
            PartVal::Str(s) if s.contains(':') => {
                srid = Srid::Authority(s.clone());
                break;
            }
            _ => {}
        }
    }
    let mut geometry_type = default_type.to_string();
    for part in &parts {
        if let PartVal::Str(s) = part
            && Srid::Authority(s.clone()) != srid
        {
            geometry_type = s.clone();
            break;
        }
    }
    Ok((geometry_type, srid))
}

impl GeometrySpec {
    /// Parse a `geometry...` or `geography...` dtype (also `geom`/`geog` aliases and PostGIS
    /// `GEOMETRY(...)` types). Defaults: type `geometry`, SRID `0`.
    pub fn parse(dtype: &str) -> Result<Option<GeometrySpec>, GeometryParseError> {
        let lower = dtype.to_lowercase();
        let base = lower.split(['[', '(']).next().unwrap_or("").trim();
        let kind = match resolve_alias(base) {
            "geometry" => SpatialKind::Geometry,
            "geography" => SpatialKind::Geography,
            _ => return Ok(None),
        };
        let (geometry_type, srid) = get_geometry_type_srid(dtype, "geometry", Srid::Code(0))?;
        Ok(Some(GeometrySpec {
            kind,
            geometry_type,
            srid,
        }))
    }

    /// The canonical dtype string, as `get_pd_type_from_db_type` builds it:
    /// `"{kind}[{type}, {srid}]"` with falsy parts (`''`, SRID `0`) omitted, or the bare kind.
    pub fn canonical(&self) -> String {
        let kind = match self.kind {
            SpatialKind::Geometry => "geometry",
            SpatialKind::Geography => "geography",
        };
        let mut modifiers = Vec::new();
        if !self.geometry_type.is_empty() {
            modifiers.push(self.geometry_type.clone());
        }
        if self.srid.truthy() {
            modifiers.push(self.srid.to_string());
        }
        if modifiers.is_empty() {
            return kind.to_string();
        }
        format!("{kind}[{}]", modifiers.join(", "))
    }

    /// The upper-cased geometry type used in SQL column types (`GEOMETRY(POINT, 4326)`).
    pub fn geometry_type_upper(&self) -> String {
        self.geometry_type.to_uppercase()
    }

    /// Whether the type has a Z dimension (`POINTZ`, `MULTIPOLYGONZ`, `LINESTRING Z`).
    pub fn has_z(&self) -> bool {
        let t = self.geometry_type.to_uppercase();
        let t = t.trim_end_matches('M').trim_end();
        t.ends_with('Z')
    }
}

/// A classified Meerschaum dtype string.
#[derive(Debug, Clone, PartialEq)]
pub enum MrsmDtype {
    /// `datetime` (timezone-aware, microseconds).
    Datetime,
    /// `int`.
    Int,
    /// `numeric`, optionally `numeric[precision,scale]`.
    Numeric(Option<(i64, i64)>),
    /// `uuid`.
    Uuid,
    /// `json`.
    Json,
    /// `bytes`.
    Bytes,
    /// `geometry[...]` or `geography[...]`.
    Spatial(GeometrySpec),
    /// `date`.
    Date,
    /// `bool`.
    Bool,
    /// `str`.
    Str,
    /// Any other (pandas-style or database) dtype string, verbatim.
    Other(String),
}

impl MrsmDtype {
    /// Classify a dtype string after alias resolution (`MRSM_ALIAS_DTYPES`).
    pub fn parse(dtype: &str) -> Result<MrsmDtype, GeometryParseError> {
        let t = resolve_alias(dtype);
        Ok(match t {
            "datetime" => MrsmDtype::Datetime,
            "int" => MrsmDtype::Int,
            "numeric" => MrsmDtype::Numeric(None),
            "uuid" => MrsmDtype::Uuid,
            "json" => MrsmDtype::Json,
            "bytes" => MrsmDtype::Bytes,
            "date" => MrsmDtype::Date,
            "bool" => MrsmDtype::Bool,
            "str" => MrsmDtype::Str,
            _ if t.starts_with("numeric") => MrsmDtype::Numeric(parse_numeric_precision_scale(t)),
            _ if t.starts_with("geometry") || t.starts_with("geography") => {
                match GeometrySpec::parse(t)? {
                    Some(spec) => MrsmDtype::Spatial(spec),
                    None => MrsmDtype::Other(dtype.to_string()),
                }
            }
            _ => MrsmDtype::Other(dtype.to_string()),
        })
    }

    /// The canonical Meerschaum string for this dtype.
    pub fn canonical(&self) -> String {
        match self {
            MrsmDtype::Datetime => "datetime".into(),
            MrsmDtype::Int => "int".into(),
            MrsmDtype::Numeric(None) => "numeric".into(),
            MrsmDtype::Numeric(Some((p, s))) => format!("numeric[{p},{s}]"),
            MrsmDtype::Uuid => "uuid".into(),
            MrsmDtype::Json => "json".into(),
            MrsmDtype::Bytes => "bytes".into(),
            MrsmDtype::Spatial(spec) => spec.canonical(),
            MrsmDtype::Date => "date".into(),
            MrsmDtype::Bool => "bool".into(),
            MrsmDtype::Str => "str".into(),
            MrsmDtype::Other(s) => s.clone(),
        }
    }
}

/// A pandas-style dtype string split into parts:
/// `datetime64[ms, UTC]` -> base `datetime64`, params `["ms", "UTC"]`;
/// `large_string[pyarrow]` -> base `large_string`, `pyarrow = true`;
/// `date32[day][pyarrow]` -> base `date32`, params `["day"]`, `pyarrow = true`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PandasDtype {
    /// The type name before the first `[`.
    pub base: String,
    /// The comma-separated parameters of the bracket groups (excluding `pyarrow`).
    pub params: Vec<String>,
    /// Whether the dtype is Arrow-backed (`[pyarrow]`).
    pub pyarrow: bool,
}

impl PandasDtype {
    /// Split a pandas-style dtype string.
    pub fn parse(dtype: &str) -> PandasDtype {
        let base = dtype.split('[').next().unwrap_or(dtype).trim().to_string();
        let mut params = Vec::new();
        let mut pyarrow = false;
        let mut rest = &dtype[base.len().min(dtype.len())..];
        while let Some(start) = rest.find('[') {
            let Some(end) = rest[start..].find(']') else {
                break;
            };
            let group = &rest[start + 1..start + end];
            if group.trim() == "pyarrow" {
                pyarrow = true;
            } else {
                params.extend(group.split(',').map(|p| p.trim().to_string()));
            }
            rest = &rest[start + end + 1..];
        }
        PandasDtype {
            base,
            params,
            pyarrow,
        }
    }

    /// For `datetime64[unit, tz]`-style dtypes: the time unit, if any.
    pub fn time_unit(&self) -> Option<&str> {
        self.params.first().map(String::as_str)
    }

    /// For `datetime64[unit, tz]`-style dtypes: the timezone, if any.
    pub fn timezone(&self) -> Option<&str> {
        self.params.get(1).map(String::as_str)
    }
}

/// A timestamp precision unit (`MRSM_PRECISION_UNITS_SCALARS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PrecisionUnit {
    /// `nanosecond`.
    Nanosecond,
    /// `microsecond`.
    Microsecond,
    /// `millisecond`.
    Millisecond,
    /// `second`.
    Second,
    /// `minute`.
    Minute,
    /// `hour`.
    Hour,
    /// `day`.
    Day,
}

impl PrecisionUnit {
    /// All units, from finest to coarsest (sorted by scalar, descending).
    pub const ALL: [PrecisionUnit; 7] = [
        PrecisionUnit::Nanosecond,
        PrecisionUnit::Microsecond,
        PrecisionUnit::Millisecond,
        PrecisionUnit::Second,
        PrecisionUnit::Minute,
        PrecisionUnit::Hour,
        PrecisionUnit::Day,
    ];

    /// The full name (`'microsecond'`).
    pub fn name(&self) -> &'static str {
        match self {
            PrecisionUnit::Nanosecond => "nanosecond",
            PrecisionUnit::Microsecond => "microsecond",
            PrecisionUnit::Millisecond => "millisecond",
            PrecisionUnit::Second => "second",
            PrecisionUnit::Minute => "minute",
            PrecisionUnit::Hour => "hour",
            PrecisionUnit::Day => "day",
        }
    }

    /// `MRSM_PRECISION_UNITS_ABBREVIATIONS`.
    pub fn abbreviation(&self) -> &'static str {
        match self {
            PrecisionUnit::Nanosecond => "ns",
            PrecisionUnit::Microsecond => "us",
            PrecisionUnit::Millisecond => "ms",
            PrecisionUnit::Second => "s",
            PrecisionUnit::Minute => "min",
            PrecisionUnit::Hour => "hr",
            PrecisionUnit::Day => "D",
        }
    }

    /// `MRSM_PRECISION_UNITS_SCALARS`: units per second.
    pub fn scalar(&self) -> f64 {
        match self {
            PrecisionUnit::Nanosecond => 1_000_000_000.0,
            PrecisionUnit::Microsecond => 1_000_000.0,
            PrecisionUnit::Millisecond => 1000.0,
            PrecisionUnit::Second => 1.0,
            PrecisionUnit::Minute => 1.0 / 60.0,
            PrecisionUnit::Hour => 1.0 / 3600.0,
            PrecisionUnit::Day => 1.0 / 86400.0,
        }
    }

    /// Parse a full name only (a key of `MRSM_PRECISION_UNITS_SCALARS`).
    pub fn from_name(name: &str) -> Option<PrecisionUnit> {
        PrecisionUnit::ALL.into_iter().find(|u| u.name() == name)
    }

    /// Parse a name or an alias (`'ms'`, `'D'`, `'millisecond'`).
    pub fn parse(unit: &str) -> Option<PrecisionUnit> {
        PrecisionUnit::from_name(precision_unit_alias(unit).unwrap_or(unit))
    }

    /// Port of `get_next_precision_unit`: the next coarser (`decrease`) or finer unit.
    pub fn next(&self, decrease: bool) -> Option<PrecisionUnit> {
        let i = PrecisionUnit::ALL.iter().position(|u| u == self)?;
        if decrease {
            PrecisionUnit::ALL.get(i + 1).copied()
        } else {
            i.checked_sub(1).map(|j| PrecisionUnit::ALL[j])
        }
    }
}

impl fmt::Display for PrecisionUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Parse a float-or-int string the way `float()` does (helper for dtype modifiers).
pub fn parse_number(s: &str) -> Option<f64> {
    parse_py_float(s)
}

/// `value_is_null(value)` for a string (`utils/dtypes/__init__.py:523`): `'None'`, `'nan'`,
/// `'NA'`, `'NaT'`, `'NaTz'`, `''` and `'<NA>'` in any case.
///
/// This is the rule for columns that are not declared as text. A column declared as text, or
/// an index column, keeps these strings as text: see [`null_marker_text_columns`].
pub fn value_is_null(value: &str) -> bool {
    matches!(
        value.to_lowercase().as_str(),
        "none" | "nan" | "na" | "nat" | "natz" | "" | "<na>"
    )
}

/// Whether `dtype` is a text dtype: `str`, `string` (`string[pyarrow]`, `string[python]`),
/// `large_string`, `string_view` or `text`. `object` is not text (it holds anything).
pub fn is_text_dtype(dtype: &str) -> bool {
    let base = dtype.split('[').next().unwrap_or(dtype).trim();
    matches!(
        base.to_lowercase().as_str(),
        "str" | "string" | "large_string" | "string_view" | "text" | "unicode"
    )
}

/// The columns whose null-like strings (`'None'`, `'nan'`, `'NA'`, `'NaT'`, `'NaTz'`, `''`,
/// `'<NA>'`, see [`value_is_null`]) are values, not nulls: a sync writes them as text and a
/// `params` filter matches them as text. Only real nulls (`None`, `NaN`, `pd.NA`, `NaT`, an
/// Arrow null) are `NULL` there.
///
/// These are the columns declared with a text dtype in `dtypes` (`explicit_dtypes`, the
/// declared dtypes only) and the index columns (`columns.*`) whose dtype is text or not known
/// (an undeclared index column holding strings). An index column whose dtype is not text
/// (`int`, `datetime`, ...) keeps v4's rule, as does every other column.
///
/// `effective_dtypes` are the pipe's dtypes including the ones inferred from the table.
pub fn null_marker_text_columns<'a>(
    explicit_dtypes: impl IntoIterator<Item = (&'a String, &'a String)>,
    index_columns: impl IntoIterator<Item = &'a String>,
    effective_dtypes: impl Fn(&str) -> Option<String>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut declared: Vec<&String> = Vec::new();
    for (col, dtype) in explicit_dtypes {
        declared.push(col);
        if is_text_dtype(dtype) && !out.contains(col) {
            out.push(col.clone());
        }
    }
    for col in index_columns {
        if col.is_empty() || out.contains(col) || declared.contains(&col) {
            continue;
        }
        let keep = match effective_dtypes(col) {
            None => true,
            Some(t) => is_text_dtype(&t) || t == "object" || t.is_empty(),
        };
        if keep {
            out.push(col.clone());
        }
    }
    out
}

/// A value of `MRSM_PRECISION_UNITS_SCALARS`. Python mixes `int` and `float` values, which is
/// observable through `str(scalar)` in generated SQL.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PrecisionScalar {
    /// An integer scalar (`nanosecond` .. `second`).
    Int(i64),
    /// A fractional scalar (`minute`, `hour`, `day`).
    Float(f64),
}

impl PrecisionScalar {
    /// The scalar as `f64`.
    pub fn as_f64(self) -> f64 {
        match self {
            PrecisionScalar::Int(i) => i as f64,
            PrecisionScalar::Float(f) => f,
        }
    }

    /// Python `str(scalar)`.
    pub fn py_str(self) -> String {
        match self {
            PrecisionScalar::Int(i) => i.to_string(),
            PrecisionScalar::Float(f) => crate::pyfmt::float_repr(f),
        }
    }
}

/// `MRSM_PRECISION_UNITS_SCALARS[unit]` for a full unit name (aliases are not resolved).
pub fn precision_unit_scalar(unit: &str) -> Option<PrecisionScalar> {
    PrecisionUnit::from_name(unit).map(|u| u.py_scalar())
}

impl PrecisionUnit {
    /// `MRSM_PRECISION_UNITS_SCALARS[unit]` with Python's `int` / `float` distinction.
    pub fn py_scalar(&self) -> PrecisionScalar {
        match self {
            PrecisionUnit::Nanosecond => PrecisionScalar::Int(1_000_000_000),
            PrecisionUnit::Microsecond => PrecisionScalar::Int(1_000_000),
            PrecisionUnit::Millisecond => PrecisionScalar::Int(1000),
            PrecisionUnit::Second => PrecisionScalar::Int(1),
            other => PrecisionScalar::Float(other.scalar()),
        }
    }

    /// Microseconds in one unit (`timedelta(**{unit + 's': 1})`); `None` for nanoseconds,
    /// which `timedelta` cannot represent.
    pub fn micros(&self) -> Option<i64> {
        Some(match self {
            PrecisionUnit::Nanosecond => return None,
            PrecisionUnit::Microsecond => 1,
            PrecisionUnit::Millisecond => 1000,
            PrecisionUnit::Second => 1_000_000,
            PrecisionUnit::Minute => 60_000_000,
            PrecisionUnit::Hour => 3_600_000_000,
            PrecisionUnit::Day => 86_400_000_000,
        })
    }
}

/// Port of `get_next_precision_unit(unit, decrease)` with Python's error messages.
pub fn get_next_precision_unit(unit: &str, decrease: bool) -> Result<&'static str, String> {
    let resolved =
        PrecisionUnit::parse(unit).ok_or_else(|| format!("Invalid precision unit '{unit}'."))?;
    resolved.next(decrease).map(|u| u.name()).ok_or_else(|| {
        format!(
            "No precision {} '{unit}'.",
            if decrease { "below" } else { "above" }
        )
    })
}

impl Srid {
    /// The CRS string used in GeoArrow metadata (`EPSG:4326`, `ESRI:102003`); `None` when falsy.
    pub fn to_crs(&self) -> Option<String> {
        if !self.truthy() {
            return None;
        }
        Some(match self {
            Srid::Code(c) => format!("EPSG:{c}"),
            Srid::Authority(a) if a.contains(':') => a.clone(),
            Srid::Authority(a) => format!("EPSG:{a}"),
        })
    }

    /// The EPSG code: the number itself, or the code of an `EPSG:` authority string.
    pub fn epsg(&self) -> Option<i64> {
        match self {
            Srid::Code(c) => Some(*c),
            Srid::Authority(a) => a
                .strip_prefix("EPSG:")
                .or_else(|| a.strip_prefix("epsg:"))
                .and_then(|c| c.parse().ok()),
        }
    }
}

/// [`are_dtype_maps_equal`] over any `(column, dtype)` pairs.
///
/// PARITY: keys are sorted and `zip`ped, so extra keys on either side are ignored
/// (`{'a': 'int64'}` equals `{'a': 'int64', 'b': 'float64'}`).
pub fn are_dtype_items_equal<'a>(
    left: impl IntoIterator<Item = (&'a str, &'a str)>,
    right: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> bool {
    let mut l: Vec<(&str, &str)> = left.into_iter().collect();
    let mut r: Vec<(&str, &str)> = right.into_iter().collect();
    l.sort_by(|a, b| a.0.cmp(b.0));
    r.sort_by(|a, b| a.0.cmp(b.0));
    l.iter()
        .zip(r.iter())
        .all(|((lk, lt), (rk, rt))| lk == rk && are_dtypes_equal(lt, rt))
}

// ---------------------------------------------------------------------------------------------
// Database types -> Meerschaum / pandas dtypes (`meerschaum/utils/dtypes/sql.py`)
// ---------------------------------------------------------------------------------------------

/// `DB_TO_PD_DTYPES` exact keys (`utils/dtypes/sql.py:79`), keyed by the upper-cased type.
pub const DB_TO_PD_DTYPES: &[(&str, &str)] = &[
    ("FLOAT", "float64[pyarrow]"),
    ("REAL", "float64[pyarrow]"),
    ("DOUBLE_PRECISION", "float64[pyarrow]"),
    ("DOUBLE", "float64[pyarrow]"),
    ("DECIMAL", "numeric"),
    ("BIGINT", "int64[pyarrow]"),
    ("INT", "int32[pyarrow]"),
    ("INTEGER", "int32[pyarrow]"),
    ("NUMBER", "numeric"),
    ("NUMERIC", "numeric"),
    ("GEOMETRY", "geometry"),
    ("GEOMETRY(GEOMETRY)", "geometry"),
    ("TIMESTAMP", "datetime64[us]"),
    ("TIMESTAMP WITHOUT TIMEZONE", "datetime64[us]"),
    ("TIMESTAMP WITH TIMEZONE", "datetime64[us, UTC]"),
    ("TIMESTAMP WITH TIME ZONE", "datetime64[us, UTC]"),
    ("TIMESTAMPTZ", "datetime64[us, UTC]"),
    ("DATE", "date"),
    ("DATETIME", "datetime64[us]"),
    ("DATETIME2", "datetime64[us]"),
    ("DATETIME2(6)", "datetime64[us]"),
    ("DATETIME2(3)", "datetime64[ms]"),
    ("DATETIME2(0)", "datetime64[s]"),
    ("DATETIMEOFFSET", "datetime64[us, UTC]"),
    ("DATETIMEOFFSET(6)", "datetime64[us, UTC]"),
    ("DATETIMEOFFSET(3)", "datetime64[ms, UTC]"),
    ("DATETIMEOFFSET(0)", "datetime64[s, UTC]"),
    ("TEXT", "string[pyarrow]"),
    ("VARCHAR", "string[pyarrow]"),
    ("CLOB", "string[pyarrow]"),
    ("BOOL", "bool[pyarrow]"),
    ("BOOLEAN", "bool[pyarrow]"),
    ("BOOLEAN()", "bool[pyarrow]"),
    ("TINYINT", "bool[pyarrow]"),
    ("TINYINT(1)", "bool[pyarrow]"),
    ("BIT", "bool[pyarrow]"),
    ("BIT(1)", "bool[pyarrow]"),
    ("JSON", "json"),
    ("JSONB", "json"),
    ("UUID", "uuid"),
    ("UNIQUEIDENTIFIER", "uuid"),
    ("BYTEA", "bytes"),
    ("BLOB", "bytes"),
    ("VARBINARY", "bytes"),
    ("VARBINARY(MAX)", "bytes"),
];

/// `DB_TO_PD_DTYPES['substrings']`, in insertion order (the order is observable).
pub const DB_TO_PD_SUBSTRINGS: &[(&str, &str)] = &[
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

/// `DB_TO_PD_DTYPES['default']`.
pub const DB_TO_PD_DEFAULT: &str = "object";

/// `get_numeric_precision_scale(None, dtype)` (`utils/dtypes/sql.py:1366`): the explicit
/// precision and scale of `numeric[p,s]` / `NUMERIC(p, s)`, else `(None, None)`. The flavor
/// defaults of the full function live in `meerschaum-connector-sql`.
pub fn get_numeric_precision_scale(dtype: Option<&str>) -> (Option<i64>, Option<i64>) {
    match dtype.and_then(parse_numeric_precision_scale) {
        Some((p, s)) => (Some(p), Some(s)),
        None => (None, None),
    }
}

/// `get_pd_type_from_db_type(db_type, allow_custom_dtypes)` (`utils/dtypes/sql.py:1162`).
///
/// PARITY: substring matching follows dict order, so `GEOGRAPHY(POINT, 4326)` matches `INT`
/// (inside `POINT`) and maps to `int64[pyarrow]`, and Python passes the matched *substring* to
/// the custom-dtype parser (`utils/dtypes/sql.py:1205`), so substring matches drop modifiers.
/// Where Python raises `ValueError` on a malformed geometry modifier (`GEOMETRY(4326.0)`), the
/// default type and SRID are used.
pub fn get_pd_type_from_db_type(db_type: &str, allow_custom_dtypes: bool) -> String {
    fn parse_custom(pd_type: &str, db_type: &str) -> String {
        if db_type.to_lowercase().contains("json") {
            return "json".to_string();
        }
        if are_dtypes_equal(pd_type, "numeric")
            && pd_type != "object"
            && let (Some(p), Some(s)) = get_numeric_precision_scale(Some(&db_type.to_uppercase()))
            && p != 0
            && s != 0
        {
            return format!("numeric[{p},{s}]");
        }
        if are_dtypes_equal(pd_type, "geometry") && pd_type != "object" {
            let (geometry_type, srid) =
                get_geometry_type_srid(&db_type.to_uppercase(), "geometry", Srid::Code(0))
                    .unwrap_or_else(|_| ("geometry".to_string(), Srid::Code(0)));
            let mut modifiers = Vec::new();
            if !geometry_type.is_empty() {
                modifiers.push(geometry_type);
            }
            if srid.truthy() {
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
    }

    let upper = db_type.to_uppercase();
    let key = crate::pyfmt::py_strip(upper.split('(').next().unwrap_or(""));
    if let Some((_, pd_type)) = DB_TO_PD_DTYPES.iter().find(|(k, _)| *k == key) {
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
    DB_TO_PD_DEFAULT.to_string()
}

// ---------------------------------------------------------------------------------------------
// Arrow field tags
// ---------------------------------------------------------------------------------------------

/// Arrow field-metadata keys that tag columns crossing crate boundaries as Arrow (SQL query
/// results, kernel frames, the Python bridge). Every producer and consumer uses these keys.
pub mod field_meta {
    /// The Meerschaum dtype of the column (`json`, `bytes`, `uuid`, `numeric`, `geometry`, ...).
    pub const DTYPE: &str = "meerschaum:dtype";
    /// The database type name reported by the driver (`TIMESTAMPTZ`, `GEOMETRY(POINT, 4326)`).
    pub const DB_TYPE: &str = "meerschaum:db_type";
    /// How a geometry column's bytes are encoded (`wkb`, `ewkb`, `mysql`).
    pub const ENCODING: &str = "meerschaum:encoding";
    /// The Arrow canonical extension name key (`arrow.json`, `arrow.uuid`, `geoarrow.wkb`).
    pub const EXTENSION_NAME: &str = "ARROW:extension:name";
    /// The Arrow canonical extension metadata key.
    pub const EXTENSION_METADATA: &str = "ARROW:extension:metadata";
}

/// The Meerschaum dtype declared by a column's field tags: the [`field_meta::DTYPE`] tag,
/// refined by the database type when that is more specific (`geometry` + `GEOMETRY(POINT, 4326)`
/// is `geometry[POINT, 4326]`; `numeric` + `NUMERIC(10, 2)` is `numeric[10,2]`), else the dtype
/// mapped from the database type alone (`get_pd_type_from_db_type`).
pub fn dtype_from_field_tags(dtype: Option<&str>, db_type: Option<&str>) -> Option<String> {
    let from_db = db_type
        .filter(|t| !t.is_empty())
        .map(|t| get_pd_type_from_db_type(t, true));
    match (dtype.filter(|d| !d.is_empty()), from_db) {
        (Some(tag), Some(db)) if db != tag && db.starts_with(tag) => Some(db),
        (Some(tag), _) => Some(tag.to_string()),
        (None, db) => db,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g(s: &str) -> (String, Srid) {
        get_geometry_type_srid(s, "geometry", Srid::Code(0)).unwrap()
    }

    #[test]
    fn geometry_forms() {
        assert_eq!(g("geometry"), ("geometry".into(), Srid::Code(0)));
        assert_eq!(g("geometry[]"), ("geometry".into(), Srid::Code(0)));
        assert_eq!(
            g("geometry[MULTIPOLYGON, 6570]"),
            ("MULTIPOLYGON".into(), Srid::Code(6570))
        );
        assert_eq!(g("geometry[3361]"), ("geometry".into(), Srid::Code(3361)));
        assert_eq!(g("geometry[0, Point]"), ("Point".into(), Srid::Code(0)));
        assert_eq!(
            g("geometry[EPSG:4326]"),
            ("geometry".into(), Srid::Authority("EPSG:4326".into()))
        );
        assert_eq!(
            g("geometry[POINT, ESRI:102003]"),
            ("POINT".into(), Srid::Authority("ESRI:102003".into()))
        );
        assert_eq!(
            g("GEOMETRY(LINESTRINGZ, 4326)"),
            ("LINESTRINGZ".into(), Srid::Code(4326))
        );
        assert!(get_geometry_type_srid("geometry[4326.0]", "geometry", Srid::Code(0)).is_err());
        let spec = GeometrySpec::parse("geometry[multipolygonz, 6570]")
            .unwrap()
            .unwrap();
        assert!(spec.has_z());
        assert_eq!(spec.canonical(), "geometry[multipolygonz, 6570]");
        assert_eq!(
            GeometrySpec::parse("geography")
                .unwrap()
                .unwrap()
                .canonical(),
            "geography[geometry]"
        );
    }

    #[test]
    fn dtype_equality() {
        assert!(are_dtypes_equal("int", "Int64"));
        assert!(are_dtypes_equal("datetime", "datetime64[ms, UTC]"));
        assert!(are_dtypes_equal("str", "large_string[pyarrow]"));
        assert!(are_dtypes_equal("json", "object"));
        assert!(!are_dtypes_equal("int", "float64"));
        assert!(are_dtypes_equal("date", "date32[day][pyarrow]"));
    }

    #[test]
    fn db_types() {
        assert_eq!(get_pd_type_from_db_type("BIGINT", true), "int64[pyarrow]");
        assert_eq!(get_pd_type_from_db_type("JSONB", true), "json");
        assert_eq!(
            get_pd_type_from_db_type("NUMERIC(10, 2)", true),
            "numeric[10,2]"
        );
        assert_eq!(
            get_pd_type_from_db_type("GEOMETRY(POINT, 4326)", true),
            "geometry[POINT, 4326]"
        );
        assert_eq!(
            get_pd_type_from_db_type("GEOMETRY", true),
            "geometry[geometry]"
        );
        // PARITY: substring order matches `INT` inside `POINT`.
        assert_eq!(
            get_pd_type_from_db_type("GEOGRAPHY(POINT, 4326)", true),
            "int64[pyarrow]"
        );
        assert_eq!(
            dtype_from_field_tags(Some("geometry"), Some("GEOMETRY(POINT, 4326)")).as_deref(),
            Some("geometry[POINT, 4326]")
        );
        assert_eq!(
            dtype_from_field_tags(Some("bytes"), Some("BLOB")).as_deref(),
            Some("bytes")
        );
        assert_eq!(
            dtype_from_field_tags(None, Some("TIMESTAMPTZ")).as_deref(),
            Some("datetime64[us, UTC]")
        );
        assert_eq!(
            GeometryParseError("4326.0".into()).to_string(),
            "invalid literal for int() with base 10: '4326.0'"
        );
        assert!(value_is_null("<NA>") && value_is_null("None") && !value_is_null("0"));
    }

    #[test]
    fn null_marker_text_columns_cover_declared_text_and_text_index_columns() {
        assert!(is_text_dtype("string") && is_text_dtype("string[pyarrow]"));
        assert!(is_text_dtype("str") && is_text_dtype("TEXT"));
        assert!(!is_text_dtype("object") && !is_text_dtype("int") && !is_text_dtype("json"));
        let explicit: Vec<(String, String)> = vec![
            ("name".into(), "string".into()),
            ("amount".into(), "float".into()),
            ("station".into(), "int".into()),
        ];
        let index: Vec<String> = vec![
            "ts".into(),
            "station".into(),
            "code".into(),
            "region".into(),
            "untyped".into(),
        ];
        let effective = |c: &str| match c {
            "ts" => Some("datetime64[ns, UTC]".to_string()),
            "code" => Some("string[pyarrow]".to_string()),
            "region" => Some("object".to_string()),
            _ => None,
        };
        let cols = null_marker_text_columns(
            explicit.iter().map(|(a, b)| (a, b)),
            index.iter(),
            effective,
        );
        // Declared text, text / object / unknown index columns; never a declared non-text
        // column (`station: int`) or a datetime index.
        assert_eq!(cols, vec!["name", "code", "region", "untyped"]);
    }

    #[test]
    fn precision_units() {
        assert_eq!(get_next_precision_unit("ms", true), Ok("second"));
        assert_eq!(
            get_next_precision_unit("ns", false).unwrap_err(),
            "No precision above 'ns'."
        );
        assert_eq!(
            PrecisionUnit::Minute.py_scalar().py_str(),
            "0.016666666666666666"
        );
        assert_eq!(PrecisionUnit::Second.py_scalar().py_str(), "1");
        assert_eq!(Srid::Code(4326).to_crs().as_deref(), Some("EPSG:4326"));
        assert_eq!(Srid::Authority("EPSG:2263".into()).epsg(), Some(2263));
        assert_eq!(PrecisionUnit::parse("ms"), Some(PrecisionUnit::Millisecond));
        assert_eq!(
            PrecisionUnit::Nanosecond.next(true),
            Some(PrecisionUnit::Microsecond)
        );
        assert_eq!(PrecisionUnit::Hour.next(false), Some(PrecisionUnit::Minute));
        let p = PandasDtype::parse("datetime64[ms, UTC]");
        assert_eq!(p.time_unit(), Some("ms"));
        assert_eq!(p.timezone(), Some("UTC"));
    }
}
