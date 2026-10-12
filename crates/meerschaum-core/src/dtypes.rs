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
#[error("invalid literal for int() with base 10: {0:?}")]
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
        if let PartVal::Str(s) = part {
            if Srid::Authority(s.clone()) != srid {
                geometry_type = s.clone();
                break;
            }
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
    fn precision_units() {
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
