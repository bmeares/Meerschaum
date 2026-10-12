//! SQL dtype mapping (`meerschaum/utils/dtypes/sql.py`) plus the few Meerschaum dtype helpers it
//! needs from `meerschaum/utils/dtypes/__init__.py`.
//!
//! TODO(core-merge): `MRSM_ALIAS_DTYPES`, `are_dtypes_equal`, `get_geometry_type_srid`,
//! `value_is_null` and the precision-unit tables belong to `meerschaum-core` / the kernel. They
//! are reproduced here (string-only) until that crate lands; switch to its exports then.

use crate::flavor::Flavor;

// ---------------------------------------------------------------------------------------------
// Helpers from `meerschaum/utils/dtypes/__init__.py` (TODO(core-merge))
// ---------------------------------------------------------------------------------------------

/// `MRSM_ALIAS_DTYPES` (`utils/dtypes/__init__.py:22`).
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

/// Resolve an alias in `MRSM_ALIAS_DTYPES`.
pub fn alias_dtype(dtype: &str) -> Option<&'static str> {
    MRSM_ALIAS_DTYPES
        .iter()
        .find(|(k, _)| *k == dtype)
        .map(|(_, v)| *v)
}

/// `MRSM_PRECISION_UNITS_ALIASES` (`utils/dtypes/__init__.py:67`).
pub fn precision_unit_alias(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "ns" => "nanosecond",
        "us" => "microsecond",
        "ms" => "millisecond",
        "s" | "sec" => "second",
        "m" | "min" => "minute",
        "h" | "hr" => "hour",
        "d" | "D" => "day",
        _ => return None,
    })
}

/// A value of `MRSM_PRECISION_UNITS_SCALARS` (Python mixes `int` and `float` values).
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
            PrecisionScalar::Float(f) => crate::pytext::py_float_repr(f),
        }
    }
}

/// `MRSM_PRECISION_UNITS_SCALARS` (`utils/dtypes/__init__.py:57`): units per second.
pub fn precision_unit_scalar(unit: &str) -> Option<PrecisionScalar> {
    Some(match unit {
        "nanosecond" => PrecisionScalar::Int(1_000_000_000),
        "microsecond" => PrecisionScalar::Int(1_000_000),
        "millisecond" => PrecisionScalar::Int(1000),
        "second" => PrecisionScalar::Int(1),
        "minute" => PrecisionScalar::Float(1.0 / 60.0),
        "hour" => PrecisionScalar::Float(1.0 / 3600.0),
        "day" => PrecisionScalar::Float(1.0 / 86400.0),
        _ => return None,
    })
}

/// `value_is_null(value)` for a string (`utils/dtypes/__init__.py:523`).
pub fn value_is_null(value: &str) -> bool {
    matches!(
        value.to_lowercase().as_str(),
        "none" | "nan" | "na" | "nat" | "natz" | "" | "<na>"
    )
}

/// `are_dtypes_equal(ldtype, rdtype)` for dtype strings (`utils/dtypes/__init__.py:134`).
pub fn are_dtypes_equal(ldtype: &str, rdtype: &str) -> bool {
    if ldtype == rdtype {
        return true;
    }
    let mut l = ldtype.split('[').next().unwrap_or("");
    let mut r = rdtype.split('[').next().unwrap_or("");
    if let Some(a) = alias_dtype(l) {
        l = a;
    }
    if let Some(a) = alias_dtype(r) {
        r = a;
    }

    let within = |set: &[&str], x: &str| set.contains(&x);

    let json_dtypes = ["json", "object"];
    if within(&json_dtypes, l) && within(&json_dtypes, r) {
        return true;
    }

    let numeric_dtypes = ["numeric", "decimal", "object"];
    if (within(&numeric_dtypes, l) || l.starts_with("decimal"))
        && (within(&numeric_dtypes, r) || r.starts_with("decimal"))
    {
        return true;
    }

    let uuid_dtypes = ["uuid", "object"];
    if within(&uuid_dtypes, l) && within(&uuid_dtypes, r) {
        return true;
    }

    let bytes_dtypes = ["bytes", "object", "binary", "large_binary"];
    if within(&bytes_dtypes, l) && within(&bytes_dtypes, r) {
        return true;
    }

    let geometry_dtypes = ["geometry", "object", "geography"];
    if within(&geometry_dtypes, l) && within(&geometry_dtypes, r) {
        return true;
    }

    let l_lower = l.to_lowercase();
    let r_lower = r.to_lowercase();
    if l_lower == r_lower {
        return true;
    }

    let mut l_dt = false;
    let mut r_dt = false;
    for prefix in ["datetime", "timestamp"] {
        l_dt = l_lower.contains(prefix) || l_dt;
        r_dt = r_lower.contains(prefix) || r_dt;
    }
    if l_dt && r_dt {
        return true;
    }

    let string_dtypes = ["str", "string", "large_string", "string_view", "object"];
    if within(&string_dtypes, l) && within(&string_dtypes, r) {
        return true;
    }

    let int_dtypes = [
        "int", "int64", "int32", "int16", "int8", "uint", "uint64", "uint32", "uint16", "uint8",
    ];
    if within(&int_dtypes, &l_lower) && within(&int_dtypes, &r_lower) {
        return true;
    }
    if l_lower.contains("int") && r_lower.contains("int") {
        return true;
    }

    let float_dtypes = [
        "float", "float64", "float32", "float16", "float128", "double",
    ];
    if within(&float_dtypes, &l_lower) && within(&float_dtypes, &r_lower) {
        return true;
    }

    let bool_dtypes = ["bool", "boolean"];
    if within(&bool_dtypes, l) && within(&bool_dtypes, r) {
        return true;
    }

    let date_dtypes = [
        "date",
        "date32",
        "date32[pyarrow]",
        "date32[day][pyarrow]",
        "date64",
        "date64[pyarrow]",
        "date64[ms][pyarrow]",
    ];
    if within(&date_dtypes, l) && within(&date_dtypes, r) {
        return true;
    }

    false
}

/// `meerschaum.utils.misc.is_int(s)`: `float(s).is_integer()`.
pub fn py_is_int(s: &str) -> bool {
    py_float(s).is_some_and(|f| f.is_finite() && f.fract() == 0.0)
}

/// Python's `float(s)` for a string (whitespace-stripped, underscores between digits allowed).
pub fn py_float(s: &str) -> Option<f64> {
    let t = crate::pytext::py_strip(s);
    if t.is_empty() {
        return None;
    }
    let lower = t.to_lowercase();
    let unsigned = lower.trim_start_matches(['+', '-']);
    if matches!(unsigned, "inf" | "infinity" | "nan") {
        return lower.parse::<f64>().ok().or(Some(f64::NAN));
    }
    let cleaned: String = if t.contains('_') {
        let chars: Vec<char> = t.chars().collect();
        let ok = chars.iter().enumerate().all(|(i, c)| {
            *c != '_'
                || (i > 0
                    && i + 1 < chars.len()
                    && chars[i - 1].is_ascii_digit()
                    && chars[i + 1].is_ascii_digit())
        });
        if !ok {
            return None;
        }
        t.replace('_', "")
    } else {
        t.to_string()
    };
    cleaned.parse::<f64>().ok()
}

/// Python's `int(s)` for a string.
pub fn py_int(s: &str) -> Option<i64> {
    let t = crate::pytext::py_strip(s);
    let cleaned = if t.contains('_') {
        if t.starts_with('_') || t.ends_with('_') || t.contains("__") {
            return None;
        }
        t.replace('_', "")
    } else {
        t.to_string()
    };
    cleaned.parse::<i64>().ok()
}

/// The SRID half of `get_geometry_type_srid` (an `int` or an `'AUTH:CODE'` string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Srid {
    /// A numeric SRID (`0` means unset).
    Int(i64),
    /// An authority string such as `EPSG:4326` or `ESRI:102003`.
    Str(String),
}

impl Srid {
    /// Python truthiness of the SRID value.
    pub fn is_truthy(&self) -> bool {
        match self {
            Srid::Int(i) => *i != 0,
            Srid::Str(s) => !s.is_empty(),
        }
    }

    /// Python `str(srid)`.
    pub fn py_str(&self) -> String {
        match self {
            Srid::Int(i) => i.to_string(),
            Srid::Str(s) => s.clone(),
        }
    }
}

/// `get_geometry_type_srid(dtype, default_type='geometry', default_srid=0)`
/// (`utils/dtypes/__init__.py:1011`).
pub fn get_geometry_type_srid(dtype: &str) -> (String, Srid) {
    let default_type = "geometry".to_string();
    let default_srid = Srid::Int(0);
    // NOTE: PostGIS syntax must also be parsed.
    let dtype = dtype.replace('(', "[").replace(')', "]");
    let bare = dtype.split('[').next().unwrap_or("").to_string();
    // PARITY: `dtype.split(bare_dtype, maxsplit=1)[-1]` (an empty `bare_dtype` would raise in
    // Python; it cannot happen for the inputs the connector produces).
    let after = if bare.is_empty() {
        dtype.as_str()
    } else {
        dtype
            .split_once(bare.as_str())
            .map(|(_, r)| r)
            .unwrap_or("")
    };
    let modifier = after.trim_start_matches('[').trim_end_matches(']');
    if modifier.is_empty() {
        return (default_type, default_srid);
    }

    enum Part {
        Int(i64),
        Str(String),
    }
    let parts: Vec<Part> = modifier
        .split(',')
        .map(|part| {
            let p = crate::pytext::py_strip(part.rsplit('=').next().unwrap_or("")).to_string();
            if py_is_int(&p) {
                match py_int(&p) {
                    Some(i) => Part::Int(i),
                    // PARITY: Python's `int('4326.0')` raises `ValueError` here; keep the string.
                    None => Part::Str(p),
                }
            } else {
                Part::Str(p)
            }
        })
        .collect();

    let mut srid = default_srid;
    for part in &parts {
        match part {
            Part::Int(i) => {
                srid = Srid::Int(*i);
                break;
            }
            Part::Str(s) if s.contains(':') => {
                srid = Srid::Str(s.clone());
                break;
            }
            _ => {}
        }
    }

    let mut geometry_type = default_type;
    for part in &parts {
        if let Part::Str(s) = part {
            if srid != Srid::Str(s.clone()) {
                geometry_type = s.clone();
                break;
            }
        }
    }
    (geometry_type, srid)
}

// ---------------------------------------------------------------------------------------------
// `meerschaum/utils/dtypes/sql.py`
// ---------------------------------------------------------------------------------------------

/// The flavor columns of [`PD_TO_DB_DTYPES_FLAVORS`]; `None` is `'default'`.
pub const PD_TO_DB_COLUMNS: [Option<Flavor>; 14] = [
    Some(Flavor::Timescaledb),
    Some(Flavor::TimescaledbHa),
    Some(Flavor::Postgresql),
    Some(Flavor::Postgis),
    Some(Flavor::Citus),
    Some(Flavor::Cockroachdb),
    Some(Flavor::Mssql),
    Some(Flavor::Mysql),
    Some(Flavor::Mariadb),
    Some(Flavor::Oracle),
    Some(Flavor::Sqlite),
    Some(Flavor::Geopackage),
    Some(Flavor::Duckdb),
    None,
];

fn pd_to_db_column(flavor: Option<Flavor>) -> usize {
    PD_TO_DB_COLUMNS
        .iter()
        .position(|f| *f == flavor)
        .unwrap_or(PD_TO_DB_COLUMNS.len() - 1)
}

/// `PD_TO_DB_DTYPES_FLAVORS` (`utils/dtypes/sql.py:146`), generated from Python.
/// Columns follow [`PD_TO_DB_COLUMNS`].
pub const PD_TO_DB_DTYPES_FLAVORS: &[(&str, [&str; 14])] = &[
    (
        "int",
        [
            "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "INT", "BIGINT", "BIGINT", "BIGINT", "INT",
        ],
    ),
    (
        "uint",
        [
            "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "INT", "BIGINT", "BIGINT", "BIGINT", "INT",
        ],
    ),
    (
        "int8",
        [
            "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT",
            "SMALLINT", "SMALLINT", "INT", "INT", "INT", "SMALLINT", "INT",
        ],
    ),
    (
        "uint8",
        [
            "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT",
            "SMALLINT", "SMALLINT", "INT", "INT", "INT", "SMALLINT", "INT",
        ],
    ),
    (
        "int16",
        [
            "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT",
            "SMALLINT", "SMALLINT", "INT", "INT", "INT", "SMALLINT", "INT",
        ],
    ),
    (
        "int32",
        [
            "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT",
            "INT", "INT",
        ],
    ),
    (
        "int64",
        [
            "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "INT", "BIGINT", "BIGINT", "BIGINT", "INT",
        ],
    ),
    (
        "float",
        [
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "FLOAT",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "FLOAT",
            "FLOAT",
            "FLOAT",
            "DOUBLE PRECISION",
            "DOUBLE",
        ],
    ),
    (
        "double",
        [
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "FLOAT",
            "DOUBLE PRECISION",
            "DOUBLE PRECISION",
            "FLOAT",
            "FLOAT",
            "FLOAT",
            "DOUBLE PRECISION",
            "DOUBLE",
        ],
    ),
    (
        "datetime64[ns]",
        [
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "DATETIME2(7)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(9)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(6)",
            "DATETIME",
        ],
    ),
    (
        "datetime64[ns, UTC]",
        [
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(9)",
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "DATETIMEOFFSET(7)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(9)",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
        ],
    ),
    (
        "datetime64[us]",
        [
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP(6)",
            "TIMESTAMP",
            "DATETIME2(6)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(6)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(6)",
            "DATETIME",
        ],
    ),
    (
        "datetime64[us, UTC]",
        [
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "TIMESTAMPTZ(6)",
            "DATETIMEOFFSET(6)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(6)",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
        ],
    ),
    (
        "datetime64[ms]",
        [
            "TIMESTAMP(3)",
            "TIMESTAMP(3)",
            "TIMESTAMP(3)",
            "TIMESTAMP(3)",
            "TIMESTAMP(3)",
            "TIMESTAMP(3)",
            "DATETIME2(3)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(3)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(3)",
            "DATETIME",
        ],
    ),
    (
        "datetime64[ms, UTC]",
        [
            "TIMESTAMPTZ(3)",
            "TIMESTAMPTZ(3)",
            "TIMESTAMPTZ(3)",
            "TIMESTAMPTZ(3)",
            "TIMESTAMPTZ(3)",
            "TIMESTAMPTZ(3)",
            "DATETIMEOFFSET(3)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(3)",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
        ],
    ),
    (
        "datetime64[s]",
        [
            "TIMESTAMP(0)",
            "TIMESTAMP(0)",
            "TIMESTAMP(0)",
            "TIMESTAMP(0)",
            "TIMESTAMP(0)",
            "TIMESTAMP(0)",
            "DATETIME2(0)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(0)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(0)",
            "DATETIME",
        ],
    ),
    (
        "datetime64[s, UTC]",
        [
            "TIMESTAMPTZ(0)",
            "TIMESTAMPTZ(0)",
            "TIMESTAMPTZ(0)",
            "TIMESTAMPTZ(0)",
            "TIMESTAMPTZ(0)",
            "TIMESTAMPTZ(0)",
            "DATETIMEOFFSET(0)",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP(0)",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ(0)",
        ],
    ),
    (
        "datetime",
        [
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "DATETIMEOFFSET",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
        ],
    ),
    (
        "datetimetz",
        [
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
            "DATETIMEOFFSET",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMPTZ",
        ],
    ),
    (
        "date",
        [
            "DATE", "DATE", "DATE", "DATE", "DATE", "DATE", "DATE", "DATE", "DATE", "DATE", "DATE",
            "DATE", "DATE", "DATE",
        ],
    ),
    (
        "bool",
        [
            "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BIT", "BOOLEAN",
            "BOOLEAN", "INTEGER", "FLOAT", "FLOAT", "BOOLEAN", "BOOLEAN",
        ],
    ),
    (
        "bool[pyarrow]",
        [
            "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BIT", "BOOLEAN",
            "BOOLEAN", "INTEGER", "FLOAT", "FLOAT", "BOOLEAN", "BOOLEAN",
        ],
    ),
    (
        "boolean",
        [
            "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BIT", "BOOLEAN",
            "BOOLEAN", "INTEGER", "FLOAT", "FLOAT", "BOOLEAN", "BOOLEAN",
        ],
    ),
    (
        "object",
        [
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "NVARCHAR(MAX)",
            "TEXT",
            "TEXT",
            "NVARCHAR2(2000)",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
        ],
    ),
    (
        "string",
        [
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "NVARCHAR(MAX)",
            "TEXT",
            "TEXT",
            "NVARCHAR2(2000)",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
        ],
    ),
    (
        "unicode",
        [
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "NVARCHAR(MAX)",
            "TEXT",
            "TEXT",
            "NVARCHAR2(2000)",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
        ],
    ),
    (
        "json",
        [
            "JSONB",
            "JSONB",
            "JSONB",
            "JSONB",
            "JSONB",
            "JSONB",
            "NVARCHAR(MAX)",
            "TEXT",
            "TEXT",
            "NVARCHAR2(2000)",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
        ],
    ),
    (
        "numeric",
        [
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC(28, 10)",
            "DECIMAL(38, 20)",
            "DECIMAL(38, 20)",
            "NUMBER",
            "TEXT",
            "TEXT",
            "TEXT",
            "NUMERIC",
        ],
    ),
    (
        "uuid",
        [
            "UUID",
            "UUID",
            "UUID",
            "UUID",
            "UUID",
            "UUID",
            "UNIQUEIDENTIFIER",
            "CHAR(36)",
            "CHAR(36)",
            "CHAR(36)",
            "TEXT",
            "TEXT",
            "VARCHAR",
            "TEXT",
        ],
    ),
    (
        "bytes",
        [
            "BYTEA",
            "BYTEA",
            "BYTEA",
            "BYTEA",
            "BYTEA",
            "BYTEA",
            "VARBINARY(MAX)",
            "BLOB",
            "BLOB",
            "BLOB",
            "BLOB",
            "BLOB",
            "BLOB",
            "BLOB",
        ],
    ),
    (
        "geometry",
        [
            "TEXT",
            "GEOMETRY",
            "TEXT",
            "GEOMETRY",
            "TEXT",
            "TEXT",
            "NVARCHAR(MAX)",
            "TEXT",
            "TEXT",
            "NVARCHAR2(2000)",
            "TEXT",
            "BLOB",
            "TEXT",
            "TEXT",
        ],
    ),
    (
        "geography",
        [
            "TEXT",
            "GEOGRAPHY",
            "TEXT",
            "GEOGRAPHY",
            "TEXT",
            "TEXT",
            "NVARCHAR(MAX)",
            "TEXT",
            "TEXT",
            "NVARCHAR2(2000)",
            "TEXT",
            "BLOB",
            "TEXT",
            "TEXT",
        ],
    ),
];

/// `DB_TO_PD_DTYPES` exact keys (`utils/dtypes/sql.py:79`), generated from Python.
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

/// `DB_TO_PD_DTYPES['substrings']`, in order.
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

/// `DB_FLAVORS_CAST_DTYPES` (`utils/dtypes/sql.py:23`, plus the numeric entries added at line 68).
pub const DB_FLAVORS_CAST_DTYPES: &[(&str, &[(&str, &str)])] = &[
    (
        "mariadb",
        &[
            ("BIGINT", "DECIMAL"),
            ("TINYINT", "SIGNED INT"),
            ("TEXT", "CHAR(10000) CHARACTER SET utf8"),
            ("BOOL", "SIGNED INT"),
            ("BOOLEAN", "SIGNED INT"),
            ("DOUBLE PRECISION", "DECIMAL"),
            ("DOUBLE", "DECIMAL"),
            ("FLOAT", "DECIMAL"),
            ("NUMERIC", "NUMERIC(38, 20)"),
            ("DECIMAL", "DECIMAL(38, 20)"),
        ],
    ),
    (
        "mysql",
        &[
            ("BIGINT", "DECIMAL"),
            ("TINYINT", "SIGNED INT"),
            ("TEXT", "CHAR(10000) CHARACTER SET utf8"),
            ("INT", "SIGNED INT"),
            ("INTEGER", "SIGNED INT"),
            ("BOOL", "SIGNED INT"),
            ("BOOLEAN", "SIGNED INT"),
            ("DOUBLE PRECISION", "DECIMAL"),
            ("DOUBLE", "DECIMAL"),
            ("FLOAT", "DECIMAL"),
            ("NUMERIC", "NUMERIC(38, 20)"),
            ("DECIMAL", "DECIMAL(38, 20)"),
        ],
    ),
    ("sqlite", &[("BOOLEAN", "INTEGER"), ("REAL", "FLOAT")]),
    ("geopackage", &[("BOOLEAN", "INTEGER"), ("REAL", "FLOAT")]),
    (
        "oracle",
        &[
            ("NVARCHAR(2000)", "NVARCHAR2(2000)"),
            ("NVARCHAR", "NVARCHAR2(2000)"),
            ("NVARCHAR2", "NVARCHAR2(2000)"),
            ("CHAR", "CHAR(36)"),
        ],
    ),
    (
        "mssql",
        &[
            (
                "NVARCHAR COLLATE \"SQL Latin1 General CP1 CI AS\"",
                "NVARCHAR(MAX)",
            ),
            (
                "NVARCHAR COLLATE \"SQL_Latin1_General_CP1_CI_AS\"",
                "NVARCHAR(MAX)",
            ),
            (
                "VARCHAR COLLATE \"SQL Latin1 General CP1 CI AS\"",
                "NVARCHAR(MAX)",
            ),
            (
                "VARCHAR COLLATE \"SQL_Latin1_General_CP1_CI_AS\"",
                "NVARCHAR(MAX)",
            ),
            ("NVARCHAR", "NVARCHAR(MAX)"),
            ("NUMERIC", "NUMERIC(28, 10)"),
            ("DECIMAL", "DECIMAL(28, 10)"),
        ],
    ),
];

/// Look up `PD_TO_DB_DTYPES_FLAVORS[pd_type][flavor]` (`None` flavor → `'default'`).
pub fn pd_to_db_entry(pd_type: &str, flavor: Option<Flavor>) -> Option<&'static str> {
    PD_TO_DB_DTYPES_FLAVORS
        .iter()
        .find(|(k, _)| *k == pd_type)
        .map(|(_, row)| row[pd_to_db_column(flavor)])
}

/// `DB_FLAVORS_CAST_DTYPES[flavor].get(db_type)`.
pub fn cast_dtype(flavor: Option<Flavor>, db_type: &str) -> Option<&'static str> {
    let flavor = flavor?;
    DB_FLAVORS_CAST_DTYPES
        .iter()
        .find(|(f, _)| *f == flavor.as_str())
        .and_then(|(_, m)| m.iter().find(|(k, _)| *k == db_type).map(|(_, v)| *v))
}

/// Whether `flavor in DB_FLAVORS_CAST_DTYPES`.
pub fn has_cast_dtypes(flavor: Option<Flavor>) -> bool {
    flavor.is_some_and(|f| DB_FLAVORS_CAST_DTYPES.iter().any(|(k, _)| *k == f.as_str()))
}

/// `get_numeric_precision_scale(flavor, dtype)` (`utils/dtypes/sql.py:1366`).
pub fn get_numeric_precision_scale(
    flavor: Option<Flavor>,
    dtype: Option<&str>,
) -> (Option<i64>, Option<i64>) {
    let Some(dtype) = dtype.filter(|d| !d.is_empty()) else {
        return (None, None);
    };
    let (lbracket, rbracket) = if dtype.contains('[') {
        ('[', ']')
    } else {
        ('(', ')')
    };
    if dtype.contains(lbracket) && dtype.matches(',').count() == 1 && dtype.ends_with(rbracket) {
        let inner = dtype
            .split_once(lbracket)
            .map(|(_, r)| r)
            .unwrap_or("")
            .trim_end_matches(rbracket);
        if let Some((p, s)) = inner.split_once(',') {
            if let (Some(p), Some(s)) = (py_int(p), py_int(s)) {
                return (Some(p), Some(s));
            }
        }
    }
    match flavor.and_then(Flavor::numeric_precision) {
        Some((p, s)) => (Some(p), Some(s)),
        None => (None, None),
    }
}

/// `get_pd_type_from_db_type(db_type, allow_custom_dtypes)` (`utils/dtypes/sql.py:1162`).
pub fn get_pd_type_from_db_type(db_type: &str, allow_custom_dtypes: bool) -> String {
    fn parse_custom(pd_type: &str, db_type: &str) -> String {
        if db_type.to_lowercase().contains("json") {
            return "json".to_string();
        }
        if are_dtypes_equal(pd_type, "numeric") && pd_type != "object" {
            let upper = db_type.to_uppercase();
            if let (Some(p), Some(s)) = get_numeric_precision_scale(None, Some(&upper)) {
                if p != 0 && s != 0 {
                    return format!("numeric[{p},{s}]");
                }
            }
        }
        if are_dtypes_equal(pd_type, "geometry") && pd_type != "object" {
            let (geometry_type, srid) = get_geometry_type_srid(&db_type.to_uppercase());
            let mut modifiers = Vec::new();
            if !geometry_type.is_empty() {
                modifiers.push(geometry_type);
            }
            if srid.is_truthy() {
                modifiers.push(srid.py_str());
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
    let key = crate::pytext::py_strip(upper.split('(').next().unwrap_or(""));
    if let Some((_, pd_type)) = DB_TO_PD_DTYPES.iter().find(|(k, _)| *k == key) {
        return if allow_custom_dtypes {
            parse_custom(pd_type, db_type)
        } else {
            pd_type.to_string()
        };
    }
    for (db_t, pd_t) in DB_TO_PD_SUBSTRINGS {
        if upper.contains(db_t) {
            // PARITY: Python passes the matched *substring* (not the full type) to
            // `parse_custom` (`utils/dtypes/sql.py:1205`), so modifiers are dropped here.
            return if allow_custom_dtypes {
                parse_custom(pd_t, db_t)
            } else {
                pd_t.to_string()
            };
        }
    }
    DB_TO_PD_DEFAULT.to_string()
}

/// `get_db_type_from_pd_type(pd_type, flavor)` with `as_sqlalchemy=False`
/// (`utils/dtypes/sql.py:1212`). `flavor=None` is the `'default'` column.
pub fn get_db_type_from_pd_type(pd_type: &str, flavor: Option<Flavor>) -> String {
    let og_pd_type = pd_type;
    let mut pd_type: String = alias_dtype(pd_type).unwrap_or(pd_type).to_string();
    let mut precision_scale = (None, None);
    let mut geometry: Option<(String, Srid)> = None;

    let in_registry = |t: &str| PD_TO_DB_DTYPES_FLAVORS.iter().any(|(k, _)| *k == t);
    let mut found = false;
    if !in_registry(&pd_type)
        && !["numeric", "geometry", "geography"]
            .iter()
            .any(|t| pd_type.starts_with(&format!("{t}[")))
    {
        for (mapped, _) in PD_TO_DB_DTYPES_FLAVORS {
            if are_dtypes_equal(mapped, &pd_type) {
                pd_type = mapped.to_string();
                found = true;
                break;
            }
        }
    } else if pd_type.starts_with("geometry[") || pd_type.starts_with("geography[") {
        let og = pd_type.clone();
        pd_type = if og.contains("geometry") {
            "geometry".to_string()
        } else {
            "geography".to_string()
        };
        let (gtype, srid) = get_geometry_type_srid(&og);
        let srid = match srid {
            Srid::Str(s) => match py_int(s.split_once(':').map(|(_, r)| r).unwrap_or(&s)) {
                Some(i) => Srid::Int(i),
                None => Srid::Str(s),
            },
            other => other,
        };
        geometry = Some((gtype, srid));
        found = true;
    } else if pd_type.starts_with("numeric[") {
        let og = pd_type.clone();
        pd_type = "numeric".to_string();
        precision_scale = get_numeric_precision_scale(flavor, Some(&og));
        found = true;
    } else {
        found = true;
    }
    let _ = og_pd_type;

    if !found {
        tracing::warn!("Unknown Pandas data type '{pd_type}'. Falling back to 'TEXT'.");
        return "TEXT".to_string();
    }
    let db_type = pd_to_db_entry(&pd_type, flavor).unwrap_or("TEXT");
    if let (Some(p), Some(s)) = precision_scale {
        let bare = db_type.split('(').next().unwrap_or("");
        return format!("{bare}({p},{s})");
    }
    if let Some((gtype, srid)) = geometry {
        let lower = db_type.to_lowercase();
        if !lower.contains("geometry") && !lower.contains("geography") {
            return db_type.to_string();
        }
        let bare = db_type.split('(').next().unwrap_or("");
        return format!("{bare}({}, {})", gtype.to_uppercase(), srid.py_str());
    }
    db_type.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pd_to_db() {
        assert_eq!(
            get_db_type_from_pd_type("int64[pyarrow]", Some(Flavor::Postgresql)),
            "BIGINT"
        );
        assert_eq!(
            get_db_type_from_pd_type("numeric", Some(Flavor::Mysql)),
            "DECIMAL(38, 20)"
        );
        assert_eq!(
            get_db_type_from_pd_type("numeric[10,2]", Some(Flavor::Mssql)),
            "NUMERIC(10,2)"
        );
        assert_eq!(
            get_db_type_from_pd_type("geometry[Point, 4326]", Some(Flavor::Postgis)),
            "GEOMETRY(POINT, 4326)"
        );
        assert_eq!(
            get_db_type_from_pd_type("geometry[POINT, EPSG:4326]", Some(Flavor::Postgresql)),
            "TEXT"
        );
    }

    #[test]
    fn db_to_pd() {
        assert_eq!(get_pd_type_from_db_type("BIGINT", true), "int64[pyarrow]");
        assert_eq!(
            get_pd_type_from_db_type("NUMERIC(10,2)", true),
            "numeric[10,2]"
        );
        assert_eq!(
            get_pd_type_from_db_type("GEOMETRY(POINT, 4326)", true),
            "geometry[POINT, 4326]"
        );
        assert_eq!(get_pd_type_from_db_type("JSONB", true), "json");
    }

    #[test]
    fn geometry_srid() {
        assert_eq!(
            get_geometry_type_srid("geometry[POINT, ESRI:102003]"),
            ("POINT".to_string(), Srid::Str("ESRI:102003".into()))
        );
        assert_eq!(
            get_geometry_type_srid("geometry[0]"),
            ("geometry".to_string(), Srid::Int(0))
        );
    }
}
