//! SQL dtype mapping (`meerschaum/utils/dtypes/sql.py`): the per-flavor pandas → database type
//! tables, the cast tables, and the flavor-aware `get_numeric_precision_scale` /
//! `get_db_type_from_pd_type`.
//!
//! The Meerschaum dtype vocabulary (`MRSM_ALIAS_DTYPES`, `are_dtypes_equal`,
//! `get_geometry_type_srid`, the precision-unit tables) and the database → pandas direction
//! (`DB_TO_PD_DTYPES`, `get_pd_type_from_db_type`) are owned by [`meerschaum_core::dtypes`] and
//! re-exported here.

use meerschaum_core::pyfmt::parse_py_int;

use crate::flavor::Flavor;

pub use meerschaum_core::dtypes::{
    DB_TO_PD_DEFAULT, DB_TO_PD_DTYPES, DB_TO_PD_SUBSTRINGS, MRSM_ALIAS_DTYPES, PrecisionScalar,
    Srid, alias_dtype, are_dtypes_equal, get_geometry_type_srid, get_pd_type_from_db_type,
    precision_unit_alias, precision_unit_scalar, value_is_null,
};

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
    if let (Some(p), Some(s)) = meerschaum_core::dtypes::get_numeric_precision_scale(Some(dtype)) {
        return (Some(p), Some(s));
    }
    match flavor.and_then(Flavor::numeric_precision) {
        Some((p, s)) => (Some(p), Some(s)),
        None => (None, None),
    }
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
        // PARITY: Python raises `ValueError` on a malformed modifier (`geometry[4326.0]`);
        // the default type and SRID are used instead.
        let (gtype, srid) = get_geometry_type_srid(&og, "geometry", Srid::Code(0))
            .unwrap_or_else(|_| ("geometry".to_string(), Srid::Code(0)));
        // `int(srid.split(':', maxsplit=1)[-1])` (`utils/dtypes/sql.py:1273`).
        let srid = match srid {
            Srid::Authority(s) => {
                match parse_py_int(s.split_once(':').map(|(_, r)| r).unwrap_or(&s)) {
                    Some(i) => Srid::Code(i),
                    None => Srid::Authority(s),
                }
            }
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
        return format!("{bare}({}, {srid})", gtype.to_uppercase());
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
        let g = |d: &str| get_geometry_type_srid(d, "geometry", Srid::Code(0)).unwrap();
        assert_eq!(
            g("geometry[POINT, ESRI:102003]"),
            ("POINT".to_string(), Srid::Authority("ESRI:102003".into()))
        );
        assert_eq!(g("geometry[0]"), ("geometry".to_string(), Srid::Code(0)));
    }
}
