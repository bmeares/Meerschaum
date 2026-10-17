//! `get_db_type_from_pd_type(pd_type, flavor='postgresql')` (`utils/dtypes/sql.py`), the only
//! flavor `ValkeyConnector.get_pipe_columns_types()` uses.
//!
//! MERGE NOTE: `meerschaum-connector-sql` has the all-flavor port; this PostgreSQL column is kept here so
//! the Valkey connector does not depend on the SQL connector. It belongs in
//! `meerschaum_core::dtypes` next to `get_pd_type_from_db_type`.

use meerschaum_core::dtypes::{alias_dtype, are_dtypes_equal, parse_numeric_precision_scale};

/// The `postgresql` column of `PD_TO_DB_DTYPES_FLAVORS` (generated from 4.1.4), in order.
pub const PD_TO_POSTGRESQL: &[(&str, &str)] = &[
    ("int", "BIGINT"),
    ("uint", "BIGINT"),
    ("int8", "SMALLINT"),
    ("uint8", "SMALLINT"),
    ("int16", "SMALLINT"),
    ("int32", "INT"),
    ("int64", "BIGINT"),
    ("float", "DOUBLE PRECISION"),
    ("double", "DOUBLE PRECISION"),
    ("datetime64[ns]", "TIMESTAMP(6)"),
    ("datetime64[ns, UTC]", "TIMESTAMPTZ(9)"),
    ("datetime64[us]", "TIMESTAMP(6)"),
    ("datetime64[us, UTC]", "TIMESTAMPTZ(6)"),
    ("datetime64[ms]", "TIMESTAMP(3)"),
    ("datetime64[ms, UTC]", "TIMESTAMPTZ(3)"),
    ("datetime64[s]", "TIMESTAMP(0)"),
    ("datetime64[s, UTC]", "TIMESTAMPTZ(0)"),
    ("datetime", "TIMESTAMPTZ"),
    ("datetimetz", "TIMESTAMPTZ"),
    ("date", "DATE"),
    ("bool", "BOOLEAN"),
    ("bool[pyarrow]", "BOOLEAN"),
    ("boolean", "BOOLEAN"),
    ("object", "TEXT"),
    ("string", "TEXT"),
    ("unicode", "TEXT"),
    ("json", "JSONB"),
    ("numeric", "NUMERIC"),
    ("uuid", "UUID"),
    ("bytes", "BYTEA"),
    ("geometry", "TEXT"),
    ("geography", "TEXT"),
];

fn lookup(pd_type: &str) -> Option<&'static str> {
    PD_TO_POSTGRESQL
        .iter()
        .find(|(k, _)| *k == pd_type)
        .map(|(_, v)| *v)
}

/// `get_db_type_from_pd_type(pd_type, flavor='postgresql')`.
pub fn get_postgresql_type_from_pd_type(pd_type: &str) -> String {
    let pd_type = alias_dtype(pd_type).unwrap_or(pd_type);
    if let Some(t) = lookup(pd_type) {
        return t.to_string();
    }
    if pd_type.starts_with("numeric[") {
        return match parse_numeric_precision_scale(pd_type) {
            Some((p, s)) => format!("NUMERIC({p},{s})"),
            None => "NUMERIC".to_string(),
        };
    }
    if pd_type.starts_with("geometry[") || pd_type.starts_with("geography[") {
        // PostgreSQL (without PostGIS) stores geometries as text.
        return "TEXT".to_string();
    }
    for (mapped, db_type) in PD_TO_POSTGRESQL {
        if are_dtypes_equal(mapped, pd_type) {
            return db_type.to_string();
        }
    }
    tracing::warn!("Unknown Pandas data type '{pd_type}'. Falling back to 'TEXT'.");
    "TEXT".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgresql_types() {
        assert_eq!(get_postgresql_type_from_pd_type("Int64"), "BIGINT");
        assert_eq!(get_postgresql_type_from_pd_type("int64[pyarrow]"), "BIGINT");
        assert_eq!(
            get_postgresql_type_from_pd_type("Float64"),
            "DOUBLE PRECISION"
        );
        assert_eq!(get_postgresql_type_from_pd_type("string"), "TEXT");
        assert_eq!(
            get_postgresql_type_from_pd_type("timestamp[us, tz=UTC][pyarrow]"),
            "TIMESTAMP(6)"
        );
        assert_eq!(
            get_postgresql_type_from_pd_type("datetime64[us, UTC]"),
            "TIMESTAMPTZ(6)"
        );
        assert_eq!(get_postgresql_type_from_pd_type("boolean"), "BOOLEAN");
        assert_eq!(
            get_postgresql_type_from_pd_type("numeric[10,2]"),
            "NUMERIC(10,2)"
        );
        assert_eq!(get_postgresql_type_from_pd_type("json"), "JSONB");
    }
}
