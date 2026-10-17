//! `VACUUM` / `ANALYZE` builders (`meerschaum/connectors/sql/_maintenance.py`).

use crate::flavor::Flavor;

/// `_get_vacuum_queries(pipe, pipe_name, full)`; `None` if the flavor cannot vacuum.
pub fn get_vacuum_queries(flavor: Flavor, pipe_name: &str, full: bool) -> Option<Vec<String>> {
    match flavor {
        Flavor::Postgresql | Flavor::Postgis | Flavor::Citus => {
            let options = if full && flavor.full_vacuum() {
                "FULL"
            } else {
                ""
            };
            let opt = if options.is_empty() {
                String::new()
            } else {
                format!("({options}) ")
            };
            Some(vec![
                meerschaum_core::pyfmt::py_strip(&format!("VACUUM {opt}{pipe_name}")).to_string(),
            ])
        }
        // `VACUUM` recurses into a hypertable's chunks; `full` is intentionally ignored.
        Flavor::Timescaledb | Flavor::TimescaledbHa => Some(vec![format!("VACUUM {pipe_name}")]),
        Flavor::Mysql | Flavor::Mariadb => Some(vec![format!("OPTIMIZE TABLE {pipe_name}")]),
        Flavor::Mssql => Some(vec![format!("ALTER TABLE {pipe_name} REBUILD")]),
        // SQLite's `VACUUM` is database-wide.
        Flavor::Sqlite | Flavor::Geopackage => Some(vec!["VACUUM".to_string()]),
        _ => None,
    }
}

/// `_get_analyze_query(pipe, pipe_name)`; `None` if the flavor cannot analyze.
pub fn get_analyze_query(flavor: Flavor, pipe_name: &str) -> Option<String> {
    match flavor {
        Flavor::Timescaledb
        | Flavor::TimescaledbHa
        | Flavor::Postgresql
        | Flavor::Postgis
        | Flavor::Citus
        | Flavor::Cockroachdb
        | Flavor::Sqlite
        | Flavor::Geopackage => Some(format!("ANALYZE {pipe_name}")),
        Flavor::Mysql | Flavor::Mariadb => Some(format!("ANALYZE TABLE {pipe_name}")),
        Flavor::Mssql => Some(format!("UPDATE STATISTICS {pipe_name}")),
        // DuckDB's `ANALYZE` refreshes statistics for the whole database.
        Flavor::Duckdb => Some("ANALYZE".to_string()),
        Flavor::Oracle => None,
    }
}
