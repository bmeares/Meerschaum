//! Row estimates from catalog statistics (`Pipe.get_rowcount(estimate=True)`, contract C1).
//!
//! Each query returns one integer (or `NULL`) and never scans the table:
//!
//! - PostgreSQL, PostGIS, Citus: `pg_class.reltuples` (`-1` when never analyzed); a
//!   partitioned table sums its partitions' statistics.
//! - TimescaleDB: `approximate_row_count()`, which covers hypertables (chunks, compressed
//!   chunks) and plain tables.
//! - MySQL, MariaDB: `information_schema.TABLES.TABLE_ROWS`.
//! - MSSQL: `sys.partitions.rows` of the heap or clustered index.
//! - Oracle: `ALL_TABLES.NUM_ROWS` (`NULL` until statistics are gathered).
//!
//! SQLite, GeoPackage, DuckDB and CockroachDB have no estimate here: their counts are exact.
//! [`use_estimate`] decides when an estimate is trusted over an exact count.

use crate::flavor::Flavor;
use crate::ident::{sql_item_name, truncate_item_name};

/// Below this many rows (by estimate) the exact count is cheap enough to run instead.
pub const EXACT_COUNT_BELOW: i64 = 100_000;

fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Whether `flavor` answers [`get_rowcount_estimate_query`].
pub fn has_row_estimates(flavor: Flavor) -> bool {
    !matches!(
        flavor,
        Flavor::Sqlite | Flavor::Geopackage | Flavor::Duckdb | Flavor::Cockroachdb
    )
}

/// The catalog query that estimates the rows of `table` (in `schema`), or `None` when the
/// flavor has no estimate ([`has_row_estimates`]).
pub fn get_rowcount_estimate_query(
    flavor: Flavor,
    table: &str,
    schema: Option<&str>,
) -> Option<String> {
    if !has_row_estimates(flavor) {
        return None;
    }
    let table_trunc = truncate_item_name(table, flavor);
    let t = esc(&table_trunc);
    Some(match flavor {
        Flavor::Timescaledb | Flavor::TimescaledbHa => {
            let name = sql_item_name(table, flavor, schema.filter(|s| !s.is_empty()));
            format!("SELECT approximate_row_count('{}'::regclass)", esc(&name))
        }
        Flavor::Postgresql | Flavor::Postgis | Flavor::Citus => {
            let schema_clause = match schema.filter(|s| !s.is_empty()) {
                Some(s) => format!("'{}'", esc(s)),
                None => "current_schema()".to_string(),
            };
            format!(
                "SELECT CAST(CASE WHEN c.relkind = 'p' THEN (\n    \
                 SELECT CASE WHEN COUNT(*) = 0 OR MAX(ch.reltuples) < 0 THEN -1\n    \
                 ELSE SUM(GREATEST(ch.reltuples, 0)) END\n    \
                 FROM pg_inherits i\n    \
                 JOIN pg_class ch ON ch.oid = i.inhrelid\n    \
                 WHERE i.inhparent = c.oid\n\
                 ) ELSE c.reltuples END AS BIGINT) AS estimate\n\
                 FROM pg_class c\n\
                 JOIN pg_namespace n ON n.oid = c.relnamespace\n\
                 WHERE n.nspname = {schema_clause} AND c.relname = '{t}'"
            )
        }
        Flavor::Mysql | Flavor::Mariadb => {
            let schema_clause = match schema.filter(|s| !s.is_empty()) {
                Some(s) => format!("'{}'", esc(s)),
                None => "DATABASE()".to_string(),
            };
            format!(
                "SELECT TABLE_ROWS FROM information_schema.TABLES\n\
                 WHERE TABLE_SCHEMA = {schema_clause} AND TABLE_NAME = '{t}'"
            )
        }
        Flavor::Mssql => {
            let s = esc(schema.filter(|s| !s.is_empty()).unwrap_or("dbo"));
            format!(
                "SELECT CAST(SUM(p.rows) AS BIGINT) AS estimate\n\
                 FROM sys.partitions p\n\
                 JOIN sys.tables t ON t.object_id = p.object_id\n\
                 JOIN sys.schemas s ON s.schema_id = t.schema_id\n\
                 WHERE s.name = N'{s}' AND t.name = N'{t}' AND p.index_id IN (0, 1)"
            )
        }
        Flavor::Oracle => {
            let owner = match schema.filter(|s| !s.is_empty()) {
                Some(s) => format!("'{}'", esc(&s.to_uppercase())),
                None => "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')".to_string(),
            };
            format!(
                "SELECT MAX(num_rows) FROM all_tables\n\
                 WHERE owner = {owner} AND table_name IN ('{t}', '{}')",
                esc(&table_trunc.to_uppercase())
            )
        }
        Flavor::Sqlite | Flavor::Geopackage | Flavor::Duckdb | Flavor::Cockroachdb => {
            return None;
        }
    })
}

/// Whether an estimate is the answer: it exists, is not negative (never analyzed), is not zero
/// (empty, or statistics not gathered yet) and is large enough that counting would be slow.
/// Otherwise the exact count is run.
pub fn use_estimate(estimate: Option<i64>) -> bool {
    estimate.is_some_and(|n| n >= EXACT_COUNT_BELOW)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_per_flavor() {
        assert!(get_rowcount_estimate_query(Flavor::Sqlite, "t", None).is_none());
        assert!(get_rowcount_estimate_query(Flavor::Duckdb, "t", None).is_none());
        let pg =
            get_rowcount_estimate_query(Flavor::Postgresql, "we'ather", Some("public")).unwrap();
        assert!(pg.contains("c.relname = 'we''ather'"), "{pg}");
        assert!(pg.contains("n.nspname = 'public'"), "{pg}");
        assert!(pg.contains("pg_inherits"), "{pg}");
        let ts =
            get_rowcount_estimate_query(Flavor::Timescaledb, "weather", Some("public")).unwrap();
        assert_eq!(
            ts,
            "SELECT approximate_row_count('\"public\".\"weather\"'::regclass)"
        );
        let my = get_rowcount_estimate_query(Flavor::Mysql, "weather", None).unwrap();
        assert!(my.contains("TABLE_SCHEMA = DATABASE()"), "{my}");
        let ms = get_rowcount_estimate_query(Flavor::Mssql, "weather", None).unwrap();
        assert!(ms.contains("s.name = N'dbo'"), "{ms}");
        assert!(ms.contains("index_id IN (0, 1)"), "{ms}");
        let ora = get_rowcount_estimate_query(Flavor::Oracle, "weather", None).unwrap();
        assert!(
            ora.contains("table_name IN ('weather', 'WEATHER')"),
            "{ora}"
        );
    }

    #[test]
    fn estimates_are_trusted_only_when_large() {
        assert!(!use_estimate(None));
        assert!(!use_estimate(Some(-1)));
        assert!(!use_estimate(Some(0)));
        assert!(!use_estimate(Some(EXACT_COUNT_BELOW - 1)));
        assert!(use_estimate(Some(EXACT_COUNT_BELOW)));
        assert!(use_estimate(Some(12_000_000)));
    }
}
