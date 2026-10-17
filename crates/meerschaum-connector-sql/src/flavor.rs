//! The `Flavor` enum and every per-flavor table from `meerschaum/utils/sql.py`,
//! `meerschaum/utils/dtypes/sql.py`, `meerschaum/connectors/sql/_create_engine.py`
//! (`STATIC_CONFIG['sql']['create_engine_flavors']`), `_partition.py`, `_compress.py`,
//! `_maintenance.py` and `_pipes.py`.
//!
//! Python keys its tables by flavor string with a `'default'` fallback. Here each table is a
//! method on [`Flavor`]; where Python can be handed a key that is not a real flavor (for example
//! the `'sqlite_delete_insert'` pseudo-flavor of `get_update_queries`), the crate passes an
//! `Option<Flavor>` and `None` means "use the `'default'` entry".

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A SQL database flavor supported by `SQLConnector`.
///
/// These are the keys of `STATIC_CONFIG['sql']['create_engine_flavors']`. The task brief counts
/// twelve; Python also ships `timescaledb-ha` (TimescaleDB with PostGIS), so there are thirteen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Flavor {
    #[serde(rename = "timescaledb")]
    Timescaledb,
    #[serde(rename = "timescaledb-ha")]
    TimescaledbHa,
    #[serde(rename = "postgresql")]
    Postgresql,
    #[serde(rename = "postgis")]
    Postgis,
    #[serde(rename = "citus")]
    Citus,
    #[serde(rename = "cockroachdb")]
    Cockroachdb,
    #[serde(rename = "mssql")]
    Mssql,
    #[serde(rename = "mysql")]
    Mysql,
    #[serde(rename = "mariadb")]
    Mariadb,
    #[serde(rename = "oracle")]
    Oracle,
    #[serde(rename = "sqlite")]
    Sqlite,
    #[serde(rename = "geopackage")]
    Geopackage,
    #[serde(rename = "duckdb")]
    Duckdb,
}

use Flavor::*;

/// Error returned when parsing an unknown flavor string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Flavor '{0}' is not supported by Meerschaum SQLConnector")]
pub struct UnknownFlavor(pub String);

/// The non-TimescaleDB driver family a flavor's connections use (D13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DriverFamily {
    /// `sqlx` PostgreSQL driver.
    Postgres,
    /// `sqlx` MySQL driver.
    Mysql,
    /// `sqlx` SQLite driver (GeoPackage is SQLite).
    Sqlite,
    /// `tiberius` (not yet implemented).
    Mssql,
    /// `oracle` crate (not yet implemented).
    Oracle,
    /// `duckdb` crate (not yet implemented).
    Duckdb,
}

impl Flavor {
    /// Every flavor, in the order of `create_engine_flavors`.
    pub const ALL: [Flavor; 13] = [
        Timescaledb,
        TimescaledbHa,
        Postgresql,
        Postgis,
        Citus,
        Mssql,
        Mysql,
        Mariadb,
        Oracle,
        Sqlite,
        Geopackage,
        Duckdb,
        Cockroachdb,
    ];

    /// The flavor's string key as used throughout the Python code and `connectors.yaml`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Timescaledb => "timescaledb",
            TimescaledbHa => "timescaledb-ha",
            Postgresql => "postgresql",
            Postgis => "postgis",
            Citus => "citus",
            Cockroachdb => "cockroachdb",
            Mssql => "mssql",
            Mysql => "mysql",
            Mariadb => "mariadb",
            Oracle => "oracle",
            Sqlite => "sqlite",
            Geopackage => "geopackage",
            Duckdb => "duckdb",
        }
    }

    /// Which driver serves this flavor (D13).
    pub const fn driver_family(self) -> DriverFamily {
        match self {
            Timescaledb | TimescaledbHa | Postgresql | Postgis | Citus | Cockroachdb => {
                DriverFamily::Postgres
            }
            Mysql | Mariadb => DriverFamily::Mysql,
            Sqlite | Geopackage => DriverFamily::Sqlite,
            Mssql => DriverFamily::Mssql,
            Oracle => DriverFamily::Oracle,
            Duckdb => DriverFamily::Duckdb,
        }
    }

    // ------------------------------------------------------------------------------------------
    // `meerschaum/utils/sql.py` tables
    // ------------------------------------------------------------------------------------------

    /// `test_queries` (`utils/sql.py:25`).
    pub const fn test_query(self) -> &'static str {
        match self {
            Oracle => "SELECT 1 FROM DUAL",
            _ => "SELECT 1",
        }
    }

    /// `exists_queries['default']` template (`utils/sql.py:33`); `{table_name}` is escaped.
    pub const fn exists_query_template(self) -> &'static str {
        "SELECT COUNT(*) FROM {table_name} WHERE 1 = 0"
    }

    /// `version_queries` template (`utils/sql.py:36`); `{version_name}` is the quoted alias.
    pub const fn version_query_template(self) -> &'static str {
        match self {
            Sqlite | Geopackage => "SELECT SQLITE_VERSION() AS {version_name}",
            Mssql => "SELECT @@version",
            Oracle => "SELECT version from PRODUCT_COMPONENT_VERSION WHERE rownum = 1",
            _ => "SELECT VERSION() AS {version_name}",
        }
    }

    /// `SKIP_IF_EXISTS_FLAVORS` (`utils/sql.py:43`).
    pub const fn skip_if_exists(self) -> bool {
        matches!(self, Mssql | Oracle)
    }

    /// `DROP_IF_EXISTS_FLAVORS` (`utils/sql.py:44`).
    pub const fn drop_if_exists(self) -> bool {
        matches!(
            self,
            Timescaledb
                | TimescaledbHa
                | Postgresql
                | Postgis
                | Citus
                | Mssql
                | Mysql
                | Mariadb
                | Sqlite
                | Geopackage
        )
    }

    /// `DROP_INDEX_IF_EXISTS_FLAVORS` (`utils/sql.py:56`).
    pub const fn drop_index_if_exists(self) -> bool {
        matches!(
            self,
            Mssql
                | Timescaledb
                | TimescaledbHa
                | Postgresql
                | Postgis
                | Sqlite
                | Geopackage
                | Citus
        )
    }

    /// `SKIP_AUTO_INCREMENT_FLAVORS` (`utils/sql.py:66`).
    pub const fn skip_auto_increment(self) -> bool {
        matches!(self, Citus | Duckdb)
    }

    /// `COALESCE_UNIQUE_INDEX_FLAVORS` (`utils/sql.py:67`).
    pub const fn coalesce_unique_index(self) -> bool {
        matches!(
            self,
            Timescaledb | TimescaledbHa | Postgresql | Postgis | Citus
        )
    }

    /// `table_wrappers` (`utils/sql.py:583`). Every flavor has an explicit entry.
    pub const fn table_wrappers(self) -> (&'static str, &'static str) {
        match self {
            Mysql | Mariadb => ("`", "`"),
            Mssql => ("[", "]"),
            _ => ("\"", "\""),
        }
    }

    /// `max_name_lens` (`utils/sql.py:599`). `duckdb` falls back to `'default'` (64).
    pub const fn max_name_len(self) -> usize {
        match self {
            Mssql => 128,
            Oracle => 30,
            Sqlite | Geopackage => 1024,
            _ => DEFAULT_MAX_NAME_LEN,
        }
    }

    /// `json_flavors` (`utils/sql.py:614`).
    pub const fn is_json_flavor(self) -> bool {
        matches!(
            self,
            Postgresql | Postgis | Timescaledb | TimescaledbHa | Citus | Cockroachdb
        )
    }

    /// `NO_SCHEMA_FLAVORS` (`utils/sql.py:622`).
    pub const fn no_schema(self) -> bool {
        matches!(
            self,
            Oracle | Sqlite | Geopackage | Mysql | Mariadb | Duckdb
        )
    }

    /// `DEFAULT_SCHEMA_FLAVORS` (`utils/sql.py:630`).
    pub const fn default_schema(self) -> Option<&'static str> {
        match self {
            Postgresql | Postgis | Timescaledb | TimescaledbHa | Citus | Cockroachdb => {
                Some("public")
            }
            Mysql | Mariadb => Some("mysql"),
            Mssql => Some("dbo"),
            _ => None,
        }
    }

    /// `OMIT_NULLSFIRST_FLAVORS` (`utils/sql.py:641`).
    pub const fn omit_nullsfirst(self) -> bool {
        matches!(self, Mariadb | Mysql | Mssql)
    }

    /// `SINGLE_ALTER_TABLE_FLAVORS` (`utils/sql.py:647`).
    pub const fn single_alter_table(self) -> bool {
        matches!(self, Duckdb | Sqlite | Geopackage | Mssql | Oracle)
    }

    /// `NO_CTE_FLAVORS` (`utils/sql.py:654`).
    pub const fn no_cte(self) -> bool {
        matches!(self, Mysql | Mariadb)
    }

    /// `NO_SELECT_INTO_FLAVORS` (`utils/sql.py:658`).
    pub const fn no_select_into(self) -> bool {
        matches!(
            self,
            Sqlite | Geopackage | Oracle | Mysql | Mariadb | Duckdb
        )
    }

    /// `hypertable_queries` (`utils/sql.py:336`); `{table_name}` is the quoted table.
    pub const fn hypertable_query_template(self) -> Option<&'static str> {
        match self {
            Timescaledb | TimescaledbHa => Some("SELECT hypertable_size('{table_name}')"),
            Citus => Some("SELECT citus_table_size('{table_name}')"),
            _ => None,
        }
    }

    /// `PG_PARTITION_FLAVORS` (`utils/sql.py:2292`, `_partition.py:28`).
    pub const fn pg_partition(self) -> bool {
        matches!(self, Postgresql | Postgis)
    }

    /// `MYSQL_PARTITION_FLAVORS` (`utils/sql.py:2293`, `_partition.py:31`).
    pub const fn mysql_partition(self) -> bool {
        matches!(self, Mysql | Mariadb)
    }

    /// `NATIVE_PARTITION_FLAVORS` (`utils/sql.py:2294`): PG-style and MySQL-style only.
    pub const fn native_partition(self) -> bool {
        self.pg_partition() || self.mysql_partition()
    }

    /// `MSSQL_PARTITION_FLAVORS` (`_partition.py:34`).
    pub const fn mssql_partition(self) -> bool {
        matches!(self, Mssql)
    }

    /// `PARTITIONABLE_FLAVORS` (`_partition.py:36`).
    pub const fn partitionable(self) -> bool {
        self.native_partition() || self.mssql_partition()
    }

    /// `_TIMESCALEDB_FLAVORS` (`_partition.py:792`).
    pub const fn is_timescaledb(self) -> bool {
        matches!(self, Timescaledb | TimescaledbHa)
    }

    /// `_POSTGRES_UNIQUE_CONSTRAINT_FLAVORS` (`_pipes.py:20`).
    pub const fn postgres_unique_constraint(self) -> bool {
        matches!(
            self,
            Citus | Postgresql | Postgis | Timescaledb | TimescaledbHa
        )
    }

    /// `COMPRESSIBLE_FLAVORS` (`_compress.py:19`).
    pub const fn compressible(self) -> bool {
        matches!(self, Timescaledb | TimescaledbHa | Mysql | Mariadb | Mssql)
    }

    /// `VACUUMABLE_FLAVORS` (`_maintenance.py:19`).
    pub const fn vacuumable(self) -> bool {
        matches!(
            self,
            Timescaledb
                | TimescaledbHa
                | Postgresql
                | Postgis
                | Citus
                | Mysql
                | Mariadb
                | Mssql
                | Sqlite
                | Geopackage
        )
    }

    /// `_AUTOCOMMIT_VACUUM_FLAVORS` (`_maintenance.py:33`).
    pub const fn autocommit_vacuum(self) -> bool {
        matches!(
            self,
            Timescaledb | TimescaledbHa | Postgresql | Postgis | Citus | Sqlite | Geopackage
        )
    }

    /// `_FULL_VACUUM_FLAVORS` (`_maintenance.py:44`).
    pub const fn full_vacuum(self) -> bool {
        matches!(self, Postgresql | Postgis | Citus)
    }

    /// `ANALYZABLE_FLAVORS` (`_maintenance.py:51`).
    pub const fn analyzable(self) -> bool {
        self.vacuumable() || matches!(self, Cockroachdb | Duckdb)
    }

    // ------------------------------------------------------------------------------------------
    // `meerschaum/utils/dtypes/sql.py` tables
    // ------------------------------------------------------------------------------------------

    /// `NUMERIC_PRECISION_FLAVORS` (`utils/dtypes/sql.py:14`).
    pub const fn numeric_precision(self) -> Option<(i64, i64)> {
        match self {
            Mariadb | Mysql => Some((38, 20)),
            Mssql => Some((28, 10)),
            _ => None,
        }
    }

    /// `NUMERIC_AS_TEXT_FLAVORS` (`utils/dtypes/sql.py:19`).
    pub const fn numeric_as_text(self) -> bool {
        matches!(self, Sqlite | Duckdb | Geopackage)
    }

    /// `TIMEZONE_NAIVE_FLAVORS` (`utils/dtypes/sql.py:20`).
    pub const fn timezone_naive(self) -> bool {
        matches!(self, Oracle | Mysql | Mariadb)
    }

    /// `AUTO_INCREMENT_COLUMN_FLAVORS` (`utils/dtypes/sql.py:1144`).
    pub const fn auto_increment_column(self) -> &'static str {
        match self {
            Mariadb | Mysql => "AUTO_INCREMENT",
            Mssql => "IDENTITY(1,1)",
            Oracle => "GENERATED BY DEFAULT ON NULL AS IDENTITY",
            Sqlite | Geopackage => "AUTOINCREMENT",
            Duckdb | Citus => "GENERATED BY DEFAULT",
            _ => "GENERATED BY DEFAULT AS IDENTITY",
        }
    }

    // ------------------------------------------------------------------------------------------
    // `STATIC_CONFIG['sql']['create_engine_flavors']` (`meerschaum/_internal/static.py:73`)
    // ------------------------------------------------------------------------------------------

    /// The SQLAlchemy engine (dialect+driver) prefix used to build URIs.
    pub const fn engine(self) -> &'static str {
        match self {
            Timescaledb | TimescaledbHa | Postgresql | Postgis | Citus => "postgresql+psycopg",
            Mssql => "mssql+pyodbc",
            Mysql | Mariadb => "mysql+pymysql",
            Oracle => "oracle+oracledb",
            Sqlite | Geopackage => "sqlite",
            Duckdb => "duckdb",
            Cockroachdb => "cockroachdb",
        }
    }

    /// The flavor's `defaults` as `(attribute, value)` pairs (strings; `port` is numeric text).
    pub const fn defaults(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Timescaledb | TimescaledbHa | Postgresql | Postgis | Citus => &[("port", "5432")],
            Mssql => &[
                ("port", "1433"),
                (
                    "options",
                    "driver=ODBC Driver 18 for SQL Server&UseFMTONLY=Yes&TrustServerCertificate=yes&Encrypt=no&MARS_Connection=yes",
                ),
            ],
            Mysql | Mariadb => &[("port", "3306")],
            Oracle => &[("port", "1521")],
            Sqlite | Geopackage | Duckdb => &[],
            Cockroachdb => &[
                ("port", "26257"),
                ("database", "defaultdb"),
                ("username", "root"),
                ("password", "admin"),
            ],
        }
    }

    /// The default port, if the flavor has one.
    pub fn default_port(self) -> Option<u16> {
        self.defaults()
            .iter()
            .find(|(k, _)| *k == "port")
            .and_then(|(_, v)| v.parse().ok())
    }

    /// The attributes required when no `uri` is given (`requirements`).
    pub const fn requirements(self) -> &'static [&'static str] {
        match self {
            Sqlite | Geopackage => &["database"],
            Duckdb => &[],
            Cockroachdb => &["host"],
            _ => &["username", "password", "host", "database"],
        }
    }

    /// Whether `create_engine` omits every keyword argument (`omit_create_engine: {'ALL'}`).
    pub const fn omit_all_create_engine_args(self) -> bool {
        matches!(self, Duckdb)
    }

    /// The Python driver packages `create_engine` installs (`install_flavor_drivers`).
    /// Informational only: the Rust drivers are listed in [`Flavor::driver_family`].
    pub const fn python_driver_packages(self) -> &'static [&'static str] {
        match self {
            Sqlite | Geopackage => &["aiosqlite"],
            Duckdb => &["duckdb", "duckdb_engine"],
            Mysql | Mariadb => &["pymysql"],
            Timescaledb | Postgresql | Citus => &["psycopg", "psycopg2"],
            TimescaledbHa => &["psycopg", "geoalchemy2"],
            Postgis => &["psycopg", "psycopg2", "geoalchemy2"],
            Cockroachdb => &[
                "psycopg",
                "psycopg2",
                "sqlalchemy_cockroachdb",
                "sqlalchemy_cockroachdb.psycopg",
            ],
            Mssql => &["pyodbc"],
            Oracle => &["oracledb"],
        }
    }

    /// `SQLConnector.IS_THREAD_SAFE` without the `:memory:` check.
    pub const fn is_thread_safe(self) -> bool {
        !matches!(self, Duckdb | Oracle)
    }
}

/// `max_name_lens['default']`.
pub const DEFAULT_MAX_NAME_LEN: usize = 64;

/// Resolve the `max_name_lens` entry for an optional flavor (`None` → `'default'`).
pub const fn max_name_len_for(flavor: Option<Flavor>) -> usize {
    match flavor {
        Some(f) => f.max_name_len(),
        None => DEFAULT_MAX_NAME_LEN,
    }
}

/// Resolve the `table_wrappers` entry for an optional flavor (`None` → `'default'`).
pub const fn table_wrappers_for(flavor: Option<Flavor>) -> (&'static str, &'static str) {
    match flavor {
        Some(f) => f.table_wrappers(),
        None => ("\"", "\""),
    }
}

impl fmt::Display for Flavor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Flavor {
    type Err = UnknownFlavor;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "timescaledb" => Timescaledb,
            "timescaledb-ha" => TimescaledbHa,
            "postgresql" => Postgresql,
            // PARITY: `SQLConnector.__init__` maps `postgres` to `postgresql`
            // (`_SQLConnector.py:254`).
            "postgres" => Postgresql,
            "postgis" => Postgis,
            "citus" => Citus,
            "cockroachdb" => Cockroachdb,
            "mssql" => Mssql,
            "mysql" => Mysql,
            "mariadb" => Mariadb,
            "oracle" => Oracle,
            "sqlite" => Sqlite,
            "geopackage" => Geopackage,
            "duckdb" => Duckdb,
            other => return Err(UnknownFlavor(other.to_string())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_names() {
        for f in Flavor::ALL {
            assert_eq!(f.as_str().parse::<Flavor>().unwrap(), f);
        }
        assert!("informix".parse::<Flavor>().is_err());
    }

    #[test]
    fn partition_sets() {
        assert!(Flavor::Postgis.native_partition());
        assert!(!Flavor::Mssql.native_partition());
        assert!(Flavor::Mssql.partitionable());
        assert!(!Flavor::Timescaledb.partitionable());
    }
}
