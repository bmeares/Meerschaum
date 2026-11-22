//! `verify schema` planning: classify database types, compare a table's columns with a pipe's
//! declared dtypes, and build each flavor's statements. Everything here is pure; the connector
//! gathers the inputs ([`get_schema_columns_query`], [`ColumnInfo`], [`TableFacts`]) in
//! `instance::schema`.
//!
//! A plan never drops a column. It adds the declared columns the table lacks (`ADD COLUMN`),
//! widens types safely (`int` → `bigint`, `int` → `float`, `float` → `numeric`, `varchar(n)` →
//! `text`, `text` → `json`/`jsonb`) and changes other types only when confirmed:
//!
//! - PostgreSQL family and DuckDB: `ALTER COLUMN ... TYPE ... USING ...` (one `ALTER TABLE` on
//!   PostgreSQL, so the table is rewritten once). A TimescaleDB hypertable with compressed
//!   chunks cannot change column types: those changes are blocked with the decompress command.
//! - MySQL / MariaDB: `MODIFY COLUMN` (nullability and `AUTO_INCREMENT` kept).
//! - MSSQL: `ALTER COLUMN` (nullability kept); indexed columns are blocked (drop the indices
//!   first).
//! - Oracle: `MODIFY` for widenings in place; other changes go through a temporary column, as
//!   `sync` does (Oracle commits each DDL statement, so this is not atomic).
//! - SQLite: `ADD COLUMN`; a type change rebuilds the table (new table, copy, drop, rename,
//!   indices replayed), which the plan says. GeoPackage tables are never rebuilt.
//!
//! A column the table is partitioned by ([`TableFacts::partitioned_by`]) keeps its type where the
//! database cannot change it in place: a PostgreSQL / PostGIS partition key (pipes with a
//! `datetime` column are partitioned by it by default), an MSSQL partitioning column, an Oracle
//! partition key; a TimescaleDB time dimension changes only between timestamp and date types or
//! between integer types, a MySQL / MariaDB `RANGE COLUMNS` column only between `DATE` and
//! `DATETIME` or between integer types. Those changes are blocked, so the others still apply.

use indexmap::{IndexMap, IndexSet};
use meerschaum_connector::schema::{SchemaChange, SchemaChangeKind, SchemaPlan};
use meerschaum_core::dtypes::are_dtypes_equal;

use crate::flavor::{DriverFamily, Flavor};
use crate::ident::{sql_item_name, truncate_item_name};

fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

// ---------------------------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------------------------

/// One column of the table as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnInfo {
    /// The column name.
    pub name: String,
    /// The column's full type (`VARCHAR(255)`, `NUMERIC(10,2)`, `TIMESTAMP WITH TIME ZONE`,
    /// `GEOMETRY(POINT, 4326)`); empty for an untyped SQLite column.
    pub db_type: String,
    /// Whether the column accepts `NULL`.
    pub nullable: bool,
    /// MySQL / MariaDB: `AUTO_INCREMENT` (kept by `MODIFY COLUMN`).
    pub auto_increment: bool,
    /// SQLite: the column's position in the primary key (`0`: not part of it).
    pub pk: i64,
    /// SQLite: the column's default expression (kept by a rebuild).
    pub default: Option<String>,
}

/// How a table is partitioned by one of its columns, which decides how that column's type may
/// change ([`get_partition_keys_queries`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionKey {
    /// The type never changes in place: a PostgreSQL partition key, an MSSQL partitioning
    /// column, an Oracle partition key, a TimescaleDB space (hash) dimension, a column inside a
    /// MySQL partitioning expression.
    Fixed,
    /// A TimescaleDB time dimension: its type changes only between timestamp and date types,
    /// or between integer types (the chunks' bounds must keep comparing).
    TimeDimension,
    /// A MySQL / MariaDB `RANGE COLUMNS` / `LIST COLUMNS` column: it stays `DATE` or
    /// `DATETIME`, or an integer type (the partitions' bounds must convert).
    RangeColumns,
}

/// What the planner knows about the table besides its columns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableFacts {
    /// The columns the table is partitioned by. A change to one of them that the database
    /// refuses is blocked, so the plan's other changes still apply.
    pub partitioned_by: IndexMap<String, PartitionKey>,
    /// A TimescaleDB hypertable with compressed chunks (column types cannot change).
    pub compressed_hypertable: bool,
    /// The command that decompresses the pipe (for the blocked message).
    pub decompress_command: Option<String>,
    /// The command that drops the pipe's indices (for the blocked message on MSSQL).
    pub drop_indices_command: Option<String>,
    /// Columns that are part of an index or the primary key.
    pub indexed: IndexSet<String>,
    /// SQLite: the `CREATE INDEX` statements replayed after a rebuild.
    pub sqlite_indexes: Vec<String>,
    /// SQLite: the table was declared with `AUTOINCREMENT`.
    pub sqlite_autoincrement: bool,
}

/// The catalog query whose rows describe the table's columns for [`columns_from_rows`]:
/// `column`, `type`, `length`, `precision`, `scale`, `nullable` (and `udt`, `extra`, `pk`,
/// `default` where the flavor has them), in column order.
pub fn get_schema_columns_query(flavor: Flavor, table: &str, schema: Option<&str>) -> String {
    let table = truncate_item_name(table, flavor);
    let t = esc(&table);
    let schema = schema.filter(|s| !s.is_empty() && !flavor.no_schema());
    match flavor.driver_family() {
        DriverFamily::Sqlite => format!(
            "SELECT name AS \"column\", type AS \"type\", \"notnull\" AS \"notnull\", \
             dflt_value AS \"default\", pk AS \"pk\"\nFROM pragma_table_info('{t}')\nORDER BY cid"
        ),
        DriverFamily::Mysql => {
            let schema_clause = match schema {
                Some(s) => format!("'{}'", esc(s)),
                None => "DATABASE()".into(),
            };
            format!(
                "SELECT COLUMN_NAME AS `column`, CAST(COLUMN_TYPE AS CHAR) AS `type`, \
                 IS_NULLABLE AS `nullable`, EXTRA AS `extra`\nFROM information_schema.COLUMNS\n\
                 WHERE TABLE_SCHEMA = {schema_clause} AND TABLE_NAME = '{t}'\n\
                 ORDER BY ORDINAL_POSITION"
            )
        }
        DriverFamily::Mssql => {
            let s = esc(schema.unwrap_or("dbo"));
            format!(
                "SELECT COLUMN_NAME AS [column], DATA_TYPE AS [type], \
                 CHARACTER_MAXIMUM_LENGTH AS [length], NUMERIC_PRECISION AS [precision], \
                 NUMERIC_SCALE AS [scale], IS_NULLABLE AS [nullable]\n\
                 FROM INFORMATION_SCHEMA.COLUMNS\n\
                 WHERE TABLE_SCHEMA = N'{s}' AND TABLE_NAME = N'{t}'\nORDER BY ORDINAL_POSITION"
            )
        }
        DriverFamily::Oracle => {
            let owner = match schema {
                Some(s) => format!("'{}'", esc(&s.to_uppercase())),
                None => "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')".into(),
            };
            format!(
                "SELECT column_name AS \"column\", data_type AS \"type\", char_length AS \"length\", \
                 data_precision AS \"precision\", data_scale AS \"scale\", nullable AS \"nullable\"\n\
                 FROM all_tab_columns\n\
                 WHERE owner = {owner} AND table_name IN ('{t}', '{}')\nORDER BY column_id",
                esc(&table.to_uppercase())
            )
        }
        DriverFamily::Postgres | DriverFamily::Duckdb => {
            let schema_clause = match schema {
                Some(s) => format!("'{}'", esc(s)),
                None => "current_schema()".into(),
            };
            let udt = if flavor.driver_family() == DriverFamily::Postgres {
                "udt_name"
            } else {
                "NULL"
            };
            format!(
                "SELECT column_name AS \"column\", data_type AS \"type\", {udt} AS \"udt\", \
                 character_maximum_length AS \"length\", numeric_precision AS \"precision\", \
                 numeric_scale AS \"scale\", is_nullable AS \"nullable\"\n\
                 FROM information_schema.columns\n\
                 WHERE table_schema = {schema_clause} AND table_name = '{t}'\n\
                 ORDER BY ordinal_position"
            )
        }
    }
}

/// One row of [`get_schema_columns_query`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnRow {
    /// `column`.
    pub column: String,
    /// `type` (`data_type`, MySQL's `COLUMN_TYPE`, SQLite's declared type).
    pub type_: String,
    /// `udt` (PostgreSQL's `udt_name`).
    pub udt: Option<String>,
    /// `length` (characters; `-1` is `MAX` on MSSQL).
    pub length: Option<i64>,
    /// `precision`.
    pub precision: Option<i64>,
    /// `scale`.
    pub scale: Option<i64>,
    /// `nullable` (`YES` / `NO`, Oracle's `Y` / `N`); SQLite's `notnull` is folded in.
    pub nullable: Option<String>,
    /// `extra` (MySQL).
    pub extra: Option<String>,
    /// `pk` (SQLite).
    pub pk: Option<i64>,
    /// `notnull` (SQLite).
    pub notnull: Option<i64>,
    /// `default` (SQLite).
    pub default: Option<String>,
}

/// Oracle reports unquoted identifiers in upper case: an upper-case name is read lower case
/// (as `get_table_cols_types` does).
fn oracle_name(name: &str) -> String {
    let has_cased = name.chars().any(char::is_alphabetic);
    let upper = has_cased && !name.chars().any(char::is_lowercase);
    let plain = name.chars().all(|c| c.is_alphanumeric() || c == '_');
    if upper && plain {
        name.to_lowercase()
    } else {
        name.to_string()
    }
}

/// The rows of [`get_schema_columns_query`] as [`ColumnInfo`]s (full types composed per
/// flavor). `geometry` holds PostGIS column types (`GEOMETRY(POINT, 4326)`) by column.
pub fn columns_from_rows(
    flavor: Flavor,
    rows: &[ColumnRow],
    geometry: &IndexMap<String, String>,
) -> Vec<ColumnInfo> {
    rows.iter()
        .map(|r| {
            let name = if flavor == Flavor::Oracle {
                oracle_name(&r.column)
            } else {
                r.column.clone()
            };
            let nullable = match flavor.driver_family() {
                DriverFamily::Sqlite => r.notnull.unwrap_or(0) == 0,
                _ => r
                    .nullable
                    .as_deref()
                    .is_none_or(|n| matches!(n.trim().to_uppercase().as_str(), "YES" | "Y")),
            };
            let db_type = match geometry.get(&r.column) {
                Some(g) => g.clone(),
                None => compose_type(flavor, r),
            };
            ColumnInfo {
                name,
                db_type,
                nullable,
                auto_increment: r
                    .extra
                    .as_deref()
                    .is_some_and(|e| e.to_lowercase().contains("auto_increment")),
                pk: r.pk.unwrap_or(0),
                default: r.default.clone(),
            }
        })
        .collect()
}

/// The catalog queries whose rows name the columns a table is partitioned by, for
/// [`partition_keys_from_rows`]: each row has `column` (MySQL: the partitioning expression) and
/// `kind` (`fixed`, `time`; MySQL: the partitioning method). PostgreSQL's declarative partition
/// keys (also on TimescaleDB, whose hypertables add their dimensions), MySQL / MariaDB partition
/// expressions, MSSQL partition schemes and Oracle partition keys; nothing on SQLite and DuckDB.
pub fn get_partition_keys_queries(
    flavor: Flavor,
    table: &str,
    schema: Option<&str>,
) -> Vec<String> {
    let table = truncate_item_name(table, flavor);
    let t = esc(&table);
    let schema = schema.filter(|s| !s.is_empty() && !flavor.no_schema());
    match flavor.driver_family() {
        DriverFamily::Postgres => {
            let schema_clause = match schema {
                Some(s) => format!("'{}'", esc(s)),
                None => "current_schema()".into(),
            };
            let mut queries = vec![format!(
                "SELECT a.attname AS \"column\", 'fixed' AS \"kind\"\n\
                 FROM pg_catalog.pg_partitioned_table pt\n\
                 JOIN pg_catalog.pg_class c ON c.oid = pt.partrelid\n\
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n\
                 JOIN pg_catalog.pg_attribute a\n    \
                 ON a.attrelid = pt.partrelid AND a.attnum = ANY (pt.partattrs::int2[])\n\
                 WHERE n.nspname = {schema_clause} AND c.relname = '{t}'"
            )];
            if flavor.is_timescaledb() {
                queries.push(format!(
                    "SELECT column_name AS \"column\",\n    \
                     CASE WHEN dimension_type = 'Time' THEN 'time' ELSE 'fixed' END AS \"kind\"\n\
                     FROM timescaledb_information.dimensions\n\
                     WHERE hypertable_schema = {schema_clause} AND hypertable_name = '{t}'"
                ));
            }
            queries
        }
        DriverFamily::Mysql => {
            let schema_clause = match schema {
                Some(s) => format!("'{}'", esc(s)),
                None => "DATABASE()".into(),
            };
            let partitions = |expression: &str, method: &str| {
                format!(
                    "SELECT DISTINCT {expression} AS `column`, {method} AS `kind`\n\
                     FROM information_schema.PARTITIONS\n\
                     WHERE TABLE_SCHEMA = {schema_clause} AND TABLE_NAME = '{t}'\n    \
                     AND {expression} IS NOT NULL"
                )
            };
            vec![
                partitions("PARTITION_EXPRESSION", "PARTITION_METHOD"),
                partitions("SUBPARTITION_EXPRESSION", "'fixed'"),
            ]
        }
        DriverFamily::Mssql => {
            let item = esc(&sql_item_name(
                &table,
                flavor,
                Some(schema.unwrap_or("dbo")),
            ));
            vec![format!(
                "SELECT c.name AS [column], 'fixed' AS [kind]\n\
                 FROM sys.indexes i\n\
                 JOIN sys.index_columns ic\n    \
                 ON ic.object_id = i.object_id AND ic.index_id = i.index_id\n\
                 JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id\n\
                 WHERE i.object_id = OBJECT_ID(N'{item}') AND i.index_id IN (0, 1)\n    \
                 AND ic.partition_ordinal > 0"
            )]
        }
        DriverFamily::Oracle => {
            let owner = match schema {
                Some(s) => format!("'{}'", esc(&s.to_uppercase())),
                None => "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')".into(),
            };
            vec![format!(
                "SELECT column_name AS \"column\", 'fixed' AS \"kind\"\n\
                 FROM all_part_key_columns\n\
                 WHERE owner = {owner} AND object_type = 'TABLE' AND name IN ('{t}', '{}')",
                esc(&table.to_uppercase())
            )]
        }
        DriverFamily::Sqlite | DriverFamily::Duckdb => Vec::new(),
    }
}

/// The partition keys named by the rows of [`get_partition_keys_queries`] (`(column, kind)`).
/// A MySQL row's `column` is a partitioning expression: every backquoted name in it is a key,
/// `RANGE COLUMNS` / `LIST COLUMNS` ones of kind [`PartitionKey::RangeColumns`].
pub fn partition_keys_from_rows(
    flavor: Flavor,
    rows: &[(String, String)],
) -> IndexMap<String, PartitionKey> {
    let mut keys: IndexMap<String, PartitionKey> = IndexMap::new();
    let mut add = |column: String, key: PartitionKey| {
        // A column that is also a fixed key (a subpartition) stays fixed.
        let entry = keys.entry(column).or_insert(key);
        if key == PartitionKey::Fixed {
            *entry = PartitionKey::Fixed;
        }
    };
    for (column, kind) in rows {
        let kind = kind.trim().to_lowercase();
        if flavor.driver_family() == DriverFamily::Mysql {
            let key = if kind.ends_with(" columns") {
                PartitionKey::RangeColumns
            } else {
                PartitionKey::Fixed
            };
            let mut parts = column.split('`');
            parts.next();
            while let (Some(name), Some(_)) = (parts.next(), parts.next()) {
                if !name.is_empty() {
                    add(name.to_string(), key);
                }
            }
            continue;
        }
        let name = if flavor == Flavor::Oracle {
            oracle_name(column)
        } else {
            column.clone()
        };
        add(
            name,
            if kind == "time" {
                PartitionKey::TimeDimension
            } else {
                PartitionKey::Fixed
            },
        );
    }
    keys
}

/// Whether a change of partition key column from `actual` to `expected` (database type
/// `target`) is one the database makes in place ([`PartitionKey`]).
fn partition_key_change_allowed(
    key: PartitionKey,
    actual: &TypeClass,
    expected: &TypeClass,
    target: &str,
) -> bool {
    use TypeClass::*;
    let temporal = |c: &TypeClass| matches!(c, Datetime { .. } | Date);
    let integer = |c: &TypeClass| matches!(c, Int(_));
    match key {
        PartitionKey::Fixed => false,
        PartitionKey::TimeDimension => {
            (temporal(actual) && temporal(expected)) || (integer(actual) && integer(expected))
        }
        PartitionKey::RangeColumns => {
            let base = split_type(target).0;
            (temporal(actual) && matches!(base.as_str(), "DATE" | "DATETIME"))
                || (integer(actual) && integer(expected))
        }
    }
}

/// The plan's reason for a blocked change of partition key column `column`.
fn partition_key_blocked(
    flavor: Flavor,
    key: PartitionKey,
    column: &str,
    current: &str,
    target: &str,
) -> String {
    let database = match flavor {
        Flavor::Timescaledb | Flavor::TimescaledbHa => "TimescaleDB",
        Flavor::Postgis => "PostGIS",
        Flavor::Citus => "Citus",
        Flavor::Cockroachdb => "CockroachDB",
        Flavor::Mysql => "MySQL",
        Flavor::Mariadb => "MariaDB",
        Flavor::Mssql => "MSSQL",
        Flavor::Oracle => "Oracle",
        _ => "PostgreSQL",
    };
    let reason = match key {
        PartitionKey::Fixed => format!(
            "{database} cannot change the type of column '{column}' because the table is \
             partitioned by it."
        ),
        PartitionKey::TimeDimension => format!(
            "TimescaleDB cannot change hypertable column '{column}' from {current} to {target}: \
             a time column changes only between timestamp and date types, or between integer \
             types."
        ),
        PartitionKey::RangeColumns => format!(
            "{database} cannot change column '{column}' to {target} because the table is \
             partitioned by it: it stays DATE or DATETIME, or an integer type."
        ),
    };
    format!(
        "{reason}\n    Keep its current type in `dtypes`, or drop the pipe and sync it again to \
         rebuild the table with the new type."
    )
}

/// The full type of a catalog row.
fn compose_type(flavor: Flavor, r: &ColumnRow) -> String {
    let t = r.type_.trim();
    let upper = t.to_uppercase();
    match flavor.driver_family() {
        DriverFamily::Sqlite | DriverFamily::Mysql | DriverFamily::Duckdb => upper,
        DriverFamily::Postgres => match upper.as_str() {
            "CHARACTER VARYING" | "CHARACTER" | "BIT VARYING" => {
                let base = if upper == "CHARACTER" {
                    "CHAR"
                } else if upper == "BIT VARYING" {
                    "VARBIT"
                } else {
                    "VARCHAR"
                };
                match r.length {
                    Some(n) => format!("{base}({n})"),
                    None => base.to_string(),
                }
            }
            "NUMERIC" => match (r.precision, r.scale) {
                (Some(p), Some(s)) => format!("NUMERIC({p},{s})"),
                (Some(p), None) => format!("NUMERIC({p},0)"),
                _ => "NUMERIC".into(),
            },
            "USER-DEFINED" | "ARRAY" => r.udt.as_deref().map(str::to_uppercase).unwrap_or(upper),
            _ => upper,
        },
        DriverFamily::Mssql => match upper.as_str() {
            "VARCHAR" | "NVARCHAR" | "CHAR" | "NCHAR" | "VARBINARY" | "BINARY" => match r.length {
                Some(-1) => format!("{upper}(MAX)"),
                Some(n) => format!("{upper}({n})"),
                None => upper,
            },
            "NUMERIC" | "DECIMAL" => match (r.precision, r.scale) {
                (Some(p), Some(s)) => format!("{upper}({p},{s})"),
                _ => upper,
            },
            "FLOAT" => match r.precision {
                Some(p) if p <= 24 => "REAL".into(),
                _ => "FLOAT".into(),
            },
            _ => upper,
        },
        DriverFamily::Oracle => match upper.as_str() {
            "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" | "RAW" => match r.length {
                Some(n) if n > 0 => format!("{upper}({n})"),
                _ => upper,
            },
            "NUMBER" => match (r.precision, r.scale) {
                (None, Some(0)) => "INTEGER".into(),
                (Some(p), Some(s)) => format!("NUMBER({p},{s})"),
                (Some(p), None) => format!("NUMBER({p},0)"),
                _ => "NUMBER".into(),
            },
            _ => upper,
        },
    }
}

// ---------------------------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------------------------

/// What a database type holds, for comparing a column with its declared dtype.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeClass {
    /// An integer of this many bits.
    Int(u16),
    /// A float of this many bits.
    Float(u8),
    /// A decimal, with `(precision, scale)` when constrained.
    Numeric(Option<(i64, i64)>),
    /// Text, with its maximum length when bounded.
    Text(Option<i64>),
    /// `JSON`.
    Json,
    /// `JSONB`.
    Jsonb,
    /// A boolean.
    Bool,
    /// A date (no time).
    Date,
    /// A timestamp, timezone-aware or naive.
    Datetime {
        /// Whether the type keeps a timezone (`TIMESTAMPTZ`, `DATETIMEOFFSET`).
        tz: bool,
    },
    /// A UUID.
    Uuid,
    /// Binary data.
    Bytes,
    /// A PostGIS geometry or geography, with its `(TYPE, SRID)` modifier when present.
    Geometry {
        /// `GEOGRAPHY` rather than `GEOMETRY`.
        geography: bool,
        /// The modifier (`POINT, 4326`), upper-cased.
        spec: Option<String>,
    },
    /// A SQLite column without a declared type.
    Untyped,
    /// Anything else (compared by name).
    Other(String),
}

/// `(BASE, [args])` of a type: `NUMERIC(10, 2)` is `("NUMERIC", ["10", "2"])`,
/// `TIMESTAMP(6) WITH TIME ZONE` is `("TIMESTAMP WITH TIME ZONE", ["6"])`.
fn split_type(db_type: &str) -> (String, Vec<String>) {
    let upper = db_type.trim().to_uppercase();
    let mut base = String::new();
    let mut args: Option<String> = None;
    let mut depth = 0usize;
    let mut current = String::new();
    for c in upper.chars() {
        match c {
            '(' => {
                if depth == 0 {
                    current.clear();
                } else {
                    current.push(c);
                }
                depth += 1;
            }
            ')' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    if args.is_none() {
                        args = Some(current.clone());
                    }
                } else {
                    current.push(c);
                }
            }
            _ if depth > 0 => current.push(c),
            _ => base.push(c),
        }
    }
    let base = base.split_whitespace().collect::<Vec<_>>().join(" ");
    let args = args
        .map(|a| {
            a.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    (base, args)
}

fn int_bits_for_digits(precision: i64) -> u16 {
    match precision {
        ..=4 => 16,
        5..=9 => 32,
        10..=18 => 64,
        _ => 128,
    }
}

/// The decimal digits an integer of `bits` bits needs.
fn digits_for_int_bits(bits: u16) -> i64 {
    match bits {
        ..=8 => 3,
        9..=16 => 5,
        17..=32 => 10,
        33..=64 => 19,
        _ => 39,
    }
}

/// Classify a database type of `flavor` ([`TypeClass`]).
pub fn classify(flavor: Flavor, db_type: &str) -> TypeClass {
    let (base, args) = split_type(db_type);
    if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
        return classify_sqlite(&base);
    }
    let first_int = || args.first().and_then(|a| a.parse::<i64>().ok());
    let mysql = flavor.driver_family() == DriverFamily::Mysql;
    match base.as_str() {
        "TINYINT" if mysql && args.first().map(String::as_str) == Some("1") => TypeClass::Bool,
        "TINYINT" | "UTINYINT" => TypeClass::Int(8),
        "SMALLINT" | "INT2" | "SMALLSERIAL" | "USMALLINT" => TypeClass::Int(16),
        "MEDIUMINT" => TypeClass::Int(24),
        "INT" | "INTEGER" | "INT4" | "SERIAL" | "UINTEGER" => {
            if flavor == Flavor::Oracle {
                // `INT` is `NUMBER(38)` on Oracle.
                TypeClass::Int(128)
            } else {
                TypeClass::Int(32)
            }
        }
        "BIGINT" | "INT8" | "BIGSERIAL" | "UBIGINT" => TypeClass::Int(64),
        "HUGEINT" | "UHUGEINT" => TypeClass::Int(128),
        "REAL" | "FLOAT4" | "BINARY_FLOAT" => TypeClass::Float(32),
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "BINARY_DOUBLE" => TypeClass::Float(64),
        "FLOAT" => match flavor {
            Flavor::Mysql | Flavor::Mariadb => {
                if first_int().is_some_and(|p| p > 24) {
                    TypeClass::Float(64)
                } else {
                    TypeClass::Float(32)
                }
            }
            Flavor::Mssql => {
                if first_int().is_some_and(|p| p <= 24) {
                    TypeClass::Float(32)
                } else {
                    TypeClass::Float(64)
                }
            }
            Flavor::Duckdb => TypeClass::Float(32),
            _ => TypeClass::Float(64),
        },
        "NUMBER" if flavor == Flavor::Oracle => {
            let p = first_int();
            let s = args.get(1).and_then(|a| a.parse::<i64>().ok()).unwrap_or(0);
            match p {
                Some(p) if s == 0 => TypeClass::Int(int_bits_for_digits(p)),
                Some(p) => TypeClass::Numeric(Some((p, s))),
                None => TypeClass::Numeric(None),
            }
        }
        "NUMERIC" | "DECIMAL" | "DEC" | "NUMBER" => {
            match (first_int(), args.get(1).and_then(|a| a.parse::<i64>().ok())) {
                (Some(p), Some(s)) => TypeClass::Numeric(Some((p, s))),
                (Some(p), None) => TypeClass::Numeric(Some((p, 0))),
                _ => TypeClass::Numeric(None),
            }
        }
        "TEXT" | "LONGTEXT" | "MEDIUMTEXT" | "NTEXT" | "CLOB" | "NCLOB" | "CITEXT" | "STRING"
        | "LONG" => TypeClass::Text(None),
        "TINYTEXT" => TypeClass::Text(Some(255)),
        "VARCHAR"
        | "CHARACTER VARYING"
        | "NVARCHAR"
        | "VARCHAR2"
        | "NVARCHAR2"
        | "CHAR"
        | "CHARACTER"
        | "NCHAR"
        | "BPCHAR"
        | "NATIONAL CHARACTER VARYING" => match args.first().map(String::as_str) {
            Some("MAX") | None
                if !matches!(base.as_str(), "CHAR" | "CHARACTER" | "NCHAR" | "BPCHAR") =>
            {
                TypeClass::Text(None)
            }
            Some(n) => TypeClass::Text(n.parse().ok()),
            None => TypeClass::Text(Some(1)),
        },
        "JSON" => TypeClass::Json,
        "JSONB" => TypeClass::Jsonb,
        "BOOLEAN" | "BOOL" | "BIT" => TypeClass::Bool,
        // Oracle's `DATE` keeps the time of day.
        "DATE" if flavor == Flavor::Oracle => TypeClass::Datetime { tz: false },
        "DATE" => TypeClass::Date,
        "TIMESTAMP"
        | "TIMESTAMP WITHOUT TIME ZONE"
        | "DATETIME"
        | "DATETIME2"
        | "SMALLDATETIME"
        | "TIMESTAMP_NS"
        | "TIMESTAMP_MS"
        | "TIMESTAMP_S" => TypeClass::Datetime { tz: false },
        "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE"
        | "DATETIMEOFFSET"
        | "TIMESTAMP WITH LOCAL TIME ZONE" => TypeClass::Datetime { tz: true },
        "UUID" | "UNIQUEIDENTIFIER" => TypeClass::Uuid,
        "BYTEA" | "BLOB" | "VARBINARY" | "BINARY" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB"
        | "RAW" | "LONG RAW" | "IMAGE" => TypeClass::Bytes,
        "GEOMETRY" | "GEOGRAPHY" => TypeClass::Geometry {
            geography: base == "GEOGRAPHY",
            spec: (!args.is_empty()).then(|| args.join(", ")),
        },
        _ => TypeClass::Other(db_type.trim().to_uppercase()),
    }
}

/// SQLite's type affinity (`https://sqlite.org/datatype3.html`), refined where Meerschaum
/// reads the declared type (dates, booleans).
fn classify_sqlite(base: &str) -> TypeClass {
    if base.is_empty() {
        return TypeClass::Untyped;
    }
    if base.contains("INT") {
        return TypeClass::Int(64);
    }
    if base.contains("CHAR") || base.contains("CLOB") || base.contains("TEXT") {
        return TypeClass::Text(None);
    }
    if base.contains("BLOB") {
        return TypeClass::Bytes;
    }
    if base.contains("REAL") || base.contains("FLOA") || base.contains("DOUB") {
        return TypeClass::Float(64);
    }
    if base.contains("BOOL") {
        return TypeClass::Bool;
    }
    if base == "DATE" {
        return TypeClass::Date;
    }
    if base.contains("DATE") || base.contains("TIME") {
        // SQLite keeps timestamps as text: naive and aware columns are the same.
        return TypeClass::Datetime { tz: false };
    }
    if base.contains("JSON") {
        return TypeClass::Json;
    }
    if base.contains("NUMERIC") || base.contains("DECIMAL") {
        return TypeClass::Numeric(None);
    }
    TypeClass::Other(base.to_string())
}

/// How a column compares with its declared type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing to do.
    Same,
    /// A safe widening.
    Widen,
    /// Another type change.
    Change,
}

/// Compare a column's `actual` class with the `expected` class of its declared dtype.
pub fn compare(
    flavor: Flavor,
    declared_dtype: &str,
    expected: &TypeClass,
    actual: &TypeClass,
) -> Verdict {
    use TypeClass::*;
    use Verdict::*;
    if expected == actual {
        return Same;
    }
    let sqlite_like = matches!(flavor, Flavor::Sqlite | Flavor::Geopackage);
    let declared_bool = are_dtypes_equal(declared_dtype, "bool");
    // A column already of the declared kind's native type is never retyped to the fallback
    // Meerschaum uses where the flavor has none (`json` as `TEXT` on MySQL, `uuid` as
    // `CHAR(36)`, geometries as text on plain PostgreSQL).
    let declared = declared_dtype.to_lowercase();
    if are_dtypes_equal(declared_dtype, "json") && matches!(actual, Json | Jsonb) {
        return if *expected == Jsonb && *actual == Json {
            Widen
        } else {
            Same
        };
    }
    if (are_dtypes_equal(declared_dtype, "uuid") && *actual == Uuid)
        || (are_dtypes_equal(declared_dtype, "bytes") && *actual == Bytes)
        || (declared_bool && *actual == Bool)
    {
        return Same;
    }
    if (declared.starts_with("geometry") || declared.starts_with("geography"))
        && matches!(actual, Geometry { .. })
        && !matches!(expected, Geometry { .. })
    {
        return Same;
    }
    match (expected, actual) {
        // Booleans kept as numbers on flavors without a boolean type (SQLite, Oracle).
        (Int(_) | Float(_), Int(_) | Float(_) | Bool) if declared_bool => Same,
        (Int(e), Int(a)) => {
            if a >= e {
                Same
            } else {
                Widen
            }
        }
        (Float(e), Float(a)) => {
            if a >= e {
                Same
            } else {
                Widen
            }
        }
        (Float(_), Int(_)) => Widen,
        (Numeric(None), Numeric(_)) => Same,
        (Numeric(None), Int(_) | Float(_)) => Widen,
        (Numeric(Some((p, s))), Int(bits)) => {
            if p - s >= digits_for_int_bits(*bits) {
                Widen
            } else {
                Change
            }
        }
        (Numeric(Some((ep, es))), Numeric(Some((ap, a_s)))) => {
            if ap >= ep && a_s >= es && ap - a_s >= ep - es {
                Same
            } else if ep >= ap && es >= a_s && ep - es >= ap - a_s {
                Widen
            } else {
                Change
            }
        }
        (Text(e), Text(a)) => match (e, a) {
            (_, None) => Same,
            (None, Some(_)) => Widen,
            (Some(e), Some(a)) => {
                if a >= e {
                    Same
                } else {
                    Widen
                }
            }
        },
        // SQLite keeps JSON as text either way.
        (Text(_), Json | Jsonb) | (Json | Jsonb, Text(_)) if sqlite_like => Same,
        (Json | Jsonb, Text(_)) => Widen,
        (Jsonb, Json) => Widen,
        (Json, Jsonb) => Same,
        // SQLite: `NUMERIC` and `TEXT` both hold Meerschaum's numeric text.
        (Text(_), Numeric(_)) if sqlite_like => Same,
        (
            Geometry {
                geography: eg,
                spec: es,
            },
            Geometry {
                geography: ag,
                spec: a_s,
            },
        ) => {
            if eg == ag && (es.is_none() || es == a_s) {
                Same
            } else {
                Change
            }
        }
        _ => Change,
    }
}

// ---------------------------------------------------------------------------------------------
// Planning
// ---------------------------------------------------------------------------------------------

/// The inputs of [`build_schema_plan`].
#[derive(Debug, Clone)]
pub struct PlanInput<'a> {
    /// The instance's flavor.
    pub flavor: Flavor,
    /// The pipe's table, quoted with its schema.
    pub table: &'a str,
    /// The bare table name (the pipe's `target`).
    pub target: &'a str,
    /// The table's columns.
    pub columns: &'a [ColumnInfo],
    /// Declared column → (Meerschaum dtype, database type of that dtype on this flavor), in
    /// declaration order.
    pub declared: &'a IndexMap<String, (String, String)>,
    /// Facts about the table (compression, indices, SQLite details).
    pub facts: &'a TableFacts,
    /// Notes the caller already has (role columns without a declared dtype).
    pub notes: Vec<String>,
}

/// A planned change with what the statement builders need.
#[derive(Debug, Clone)]
struct Planned {
    change: SchemaChange,
    expected: TypeClass,
    actual: Option<TypeClass>,
    info: Option<ColumnInfo>,
    /// Applied by rebuilding the table (SQLite type changes).
    rebuild: bool,
    /// Why it cannot be applied (the plan shows it; the statements leave it out).
    blocked: Option<String>,
}

/// Find a declared column among the table's columns (exactly, or ignoring case where the
/// database does).
fn find_column<'a>(
    flavor: Flavor,
    columns: &'a [ColumnInfo],
    name: &str,
) -> Option<&'a ColumnInfo> {
    columns.iter().find(|c| c.name == name).or_else(|| {
        (flavor.driver_family() != DriverFamily::Postgres)
            .then(|| columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)))
            .flatten()
    })
}

/// The `USING` expression of a PostgreSQL / DuckDB type change.
fn using_expr(col: &str, actual: &TypeClass, expected: &TypeClass, target: &str) -> String {
    use TypeClass::*;
    match (actual, expected) {
        (Datetime { tz: false }, Datetime { tz: true })
        | (Datetime { tz: true }, Datetime { tz: false }) => format!("{col} AT TIME ZONE 'UTC'"),
        (Int(_) | Float(_) | Numeric(_), Bool) => format!("{col} <> 0"),
        (Bool, Int(_)) => format!("CASE WHEN {col} THEN 1 ELSE 0 END"),
        (Bool, Float(_) | Numeric(_)) => format!("CASE WHEN {col} THEN 1.0 ELSE 0.0 END"),
        (
            Geometry { spec: None, .. },
            Geometry {
                spec: Some(spec), ..
            },
        )
        | (
            Text(_),
            Geometry {
                spec: Some(spec), ..
            },
        ) => {
            match spec
                .rsplit(',')
                .next()
                .and_then(|s| s.trim().parse::<i64>().ok())
            {
                Some(srid) if srid != 0 => {
                    format!("ST_SetSRID(CAST({col} AS GEOMETRY), {srid})")
                }
                _ => format!("CAST({col} AS {target})"),
            }
        }
        (
            Geometry { spec: Some(_), .. },
            Geometry {
                spec: Some(spec), ..
            },
        ) => {
            match spec
                .rsplit(',')
                .next()
                .and_then(|s| s.trim().parse::<i64>().ok())
            {
                Some(srid) if srid != 0 => format!("ST_Transform({col}, {srid})"),
                _ => format!("CAST({col} AS {target})"),
            }
        }
        _ => format!("CAST({col} AS {target})"),
    }
}

/// How the plan table shows a type change's conversion.
fn change_note(flavor: Flavor, p: &Planned, col: &str) -> Option<String> {
    if p.rebuild {
        return Some("rebuilds the table".into());
    }
    let actual = p.actual.as_ref()?;
    match flavor.driver_family() {
        DriverFamily::Postgres | DriverFamily::Duckdb
            if p.change.kind == SchemaChangeKind::Change =>
        {
            Some(format!(
                "USING {}",
                using_expr(col, actual, &p.expected, &p.change.target)
            ))
        }
        DriverFamily::Oracle if p.change.kind == SchemaChangeKind::Change => {
            Some("copies through a temporary column".into())
        }
        _ => None,
    }
}

/// Build the plan for one table (see the module docs).
pub fn build_schema_plan(input: &PlanInput<'_>) -> SchemaPlan {
    let flavor = input.flavor;
    let mut notes = input.notes.clone();
    let mut planned: Vec<Planned> = Vec::new();
    for (column, (dtype, target)) in input.declared {
        let expected = classify(flavor, target);
        let Some(info) = find_column(flavor, input.columns, column) else {
            planned.push(Planned {
                change: SchemaChange {
                    column: column.clone(),
                    kind: SchemaChangeKind::AddColumn,
                    current: None,
                    target: target.clone(),
                    dtype: dtype.clone(),
                    note: None,
                    blocked: None,
                },
                expected,
                actual: None,
                info: None,
                rebuild: false,
                blocked: None,
            });
            continue;
        };
        let actual = classify(flavor, &info.db_type);
        let verdict = compare(flavor, dtype, &expected, &actual);
        if verdict == Verdict::Same {
            continue;
        }
        let indexed = input.facts.indexed.contains(&info.name);
        // MySQL cannot index unbounded text without a prefix length, and MSSQL keys cannot be
        // `NVARCHAR(MAX)`: an indexed bounded text column keeps its type.
        if indexed
            && matches!(expected, TypeClass::Text(None))
            && matches!(actual, TypeClass::Text(Some(_)))
            && matches!(
                flavor.driver_family(),
                DriverFamily::Mysql | DriverFamily::Mssql | DriverFamily::Oracle
            )
        {
            notes.push(format!(
                "Column '{}' is indexed: its {} type is kept.",
                info.name, info.db_type
            ));
            continue;
        }
        let kind = match verdict {
            Verdict::Widen => SchemaChangeKind::Widen,
            _ => SchemaChangeKind::Change,
        };
        // A SQLite type change rebuilds the table, which needs confirmation even for a
        // widening. Within an affinity, SQLite needs nothing (`INT` and `BIGINT` are the same).
        let rebuild = flavor == Flavor::Sqlite;
        let kind = if rebuild {
            SchemaChangeKind::Change
        } else {
            kind
        };
        let mut blocked = None;
        if flavor == Flavor::Geopackage {
            blocked = Some(format!(
                "GeoPackage tables are not rebuilt: column '{}' keeps its {} type.",
                info.name,
                if info.db_type.is_empty() {
                    "missing"
                } else {
                    &info.db_type
                }
            ));
        } else if let Some(key) = input.facts.partitioned_by.get(&info.name).copied()
            && !partition_key_change_allowed(key, &actual, &expected, target)
        {
            blocked = Some(partition_key_blocked(
                flavor,
                key,
                &info.name,
                &info.db_type,
                target,
            ));
        } else if input.facts.compressed_hypertable {
            blocked = Some(format!(
                "TimescaleDB cannot change column types while chunks are compressed.\n    \
                 Decompress first{}, run `verify schema` again, then compress it again.",
                input
                    .facts
                    .decompress_command
                    .as_ref()
                    .map(|c| format!(" (`{c}`)"))
                    .unwrap_or_default()
            ));
        } else if indexed && flavor == Flavor::Mssql {
            blocked = Some(format!(
                "MSSQL cannot change the type of indexed column '{}'.\n    \
                 Drop the pipe's indices first{}, run `verify schema` again, then rebuild them \
                 (`index pipes`).",
                info.name,
                input
                    .facts
                    .drop_indices_command
                    .as_ref()
                    .map(|c| format!(" (`{c}`)"))
                    .unwrap_or_default()
            ));
        } else if flavor == Flavor::Oracle && kind == SchemaChangeKind::Change && !info.nullable {
            blocked = Some(format!(
                "Oracle cannot change the type of NOT NULL column '{}' in place.",
                info.name
            ));
        }
        planned.push(Planned {
            change: SchemaChange {
                column: info.name.clone(),
                kind,
                current: Some(info.db_type.clone()),
                target: target.clone(),
                dtype: dtype.clone(),
                note: None,
                blocked: blocked.clone(),
            },
            expected,
            actual: Some(actual),
            info: Some(info.clone()),
            rebuild,
            blocked,
        });
    }
    for p in &mut planned {
        let col = sql_item_name(&p.change.column, flavor, None);
        p.change.note = change_note(flavor, p, &col);
    }
    let blocked: Vec<String> = planned.iter().filter_map(|p| p.blocked.clone()).collect();
    let mut seen = IndexSet::new();
    let blocked: Vec<String> = blocked
        .into_iter()
        .filter(|b| seen.insert(b.clone()))
        .collect();
    let applicable: Vec<&Planned> = planned.iter().filter(|p| p.blocked.is_none()).collect();
    let safe: Vec<&Planned> = applicable
        .iter()
        .copied()
        .filter(|p| p.change.is_safe())
        .collect();
    SchemaPlan {
        table: input.table.to_string(),
        exists: true,
        queries: statements(input, &applicable),
        safe_queries: statements(input, &safe),
        changes: planned.into_iter().map(|p| p.change).collect(),
        notes,
        blocked: (!blocked.is_empty()).then(|| blocked.join("\n")),
    }
}

// ---------------------------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------------------------

fn statements(input: &PlanInput<'_>, planned: &[&Planned]) -> Vec<String> {
    if planned.is_empty() {
        return Vec::new();
    }
    let flavor = input.flavor;
    let table = input.table;
    let q = |c: &str| sql_item_name(c, flavor, None);
    let adds: Vec<&&Planned> = planned
        .iter()
        .filter(|p| p.change.kind == SchemaChangeKind::AddColumn)
        .collect();
    let alters: Vec<&&Planned> = planned
        .iter()
        .filter(|p| p.change.kind != SchemaChangeKind::AddColumn)
        .collect();
    match flavor.driver_family() {
        DriverFamily::Postgres => {
            let mut actions: Vec<String> = adds
                .iter()
                .map(|p| format!("ADD COLUMN {} {}", q(&p.change.column), p.change.target))
                .collect();
            for p in &alters {
                let col = q(&p.change.column);
                let using = using_expr(
                    &col,
                    p.actual.as_ref().unwrap_or(&TypeClass::Untyped),
                    &p.expected,
                    &p.change.target,
                );
                actions.push(format!(
                    "ALTER COLUMN {col} TYPE {} USING {using}",
                    p.change.target
                ));
            }
            vec![format!(
                "ALTER TABLE {table}\n    {}",
                actions.join(",\n    ")
            )]
        }
        DriverFamily::Duckdb => {
            let mut out: Vec<String> = adds
                .iter()
                .map(|p| {
                    format!(
                        "ALTER TABLE {table} ADD COLUMN {} {}",
                        q(&p.change.column),
                        p.change.target
                    )
                })
                .collect();
            for p in &alters {
                let col = q(&p.change.column);
                let using = using_expr(
                    &col,
                    p.actual.as_ref().unwrap_or(&TypeClass::Untyped),
                    &p.expected,
                    &p.change.target,
                );
                out.push(format!(
                    "ALTER TABLE {table} ALTER COLUMN {col} TYPE {} USING {using}",
                    p.change.target
                ));
            }
            out
        }
        DriverFamily::Mysql => {
            let mut actions: Vec<String> = adds
                .iter()
                .map(|p| format!("ADD COLUMN {} {}", q(&p.change.column), p.change.target))
                .collect();
            for p in &alters {
                let info = p.info.clone().unwrap_or_default();
                actions.push(format!(
                    "MODIFY COLUMN {} {}{}{}",
                    q(&p.change.column),
                    p.change.target,
                    if info.nullable { "" } else { " NOT NULL" },
                    if info.auto_increment {
                        " AUTO_INCREMENT"
                    } else {
                        ""
                    }
                ));
            }
            vec![format!(
                "ALTER TABLE {table}\n    {}",
                actions.join(",\n    ")
            )]
        }
        DriverFamily::Mssql => {
            let mut out = Vec::new();
            if !adds.is_empty() {
                out.push(format!(
                    "ALTER TABLE {table} ADD {}",
                    adds.iter()
                        .map(|p| format!("{} {}", q(&p.change.column), p.change.target))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            for p in &alters {
                let info = p.info.clone().unwrap_or_default();
                out.push(format!(
                    "ALTER TABLE {table} ALTER COLUMN {} {}{}",
                    q(&p.change.column),
                    p.change.target,
                    if info.nullable { " NULL" } else { " NOT NULL" }
                ));
            }
            out
        }
        DriverFamily::Oracle => {
            let mut out = Vec::new();
            for p in &adds {
                out.push(format!(
                    "ALTER TABLE {table} ADD ({} {})",
                    q(&p.change.column),
                    p.change.target
                ));
            }
            for p in &alters {
                let col = q(&p.change.column);
                let in_place = p.change.kind == SchemaChangeKind::Widen
                    && matches!(
                        (&p.expected, &p.actual),
                        (TypeClass::Text(_), Some(TypeClass::Text(_)))
                            | (TypeClass::Int(_), Some(TypeClass::Int(_)))
                            | (TypeClass::Numeric(_), Some(TypeClass::Numeric(_)))
                    );
                if in_place {
                    out.push(format!(
                        "ALTER TABLE {table} MODIFY ({col} {})",
                        p.change.target
                    ));
                    continue;
                }
                let temp = q(&truncate_item_name(
                    &format!("{}_mrsm_tmp", p.change.column),
                    flavor,
                ));
                let target = &p.change.target;
                out.push(format!("ALTER TABLE {table} ADD ({temp} {target})"));
                out.push(format!("UPDATE {table} SET {temp} = {col}"));
                out.push(format!("UPDATE {table} SET {col} = NULL"));
                out.push(format!("ALTER TABLE {table} MODIFY ({col} {target})"));
                out.push(format!("UPDATE {table} SET {col} = {temp}"));
                out.push(format!("ALTER TABLE {table} DROP COLUMN {temp}"));
            }
            out
        }
        DriverFamily::Sqlite => {
            if alters.iter().any(|p| p.rebuild) {
                return sqlite_rebuild(input, planned);
            }
            adds.iter()
                .map(|p| {
                    format!(
                        "ALTER TABLE {table} ADD COLUMN {} {}",
                        q(&p.change.column),
                        p.change.target
                    )
                })
                .collect()
        }
    }
}

/// SQLite's generalized `ALTER TABLE` (`https://sqlite.org/lang_altertable.html`): a new table
/// with the planned types (and the planned added columns), the rows copied as they are (the new
/// columns' affinities convert what they can without losing anything), the old table dropped,
/// the new one renamed, and the indices replayed.
fn sqlite_rebuild(input: &PlanInput<'_>, planned: &[&Planned]) -> Vec<String> {
    let flavor = input.flavor;
    let q = |c: &str| sql_item_name(c, flavor, None);
    let new_types: IndexMap<&str, &str> = planned
        .iter()
        .filter(|p| p.change.kind != SchemaChangeKind::AddColumn)
        .map(|p| (p.change.column.as_str(), p.change.target.as_str()))
        .collect();
    let temp = format!("{}_mrsm_verify", input.target);
    let temp_name = q(&temp);
    let mut pk: Vec<&ColumnInfo> = input.columns.iter().filter(|c| c.pk > 0).collect();
    pk.sort_by_key(|c| c.pk);
    let single_pk = pk.len() == 1;
    let mut defs: Vec<String> = Vec::new();
    for c in input.columns {
        let typ = new_types
            .get(c.name.as_str())
            .copied()
            .unwrap_or(c.db_type.as_str());
        let mut def = q(&c.name);
        if !typ.is_empty() {
            def.push(' ');
            def.push_str(typ);
        }
        if single_pk && c.pk > 0 {
            def.push_str(" PRIMARY KEY");
            if input.facts.sqlite_autoincrement {
                def.push_str(" AUTOINCREMENT");
            }
        }
        if !c.nullable {
            def.push_str(" NOT NULL");
        }
        if let Some(d) = &c.default {
            def.push_str(&format!(" DEFAULT {d}"));
        }
        defs.push(def);
    }
    for p in planned
        .iter()
        .filter(|p| p.change.kind == SchemaChangeKind::AddColumn)
    {
        defs.push(format!("{} {}", q(&p.change.column), p.change.target));
    }
    if pk.len() > 1 {
        defs.push(format!(
            "PRIMARY KEY ({})",
            pk.iter().map(|c| q(&c.name)).collect::<Vec<_>>().join(", ")
        ));
    }
    let cols = input
        .columns
        .iter()
        .map(|c| q(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut out = vec![
        format!("DROP TABLE IF EXISTS {temp_name}"),
        format!(
            "CREATE TABLE {temp_name} (\n    {}\n)",
            defs.join(",\n    ")
        ),
        format!(
            "INSERT INTO {temp_name} ({cols})\nSELECT {cols}\nFROM {}",
            input.table
        ),
        format!("DROP TABLE {}", input.table),
        format!("ALTER TABLE {temp_name} RENAME TO {}", q(input.target)),
    ];
    out.extend(input.facts.sqlite_indexes.iter().cloned());
    out
}

#[cfg(test)]
mod tests;
