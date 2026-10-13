//! Bound parameters: the safe path for user values in the instance methods (internal tables,
//! row inserts). Values never get spliced into SQL text; each driver family binds them with its
//! native encoding.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use crate::flavor::{DriverFamily, Flavor};

/// A value bound to a query parameter.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam {
    /// `NULL` (bound as a nullable text parameter).
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// A JSON document (PostgreSQL `JSONB`; text elsewhere).
    Json(serde_json::Value),
    /// A naive timestamp (bound natively).
    Timestamp(NaiveDateTime),
    /// An aware timestamp (bound natively).
    TimestampTz(DateTime<Utc>),
    /// A date (bound natively).
    Date(NaiveDate),
}

impl SqlParam {
    /// A text parameter, or `NULL` for `None`.
    pub fn opt_text(s: Option<&str>) -> SqlParam {
        match s {
            Some(s) => SqlParam::Text(s.to_string()),
            None => SqlParam::Null,
        }
    }
}

/// The `n`-th (1-based) placeholder for a flavor: `$n` on PostgreSQL and DuckDB, `@Pn` on
/// MSSQL (`tiberius`), `:n` on Oracle, `?` elsewhere.
pub fn placeholder(flavor: Flavor, n: usize) -> String {
    match flavor.driver_family() {
        DriverFamily::Postgres | DriverFamily::Duckdb => format!("${n}"),
        DriverFamily::Mssql => format!("@P{n}"),
        DriverFamily::Oracle => format!(":{n}"),
        _ => "?".to_string(),
    }
}

/// `n` comma-separated placeholders starting at `start` (1-based).
pub fn placeholders(flavor: Flavor, start: usize, n: usize) -> String {
    (start..start + n)
        .map(|i| placeholder(flavor, i))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The most bound parameters one statement may carry for a flavor.
pub fn max_bind_params(flavor: Flavor) -> usize {
    match flavor.driver_family() {
        // SQLite's `SQLITE_MAX_VARIABLE_NUMBER` is 32766 since 3.32; stay well below.
        DriverFamily::Sqlite => 30_000,
        // MSSQL accepts at most 2,100 parameters per request.
        DriverFamily::Mssql => 2_000,
        DriverFamily::Oracle => 30_000,
        _ => 60_000,
    }
}

macro_rules! bind_param_impl {
    ($q:expr, $p:expr, json_native = $json:expr) => {{
        let q = $q;
        match $p {
            SqlParam::Null => q.bind(Option::<String>::None),
            SqlParam::Bool(b) => q.bind(*b),
            SqlParam::Int(i) => q.bind(*i),
            SqlParam::Float(f) => q.bind(*f),
            SqlParam::Text(s) => q.bind(s.clone()),
            SqlParam::Bytes(b) => q.bind(b.clone()),
            SqlParam::Json(j) => {
                if $json {
                    q.bind(sqlx::types::Json(j.clone()))
                } else {
                    q.bind(serde_json::to_string(j).unwrap_or_default())
                }
            }
            SqlParam::Timestamp(t) => q.bind(*t),
            SqlParam::TimestampTz(t) => q.bind(*t),
            SqlParam::Date(d) => q.bind(*d),
        }
    }};
}

pub(crate) fn bind_pg<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    p: &SqlParam,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    bind_param_impl!(q, p, json_native = true)
}

pub(crate) fn bind_mysql<'q>(
    q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    p: &SqlParam,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    bind_param_impl!(q, p, json_native = false)
}

pub(crate) fn bind_sqlite<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    p: &SqlParam,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    bind_param_impl!(q, p, json_native = false)
}
