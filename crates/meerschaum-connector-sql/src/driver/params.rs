//! Bound parameters: the safe path for user values in the instance methods (internal tables,
//! row inserts). Values never get spliced into SQL text; each driver family binds them with its
//! native encoding.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use crate::flavor::{DriverFamily, Flavor};

/// A value bound to a query parameter.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam {
    /// `NULL` (bound as a nullable text parameter; untyped on PostgreSQL).
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
    /// An empty untyped array (`[]`); rewritten to `'{}'` on PostgreSQL so `= ANY(:ids)` works
    /// with every element type, and serialized as `"[]"` elsewhere.
    EmptyArray,
    /// A PostgreSQL `TEXT[]` (JSON text on drivers without native array binds).
    TextArray(Vec<Option<String>>),
    /// A PostgreSQL `BIGINT[]` (JSON text on drivers without native array binds).
    IntArray(Vec<Option<i64>>),
    /// A PostgreSQL `DOUBLE PRECISION[]` (JSON text on drivers without native array binds).
    FloatArray(Vec<Option<f64>>),
    /// A PostgreSQL `BOOLEAN[]` (JSON text on drivers without native array binds).
    BoolArray(Vec<Option<bool>>),
    /// A PostgreSQL `TIMESTAMP[]` (JSON text on drivers without native array binds).
    TimestampArray(Vec<Option<NaiveDateTime>>),
    /// A PostgreSQL `TIMESTAMPTZ[]` (JSON text on drivers without native array binds).
    TimestampTzArray(Vec<Option<DateTime<Utc>>>),
    /// A PostgreSQL `DATE[]` (JSON text on drivers without native array binds).
    DateArray(Vec<Option<NaiveDate>>),
}

impl SqlParam {
    /// A text parameter, or `NULL` for `None`.
    pub fn opt_text(s: Option<&str>) -> SqlParam {
        match s {
            Some(s) => SqlParam::Text(s.to_string()),
            None => SqlParam::Null,
        }
    }

    /// Serialize an array variant to JSON text for drivers without native array parameters.
    pub(crate) fn array_as_json(&self) -> Option<String> {
        let value = match self {
            SqlParam::EmptyArray => serde_json::Value::Array(Vec::new()),
            SqlParam::TextArray(v) => serde_json::json!(v),
            SqlParam::IntArray(v) => serde_json::json!(v),
            SqlParam::FloatArray(v) => serde_json::json!(v),
            SqlParam::BoolArray(v) => serde_json::json!(v),
            SqlParam::TimestampArray(v) => serde_json::json!(
                v.iter()
                    .map(|t| t.map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()))
                    .collect::<Vec<_>>()
            ),
            SqlParam::TimestampTzArray(v) => serde_json::json!(
                v.iter()
                    .map(|t| t.map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, false)))
                    .collect::<Vec<_>>()
            ),
            SqlParam::DateArray(v) => serde_json::json!(
                v.iter()
                    .map(|d| d.map(|d| d.format("%Y-%m-%d").to_string()))
                    .collect::<Vec<_>>()
            ),
            _ => return None,
        };
        Some(value.to_string())
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
            SqlParam::EmptyArray
            | SqlParam::TextArray(_)
            | SqlParam::IntArray(_)
            | SqlParam::FloatArray(_)
            | SqlParam::BoolArray(_)
            | SqlParam::TimestampArray(_)
            | SqlParam::TimestampTzArray(_)
            | SqlParam::DateArray(_) => q.bind($p.array_as_json().unwrap_or_else(|| "[]".into())),
        }
    }};
}

/// A `NULL` bound with an unspecified type (OID 0), so PostgreSQL infers it from the context
/// (the target column of an `INSERT`, the other operand of a comparison) as an untyped literal
/// `NULL` would be. A `NULL` declared as `TEXT` is rejected by any non-text column ("column ...
/// is of type timestamp with time zone but expression is of type text"); SQLAlchemy and
/// psycopg send `None` untyped as well.
struct PgUntypedNull;

impl sqlx::Type<sqlx::Postgres> for PgUntypedNull {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_oid(sqlx::postgres::types::Oid(0))
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for PgUntypedNull {
    fn encode_by_ref(
        &self,
        _buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        Ok(sqlx::encode::IsNull::Yes)
    }
}

pub(crate) fn bind_pg<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    p: &SqlParam,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match p {
        SqlParam::Null => q.bind(PgUntypedNull),
        SqlParam::EmptyArray => q.bind(Vec::<String>::new()),
        SqlParam::TextArray(v) => q.bind(v.clone()),
        SqlParam::IntArray(v) => q.bind(v.clone()),
        SqlParam::FloatArray(v) => q.bind(v.clone()),
        SqlParam::BoolArray(v) => q.bind(v.clone()),
        SqlParam::TimestampArray(v) => q.bind(v.clone()),
        SqlParam::TimestampTzArray(v) => q.bind(v.clone()),
        SqlParam::DateArray(v) => q.bind(v.clone()),
        _ => bind_param_impl!(q, p, json_native = true),
    }
}

/// The placeholder for one bound cell of a row `INSERT`, cast to the column's database type on
/// PostgreSQL when the cell is bound as text (decimals, UUIDs, ...): PostgreSQL has no implicit
/// cast from `text` to `numeric`, `uuid`, `date` or the timestamp types, while psycopg binds
/// those Python values with their own types. Text-like, array, user-defined and spatial columns
/// (PostGIS casts text implicitly) keep the bare placeholder; other flavors always do.
pub fn typed_placeholder(flavor: Flavor, n: usize, param: &SqlParam, db_type: &str) -> String {
    let ph = placeholder(flavor, n);
    if flavor.driver_family() != DriverFamily::Postgres || !matches!(param, SqlParam::Text(_)) {
        return ph;
    }
    let typ = db_type.trim();
    let upper = typ.to_ascii_uppercase();
    let passthrough = typ.is_empty()
        || upper == "ARRAY"
        || upper == "USER-DEFINED"
        || [
            "TEXT",
            "CHAR",
            "VARCHAR",
            "CHARACTER",
            "NAME",
            "CITEXT",
            "GEOMETRY",
            "GEOGRAPHY",
        ]
        .iter()
        .any(|p| upper.starts_with(p))
        || !typ
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || " _(),".contains(c));
    if passthrough {
        ph
    } else {
        format!("CAST({ph} AS {typ})")
    }
}

pub(crate) fn bind_mysql<'q>(
    q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    p: &SqlParam,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    bind_param_impl!(q, p, json_native = false)
}

pub(crate) fn bind_sqlite<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments>,
    p: &SqlParam,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    bind_param_impl!(q, p, json_native = false)
}
