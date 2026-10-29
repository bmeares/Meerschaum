//! Shared harness for the instance-connector integration tests: one `SqlConnector` per flavor
//! (SQLite always, the container flavors when their `MRSM_TEST_*_URI` is set), unique pipe keys
//! per test, and helpers mirroring the Python test-suite calls (`pipe.sync(docs)`,
//! `pipe.get_data()`, ...).
#![allow(dead_code)]

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use indexmap::IndexMap;
use meerschaum_connector::{DataQuery, DateBound, SyncOptions};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

/// A row as the Python tests write it.
pub type Doc = IndexMap<String, KValue>;

/// The flavors under test and the environment variable holding each URI.
pub const FLAVOR_ENVS: &[(&str, &str)] = &[
    ("sqlite", ""),
    ("postgresql", "MRSM_TEST_PG_URI"),
    ("timescaledb", "MRSM_TEST_TIMESCALE_URI"),
    ("postgis", "MRSM_TEST_POSTGIS_URI"),
    // The TimescaleDB-HA image (TimescaleDB + PostGIS), e.g.
    // `timescaledb-ha://test:test1234@localhost:5459/testdb`.
    ("timescaledb-ha", "MRSM_TEST_TIMESCALE_HA_URI"),
    ("mysql", "MRSM_TEST_MYSQL_URI"),
    ("mariadb", "MRSM_TEST_MARIADB_URI"),
    ("mssql", "MRSM_TEST_MSSQL_URI"),
    ("oracle", "MRSM_TEST_ORACLE_URI"),
    // File-based flavors: a directory for temporary databases (empty: the system temp dir).
    ("duckdb", "MRSM_TEST_DUCKDB_PATH"),
    ("geopackage", "MRSM_TEST_GPKG_DIR"),
];

/// Whether `flavor` stores its databases as files in a directory read from its env var.
fn is_file_flavor(flavor: &str) -> bool {
    matches!(flavor, "duckdb" | "geopackage")
}

/// A connected instance for one flavor plus a unique key prefix for this test.
pub struct Ctx {
    pub flavor: &'static str,
    pub conn: SqlConnector,
    pub prefix: String,
    sqlite_path: Option<std::path::PathBuf>,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        if let Some(p) = &self.sqlite_path {
            let _ = std::fs::remove_file(p);
            // DuckDB's write-ahead log.
            let mut wal = p.clone().into_os_string();
            wal.push(".wal");
            let _ = std::fs::remove_file(wal);
        }
    }
}

/// A temporary SQLite database path.
pub fn temp_sqlite_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "_mrsm_rs_{tag}_{}.db",
        uuid::Uuid::new_v4().simple()
    ))
}

/// Connect to `flavor`, or `None` when its URI is not set.
pub async fn ctx(flavor: &'static str, test: &str) -> Option<Ctx> {
    let env = FLAVOR_ENVS
        .iter()
        .find(|(f, _)| *f == flavor)
        .map(|(_, e)| *e)
        .unwrap_or("");
    let short = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let prefix = format!("w5sql_{short}{}", &test[..test.len().min(12)]);
    if flavor == "sqlite" {
        let path = temp_sqlite_path(test);
        let uri = format!("sqlite:///{}", path.display());
        let conn = SqlConnector::from_uri("rs_test_sqlite", &uri)
            .await
            .expect("sqlite connects");
        return Some(Ctx {
            flavor,
            conn,
            prefix,
            sqlite_path: Some(path),
        });
    }
    let Ok(uri) = std::env::var(env) else {
        eprintln!("Skipping {flavor}: set {env}.");
        return None;
    };
    if is_file_flavor(flavor) {
        let dir = if uri.is_empty() {
            std::env::temp_dir()
        } else {
            std::path::PathBuf::from(&uri)
        };
        let ext = if flavor == "duckdb" { "duckdb" } else { "gpkg" };
        let path = dir.join(format!(
            "_mrsm_rs_{test}_{}.{ext}",
            uuid::Uuid::new_v4().simple()
        ));
        let uri = format!("{flavor}:///{}", path.display());
        let conn = SqlConnector::from_uri(&format!("rs_test_{flavor}"), &uri)
            .await
            .expect("file database connects");
        return Some(Ctx {
            flavor,
            conn,
            prefix,
            sqlite_path: Some(path),
        });
    }
    let spec = ConnectionSpec::from_uri(&uri, Some(&format!("rs_test_{flavor}")), None)
        .expect("valid URI");
    let conn = SqlConnector::connect(&format!("rs_test_{flavor}"), spec)
        .await
        .expect("connects");
    Some(Ctx {
        flavor,
        conn,
        prefix,
        sqlite_path: None,
    })
}

/// Expand a test body into one `#[tokio::test]` per flavor.
#[macro_export]
macro_rules! flavor_tests {
    ($name:ident, $body:expr) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            async fn run(flavor: &'static str) {
                let Some(ctx) = $crate::common::ctx(flavor, stringify!($name)).await else {
                    return;
                };
                let f = $body;
                f(ctx).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn sqlite() {
                run("sqlite").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn postgresql() {
                run("postgresql").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn timescaledb() {
                run("timescaledb").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn postgis() {
                run("postgis").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn mysql() {
                run("mysql").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn mariadb() {
                run("mariadb").await
            }
            #[cfg(feature = "mssql")]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn mssql() {
                run("mssql").await
            }
            #[cfg(feature = "oracle")]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn oracle() {
                run("oracle").await
            }
            #[cfg(feature = "duckdb")]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn duckdb() {
                run("duckdb").await
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn geopackage() {
                run("geopackage").await
            }
        }
    };
}

/// Parse a JSON object literal into a parameters map.
pub fn params(json: &str) -> Map {
    Value::from_json_str(json).expect("valid JSON").to_map()
}

impl Ctx {
    /// A context over a connector the test built itself.
    pub fn with_conn(flavor: &'static str, conn: SqlConnector, test: &str) -> Ctx {
        let short = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        Ctx {
            flavor,
            conn,
            prefix: format!("w5sql_{short}{}", &test[..test.len().min(12)]),
            sqlite_path: None,
        }
    }

    /// `mrsm.Pipe(ck, mk, lk, instance=conn, **parameters)` with this test's unique prefix on
    /// the connector keys.
    pub fn pipe(&self, ck: &str, mk: &str, lk: Option<&str>, parameters: &str) -> Pipe {
        let ck = format!("{}{}", self.prefix, ck);
        Pipe::builder(&ck, mk)
            .location(lk)
            .instance(&self.conn.keys())
            .parameters(params(parameters))
            .build()
            .expect("valid pipe")
    }

    /// `pipe.sync(docs)`.
    pub async fn sync(&self, pipe: &Pipe, docs: Vec<Doc>) -> SuccessTuple {
        self.conn
            .sync(pipe, PipeInput::Docs(docs), &SyncOptions::default())
            .await
    }

    /// `pipe.sync(docs, **opts)`.
    pub async fn sync_with(&self, pipe: &Pipe, docs: Vec<Doc>, opts: SyncOptions) -> SuccessTuple {
        self.conn.sync(pipe, PipeInput::Docs(docs), &opts).await
    }

    /// `pipe.sync(df)`.
    pub async fn sync_frame(&self, pipe: &Pipe, df: Frame) -> SuccessTuple {
        self.conn
            .sync(pipe, PipeInput::Frame(df), &SyncOptions::default())
            .await
    }

    /// `pipe.get_data(**query)`.
    pub async fn get_data(&self, pipe: &Pipe, query: DataQuery) -> Option<Frame> {
        self.conn
            .get_pipe_data(pipe, &query)
            .await
            .expect("get_data")
    }

    /// `pipe.get_data().to_dict(orient='records')`.
    pub async fn docs(&self, pipe: &Pipe) -> Vec<Doc> {
        self.get_data(pipe, DataQuery::default())
            .await
            .map(|df| meerschaum_connector::pipe_ops::frame_to_docs(&df))
            .unwrap_or_default()
    }

    /// `pipe.get_rowcount(...)`.
    pub async fn rowcount(&self, pipe: &Pipe, query: DataQuery) -> Option<i64> {
        self.conn
            .get_pipe_rowcount(pipe, &query)
            .await
            .expect("rowcount")
    }

    /// `pipe.delete()` (drop + delete registration).
    pub async fn delete(&self, pipe: &Pipe) {
        let _ = self.conn.drop_pipe(pipe).await;
        let _ = self.conn.delete_pipe(pipe).await;
    }

    /// The pipe's registered parameters (`pipe.parameters` after a refresh).
    pub async fn registered(&self, pipe: &Pipe) -> Map {
        self.conn
            .get_pipe_parameters(pipe)
            .await
            .expect("attributes")
            .unwrap_or_default()
    }

    /// Whether the flavor is in the PostgreSQL family.
    pub fn is_pg(&self) -> bool {
        matches!(self.flavor, "postgresql" | "timescaledb" | "postgis")
    }

    /// Whether the flavor is MSSQL.
    pub fn is_mssql(&self) -> bool {
        self.flavor == "mssql"
    }

    /// Whether the flavor is Oracle.
    pub fn is_oracle(&self) -> bool {
        self.flavor == "oracle"
    }

    /// Whether the flavor is DuckDB.
    pub fn is_duckdb(&self) -> bool {
        self.flavor == "duckdb"
    }

    /// Whether the flavor is SQLite or GeoPackage.
    pub fn is_sqlite(&self) -> bool {
        matches!(self.flavor, "sqlite" | "geopackage")
    }

    /// Whether the flavor is MySQL / MariaDB.
    pub fn is_mysql(&self) -> bool {
        matches!(self.flavor, "mysql" | "mariadb")
    }
}

/// Build a row from `(column, value)` pairs.
pub fn doc(pairs: Vec<(&str, KValue)>) -> Doc {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// A naive `datetime(y, m, d, h, mi)`.
pub fn ndt(y: i32, m: u32, d: u32, h: u32, mi: u32) -> KValue {
    KValue::Datetime(DateTimeValue::naive(naive(y, m, d, h, mi, 0)))
}

/// A UTC-aware `datetime(y, m, d, h, mi, tzinfo=timezone.utc)`.
pub fn udt(y: i32, m: u32, d: u32, h: u32, mi: u32) -> KValue {
    KValue::Datetime(DateTimeValue::utc(naive(y, m, d, h, mi, 0)))
}

/// A naive datetime.
pub fn naive(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, mi, s)
        .unwrap()
}

/// A UTC bound.
pub fn ubound(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateBound {
    DateBound::utc(Utc.from_utc_datetime(&naive(y, m, d, h, mi, 0)))
}

/// A naive bound.
pub fn nbound(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateBound {
    DateBound::Datetime(DateTimeValue::naive(naive(y, m, d, h, mi, 0)))
}

/// A string cell.
pub fn s(v: &str) -> KValue {
    KValue::Str(v.to_string())
}

/// An integer cell.
pub fn i(v: i64) -> KValue {
    KValue::Int(v)
}

/// A float cell.
pub fn fl(v: f64) -> KValue {
    KValue::Float(v)
}

/// A JSON cell.
pub fn j(v: &str) -> KValue {
    KValue::Json(serde_json::from_str(v).unwrap())
}

/// A cell's integer value (ints, whole floats, decimals).
pub fn as_int(v: &KValue) -> Option<i64> {
    match v {
        KValue::Int(i) => Some(*i),
        KValue::Float(f) if f.fract() == 0.0 => Some(*f as i64),
        KValue::Decimal(d) => d.to_plain_string().split('.').next()?.parse().ok(),
        KValue::Bool(b) => Some(*b as i64),
        _ => None,
    }
}

/// A cell as an aware UTC timestamp's naive value (naive values pass through).
pub fn as_naive_utc(v: &KValue) -> Option<NaiveDateTime> {
    match v {
        KValue::Datetime(dt) => Some(dt.utc),
        _ => None,
    }
}

/// Assert a sync succeeded.
#[track_caller]
pub fn ok(t: SuccessTuple) -> SuccessTuple {
    assert!(t.success, "{}", t.message);
    t
}

/// A `DataQuery` with `params`.
pub fn q_params(json: &str) -> DataQuery {
    DataQuery {
        params: Some(params(json)),
        ..Default::default()
    }
}

/// [`ctx`] with the instance's default schema set to `schema` (created when missing) on the
/// PostgreSQL-family flavors; every other flavor is returned as [`ctx`] builds it.
pub async fn ctx_in_schema(flavor: &'static str, test: &str, schema: &str) -> Option<Ctx> {
    if !matches!(flavor, "postgresql" | "timescaledb" | "postgis") {
        return ctx(flavor, test).await;
    }
    let env = FLAVOR_ENVS
        .iter()
        .find(|(f, _)| *f == flavor)
        .map(|(_, e)| *e)
        .expect("flavor");
    let Ok(uri) = std::env::var(env) else {
        eprintln!("Skipping {flavor}: set {env}.");
        return None;
    };
    let mut spec = ConnectionSpec::from_uri(&uri, Some(&format!("rs_test_{flavor}")), None)
        .expect("valid URI");
    let admin = SqlConnector::connect(&format!("rs_admin_{flavor}"), spec.clone())
        .await
        .expect("connects");
    // `CREATE SCHEMA IF NOT EXISTS` races between parallel tests of one binary on PostgreSQL
    // (23505 on pg_namespace when two creators collide); the schema exists afterwards either way.
    if let Err(error) = admin
        .driver()
        .exec(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
    {
        let text = error.to_string();
        assert!(
            text.contains("23505") || text.contains("already exists"),
            "create schema: {text}"
        );
    }
    spec.schema = Some(schema.to_string());
    let conn = SqlConnector::connect(&format!("rs_test_{flavor}"), spec)
        .await
        .expect("connects");
    let short = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    Some(Ctx {
        flavor,
        conn,
        prefix: format!("w5sql_{short}{}", &test[..test.len().min(12)]),
        sqlite_path: None,
    })
}
