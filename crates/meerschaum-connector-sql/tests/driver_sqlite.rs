//! SQLite driver integration tests (always run; no external database needed).

use arrow::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use futures_util::StreamExt;
use meerschaum_connector_sql::driver::batch::{META_DB_TYPE, META_DTYPE};
use meerschaum_connector_sql::driver::{SqlDriver, SqlValue};
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_connector_sql::{Flavor, SqlError};

struct TempDb {
    path: std::path::PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("w5sql_test_{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }

    fn uri(&self) -> String {
        format!("sqlite:///{}", self.path.display())
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{}", self.path.display(), suffix));
        }
    }
}

async fn driver_for(uri: &str) -> SqlDriver {
    let spec = ConnectionSpec::from_uri(uri, None, None).unwrap();
    SqlDriver::connect(&spec).await.unwrap()
}

#[tokio::test]
async fn read_only_probe_refuses_writes_and_file_creation() {
    let db = TempDb::new();
    let spec = ConnectionSpec::from_uri(&db.uri(), None, None).unwrap();
    let probe = SqlDriver::connect_lazy_sqlite_read_only(&spec).unwrap();
    let missing =
        tokio::time::timeout(std::time::Duration::from_secs(2), probe.read("SELECT 1")).await;
    assert!(
        !matches!(missing, Ok(Ok(_))),
        "read-only probe opened a missing file"
    );
    assert!(!db.path.exists());
    probe.close().await;

    let writer = driver_for(&db.uri()).await;
    writer
        .exec("CREATE TABLE sentinel (id INTEGER); INSERT INTO sentinel VALUES (1)")
        .await
        .unwrap();
    writer.close().await;
    let before = std::fs::read(&db.path).unwrap();
    let probe = SqlDriver::connect_lazy_sqlite_read_only(&spec).unwrap();
    assert_eq!(
        probe.value("SELECT id FROM sentinel").await.unwrap(),
        Some(SqlValue::Int(1))
    );
    assert!(probe.exec("INSERT INTO sentinel VALUES (2)").await.is_err());
    assert!(probe.exec("PRAGMA user_version=99").await.is_err());
    probe.close().await;
    assert_eq!(std::fs::read(&db.path).unwrap(), before);
}

#[tokio::test]
async fn exec_read_and_type_mapping() {
    let db = TempDb::new();
    let driver = driver_for(&db.uri()).await;
    assert_eq!(driver.flavor(), Flavor::Sqlite);

    // Multi-statement batch through `exec` (simple-query semantics, like SQLAlchemy `text()`).
    driver
        .exec(
            "CREATE TABLE \"t\" (\
                \"id\" BIGINT, \"dt\" DATETIME, \"dt_utc\" TIMESTAMP, \"val\" FLOAT,\
                \"name\" TEXT, \"flag\" BOOLEAN, \"blob\" BLOB);\
             INSERT INTO \"t\" VALUES (1, '2024-01-01 00:00:00', '2024-01-01 00:00:00+00:00', 1.5, 'a', 1, X'0102');\
             INSERT INTO \"t\" VALUES (2, '2024-01-02 12:30:00.250000', '2024-01-02 05:00:00-05:00', 2, 'b', 0, NULL);\
             INSERT INTO \"t\" VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL);",
        )
        .await
        .unwrap();

    let batches = driver
        .read("SELECT * FROM \"t\" ORDER BY \"id\"")
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);
    let schema = batch.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        schema.field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(schema.field(3).data_type(), &DataType::Float64);
    assert_eq!(schema.field(4).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(5).data_type(), &DataType::Boolean);
    assert_eq!(schema.field(6).data_type(), &DataType::Binary);
    assert_eq!(
        schema
            .field(6)
            .metadata()
            .get(META_DTYPE)
            .map(String::as_str),
        Some("bytes")
    );
    assert!(schema.field(0).metadata().contains_key(META_DB_TYPE));

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3]);
    let dts = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(dts.value(0), 1_704_067_200_000_000);
    assert_eq!(dts.value(1), 1_704_198_600_250_000);
    assert!(dts.is_null(2));
    let utc = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    // 05:00 at -05:00 is 10:00 UTC.
    assert_eq!(utc.value(1), 1_704_189_600_000_000);
    let vals = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(vals.value(0), 1.5);
    assert_eq!(vals.value(1), 2.0);
    let names = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(1), "b");
    let flags = batch
        .column(5)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(flags.value(0));
    assert!(!flags.value(1));
    let blobs = batch
        .column(6)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(blobs.value(0), &[1, 2]);
    assert!(blobs.is_null(1));

    driver.close().await;
}

#[tokio::test]
async fn value_and_expressions() {
    let driver = driver_for("sqlite:///:memory:").await;
    assert_eq!(
        driver.value("SELECT 1 + 1").await.unwrap(),
        Some(SqlValue::Int(2))
    );
    assert_eq!(
        driver
            .value("SELECT SQLITE_VERSION() AS \"version\"")
            .await
            .unwrap()
            .map(|v| v.is_null()),
        Some(false)
    );
    assert_eq!(
        driver.value("SELECT NULL").await.unwrap(),
        Some(SqlValue::Null)
    );
    driver
        .exec("CREATE TABLE \"e\" (\"a\" INTEGER)")
        .await
        .unwrap();
    assert_eq!(driver.value("SELECT \"a\" FROM \"e\"").await.unwrap(), None);

    // Expression columns have no declared type: inferred from the values.
    let batches = driver
        .read("SELECT 1 AS \"i\", 2.5 AS \"f\", 'x' AS \"s\", NULL AS \"n\"")
        .await
        .unwrap();
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
}

#[tokio::test]
async fn memory_database_persists_across_calls() {
    let driver = driver_for("sqlite:///:memory:").await;
    driver
        .exec("CREATE TABLE \"m\" (\"a\" INTEGER); INSERT INTO \"m\" VALUES (1), (2)")
        .await
        .unwrap();
    assert_eq!(
        driver.value("SELECT COUNT(*) FROM \"m\"").await.unwrap(),
        Some(SqlValue::Int(2))
    );
}

#[tokio::test]
async fn exec_queries_semantics() {
    let db = TempDb::new();
    let driver = driver_for(&db.uri()).await;
    driver
        .exec("CREATE TABLE \"q\" (\"a\" INTEGER)")
        .await
        .unwrap();

    // Without break_on_error: every query runs, failures are `None`, the rest commit.
    let results = driver
        .exec_queries(
            &[
                "INSERT INTO \"q\" VALUES (1)".to_string(),
                "INSERT INTO \"missing\" VALUES (1)".to_string(),
                "INSERT INTO \"q\" VALUES (2), (3)".to_string(),
            ],
            false,
            true,
        )
        .await
        .unwrap();
    assert_eq!(results, vec![Some(1), None, Some(2)]);
    assert_eq!(
        driver.value("SELECT COUNT(*) FROM \"q\"").await.unwrap(),
        Some(SqlValue::Int(3))
    );

    // break_on_error + rollback: stop at the failure and roll the transaction back.
    let results = driver
        .exec_queries(
            &[
                "INSERT INTO \"q\" VALUES (4)".to_string(),
                "INSERT INTO \"missing\" VALUES (1)".to_string(),
                "INSERT INTO \"q\" VALUES (5)".to_string(),
            ],
            true,
            true,
        )
        .await
        .unwrap();
    assert_eq!(results, vec![Some(1), None]);
    assert_eq!(
        driver.value("SELECT COUNT(*) FROM \"q\"").await.unwrap(),
        Some(SqlValue::Int(3))
    );

    // break_on_error without rollback: stop, but commit what ran.
    let results = driver
        .exec_queries(
            &[
                "INSERT INTO \"q\" VALUES (6)".to_string(),
                "INSERT INTO \"missing\" VALUES (1)".to_string(),
                "INSERT INTO \"q\" VALUES (7)".to_string(),
            ],
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(results, vec![Some(1), None]);
    assert_eq!(
        driver.value("SELECT COUNT(*) FROM \"q\"").await.unwrap(),
        Some(SqlValue::Int(4))
    );

    // VACUUM must run outside a transaction.
    assert_eq!(
        driver
            .exec_queries(&["VACUUM".to_string()], true, true)
            .await
            .unwrap(),
        vec![None]
    );
    assert!(
        driver
            .exec_autocommit(&["VACUUM".to_string()])
            .await
            .unwrap()
    );
    assert!(
        !driver
            .exec_autocommit(&["SELECT * FROM \"missing\"".to_string()])
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn chunked_stream() {
    let db = TempDb::new();
    let driver = driver_for(&db.uri()).await;
    driver
        .exec(
            "CREATE TABLE \"c\" (\"i\" INTEGER, \"s\" TEXT);\
             WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 25)\
             INSERT INTO \"c\" SELECT x, 'row' || x FROM n;",
        )
        .await
        .unwrap();
    let stream = driver
        .read_stream("SELECT * FROM \"c\" ORDER BY \"i\"", 10)
        .await
        .unwrap();
    let schema = stream.schema();
    assert_eq!(schema.fields().len(), 2);
    let batches: Vec<_> = stream.collect().await;
    let sizes: Vec<usize> = batches
        .iter()
        .map(|b| b.as_ref().unwrap().num_rows())
        .collect();
    assert_eq!(sizes, vec![10, 10, 5]);
    for b in &batches {
        assert_eq!(b.as_ref().unwrap().schema(), schema);
    }

    // Zero rows: the schema still comes from the statement's columns.
    let stream = driver
        .read_stream("SELECT * FROM \"c\" WHERE 1 = 0", 10)
        .await
        .unwrap();
    assert_eq!(stream.schema().fields().len(), 2);
    assert_eq!(stream.schema().field(0).name(), "i");
    assert_eq!(stream.collect_batches().await.unwrap().len(), 0);

    // Errors surface from `read_stream` itself.
    assert!(
        driver
            .read_stream("SELECT * FROM \"nope\"", 10)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn geopackage_is_sqlite() {
    let db = TempDb::new();
    let spec = ConnectionSpec::from_uri(&db.uri(), None, Some(Flavor::Geopackage)).unwrap();
    // A `sqlite://` URI keeps its scheme flavor unless one is given explicitly.
    assert_eq!(spec.flavor, Flavor::Geopackage);
    let driver = SqlDriver::connect(&spec).await.unwrap();
    assert_eq!(
        driver.value("SELECT 1").await.unwrap(),
        Some(SqlValue::Int(1))
    );
}

#[tokio::test]
async fn unsupported_flavors_error_cleanly() {
    // Only the drivers whose cargo feature is off are refused.
    for (uri, flavor, enabled) in [
        (
            "mssql+pyodbc://sa:pw@localhost:1439/master",
            Flavor::Mssql,
            cfg!(feature = "mssql"),
        ),
        (
            "oracle+oracledb://system:oracle@localhost:1529/xe",
            Flavor::Oracle,
            cfg!(feature = "oracle"),
        ),
        (
            "duckdb:///x.duckdb",
            Flavor::Duckdb,
            cfg!(feature = "duckdb"),
        ),
    ] {
        if enabled {
            continue;
        }
        let spec = ConnectionSpec::from_uri(uri, None, None).unwrap();
        match SqlDriver::connect(&spec).await {
            Err(SqlError::DriverUnavailable { flavor: f, reason }) => {
                assert_eq!(f, flavor);
                assert!(reason.contains("cargo feature"), "{reason}");
            }
            other => panic!("expected DriverUnavailable for {uri}, got {other:?}"),
        }
    }
}
